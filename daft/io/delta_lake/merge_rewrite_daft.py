"""Phase-2 MERGE rewrite using Daft DataFrames (native/Ray runnable).

Applies an ordered ``MergePlan`` (v1–v3 clauses) via prefixed source/target
joins, writes new UUID Parquet with ``DataFrame.write_parquet``, and returns
``RewriteResult`` / ``AddAction``s for phase-3 commit.

Row data stays on the runner; the driver only collects written paths, sizes,
and stats for ``AddAction`` metadata.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

import pyarrow.parquet as pq  # noqa: TID253
from deltalake import DeltaTable
from deltalake.transaction import AddAction

from daft.io.delta_lake.merge_plan import (
    MergeClause,
    MergePlan,
    prefix_column,
    resolve_merge_plan,
    rewrite_merge_sql,
)
from daft.io.delta_lake.merge_rewrite import RewriteResult
from daft.io.delta_lake.merge_touched_files import (
    TouchedFilesResult,
    _full_path,
    _normalize_table_uri,
)

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


def _target_column_names(dt: DeltaTable) -> list[str]:
    """Return Delta table column names in schema order."""
    arrow_schema = dt.schema().to_arrow()
    return list(arrow_schema.names)


def _relative_path(table_uri: str, full_path: str) -> str:
    """Map a written Parquet path to a Delta-log relative path."""
    path = full_path
    # lowercase comparison for file scheme
    if path.lower().startswith("file://"):
        path = urlparse(path).path
    prefix = table_uri.rstrip("/") + "/"
    if path.startswith(prefix):
        return path[len(prefix) :]
    return os.path.basename(path)


def _add_action_for_path(table_uri: str, full_path: str) -> AddAction:
    """Build an ``AddAction`` from a written Parquet file path."""
    rel = _relative_path(table_uri, full_path)
    local = full_path
    # lowercase comparison for file scheme
    if local.lower().startswith("file://"):
        local = urlparse(local).path
    if not os.path.exists(local):
        raise FileNotFoundError(f"rewrite wrote no file at {full_path}")
    size = os.path.getsize(local)
    if size <= 0:
        raise RuntimeError(f"rewrite produced empty parquet at {full_path}")
    metadata = pq.read_metadata(local)
    from daft.io.delta_lake.delta_lake_write import DeltaJSONEncoder, get_file_stats_from_metadata

    stats = get_file_stats_from_metadata(metadata)
    return AddAction(
        rel,
        size,
        {},
        int(datetime.now().timestamp() * 1000),
        True,
        json.dumps(stats, cls=DeltaJSONEncoder),
    )


def _concat_parts(parts: list[tuple[DataFrame, int]]) -> DataFrame | None:
    """Concat DataFrames with positive row counts (avoids empty Null schemas)."""
    live = [df for df, n in parts if n > 0]
    if not live:
        return None
    out = live[0]
    for part in live[1:]:
        out = out.concat(part)
    return out


def _prefix_df(df: DataFrame, side: Literal["source", "target"], columns: Sequence[str]) -> DataFrame:
    """Rename selected columns to ``source_*`` / ``target_*``."""
    from daft import col

    return df.select(*[col(c).alias(prefix_column(side, c)) for c in columns])


def _take_clause_rows(
    remaining: DataFrame,
    clause: MergeClause,
    key_cols: Sequence[str],
) -> tuple[DataFrame, DataFrame, int]:
    """Split ``remaining`` into (hit, still_remaining, hit_count) for one clause."""
    from daft import col
    from daft.sql import sql_expr

    if clause.condition:
        rewritten = rewrite_merge_sql(clause.condition)
        hit = remaining.where(sql_expr(rewritten))
    else:
        hit = remaining
    n = hit.count_rows()
    if n == 0:
        return hit, remaining, 0
    keys = list(key_cols)
    right_hit = [f"_hit_{i}" for i in range(len(keys))]
    hit_keys = hit.select(*[col(k).alias(alias) for k, alias in zip(keys, right_hit)])
    still = remaining.join(
        hit_keys,
        left_on=keys,  # type: ignore[arg-type]
        right_on=right_hit,  # type: ignore[arg-type]
        how="anti",
    )
    return hit, still, n


def _project_update_all(hit: DataFrame, column_names: Sequence[str]) -> DataFrame:
    """Matched update-all: take source column values."""
    from daft import col

    return hit.select(*[col(prefix_column("source", c)).alias(c) for c in column_names])


def _project_update_map(hit: DataFrame, column_names: Sequence[str], updates: dict[str, str]) -> DataFrame:
    """Matched / NMBS partial update: expr map over target baseline."""
    from daft import col
    from daft.sql import sql_expr

    exprs = []
    update_keys_lower = {k.lower(): k for k in updates}
    for c in column_names:
        # lowercase comparison for update map keys
        if c.lower() in update_keys_lower:
            orig = update_keys_lower[c.lower()]
            exprs.append(sql_expr(rewrite_merge_sql(updates[orig])).alias(c))
        else:
            exprs.append(col(prefix_column("target", c)).alias(c))
    return hit.select(*exprs)


def _project_target(hit: DataFrame, column_names: Sequence[str]) -> DataFrame:
    """Project target_* columns to unprefixed names."""
    from daft import col

    return hit.select(*[col(prefix_column("target", c)).alias(c) for c in column_names])


def _project_insert_all(rows: DataFrame, column_names: Sequence[str]) -> DataFrame:
    """Insert-all from source_* columns."""
    from daft import col

    return rows.select(*[col(prefix_column("source", c)).alias(c) for c in column_names])


def _project_insert_map(rows: DataFrame, column_names: Sequence[str], inserts: dict[str, str]) -> DataFrame:
    """Insert with column → SQL map; unspecified columns become null via missing source."""
    from daft import lit
    from daft.sql import sql_expr

    insert_keys_lower = {k.lower(): k for k in inserts}
    exprs = []
    for c in column_names:
        # lowercase comparison for insert map keys
        if c.lower() in insert_keys_lower:
            orig = insert_keys_lower[c.lower()]
            exprs.append(sql_expr(rewrite_merge_sql(inserts[orig])).alias(c))
        else:
            exprs.append(lit(None).alias(c))
    return rows.select(*exprs)


def build_rewrite_dataframe_from_plan(
    source: DataFrame,
    target_touched: DataFrame,
    plan: MergePlan,
    column_names: Sequence[str],
) -> tuple[DataFrame | None, int, int, int, int]:
    """Build rewrite output by applying ordered merge clauses.

    Args:
        source: Source DataFrame (unprefixed columns).
        target_touched: Touched target rows (unprefixed).
        plan: Resolved merge plan.
        column_names: Output columns (target schema, optionally evolved).

    Returns:
        ``(output_df_or_none, num_updated, num_inserted, num_copied, num_deleted)``.
    """
    cols = list(column_names)
    keys = list(plan.merge_keys)
    for key in keys:
        if key not in source.column_names:
            raise ValueError(f"source is missing merge key column(s): {[key]}")
        if key not in target_touched.column_names:
            raise ValueError(f"target is missing merge key column(s): {[key]}")

    from daft import lit

    # Output schema columns must exist on source (null-fill when merge_schema).
    for c in cols:
        if c not in source.column_names:
            if plan.merge_schema:
                source = source.with_column(c, lit(None))
            else:
                raise ValueError(f"source is missing target column(s): {[c]}")

    # Keep extra source columns (flags, etc.) for clause SQL conditions.
    source_prefix_cols = list(dict.fromkeys([*cols, *source.column_names]))
    source = source.select(*source_prefix_cols)

    target_touched = target_touched.select(*[c for c in cols if c in target_touched.column_names])
    for c in cols:
        if c not in target_touched.column_names:
            target_touched = target_touched.with_column(c, lit(None))
    target_touched = target_touched.select(*cols)

    source_p = _prefix_df(source, "source", source_prefix_cols)
    target_p = _prefix_df(target_touched, "target", cols)

    left_on = [prefix_column("source", k) for k in keys]
    right_on = [prefix_column("target", k) for k in keys]

    matched = source_p.join(
        target_p,
        left_on=left_on,  # type: ignore[arg-type]
        right_on=right_on,  # type: ignore[arg-type]
        how="inner",
    )
    unmatched_target = target_p.join(
        source_p.select(*left_on),
        left_on=right_on,  # type: ignore[arg-type]
        right_on=left_on,  # type: ignore[arg-type]
        how="anti",
    )
    unmatched_source = source_p.join(
        target_p.select(*right_on),
        left_on=left_on,  # type: ignore[arg-type]
        right_on=right_on,  # type: ignore[arg-type]
        how="anti",
    )

    n_upd = 0
    n_ins = 0
    n_copy = 0
    n_del = 0
    out_parts: list[tuple[DataFrame, int]] = []

    # --- MATCHED clauses (ordered) ---
    remaining = matched
    matched_clauses = [c for c in plan.clauses if c.kind in ("matched_update", "matched_delete")]
    for clause in matched_clauses:
        hit, remaining, n = _take_clause_rows(remaining, clause, left_on)
        if n == 0:
            continue
        if clause.kind == "matched_delete":
            n_del += n
            continue
        if clause.updates is None:
            projected = _project_update_all(hit, cols)
        else:
            projected = _project_update_map(hit, cols, dict(clause.updates))
        out_parts.append((projected, n))
        n_upd += n

    # Matched rows that hit no clause → copy target (Spark unchanged).
    n_rem = remaining.count_rows()
    if n_rem > 0:
        out_parts.append((_project_target(remaining, cols), n_rem))
        n_copy += n_rem

    # --- NOT MATCHED BY SOURCE (target-only in touched files) ---
    nmbs_clauses = [
        c for c in plan.clauses if c.kind in ("not_matched_by_source_update", "not_matched_by_source_delete")
    ]
    remaining_tgt = unmatched_target
    # NMBS clauses need target_* only; fabricate null source_* for SQL that references source (rare).
    if nmbs_clauses:
        for c in cols:
            sc = prefix_column("source", c)
            if sc not in remaining_tgt.column_names:
                from daft import lit

                remaining_tgt = remaining_tgt.with_column(sc, lit(None))
    for clause in nmbs_clauses:
        hit, remaining_tgt, n = _take_clause_rows(remaining_tgt, clause, right_on)
        if n == 0:
            continue
        if clause.kind == "not_matched_by_source_delete":
            n_del += n
            continue
        if clause.updates is None:
            # update-all from source is meaningless for NMBS; keep target.
            projected = _project_target(hit, cols)
        else:
            projected = _project_update_map(hit, cols, dict(clause.updates))
        out_parts.append((projected, n))
        n_upd += n

    # Remaining target-only → copy
    n_tgt = remaining_tgt.count_rows()
    if n_tgt > 0:
        out_parts.append((_project_target(remaining_tgt, cols), n_tgt))
        n_copy += n_tgt

    # --- NOT MATCHED (source-only) inserts ---
    insert_clauses = [c for c in plan.clauses if c.kind == "not_matched_insert"]
    remaining_src = unmatched_source
    for clause in insert_clauses:
        # Inserts need target_* nulls if condition references target.
        for c in cols:
            tc = prefix_column("target", c)
            if tc not in remaining_src.column_names:
                from daft import lit

                remaining_src = remaining_src.with_column(tc, lit(None))
        hit, remaining_src, n = _take_clause_rows(remaining_src, clause, left_on)
        if n == 0:
            continue
        if clause.inserts is None or (isinstance(clause.inserts, str) and clause.inserts.lower() == "all"):
            projected = _project_insert_all(hit, cols)
        else:
            assert isinstance(clause.inserts, Mapping)
            projected = _project_insert_map(hit, cols, dict(clause.inserts))
        out_parts.append((projected, n))
        n_ins += n

    out = _concat_parts(out_parts)
    return out, n_upd, n_ins, n_copy, n_del


def build_rewrite_dataframe(
    source: DataFrame,
    target_touched: DataFrame,
    merge_keys: Sequence[str],
    column_names: Sequence[str],
    *,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
) -> tuple[DataFrame | None, int, int, int]:
    """v1-compatible wrapper around ``build_rewrite_dataframe_from_plan``."""
    plan = resolve_merge_plan(
        merge_keys=merge_keys,
        when_matched=when_matched,
        when_not_matched=when_not_matched,
    )
    out, n_upd, n_ins, n_copy, _n_del = build_rewrite_dataframe_from_plan(source, target_touched, plan, column_names)
    return out, n_upd, n_ins, n_copy


def rewrite_touched_files_daft(
    source: DataFrame,
    table: DeltaTable | str,
    merge_keys: Sequence[str] | None = None,
    touched_relative_paths: Sequence[str] = (),
    *,
    plan: MergePlan | None = None,
    storage_options: dict[str, str] | None = None,
    io_config: IOConfig | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
) -> RewriteResult:
    """Rewrite touched files via Daft clause evaluation + ``write_parquet``.

    Args:
        source: Source DataFrame (not collected).
        table: Target Delta table or URI.
        merge_keys: Join keys when ``plan`` is omitted (v1).
        touched_relative_paths: Paths from phase-1.
        plan: Optional resolved ``MergePlan`` (v2/v3).
        storage_options: delta-rs options when ``table`` is a URI.
        io_config: Optional Daft IO config.
        when_matched: v1 sugar when ``plan`` is None.
        when_not_matched: v1 sugar when ``plan`` is None.

    Returns:
        ``RewriteResult`` with on-disk ``AddAction``s and paths to remove.
    """
    import daft

    if plan is None:
        if not merge_keys:
            raise ValueError("merge_keys or plan is required")
        plan = resolve_merge_plan(
            merge_keys=merge_keys,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
        )

    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    table_uri = _normalize_table_uri(dt.table_uri)
    cols = _target_column_names(dt)

    if plan.merge_schema:
        for name in source.column_names:
            # lowercase comparison for schema evolution membership
            if name.lower() not in {c.lower() for c in cols}:
                cols.append(name)

    if touched_relative_paths:
        full_paths = [_full_path(table_uri, rel) for rel in touched_relative_paths]
        target_touched = daft.read_parquet(full_paths, io_config=io_config)
        present = [c for c in cols if c in target_touched.column_names]
        target_touched = target_touched.select(*present)
    else:
        target_touched = source.select(*[c for c in cols if c in source.column_names]).limit(0)

    output, n_upd, n_ins, n_copy, n_del = build_rewrite_dataframe_from_plan(
        source,
        target_touched,
        plan,
        cols,
    )

    remove_paths = tuple(touched_relative_paths)
    if output is None:
        return RewriteResult(
            add_actions=(),
            remove_relative_paths=remove_paths,
            num_updated=n_upd,
            num_inserted=n_ins,
            num_copied=n_copy,
            output_relative_paths=(),
            num_deleted=n_del,
            commit_schema=None,
        )

    written = output.write_parquet(table_uri, write_mode="append", io_config=io_config)
    full_written = [p for p in written.to_pydict().get("path", []) if p]
    if not full_written:
        raise RuntimeError("rewrite write_parquet returned no output paths")

    actions = tuple(_add_action_for_path(table_uri, p) for p in full_written)
    commit_schema = None
    if plan.merge_schema:
        # Evolve Delta schema from the written Parquet schema (includes new cols).
        commit_schema = pq.read_schema(full_written[0])
    return RewriteResult(
        add_actions=actions,
        remove_relative_paths=remove_paths,
        num_updated=n_upd,
        num_inserted=n_ins,
        num_copied=n_copy,
        output_relative_paths=tuple(a.path for a in actions),
        num_deleted=n_del,
        commit_schema=commit_schema,
    )


def plan_merge_rewrite_daft(
    source: DataFrame,
    table: DeltaTable | str,
    merge_keys: Sequence[str] | None = None,
    *,
    plan: MergePlan | None = None,
    storage_options: dict[str, str] | None = None,
    io_config: IOConfig | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
    allow_multiple_matches: bool = False,
    touched: TouchedFilesResult | None = None,
) -> tuple[TouchedFilesResult, RewriteResult]:
    """Run Daft phase-1 (optional) then Daft phase-2 for a single merge plan."""
    if plan is None:
        plan = resolve_merge_plan(
            merge_keys=merge_keys,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            allow_multiple_matches=allow_multiple_matches,
        )

    if touched is None:
        from daft.io.delta_lake.merge_touched_daft import find_touched_files_daft
        from daft.io.delta_lake.merge_touched_files import TouchedFilesResult, list_delta_data_files

        # NOT MATCHED BY SOURCE can rewrite any target file, so scan all files.
        needs_all = any(c.kind.startswith("not_matched_by_source") for c in plan.clauses)
        if needs_all:
            dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
            all_paths = tuple(list_delta_data_files(dt))
            touched = TouchedFilesResult(
                relative_paths=all_paths,
                num_source_rows=source.count_rows(),
                num_matched_target_rows=0,
                num_target_files_scanned=len(all_paths),
            )
        else:
            touched = find_touched_files_daft(
                source,
                table,
                plan.merge_keys,
                storage_options=storage_options,
                io_config=io_config,
                allow_multiple_matches=plan.allow_multiple_matches,
            )
    rewritten = rewrite_touched_files_daft(
        source,
        table,
        merge_keys=list(plan.merge_keys),
        touched_relative_paths=touched.relative_paths,
        plan=plan,
        storage_options=storage_options,
        io_config=io_config,
    )
    return touched, rewritten


__all__ = [
    "build_rewrite_dataframe",
    "build_rewrite_dataframe_from_plan",
    "plan_merge_rewrite_daft",
    "rewrite_touched_files_daft",
]
