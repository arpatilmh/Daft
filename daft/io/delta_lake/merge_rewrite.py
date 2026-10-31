"""Phase-2 of distributed Delta MERGE: rewrite touched files + inserts.

Mirrors Spark ``ClassicMergeExecutor.writeAllChanges`` for the common case:

* ``WHEN MATCHED THEN UPDATE`` — replace matched target rows with source rows
* ``WHEN NOT MATCHED THEN INSERT`` — append source rows with no target match
* Unmatched rows in touched files are copied unchanged into the rewrite output

Writes new UUID-named Parquet files under the table root and returns
``AddAction``s. Does **not** commit; phase-3 applies Add+Remove atomically.

Deliberately free of ``import daft`` so tests run without a rebuilt native
extension.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pyarrow as pa  # noqa: TID253
import pyarrow.parquet as pq  # noqa: TID253
from deltalake import DeltaTable
from deltalake.transaction import AddAction

try:
    from .merge_touched_files import (
        TouchedFilesResult,
        _ensure_merge_keys,
        _full_path,
        _normalize_table_uri,
        find_touched_files,
    )
except ImportError:  # pragma: no cover - file-path / sys.path test loading
    from merge_touched_files import (  # type: ignore[no-redef]  # noqa: TC002
        TouchedFilesResult,
        _ensure_merge_keys,
        _full_path,
        _normalize_table_uri,
        find_touched_files,
    )


@dataclass(frozen=True)
class RewriteResult:
    """Outcome of phase-2 rewrite (files on disk, not yet committed).

    Attributes:
        add_actions: New data files to commit.
        remove_relative_paths: Touched paths that must be removed in phase-3.
        num_updated: Target rows replaced / partially updated from source.
        num_inserted: Source rows inserted (not matched).
        num_copied: Target rows copied unchanged out of rewritten files.
        output_relative_paths: Relative paths of the new Parquet files.
        num_deleted: Matched (or not-matched-by-source) rows deleted.
        commit_schema: Optional Arrow schema for phase-3 when evolving columns.
    """

    add_actions: tuple[AddAction, ...]
    remove_relative_paths: tuple[str, ...]
    num_updated: int
    num_inserted: int
    num_copied: int
    output_relative_paths: tuple[str, ...]
    num_deleted: int = 0
    commit_schema: object | None = None


class DeltaJSONEncoder(json.JSONEncoder):
    """JSON encoder for Delta file stats (dates/decimals/bytes)."""

    def default(self, obj: object) -> object:
        from datetime import date
        from decimal import Decimal

        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def _row_key(table: pa.Table, merge_keys: Sequence[str], index: int) -> tuple[object, ...]:
    """Build a hashable merge-key tuple for one row."""
    return tuple(table.column(k)[index].as_py() for k in merge_keys)


def _key_to_first_index(table: pa.Table, merge_keys: Sequence[str]) -> dict[tuple[object, ...], int]:
    """Map merge-key → first row index (phase-1 guarantees 1:1 for matches)."""
    out: dict[tuple[object, ...], int] = {}
    for i in range(table.num_rows):
        key = _row_key(table, merge_keys, i)
        if key not in out:
            out[key] = i
    return out


def _read_files(table_uri: str, relative_paths: Sequence[str]) -> pa.Table:
    """Read and concat Parquet files; empty if no paths."""
    tables: list[pa.Table] = []
    for relative in relative_paths:
        full = _full_path(table_uri, relative)
        part = pq.read_table(full)
        if part.num_rows > 0:
            tables.append(part)
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def _align_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Project/cast columns to match ``schema`` column order and names."""
    cols = []
    for field in schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                col = col.cast(field.type)
            cols.append(col)
        else:
            cols.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(cols, schema=schema)


def _build_file_stats(table: pa.Table) -> str:
    """Build a minimal Delta stats JSON string for an AddAction."""
    min_values: dict[str, object] = {}
    max_values: dict[str, object] = {}
    null_count: dict[str, int] = {}
    for name in table.column_names:
        col = table.column(name)
        null_count[name] = col.null_count
        # Skip nested / binary-ish for min/max; keep scalars when cheap.
        if pa.types.is_nested(col.type) or pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
            continue
        try:
            import pyarrow.compute as pc

            if col.null_count == len(col):
                continue
            mn = pc.min(col).as_py()
            mx = pc.max(col).as_py()
            if mn is not None:
                min_values[name] = mn
            if mx is not None:
                max_values[name] = mx
        except Exception:
            continue
    stats = {
        "numRecords": table.num_rows,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count,
    }
    return json.dumps(stats, cls=DeltaJSONEncoder)


def _write_add_action(table_uri: str, table: pa.Table, file_idx: int) -> AddAction:
    """Write ``table`` as ``{uuid}-{idx}.parquet`` and return an AddAction."""
    if table.num_rows == 0:
        raise ValueError("refusing to write an empty rewrite file")
    filename = f"{uuid.uuid4()}-{file_idx}.parquet"
    full = _full_path(table_uri, filename)
    parent = os.path.dirname(full)
    # lowercase comparison for object-store schemes (no local makedirs)
    scheme = full.split("://", 1)[0].lower() if "://" in full else ""
    if parent and scheme not in ("s3", "s3a", "gs", "gcs", "az", "abfs", "abfss"):
        os.makedirs(parent, exist_ok=True)
    pq.write_table(table, full)
    if not os.path.exists(full):
        raise FileNotFoundError(f"rewrite wrote no file at {full}")
    size = os.path.getsize(full)
    if size <= 0:
        raise RuntimeError(f"rewrite produced empty parquet at {full}")
    return AddAction(
        filename,
        size,
        {},
        int(datetime.now().timestamp() * 1000),
        True,
        _build_file_stats(table),
    )


def build_rewrite_table(
    source: pa.Table,
    target_touched: pa.Table,
    merge_keys: Sequence[str],
    *,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
) -> tuple[pa.Table, int, int, int]:
    """Compute the logical rewrite output table (not yet written).

    Args:
        source: Merge source rows.
        target_touched: Rows from files phase-1 marked for rewrite (may be empty).
        merge_keys: Equality keys.
        when_matched: Only ``update`` (take source row) is supported in v1.
        when_not_matched: ``insert`` or ``ignore`` for source-only rows.

    Returns:
        ``(output_table, num_updated, num_inserted, num_copied)``.
    """
    if when_matched != "update":
        raise ValueError(f"unsupported when_matched={when_matched!r}")
    # lowercase comparison for when_not_matched
    not_matched = when_not_matched.lower()
    if not_matched not in ("insert", "ignore"):
        raise ValueError(f"unsupported when_not_matched={when_not_matched!r}")

    if target_touched.num_rows == 0 and source.num_rows == 0:
        return pa.table({}), 0, 0, 0

    schema = target_touched.schema if target_touched.num_rows > 0 else source.schema
    source_aligned = _align_to_schema(source, schema) if source.num_rows else source
    target_aligned = _align_to_schema(target_touched, schema) if target_touched.num_rows else target_touched

    source_by_key = _key_to_first_index(source_aligned, merge_keys) if source_aligned.num_rows else {}
    target_keys = (
        {_row_key(target_aligned, merge_keys, i) for i in range(target_aligned.num_rows)}
        if target_aligned.num_rows
        else set()
    )

    updated_indices: list[int] = []
    copied_indices: list[int] = []
    for i in range(target_aligned.num_rows):
        key = _row_key(target_aligned, merge_keys, i)
        if key in source_by_key:
            updated_indices.append(source_by_key[key])
        else:
            copied_indices.append(i)

    inserted_indices: list[int] = []
    if not_matched == "insert":
        for i in range(source_aligned.num_rows):
            key = _row_key(source_aligned, merge_keys, i)
            if key not in target_keys:
                inserted_indices.append(i)

    parts: list[pa.Table] = []
    if updated_indices:
        parts.append(source_aligned.take(pa.array(updated_indices, type=pa.int64())))
    if copied_indices:
        parts.append(target_aligned.take(pa.array(copied_indices, type=pa.int64())))
    if inserted_indices:
        parts.append(source_aligned.take(pa.array(inserted_indices, type=pa.int64())))

    if not parts:
        return pa.table({name: pa.array([], type=schema.field(name).type) for name in schema.names}), 0, 0, 0

    out = pa.concat_tables(parts, promote_options="default")
    return out, len(updated_indices), len(inserted_indices), len(copied_indices)


def rewrite_touched_files(
    source: pa.Table,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    touched_relative_paths: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
) -> RewriteResult:
    """Rewrite touched target files combining updates, copies, and inserts.

    Args:
        source: Source Arrow table.
        table: Target Delta table or URI.
        merge_keys: Join keys.
        touched_relative_paths: Paths from phase-1 (may be empty for insert-only).
        storage_options: delta-rs options when ``table`` is a URI.
        when_matched: ``update`` only in v1.
        when_not_matched: ``insert`` or ``ignore``.

    Returns:
        ``RewriteResult`` with on-disk ``AddAction``s and paths to remove.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be a non-empty sequence")
    _ensure_merge_keys(source, merge_keys, "source")

    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    table_uri = _normalize_table_uri(dt.table_uri)

    target_touched = _read_files(table_uri, touched_relative_paths)
    if target_touched.num_rows > 0:
        _ensure_merge_keys(target_touched, merge_keys, "target")

    output, n_upd, n_ins, n_copy = build_rewrite_table(
        source,
        target_touched,
        merge_keys,
        when_matched=when_matched,
        when_not_matched=when_not_matched,
    )

    remove_paths = tuple(touched_relative_paths)
    if output.num_rows == 0:
        # Nothing to write — still may need to remove empty touched files (rare).
        return RewriteResult(
            add_actions=(),
            remove_relative_paths=remove_paths,
            num_updated=n_upd,
            num_inserted=n_ins,
            num_copied=n_copy,
            output_relative_paths=(),
        )

    action = _write_add_action(table_uri, output, file_idx=0)
    return RewriteResult(
        add_actions=(action,),
        remove_relative_paths=remove_paths,
        num_updated=n_upd,
        num_inserted=n_ins,
        num_copied=n_copy,
        output_relative_paths=(action.path,),
    )


def plan_merge_rewrite(
    source: pa.Table,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
    allow_multiple_matches: bool = False,
    touched: TouchedFilesResult | None = None,
) -> tuple[TouchedFilesResult, RewriteResult]:
    """Run phase-1 then phase-2 for a single merge plan (no commit).

    Args:
        source: Source rows.
        table: Target table.
        merge_keys: Join keys.
        storage_options: Optional storage options.
        when_matched: Update action for matches.
        when_not_matched: Insert/ignore for source-only rows.
        allow_multiple_matches: Forwarded to phase-1 when ``touched`` is None.
        touched: Optional precomputed phase-1 result (e.g. from Daft discovery).

    Returns:
        ``(touched_files_result, rewrite_result)``.
    """
    if touched is None:
        touched = find_touched_files(
            source,
            table,
            merge_keys,
            storage_options=storage_options,
            allow_multiple_matches=allow_multiple_matches,
        )
    rewritten = rewrite_touched_files(
        source,
        table,
        merge_keys,
        touched.relative_paths,
        storage_options=storage_options,
        when_matched=when_matched,
        when_not_matched=when_not_matched,
    )
    return touched, rewritten


__all__ = [
    "RewriteResult",
    "build_rewrite_table",
    "plan_merge_rewrite",
    "rewrite_touched_files",
]
