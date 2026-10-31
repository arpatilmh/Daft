"""Phase-3 of distributed Delta MERGE: atomic Add+Remove commit.

Takes a phase-2 ``RewriteResult`` (or runs phase-1+2 via ``merge_into``) and
commits with ``DeltaTable.create_write_transaction``. Requires
``deltalake>=1.6.4`` for ``RemoveAction`` support (prefer ``>=1.6.6``).
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pyarrow as pa  # noqa: TID253
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

try:
    from deltalake.transaction import RemoveAction

    _HAS_REMOVE_ACTION = True
except ImportError:  # pragma: no cover
    RemoveAction = None
    _HAS_REMOVE_ACTION = False

try:
    from .merge_rewrite import RewriteResult, plan_merge_rewrite
    from .merge_touched_files import TouchedFilesResult
except ImportError:  # pragma: no cover
    from merge_rewrite import RewriteResult, plan_merge_rewrite  # type: ignore[no-redef]  # noqa: TC002
    from merge_touched_files import TouchedFilesResult  # type: ignore[no-redef]  # noqa: TC002


class DeltaMergeCommitUnsupportedError(RuntimeError):
    """Raised when the installed deltalake cannot commit RemoveActions."""


@dataclass(frozen=True)
class MergeCommitResult:
    """Outcome of a committed merge.

    Attributes:
        version: Table version after commit (unchanged on no-op).
        touched: Phase-1 discovery result.
        rewritten: Phase-2 rewrite result (files may already exist on disk).
        committed: False when there was nothing to commit.
        commit_attempts: How many create_write_transaction attempts ran.
    """

    version: int
    touched: TouchedFilesResult
    rewritten: RewriteResult
    committed: bool
    commit_attempts: int


def require_remove_action_support() -> None:
    """Fail fast if RemoveAction is unavailable on this deltalake wheel."""
    if not _HAS_REMOVE_ACTION:
        raise DeltaMergeCommitUnsupportedError(
            "deltalake>=1.6.4 required for RemoveAction in create_write_transaction "
            "(prefer deltalake>=1.6.6; 1.6.5 was yanked on PyPI)"
        )


def build_remove_actions(
    table: DeltaTable,
    relative_paths: Sequence[str],
    *,
    data_change: bool = True,
) -> list[object]:
    """Build ``RemoveAction`` entries for the given relative data-file paths.

    Args:
        table: Open Delta table (used for size metadata when available).
        relative_paths: Paths to remove (from phase-2).
        data_change: Delta ``dataChange`` flag (True for MERGE rewrites).

    Returns:
        List of ``RemoveAction`` instances.
    """
    require_remove_action_support()
    assert RemoveAction is not None

    paths = list(relative_paths)
    if not paths:
        return []

    add_table = pa.table(table.get_add_actions())
    path_col = add_table.column("path").to_pylist()
    size_by_path: dict[str, int | None] = {}
    part_by_path: dict[str, dict[str, str | None] | None] = {}
    sizes = (
        add_table.column("size_bytes").to_pylist() if "size_bytes" in add_table.column_names else [None] * len(path_col)
    )

    partition_field = None
    for candidate in ("partition", "partition_values"):
        if candidate in add_table.column_names:
            partition_field = candidate
            break

    for i, path in enumerate(path_col):
        size_by_path[path] = sizes[i]
        if partition_field is None:
            part_by_path[path] = {}
        else:
            raw = add_table.column(partition_field)[i]
            try:
                part_by_path[path] = raw.as_py() if hasattr(raw, "as_py") else dict(raw)
            except Exception:
                part_by_path[path] = {}

    ts = int(datetime.now().timestamp() * 1000)
    missing = [p for p in paths if p not in size_by_path]
    if missing:
        raise ValueError(f"remove paths not present in current snapshot: {missing}")

    return [
        RemoveAction(
            path=p,
            data_change=data_change,
            deletion_timestamp=ts,
            size=size_by_path[p],
            partition_values=part_by_path.get(p) or {},
        )
        for p in paths
    ]


def _is_commit_conflict(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like an optimistic-concurrency failure."""
    if isinstance(exc, CommitFailedError):
        return True
    # lowercase comparison for wrapped conflict errors
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    return "commit" in name or "conflict" in msg


def commit_rewrite_result(
    table: DeltaTable | str,
    rewritten: RewriteResult,
    *,
    storage_options: dict[str, str] | None = None,
    max_retries: int = 3,
    initial_backoff_sec: float = 0.05,
) -> tuple[int, int, bool]:
    """Commit phase-2 ``RewriteResult`` via Add+Remove transaction.

    Args:
        table: Target table or URI.
        rewritten: Output of ``rewrite_touched_files`` / ``plan_merge_rewrite``.
        storage_options: Storage options when ``table`` is a URI.
        max_retries: Attempts on ``CommitFailedError`` (OCC).
        initial_backoff_sec: Base delay before retry (doubles each attempt).

    Returns:
        ``(version, attempts, committed)``.
    """
    if rewritten.remove_relative_paths:
        require_remove_action_support()

    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    table_uri = dt.table_uri

    if not rewritten.add_actions and not rewritten.remove_relative_paths:
        return dt.version(), 0, False

    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            live = DeltaTable(table_uri, storage_options=storage_options)
            live_actions: list[object] = list(rewritten.add_actions)
            if rewritten.remove_relative_paths:
                live_actions.extend(build_remove_actions(live, rewritten.remove_relative_paths))
            if rewritten.commit_schema is not None:
                from deltalake.schema import Schema as DeltaSchema

                desired = DeltaSchema.from_arrow(rewritten.commit_schema)
                # lowercase comparison for existing field names
                existing = {f.name.lower() for f in live.schema().fields}
                to_add = [f for f in desired.fields if f.name.lower() not in existing]
                if to_add:
                    live.alter.add_columns(to_add)
                    live = DeltaTable(table_uri, storage_options=storage_options)
            live.create_write_transaction(
                live_actions,
                mode="append",
                schema=live.schema(),
            )
            live.update_incremental()
            return live.version(), attempt, True
        except Exception as exc:
            if not _is_commit_conflict(exc):
                raise
            last_err = exc
            if attempt >= max_retries:
                break
            time.sleep(initial_backoff_sec * (2 ** (attempt - 1)))

    assert last_err is not None
    raise last_err


def merge_into(
    source: pa.Table,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
    allow_multiple_matches: bool = False,
    max_retries: int = 3,
    initial_backoff_sec: float = 0.05,
    touched: TouchedFilesResult | None = None,
) -> MergeCommitResult:
    """Run phase-1 + phase-2 + phase-3 commit for a Delta MERGE.

    On commit conflict, re-runs phase-1+2 against the latest snapshot then
    retries the transaction (Spark-style full retry, not blind recommit).
    When ``touched`` is supplied, it is used only on the first attempt;
    retries recompute phase-1 via PyArrow ``find_touched_files``.

    Args:
        source: Source Arrow table.
        table: Target Delta table or URI.
        merge_keys: Equality join keys.
        storage_options: Optional delta-rs storage options.
        when_matched: ``update`` only in v1.
        when_not_matched: ``insert`` or ``ignore``.
        allow_multiple_matches: Forwarded to phase-1.
        max_retries: Full plan+commit attempts on OCC.
        initial_backoff_sec: Base delay between full retries.
        touched: Optional precomputed phase-1 result (first attempt only).

    Returns:
        ``MergeCommitResult`` with final version and metrics.
    """
    last_err: Exception | None = None
    touched_out: TouchedFilesResult | None = None
    rewritten: RewriteResult | None = None

    for attempt in range(1, max_retries + 1):
        plan_touched = touched if attempt == 1 else None
        touched_out, rewritten = plan_merge_rewrite(
            source,
            table,
            merge_keys,
            storage_options=storage_options,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            allow_multiple_matches=allow_multiple_matches,
            touched=plan_touched,
        )
        try:
            version, _, committed = commit_rewrite_result(
                table,
                rewritten,
                storage_options=storage_options,
                max_retries=1,
            )
            return MergeCommitResult(
                version=version,
                touched=touched_out,
                rewritten=rewritten,
                committed=committed,
                commit_attempts=attempt,
            )
        except Exception as exc:
            if not _is_commit_conflict(exc):
                raise
            last_err = exc
            if attempt >= max_retries:
                break
            time.sleep(initial_backoff_sec * (2 ** (attempt - 1)))

    assert last_err is not None and touched_out is not None and rewritten is not None
    raise last_err


def merge_into_dataframe(
    source_df: object,
    table: DeltaTable | str,
    merge_keys: Sequence[str] | None = None,
    *,
    plan: object | None = None,
    storage_options: dict[str, str] | None = None,
    io_config: object | None = None,
    when_matched: Literal["update", "delete"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
    allow_multiple_matches: bool | None = None,
    max_retries: int = 3,
    initial_backoff_sec: float = 0.05,
) -> MergeCommitResult:
    """MERGE using Daft phase-1 + phase-2, then delta-rs commit.

    Both discovery and rewrite run on the Daft runner (native/Ray). The driver
    only collects touched paths and ``AddAction`` metadata — no ``to_arrow()``
    gather of the source for the rewrite path.

    Args:
        source_df: Daft ``DataFrame`` source.
        table: Target Delta table or URI.
        merge_keys: Equality join keys when ``plan`` is omitted.
        plan: Optional resolved ``MergePlan`` (v2/v3).
        storage_options: Optional delta-rs storage options.
        io_config: Optional Daft IO config for Parquet reads/writes.
        when_matched: v1 sugar when ``plan`` is None.
        when_not_matched: v1 sugar when ``plan`` is None.
        allow_multiple_matches: Forwarded when building a plan.
        max_retries: Full plan+commit attempts on OCC.
        initial_backoff_sec: Base delay between full retries.

    Returns:
        ``MergeCommitResult`` with final version and metrics.
    """
    from daft.io.delta_lake.merge_plan import MergePlan, resolve_merge_plan
    from daft.io.delta_lake.merge_rewrite_daft import plan_merge_rewrite_daft

    if plan is None:
        plan = resolve_merge_plan(
            merge_keys=merge_keys,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            allow_multiple_matches=allow_multiple_matches,
        )
    elif not isinstance(plan, MergePlan):
        raise TypeError(f"plan must be a MergePlan, got {type(plan)}")

    last_err: Exception | None = None
    touched_out: TouchedFilesResult | None = None
    rewritten: RewriteResult | None = None

    for attempt in range(1, max_retries + 1):
        touched_out, rewritten = plan_merge_rewrite_daft(
            source_df,  # type: ignore[arg-type]
            table,
            merge_keys=list(plan.merge_keys),
            plan=plan,
            storage_options=storage_options,
            io_config=io_config,  # type: ignore[arg-type]
        )
        try:
            version, _, committed = commit_rewrite_result(
                table,
                rewritten,
                storage_options=storage_options,
                max_retries=1,
            )
            return MergeCommitResult(
                version=version,
                touched=touched_out,
                rewritten=rewritten,
                committed=committed,
                commit_attempts=attempt,
            )
        except Exception as exc:
            if not _is_commit_conflict(exc):
                raise
            last_err = exc
            if attempt >= max_retries:
                break
            time.sleep(initial_backoff_sec * (2 ** (attempt - 1)))

    assert last_err is not None and touched_out is not None and rewritten is not None
    raise last_err


__all__ = [
    "DeltaMergeCommitUnsupportedError",
    "MergeCommitResult",
    "build_remove_actions",
    "commit_rewrite_result",
    "merge_into",
    "merge_into_dataframe",
    "require_remove_action_support",
]
