"""Phase-1 of distributed Delta MERGE: find target files touched by the source.

Mirrors Spark ``ClassicMergeExecutor.findTouchedFiles``:

1. Inner-join source with target on the merge keys.
2. Reject when multiple source rows match the same target row (unless
   ``allow_multiple_matches`` is set — reserved for delete-only later).
3. Return the distinct **relative** Delta data-file paths that must be rewritten.

This module is deliberately free of ``import daft`` so it can be unit-tested
while the local native extension is being rebuilt. A thin Daft DataFrame
wrapper can call into these helpers once the extension is current.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from urllib.parse import urlparse

import pyarrow as pa  # noqa: TID253
import pyarrow.compute as pc  # noqa: TID253
import pyarrow.parquet as pq  # noqa: TID253
from deltalake import DeltaTable

# Internal column names — lowercase comparison used when checking collisions.
_FILE_PATH_COL = "_daft_delta_file_path"
_TARGET_ROW_ID_COL = "_daft_delta_target_row_id"


class DeltaMergeMultipleMatchesError(ValueError):
    """Raised when more than one source row matches a single target row."""


@dataclass(frozen=True)
class TouchedFilesResult:
    """Outcome of phase-1 touched-file discovery.

    Attributes:
        relative_paths: Distinct Delta log-relative paths to rewrite.
        num_source_rows: Rows in the source table passed in.
        num_matched_target_rows: Distinct target rows that joined.
        num_target_files_scanned: Add-action files considered before the join.
    """

    relative_paths: tuple[str, ...]
    num_source_rows: int
    num_matched_target_rows: int
    num_target_files_scanned: int


def _normalize_table_uri(table_uri: str) -> str:
    """Normalize table URI for IO.

    Object-store URIs keep their scheme. Local ``file://`` URIs are converted to
    absolute filesystem paths so ``os.path`` / PyArrow local writes work.
    """
    if table_uri.startswith(("s3://", "s3a://", "gs://", "gcs://", "abfs://", "abfss://", "az://")):
        return table_uri.rstrip("/")
    path = os.path.expanduser(table_uri)
    # lowercase comparison for file scheme
    if path.lower().startswith("file://"):
        parsed = urlparse(path)
        # urlparse('file:///tmp/x') -> path='/tmp/x'
        path = parsed.path
    return os.path.abspath(path).rstrip("/")


def _full_path(table_uri: str, relative_path: str) -> str:
    """Build a readable path for a relative Delta data file."""
    scheme = urlparse(table_uri).scheme
    # lowercase comparison for scheme family
    if scheme.lower() in ("s3", "s3a", "gs", "gcs", "az", "abfs", "abfss"):
        return table_uri.rstrip("/") + "/" + relative_path
    return os.path.join(table_uri, relative_path)


def list_delta_data_files(table: DeltaTable | str, storage_options: dict[str, str] | None = None) -> list[str]:
    """Return relative data-file paths currently active in the Delta table.

    Args:
        table: ``DeltaTable`` or table URI.
        storage_options: Passed through when ``table`` is a URI string.

    Returns:
        Relative paths as stored in the Delta log (suitable for RemoveAction).
    """
    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    add_actions = pa.table(dt.get_add_actions())
    if add_actions.num_rows == 0:
        return []
    return list(add_actions.column("path").to_pylist())


def _read_target_with_file_and_row_id(table_uri: str, relative_paths: Sequence[str]) -> pa.Table:
    """Read all target Parquet files, tagging each row with path + stable row id."""
    tables: list[pa.Table] = []
    row_id = 0
    for relative in relative_paths:
        full = _full_path(table_uri, relative)
        file_table = pq.read_table(full)
        n = file_table.num_rows
        if n == 0:
            continue
        ids = pa.array(range(row_id, row_id + n), type=pa.int64())
        paths = pa.array([relative] * n, type=pa.string())
        file_table = file_table.append_column(_TARGET_ROW_ID_COL, ids).append_column(_FILE_PATH_COL, paths)
        tables.append(file_table)
        row_id += n
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def _ensure_merge_keys(table: pa.Table, merge_keys: Sequence[str], side: str) -> None:
    """Validate merge keys exist on a table."""
    names = set(table.column_names)
    missing = [k for k in merge_keys if k not in names]
    if missing:
        raise ValueError(f"{side} is missing merge key column(s): {missing}")


def _check_column_collisions(source: pa.Table, merge_keys: Sequence[str]) -> None:
    """Reject source columns that collide with internal phase-1 column names."""
    # lowercase comparison for reserved internal names
    reserved = {_FILE_PATH_COL.lower(), _TARGET_ROW_ID_COL.lower()}
    for name in source.column_names:
        if name.lower() in reserved:
            raise ValueError(f"source column name collides with reserved merge column: {name}")
    for key in merge_keys:
        if key.lower() in reserved:
            raise ValueError(f"merge key collides with reserved merge column: {key}")


def _join_inner_on_keys(source: pa.Table, target: pa.Table, merge_keys: Sequence[str]) -> pa.Table:
    """Inner-join source and target on equality of all merge keys."""
    if source.num_rows == 0 or target.num_rows == 0:
        return source.select([]).slice(0, 0)

    # pyarrow.Table.join requires unique right join keys for some versions when
    # using a simple equality join; we use a hash-join via pandas-free approach:
    # build a lookup from key-tuple → list of target row indices, then expand.
    key_arrays_target = [target.column(k) for k in merge_keys]
    target_index_by_key: dict[tuple[object, ...], list[int]] = {}
    for i in range(target.num_rows):
        key = tuple(col[i].as_py() for col in key_arrays_target)
        target_index_by_key.setdefault(key, []).append(i)

    src_indices: list[int] = []
    tgt_indices: list[int] = []
    key_arrays_source = [source.column(k) for k in merge_keys]
    for i in range(source.num_rows):
        key = tuple(col[i].as_py() for col in key_arrays_source)
        matches = target_index_by_key.get(key)
        if not matches:
            continue
        for j in matches:
            src_indices.append(i)
            tgt_indices.append(j)

    if not src_indices:
        # Empty joined schema: keep target meta columns for downstream.
        return pa.table(
            {
                _TARGET_ROW_ID_COL: pa.array([], type=pa.int64()),
                _FILE_PATH_COL: pa.array([], type=pa.string()),
            }
        )

    joined_target = target.take(pa.array(tgt_indices, type=pa.int64()))
    return joined_target.select([_TARGET_ROW_ID_COL, _FILE_PATH_COL])


def _assert_no_multiple_matches(joined: pa.Table, allow_multiple_matches: bool) -> int:
    """Ensure each target row id appears at most once in the join result.

    Returns:
        Number of distinct matched target rows.
    """
    if joined.num_rows == 0:
        return 0
    row_ids = joined.column(_TARGET_ROW_ID_COL)
    # value_counts gives distinct ids + frequencies
    vc = pc.value_counts(row_ids)
    values = vc.field(0)
    counts = vc.field(1)
    distinct = len(values)
    if allow_multiple_matches:
        return distinct
    # Any frequency > 1 is a Spark-style multiple-match violation.
    max_count = pc.max(counts).as_py()
    if max_count is not None and max_count > 1:
        raise DeltaMergeMultipleMatchesError(
            "MERGE matched multiple source rows to the same target row; "
            "this is only allowed for delete-only merges (not yet supported)."
        )
    return distinct


def find_touched_files(
    source: pa.Table,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    allow_multiple_matches: bool = False,
) -> TouchedFilesResult:
    """Find relative Delta data-file paths touched by ``source`` under ``merge_keys``.

    Args:
        source: Source batch to merge (Arrow table).
        table: Target ``DeltaTable`` or URI.
        merge_keys: Equality join columns present on both source and target.
        storage_options: Optional delta-rs storage options when ``table`` is a URI.
        allow_multiple_matches: If True, skip the multi-match cardinality check
            (delete-only Spark path; not wired to a rewrite planner yet).

    Returns:
        ``TouchedFilesResult`` with distinct relative paths to rewrite.

    Raises:
        ValueError: Missing merge keys or reserved column name collisions.
        DeltaMergeMultipleMatchesError: Multiple source rows match one target row.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be a non-empty sequence")

    _check_column_collisions(source, merge_keys)
    _ensure_merge_keys(source, merge_keys, "source")

    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    table_uri = _normalize_table_uri(dt.table_uri)
    relative_paths = list_delta_data_files(dt)
    if not relative_paths:
        return TouchedFilesResult(
            relative_paths=(),
            num_source_rows=source.num_rows,
            num_matched_target_rows=0,
            num_target_files_scanned=0,
        )

    if source.num_rows == 0:
        return TouchedFilesResult(
            relative_paths=(),
            num_source_rows=0,
            num_matched_target_rows=0,
            num_target_files_scanned=len(relative_paths),
        )

    target = _read_target_with_file_and_row_id(table_uri, relative_paths)
    _ensure_merge_keys(target, merge_keys, "target")

    joined = _join_inner_on_keys(source, target, merge_keys)
    matched_rows = _assert_no_multiple_matches(joined, allow_multiple_matches)

    if joined.num_rows == 0:
        touched: tuple[str, ...] = ()
    else:
        # Distinct relative paths — lowercase not applied; paths are case-sensitive on S3 keys.
        touched = tuple(sorted(set(joined.column(_FILE_PATH_COL).to_pylist())))

    return TouchedFilesResult(
        relative_paths=touched,
        num_source_rows=source.num_rows,
        num_matched_target_rows=matched_rows,
        num_target_files_scanned=len(relative_paths),
    )


__all__ = [
    "DeltaMergeMultipleMatchesError",
    "TouchedFilesResult",
    "find_touched_files",
    "list_delta_data_files",
]
