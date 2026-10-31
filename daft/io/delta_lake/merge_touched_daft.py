"""Phase-1 MERGE discovery using Daft DataFrames (native/Ray runnable).

Unlike ``merge_touched_files.find_touched_files`` (PyArrow in-process), this
path scans target Parquet with ``daft.read_parquet`` and joins with the source
DataFrame so the runner can parallelize IO/join. Only **file paths** and
cardinality aggregates are collected to the driver — not full merge output.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from deltalake import DeltaTable

from daft.functions import monotonically_increasing_id
from daft.io.delta_lake.merge_touched_files import (
    DeltaMergeMultipleMatchesError,
    TouchedFilesResult,
    _full_path,
    _normalize_table_uri,
    list_delta_data_files,
)

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame

_FILE_PATH_COL = "_daft_delta_file_path"
_TARGET_ROW_ID_COL = "_daft_delta_target_row_id"


def _relative_from_full(table_uri: str, full_path: str, known_relative: set[str]) -> str:
    """Map a full Parquet path back to a Delta-log relative path."""
    from urllib.parse import urlparse

    path = full_path
    # lowercase comparison for file scheme
    if path.lower().startswith("file://"):
        path = urlparse(path).path

    # Prefer longest known relative suffix (handles partition prefixes).
    matches = [rel for rel in known_relative if path.endswith(rel) or path.endswith("/" + rel)]
    if matches:
        return max(matches, key=len)

    prefix = table_uri.rstrip("/") + "/"
    if path.startswith(prefix):
        return path[len(prefix) :]
    raise ValueError(f"cannot map full path to Delta relative path: {full_path!r} under {table_uri!r}")


def find_touched_files_daft(
    source: DataFrame,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    io_config: IOConfig | None = None,
    allow_multiple_matches: bool = False,
) -> TouchedFilesResult:
    """Find touched Delta data files via a Daft inner-join (Spark phase-1 analogue).

    Args:
        source: Source DataFrame (not collected).
        table: Target Delta table or URI.
        merge_keys: Equality join columns on both sides.
        storage_options: Optional delta-rs options when ``table`` is a URI.
        io_config: Optional Daft IO config for Parquet reads.
        allow_multiple_matches: If True, skip multi-match error (delete-only path).

    Returns:
        ``TouchedFilesResult`` with distinct **relative** paths to rewrite.

    Raises:
        ValueError: Missing keys or empty ``merge_keys``.
        DeltaMergeMultipleMatchesError: Multiple source rows match one target row.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be a non-empty sequence")

    # lowercase comparison for reserved column names
    reserved = {_FILE_PATH_COL.lower(), _TARGET_ROW_ID_COL.lower()}
    for name in source.column_names:
        if name.lower() in reserved:
            raise ValueError(f"source column name collides with reserved merge column: {name}")
    for key in merge_keys:
        if key.lower() in reserved:
            raise ValueError(f"merge key collides with reserved merge column: {key}")
        if key not in source.column_names:
            raise ValueError(f"source is missing merge key column(s): {[key]}")

    dt = table if isinstance(table, DeltaTable) else DeltaTable(table, storage_options=storage_options)
    table_uri = _normalize_table_uri(dt.table_uri)
    relative_paths = list_delta_data_files(dt)
    num_files = len(relative_paths)

    if num_files == 0:
        # Still need source row count for metrics — lightweight collect of count only.
        num_source = source.count_rows()
        return TouchedFilesResult(
            relative_paths=(),
            num_source_rows=num_source,
            num_matched_target_rows=0,
            num_target_files_scanned=0,
        )

    import daft

    full_paths = [_full_path(table_uri, rel) for rel in relative_paths]
    known = set(relative_paths)

    target = daft.read_parquet(
        full_paths,
        file_path_column=_FILE_PATH_COL,
        io_config=io_config,
    )
    for key in merge_keys:
        if key not in target.column_names:
            raise ValueError(f"target is missing merge key column(s): {[key]}")

    target = target.with_column(_TARGET_ROW_ID_COL, monotonically_increasing_id())

    # Project only keys + path + row id for the join (column prune).
    target_slim = target.select(*merge_keys, _FILE_PATH_COL, _TARGET_ROW_ID_COL)
    source_slim = source.select(*merge_keys)

    joined = source_slim.join(target_slim, on=list(merge_keys), how="inner")

    # Multi-match: same target row id appearing more than once after join.
    from daft import col
    from daft.sql import sql_expr

    counts = (
        joined.groupby(_TARGET_ROW_ID_COL)
        .agg(col(_TARGET_ROW_ID_COL).count().alias("_match_count"))
        .where(sql_expr("_match_count > 1"))
    )
    multi = counts.count_rows()
    if multi > 0 and not allow_multiple_matches:
        raise DeltaMergeMultipleMatchesError(
            "MERGE matched multiple source rows to the same target row; "
            "this is only allowed for delete-only merges (not yet supported)."
        )

    path_df = joined.select(_FILE_PATH_COL).distinct()
    path_rows = path_df.to_pydict().get(_FILE_PATH_COL, [])
    relative_touched = tuple(sorted({_relative_from_full(table_uri, p, known) for p in path_rows if p is not None}))

    matched_rows = joined.select(_TARGET_ROW_ID_COL).distinct().count_rows()
    num_source = source.count_rows()

    return TouchedFilesResult(
        relative_paths=relative_touched,
        num_source_rows=num_source,
        num_matched_target_rows=matched_rows,
        num_target_files_scanned=num_files,
    )


__all__ = ["find_touched_files_daft"]
