"""Tests for Daft-based phase-1 ``find_touched_files_daft``."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("deltalake")

import pyarrow as pa
from deltalake import write_deltalake

import daft
from daft.io.delta_lake.merge_touched_daft import find_touched_files_daft
from daft.io.delta_lake.merge_touched_files import (
    DeltaMergeMultipleMatchesError,
    list_delta_data_files,
)


def _seed_multi_file_table(table_path: Path) -> None:
    """Write three small files via separate appends so paths are distinct."""
    write_deltalake(
        str(table_path),
        pa.table({"id": [1, 2], "val": ["a", "b"]}),
        mode="overwrite",
    )
    write_deltalake(
        str(table_path),
        pa.table({"id": [3, 4], "val": ["c", "d"]}),
        mode="append",
    )
    write_deltalake(
        str(table_path),
        pa.table({"id": [5, 6], "val": ["e", "f"]}),
        mode="append",
    )


def test_find_touched_files_daft_returns_only_matching_files(tmp_path: Path) -> None:
    """Inner-join discovery returns only files that contain matched keys."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    all_files = list_delta_data_files(str(table_path))
    assert len(all_files) == 3

    source = daft.from_pydict({"id": [2, 5], "val": ["B", "E"]})
    result = find_touched_files_daft(source, str(table_path), merge_keys=["id"])

    assert result.num_source_rows == 2
    assert result.num_matched_target_rows == 2
    assert result.num_target_files_scanned == 3
    assert len(result.relative_paths) == 2
    assert set(result.relative_paths).issubset(set(all_files))


def test_find_touched_files_daft_empty_source(tmp_path: Path) -> None:
    """Empty source touches no files."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = daft.from_pydict({"id": [], "val": []})
    result = find_touched_files_daft(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_source_rows == 0
    assert result.num_matched_target_rows == 0


def test_find_touched_files_daft_no_matches(tmp_path: Path) -> None:
    """Non-matching keys produce empty touched set (insert-only candidate)."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = daft.from_pydict({"id": [99, 100], "val": ["x", "y"]})
    result = find_touched_files_daft(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_matched_target_rows == 0


def test_find_touched_files_daft_multiple_matches_raises(tmp_path: Path) -> None:
    """Multiple source rows for one target row raise by default."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = daft.from_pydict({"id": [2, 2], "val": ["x", "y"]})
    with pytest.raises(DeltaMergeMultipleMatchesError):
        find_touched_files_daft(source, str(table_path), merge_keys=["id"])


def test_find_touched_files_daft_allow_multiple_matches(tmp_path: Path) -> None:
    """allow_multiple_matches=True suppresses the multi-match error."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = daft.from_pydict({"id": [2, 2], "val": ["x", "y"]})
    result = find_touched_files_daft(
        source,
        str(table_path),
        merge_keys=["id"],
        allow_multiple_matches=True,
    )
    assert len(result.relative_paths) == 1


def test_find_touched_files_daft_composite_keys(tmp_path: Path) -> None:
    """Composite merge keys join correctly."""
    table_path = tmp_path / "tgt"
    write_deltalake(
        str(table_path),
        pa.table({"pk": [1, 1], "sk": ["a", "b"], "val": [10, 20]}),
        mode="overwrite",
    )
    source = daft.from_pydict({"pk": [1], "sk": ["b"], "val": [99]})
    result = find_touched_files_daft(source, str(table_path), merge_keys=["pk", "sk"])
    assert result.num_matched_target_rows == 1
    assert len(result.relative_paths) == 1


def test_find_touched_files_daft_rejects_empty_merge_keys(tmp_path: Path) -> None:
    """Empty merge_keys is rejected."""
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = daft.from_pydict({"id": [1], "val": ["x"]})
    with pytest.raises(ValueError, match="merge_keys"):
        find_touched_files_daft(source, str(table_path), merge_keys=[])


def test_find_touched_files_daft_empty_target_table(tmp_path: Path) -> None:
    """Target with schema but no data files returns empty touched paths."""
    table_path = tmp_path / "tgt"
    write_deltalake(
        str(table_path),
        pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())}),
        mode="overwrite",
    )
    source = daft.from_pydict({"id": [1], "val": ["x"]})
    result = find_touched_files_daft(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_target_files_scanned == 0
    assert result.num_source_rows == 1


def test_find_touched_files_daft_matches_pyarrow_phase1(tmp_path: Path) -> None:
    """Daft and PyArrow phase-1 return the same relative paths for the same input."""
    from daft.io.delta_lake.merge_touched_files import find_touched_files

    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source_dict = {"id": [2, 6, 99], "val": ["B", "F", "NEW"]}
    source_df = daft.from_pydict(source_dict)
    source_pa = pa.table(source_dict)

    daft_result = find_touched_files_daft(source_df, str(table_path), merge_keys=["id"])
    pa_result = find_touched_files(source_pa, str(table_path), merge_keys=["id"])

    assert set(daft_result.relative_paths) == set(pa_result.relative_paths)
    assert daft_result.num_matched_target_rows == pa_result.num_matched_target_rows
    assert daft_result.num_source_rows == pa_result.num_source_rows
