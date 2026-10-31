"""Tests for phase-1 ``find_touched_files`` (Spark-style touched file discovery).

Loads the module by file path so tests run even when the local ``daft.daft``
native extension is stale / being rebuilt.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("deltalake")

from deltalake import write_deltalake


def _load_merge_module():
    """Import merge_touched_files without importing the daft package root."""
    import sys

    repo = Path(__file__).resolve().parents[3]
    module_path = repo / "daft" / "io" / "delta_lake" / "merge_touched_files.py"
    spec = importlib.util.spec_from_file_location("merge_touched_files", module_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # dataclass looks up the module in sys.modules during class creation
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


merge = _load_merge_module()


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


def test_find_touched_files_returns_only_matching_files(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    all_files = merge.list_delta_data_files(str(table_path))
    assert len(all_files) == 3

    source = pa.table({"id": [2, 5], "val": ["x", "y"]})
    result = merge.find_touched_files(source, str(table_path), merge_keys=["id"])

    assert result.num_source_rows == 2
    assert result.num_matched_target_rows == 2
    assert result.num_target_files_scanned == 3
    assert len(result.relative_paths) == 2
    assert set(result.relative_paths).issubset(set(all_files))
    # id=2 and id=5 live in different append files
    assert len(set(result.relative_paths)) == 2


def test_find_touched_files_empty_source(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())})
    result = merge.find_touched_files(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_source_rows == 0
    assert result.num_matched_target_rows == 0


def test_find_touched_files_no_matches(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file_table(table_path)
    source = pa.table({"id": [999], "val": ["z"]})
    result = merge.find_touched_files(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_matched_target_rows == 0


def test_find_touched_files_multiple_matches_raises(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    write_deltalake(str(table_path), pa.table({"id": [1], "val": ["a"]}), mode="overwrite")
    # Two source rows both match target id=1
    source = pa.table({"id": [1, 1], "val": ["x", "y"]})
    with pytest.raises(merge.DeltaMergeMultipleMatchesError):
        merge.find_touched_files(source, str(table_path), merge_keys=["id"])


def test_find_touched_files_allow_multiple_matches(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    write_deltalake(str(table_path), pa.table({"id": [1], "val": ["a"]}), mode="overwrite")
    source = pa.table({"id": [1, 1], "val": ["x", "y"]})
    result = merge.find_touched_files(
        source,
        str(table_path),
        merge_keys=["id"],
        allow_multiple_matches=True,
    )
    assert len(result.relative_paths) == 1
    assert result.num_matched_target_rows == 1


def test_find_touched_files_composite_keys(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    write_deltalake(
        str(table_path),
        pa.table({"pk": [1, 1, 2], "sk": ["a", "b", "a"], "val": [10, 20, 30]}),
        mode="overwrite",
    )
    source = pa.table({"pk": [1], "sk": ["b"], "val": [99]})
    result = merge.find_touched_files(source, str(table_path), merge_keys=["pk", "sk"])
    assert len(result.relative_paths) == 1
    assert result.num_matched_target_rows == 1


def test_find_touched_files_rejects_empty_merge_keys(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    write_deltalake(str(table_path), pa.table({"id": [1]}), mode="overwrite")
    with pytest.raises(ValueError, match="merge_keys"):
        merge.find_touched_files(pa.table({"id": [1]}), str(table_path), merge_keys=[])


def test_find_touched_files_empty_target_table(tmp_path: Path) -> None:
    table_path = tmp_path / "empty"
    write_deltalake(
        str(table_path),
        pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())}),
        mode="overwrite",
    )
    files = merge.list_delta_data_files(str(table_path))
    source = pa.table({"id": [1], "val": ["z"]})
    result = merge.find_touched_files(source, str(table_path), merge_keys=["id"])
    assert result.relative_paths == ()
    assert result.num_matched_target_rows == 0
    assert result.num_target_files_scanned == len(files)
