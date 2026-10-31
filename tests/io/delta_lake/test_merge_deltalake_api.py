"""API tests for ``DataFrame.write_deltalake(..., mode="merge")``."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("deltalake")

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from packaging.version import parse as parse_version

import daft

requires_remove = pytest.mark.skipif(
    parse_version(__import__("deltalake").__version__) < parse_version("1.6.4"),
    reason="deltalake>=1.6.4 required for RemoveAction commits",
)


def _seed_multi_file(table_path: Path) -> None:
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


@requires_remove
def test_write_deltalake_mode_merge_update_and_insert(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    version_before = DeltaTable(str(table_path)).version()

    source = daft.from_pydict({"id": [2, 50], "val": ["B", "NEW"]})
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_keys=["id"],
    ).to_pydict()

    assert summary["committed"] == [True]
    assert summary["version"] == [version_before + 1]
    assert summary["num_updated"] == [1]
    assert summary["num_inserted"] == [1]
    assert summary["num_files_rewritten"] == [1]

    by_id = {r["id"]: r["val"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert by_id[2] == "B"
    assert by_id[50] == "NEW"
    assert by_id[1] == "a"
    assert by_id[3] == "c"


@requires_remove
def test_write_deltalake_mode_merge_empty_source_noop(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    version_before = DeltaTable(str(table_path)).version()

    source = daft.from_pydict({"id": [], "val": []})
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_keys=["id"],
    ).to_pydict()

    assert summary["committed"] == [False]
    assert summary["version"] == [version_before]
    assert DeltaTable(str(table_path)).version() == version_before


@requires_remove
def test_write_deltalake_mode_merge_requires_keys(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = daft.from_pydict({"id": [1], "val": ["x"]})
    with pytest.raises(ValueError, match="merge_keys"):
        source.write_deltalake(str(table_path), mode="merge")
    with pytest.raises(ValueError, match="merge_keys"):
        source.write_deltalake(str(table_path), mode="merge", merge_keys=[])


@requires_remove
def test_write_deltalake_merge_keys_only_valid_with_merge_mode(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = daft.from_pydict({"id": [1], "val": ["x"]})
    with pytest.raises(ValueError, match="merge_keys / merge_predicate / merge_clauses are only valid"):
        source.write_deltalake(str(table_path), mode="append", merge_keys=["id"])


@requires_remove
def test_write_deltalake_mode_merge_requires_existing_table(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    source = daft.from_pydict({"id": [1], "val": ["x"]})
    with pytest.raises(Exception):
        source.write_deltalake(str(missing), mode="merge", merge_keys=["id"])
