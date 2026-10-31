"""Tests for phase-3 ``merge_into`` / ``commit_rewrite_result``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("deltalake")

from deltalake import DeltaTable, write_deltalake
from packaging.version import parse as parse_version


def _load_modules():
    """Load merge modules without importing the daft package root."""
    repo = Path(__file__).resolve().parents[3]
    delta_dir = repo / "daft" / "io" / "delta_lake"
    if str(delta_dir) not in sys.path:
        sys.path.insert(0, str(delta_dir))

    for name in ("merge_touched_files", "merge_rewrite", "merge_commit"):
        if name in sys.modules:
            continue
        path = delta_dir / f"{name}.py"
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)

    return (
        sys.modules["merge_touched_files"],
        sys.modules["merge_rewrite"],
        sys.modules["merge_commit"],
    )


mt, rw, mc = _load_modules()

requires_remove = pytest.mark.skipif(
    not mc._HAS_REMOVE_ACTION,
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
    write_deltalake(
        str(table_path),
        pa.table({"id": [5, 6], "val": ["e", "f"]}),
        mode="append",
    )


@requires_remove
def test_merge_into_update_and_insert(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    dt = DeltaTable(str(table_path))
    version_before = dt.version()

    source = pa.table({"id": [2, 99], "val": ["B", "NEW"]})
    result = mc.merge_into(source, str(table_path), merge_keys=["id"])

    assert result.committed is True
    assert result.version == version_before + 1
    assert result.rewritten.num_updated == 1
    assert result.rewritten.num_inserted == 1
    assert len(result.touched.relative_paths) == 1

    dt2 = DeltaTable(str(table_path))
    by_id = {row["id"]: row["val"] for row in dt2.to_pyarrow_table().to_pylist()}
    assert by_id[2] == "B"
    assert by_id[99] == "NEW"
    assert by_id[1] == "a"
    assert by_id[5] == "e"
    # Removed file paths must not remain active
    active = set(pa.table(dt2.get_add_actions()).column("path").to_pylist())
    for removed in result.rewritten.remove_relative_paths:
        assert removed not in active
    for added in result.rewritten.output_relative_paths:
        assert added in active


@requires_remove
def test_merge_into_insert_only(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    version_before = DeltaTable(str(table_path)).version()
    source = pa.table({"id": [100], "val": ["x"]})
    result = mc.merge_into(source, str(table_path), merge_keys=["id"])
    assert result.committed is True
    assert result.version == version_before + 1
    assert result.rewritten.remove_relative_paths == ()
    assert result.rewritten.num_inserted == 1
    ids = DeltaTable(str(table_path)).to_pyarrow_table().column("id").to_pylist()
    assert 100 in ids


@requires_remove
def test_merge_into_empty_source_noop(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    version_before = DeltaTable(str(table_path)).version()
    source = pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())})
    result = mc.merge_into(source, str(table_path), merge_keys=["id"])
    assert result.committed is False
    assert result.version == version_before
    assert DeltaTable(str(table_path)).version() == version_before


@requires_remove
def test_commit_rewrite_result_idempotent_paths(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = pa.table({"id": [5], "val": ["E"]})
    _, rewritten = rw.plan_merge_rewrite(source, str(table_path), ["id"])
    version, attempts, committed = mc.commit_rewrite_result(str(table_path), rewritten)
    assert committed is True
    assert attempts == 1
    dt = DeltaTable(str(table_path))
    assert dt.version() == version
    vals = {r["id"]: r["val"] for r in dt.to_pyarrow_table().to_pylist()}
    assert vals[5] == "E"
    assert vals[6] == "f"


def test_require_remove_action_documents_gate() -> None:
    import deltalake

    if parse_version(deltalake.__version__) < parse_version("1.6.4"):
        with pytest.raises(mc.DeltaMergeCommitUnsupportedError):
            mc.require_remove_action_support()
    else:
        mc.require_remove_action_support()
