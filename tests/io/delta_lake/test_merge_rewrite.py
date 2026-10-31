"""Tests for phase-2 ``rewrite_touched_files`` / ``plan_merge_rewrite``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("deltalake")

from deltalake import DeltaTable, write_deltalake


def _load_modules():
    """Load phase-1/2 modules without importing the daft package root."""
    repo = Path(__file__).resolve().parents[3]
    delta_dir = repo / "daft" / "io" / "delta_lake"
    if str(delta_dir) not in sys.path:
        sys.path.insert(0, str(delta_dir))

    # Ensure sibling imports resolve for merge_rewrite → merge_touched_files
    touched_path = delta_dir / "merge_touched_files.py"
    rewrite_path = delta_dir / "merge_rewrite.py"

    for name, path in (
        ("merge_touched_files", touched_path),
        ("merge_rewrite", rewrite_path),
    ):
        if name in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)

    return sys.modules["merge_touched_files"], sys.modules["merge_rewrite"]


mt, rw = _load_modules()


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


def test_build_rewrite_table_update_copy_insert() -> None:
    target = pa.table({"id": [1, 2], "val": ["a", "b"]})
    source = pa.table({"id": [1, 3], "val": ["A", "C"]})
    out, n_upd, n_ins, n_copy = rw.build_rewrite_table(source, target, ["id"])
    assert n_upd == 1
    assert n_ins == 1
    assert n_copy == 1
    by_id = {out.column("id")[i].as_py(): out.column("val")[i].as_py() for i in range(out.num_rows)}
    assert by_id == {1: "A", 2: "b", 3: "C"}


def test_build_rewrite_table_ignore_inserts() -> None:
    target = pa.table({"id": [1], "val": ["a"]})
    source = pa.table({"id": [1, 9], "val": ["A", "Z"]})
    out, n_upd, n_ins, n_copy = rw.build_rewrite_table(source, target, ["id"], when_not_matched="ignore")
    assert n_upd == 1
    assert n_ins == 0
    assert n_copy == 0
    assert out.column("id").to_pylist() == [1]
    assert out.column("val").to_pylist() == ["A"]


def test_rewrite_updates_one_file_copies_unmatched(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = pa.table({"id": [2], "val": ["B"]})
    touched = mt.find_touched_files(source, str(table_path), ["id"])
    assert len(touched.relative_paths) == 1

    result = rw.rewrite_touched_files(source, str(table_path), ["id"], touched.relative_paths)
    assert result.num_updated == 1
    assert result.num_copied == 1  # id=1 stays in same file
    assert result.num_inserted == 0
    assert len(result.add_actions) == 1
    assert result.remove_relative_paths == touched.relative_paths

    written = pq.read_table(table_path / result.output_relative_paths[0])
    by_id = {written.column("id")[i].as_py(): written.column("val")[i].as_py() for i in range(written.num_rows)}
    assert by_id[2] == "B"
    assert by_id[1] == "a"


def test_rewrite_insert_only_no_removes(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = pa.table({"id": [99], "val": ["new"]})
    touched = mt.find_touched_files(source, str(table_path), ["id"])
    assert touched.relative_paths == ()

    result = rw.rewrite_touched_files(source, str(table_path), ["id"], touched.relative_paths)
    assert result.num_inserted == 1
    assert result.num_updated == 0
    assert result.remove_relative_paths == ()
    assert len(result.add_actions) == 1
    written = pq.read_table(table_path / result.output_relative_paths[0])
    assert written.column("id").to_pylist() == [99]


def test_plan_merge_rewrite_end_to_end_logical(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    # update id=5, insert id=7
    source = pa.table({"id": [5, 7], "val": ["E", "G"]})
    touched, rewritten = rw.plan_merge_rewrite(source, str(table_path), ["id"])
    assert len(touched.relative_paths) == 1
    assert rewritten.num_updated == 1
    assert rewritten.num_inserted == 1
    assert rewritten.num_copied == 1  # id=6 in same file as 5
    assert rewritten.remove_relative_paths == touched.relative_paths

    # Logical view as if we applied add+remove without committing:
    dt = DeltaTable(str(table_path))
    remaining = [p for p in mt.list_delta_data_files(dt) if p not in set(rewritten.remove_relative_paths)]
    parts = [pq.read_table(table_path / p) for p in remaining]
    parts.append(pq.read_table(table_path / rewritten.output_relative_paths[0]))
    merged = pa.concat_tables(parts)
    by_id = {merged.column("id")[i].as_py(): merged.column("val")[i].as_py() for i in range(merged.num_rows)}
    assert by_id[5] == "E"
    assert by_id[7] == "G"
    assert by_id[6] == "f"
    assert by_id[1] == "a"


def test_rewrite_empty_source(tmp_path: Path) -> None:
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())})
    result = rw.rewrite_touched_files(source, str(table_path), ["id"], ())
    assert result.add_actions == ()
    assert result.num_updated == 0
    assert result.num_inserted == 0
