"""Tests for Daft-based phase-2 ``rewrite_touched_files_daft``."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("deltalake")

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from packaging.version import parse as parse_version

import daft
from daft.io.delta_lake.merge_rewrite_daft import (
    plan_merge_rewrite_daft,
    rewrite_touched_files_daft,
)
from daft.io.delta_lake.merge_touched_daft import find_touched_files_daft

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


def test_rewrite_daft_updates_one_file_copies_unmatched(tmp_path: Path) -> None:
    """Matched rows update; unmatched rows in the touched file are copied."""
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)

    source = daft.from_pydict({"id": [2], "val": ["B"]})
    touched = find_touched_files_daft(source, str(table_path), merge_keys=["id"])
    assert len(touched.relative_paths) == 1

    rewritten = rewrite_touched_files_daft(
        source,
        str(table_path),
        ["id"],
        touched.relative_paths,
    )
    assert rewritten.num_updated == 1
    assert rewritten.num_copied == 1
    assert rewritten.num_inserted == 0
    assert len(rewritten.add_actions) == 1
    assert rewritten.remove_relative_paths == touched.relative_paths

    # New parquet exists on disk under the table root.
    new_path = table_path / rewritten.output_relative_paths[0]
    assert new_path.is_file()
    out = pa.parquet.read_table(new_path).to_pylist()
    by_id = {r["id"]: r["val"] for r in out}
    assert by_id[2] == "B"
    assert by_id[1] == "a"


def test_rewrite_daft_insert_only_no_removes(tmp_path: Path) -> None:
    """Insert-only source writes a new file and removes nothing."""
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)

    source = daft.from_pydict({"id": [99], "val": ["NEW"]})
    rewritten = rewrite_touched_files_daft(
        source,
        str(table_path),
        ["id"],
        touched_relative_paths=(),
    )
    assert rewritten.num_inserted == 1
    assert rewritten.num_updated == 0
    assert rewritten.num_copied == 0
    assert rewritten.remove_relative_paths == ()
    assert len(rewritten.add_actions) == 1


def test_rewrite_daft_ignore_inserts(tmp_path: Path) -> None:
    """when_not_matched=ignore skips source-only rows."""
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)

    source = daft.from_pydict({"id": [2, 99], "val": ["B", "NEW"]})
    touched = find_touched_files_daft(source, str(table_path), merge_keys=["id"])
    rewritten = rewrite_touched_files_daft(
        source,
        str(table_path),
        ["id"],
        touched.relative_paths,
        when_not_matched="ignore",
    )
    assert rewritten.num_updated == 1
    assert rewritten.num_inserted == 0
    assert rewritten.num_copied == 1


def test_rewrite_daft_empty_source(tmp_path: Path) -> None:
    """Empty source produces no rewrite output."""
    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source = daft.from_pydict({"id": [], "val": []})
    rewritten = rewrite_touched_files_daft(
        source,
        str(table_path),
        ["id"],
        touched_relative_paths=(),
    )
    assert rewritten.add_actions == ()
    assert rewritten.num_updated == 0
    assert rewritten.num_inserted == 0


def test_plan_merge_rewrite_daft_matches_arrow_metrics(tmp_path: Path) -> None:
    """Daft plan metrics match PyArrow rewrite for the same logical merge."""
    from daft.io.delta_lake.merge_rewrite import plan_merge_rewrite

    table_path = tmp_path / "tgt"
    _seed_multi_file(table_path)
    source_dict = {"id": [2, 50], "val": ["B", "NEW"]}
    source_df = daft.from_pydict(source_dict)
    source_pa = pa.table(source_dict)

    _, daft_rewritten = plan_merge_rewrite_daft(source_df, str(table_path), ["id"])
    _, pa_rewritten = plan_merge_rewrite(source_pa, str(table_path), ["id"])

    assert daft_rewritten.num_updated == pa_rewritten.num_updated
    assert daft_rewritten.num_inserted == pa_rewritten.num_inserted
    assert daft_rewritten.num_copied == pa_rewritten.num_copied
    assert set(daft_rewritten.remove_relative_paths) == set(pa_rewritten.remove_relative_paths)


@requires_remove  # type: ignore[untyped-decorator]
def test_write_deltalake_merge_uses_daft_rewrite(tmp_path: Path) -> None:
    """End-to-end mode=merge still updates+inserts after Daft phase-2 wiring."""
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

    by_id = {r["id"]: r["val"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert by_id[2] == "B"
    assert by_id[50] == "NEW"
    assert by_id[1] == "a"
    assert by_id[3] == "c"
