"""v2/v3 MERGE clause tests (partial update, delete, NMBS, fluent builder)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("deltalake")

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from packaging.version import parse as parse_version

import daft
from daft.io.delta_lake.merge_plan import (
    MatchedUpdate,
    NotMatchedBySourceDelete,
    NotMatchedInsertAll,
)

requires_remove = pytest.mark.skipif(
    parse_version(__import__("deltalake").__version__) < parse_version("1.6.4"),
    reason="deltalake>=1.6.4 required for RemoveAction commits",
)


def _seed(table_path: Path, rows: dict) -> None:
    write_deltalake(str(table_path), pa.table(rows), mode="overwrite")


@requires_remove
def test_v2_merge_predicate_and_partial_update(tmp_path: Path) -> None:
    """Partial SET keeps other target columns; merge_predicate drives the join."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1, 2], "status": ["a", "b"], "note": ["keep", "keep"]})

    source = daft.from_pydict({"id": [2], "status": ["B"], "note": ["ignored"]})
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_predicate="target.id = source.id",
        when_matched_update={"status": "source.status"},
        when_not_matched="ignore",
    ).to_pydict()

    assert summary["num_updated"] == [1]
    assert summary["num_inserted"] == [0]
    by_id = {r["id"]: r for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert by_id[2]["status"] == "B"
    assert by_id[2]["note"] == "keep"  # not overwritten
    assert by_id[1]["status"] == "a"


@requires_remove
def test_v2_matched_delete_condition(tmp_path: Path) -> None:
    """CDC: update when status changes; delete when delete_flag is true."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1, 2, 3], "status": ["a", "b", "c"]})

    source = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "status": ["A", "b", "c"],
            "delete_flag": [False, True, False],
        }
    )
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_keys=["id"],
        when_matched_update={"status": "source.status"},
        when_matched_update_condition="target.status != source.status",
        when_matched_delete_condition="source.delete_flag = true",
        when_not_matched="ignore",
    ).to_pydict()

    assert summary["num_updated"] == [1]  # id=1
    assert summary["num_deleted"] == [1]  # id=2
    rows = DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()
    by_id = {r["id"]: r["status"] for r in rows}
    assert by_id == {1: "A", 3: "c"}


@requires_remove
def test_v2_insert_condition(tmp_path: Path) -> None:
    """Insert only when condition holds."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1], "status": ["a"]})

    source = daft.from_pydict({"id": [2, 3], "status": ["b", "c"], "active": [True, False]})
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_keys=["id"],
        when_matched="update",
        when_not_matched_insert="all",
        when_not_matched_insert_condition="source.active = true",
    ).to_pydict()

    assert summary["num_inserted"] == [1]
    ids = {r["id"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert ids == {1, 2}


@requires_remove
def test_v3_merge_clauses_and_nmbs_delete(tmp_path: Path) -> None:
    """Ordered clauses + NOT MATCHED BY SOURCE DELETE."""
    table_path = tmp_path / "tgt"
    _seed(
        table_path,
        {"id": [1, 2, 3], "status": ["a", "b", "c"], "expired": [False, False, True]},
    )

    source = daft.from_pydict({"id": [1], "status": ["A"], "expired": [False]})
    summary = source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_predicate="target.id = source.id",
        merge_clauses=[
            MatchedUpdate(set={"status": "source.status"}),
            NotMatchedInsertAll(),
            NotMatchedBySourceDelete(condition="target.expired = true"),
        ],
    ).to_pydict()

    assert summary["num_updated"] == [1]
    assert summary["num_deleted"] == [1]
    by_id = {r["id"]: r["status"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert by_id == {1: "A", 2: "b"}  # id=3 expired deleted; id=2 copied


@requires_remove
def test_v3_fluent_builder(tmp_path: Path) -> None:
    """Fluent merge_into_deltalake builder executes the same engine."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1, 2], "status": ["a", "b"]})

    source = daft.from_pydict({"id": [2, 9], "status": ["B", "NEW"], "delete_flag": [False, False]})
    summary = (
        source.merge_into_deltalake(str(table_path))
        .on("target.id = source.id")
        .when_matched_update(set={"status": "source.status"})
        .when_matched_delete(condition="source.delete_flag = true")
        .when_not_matched_insert_all()
        .execute()
        .to_pydict()
    )

    assert summary["committed"] == [True]
    assert summary["num_updated"] == [1]
    assert summary["num_inserted"] == [1]
    by_id = {r["id"]: r["status"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert by_id[2] == "B"
    assert by_id[9] == "NEW"
    assert by_id[1] == "a"


@requires_remove
def test_v3_matched_delete_only_via_builder(tmp_path: Path) -> None:
    """Delete-only matched clause removes rows."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1, 2, 3], "status": ["a", "b", "c"]})

    source = daft.from_pydict({"id": [2], "status": ["b"]})
    summary = source.merge_into_deltalake(str(table_path)).on_keys(["id"]).when_matched_delete().execute().to_pydict()
    assert summary["num_deleted"] == [1]
    ids = {r["id"] for r in DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()}
    assert ids == {1, 3}


@requires_remove
def test_v3_schema_evolution_adds_column(tmp_path: Path) -> None:
    """merge_schema=True adds source-only columns to the Delta schema."""
    table_path = tmp_path / "tgt"
    _seed(table_path, {"id": [1], "val": ["a"]})

    source = daft.from_pydict({"id": [1], "val": ["A"], "extra": [99]})
    source.write_deltalake(
        str(table_path),
        mode="merge",
        merge_keys=["id"],
        merge_schema=True,
        when_not_matched="ignore",
    )
    dt = DeltaTable(str(table_path))
    names = [f.name for f in dt.schema().fields]
    assert "extra" in names
    row = dt.to_pyarrow_table().to_pylist()[0]
    assert row["val"] == "A"
    assert row["extra"] == 99
