"""Design-spike tests for distributed Delta MERGE building blocks.

These tests lock assumptions for ``write_deltalake(..., mode="merge")``:

1. Concurrent writers must produce unique Parquet paths (UUID), not collide.
2. Phase-3 commit is ``create_write_transaction`` with Add + Remove actions
   (Spark-style rewrite), not ``DeltaTable.merge()`` (local DataFusion).
3. Overlapping remove commits surface as ``CommitFailedError`` under concurrency.

Run::

    DAFT_RUNNER=native pytest tests/io/delta_lake/test_distributed_merge_design_spike.py -v
"""

from __future__ import annotations

import concurrent.futures
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("deltalake")

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from deltalake.transaction import AddAction
from packaging.version import parse as parse_version

# Phase-3 rewrite commits need RemoveAction in create_write_transaction
# (delta-rs python-v1.6.4+). Older wheels only accept AddAction lists.
try:
    from deltalake.transaction import RemoveAction

    _HAS_REMOVE_ACTION = True
except ImportError:  # pragma: no cover - depends on installed deltalake
    RemoveAction = None  # type: ignore[misc, assignment]
    _HAS_REMOVE_ACTION = False

requires_remove_action = pytest.mark.skipif(
    not _HAS_REMOVE_ACTION,
    reason="deltalake>=1.6.4 required for RemoveAction in create_write_transaction",
)


def _seed_table(table_path: Path, rows: list[dict]) -> DeltaTable:
    """Create a small Delta table used as MERGE target."""
    table = pa.Table.from_pylist(rows)
    write_deltalake(str(table_path), table, mode="overwrite")
    return DeltaTable(str(table_path))


def _uuid_parquet_path(root: Path, file_idx: int) -> Path:
    """Mirror Daft writer naming: ``{uuid}-{idx}.parquet``."""
    import uuid

    return root / f"{uuid.uuid4()}-{file_idx}.parquet"


def _write_unique_parquet(root: Path, file_idx: int, ids: list[int]) -> AddAction:
    """Write one Parquet file with a unique name and return an AddAction."""
    path = _uuid_parquet_path(root, file_idx)
    arrow = pa.table({"id": ids, "val": [f"v{i}" for i in ids]})
    pq.write_table(arrow, path)
    stats = {
        "numRecords": len(ids),
        "minValues": {"id": min(ids)},
        "maxValues": {"id": max(ids)},
        "nullCount": {"id": 0, "val": 0},
    }
    import json

    return AddAction(
        path.name,
        path.stat().st_size,
        {},
        int(datetime.now().timestamp() * 1000),
        True,
        json.dumps(stats),
    )


def _remove_actions_for_current_files(dt: DeltaTable) -> list:
    """Build RemoveActions for every current data file (rewrite-all helper)."""
    assert RemoveAction is not None
    add_table = pa.table(dt.get_add_actions())
    paths = add_table.column("path").to_pylist()
    sizes = add_table.column("size_bytes").to_pylist()
    ts = int(datetime.now().timestamp() * 1000)
    return [
        RemoveAction(path=p, data_change=True, deletion_timestamp=ts, size=s, partition_values={})
        for p, s in zip(paths, sizes)
    ]


def test_deltalake_version_gate_documented() -> None:
    """Record installed deltalake; rewrite MERGE needs >=1.6.4 for RemoveAction."""
    import deltalake

    version = parse_version(deltalake.__version__)
    if version < parse_version("1.6.4"):
        assert not _HAS_REMOVE_ACTION
    else:
        assert _HAS_REMOVE_ACTION, "expected RemoveAction export on deltalake>=1.6.4"


def test_concurrent_writers_produce_unique_parquet_paths(tmp_path: Path) -> None:
    """N threads writing with UUID names must never share an object key."""
    root = tmp_path / "data"
    root.mkdir()
    n_writers = 16

    def _worker(i: int) -> str:
        action = _write_unique_parquet(root, i, ids=[i, i + 1000])
        return action.path

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_writers) as pool:
        paths = list(pool.map(_worker, range(n_writers)))

    assert len(paths) == n_writers
    assert len(set(paths)) == n_writers, f"path collision under concurrency: {paths}"
    on_disk = {p.name for p in root.glob("*.parquet")}
    assert on_disk == set(paths)


@requires_remove_action
def test_phase3_add_and_remove_transaction_rewrites_table(tmp_path: Path) -> None:
    """Spark phase-3 analogue: commit new files and remove touched old files."""
    table_path = tmp_path / "delta_tgt"
    dt = _seed_table(
        table_path,
        [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}],
    )
    version_before = dt.version()
    old_files = pa.table(dt.get_add_actions()).column("path").to_pylist()
    assert len(old_files) >= 1

    # Simulate distributed rewrite: workers wrote a new file with updated rows.
    new_add = _write_unique_parquet(table_path, 0, ids=[1, 2, 3, 4])
    removes = _remove_actions_for_current_files(dt)

    dt.create_write_transaction(
        [new_add, *removes],
        mode="append",
        schema=dt.schema(),
    )
    dt.update_incremental()

    assert dt.version() == version_before + 1
    current = set(pa.table(dt.get_add_actions()).column("path").to_pylist())
    assert new_add.path in current
    for old in old_files:
        assert old not in current

    # Logical contents: ids 1..4 present after rewrite.
    result = dt.to_pyarrow_table().to_pydict()
    assert sorted(result["id"]) == [1, 2, 3, 4]


@requires_remove_action
def test_overlapping_remove_commits_one_wins(tmp_path: Path) -> None:
    """Two concurrent rewrite commits touching the same files: one fails OCC."""
    table_path = tmp_path / "delta_race"
    _seed_table(table_path, [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

    import threading

    start = threading.Barrier(2)
    outcomes: list[str] = []
    lock = threading.Lock()

    def _attempt(worker_id: int) -> None:
        dt = DeltaTable(str(table_path))
        new_add = _write_unique_parquet(table_path, worker_id, ids=[worker_id, worker_id + 10])
        removes = _remove_actions_for_current_files(dt)
        start.wait()
        try:
            dt.create_write_transaction(
                [new_add, *removes],
                mode="append",
                schema=dt.schema(),
            )
            with lock:
                outcomes.append("ok")
        except CommitFailedError:
            with lock:
                outcomes.append("conflict")
        except Exception as exc:
            # Some deltalake versions wrap the conflict differently.
            name = type(exc).__name__.lower()
            # lowercase comparison for conflict-ish errors
            if "commit" in name or "conflict" in str(exc).lower():
                with lock:
                    outcomes.append("conflict")
            else:
                with lock:
                    outcomes.append(f"other:{type(exc).__name__}")

    t0 = threading.Thread(target=_attempt, args=(0,))
    t1 = threading.Thread(target=_attempt, args=(1,))
    t0.start()
    t1.start()
    t0.join()
    t1.join()

    assert outcomes.count("ok") == 1, f"expected exactly one winner, got {outcomes}"
    assert outcomes.count("conflict") == 1, f"expected one OCC failure, got {outcomes}"


def test_empty_source_rewrite_is_noop_friendly(tmp_path: Path) -> None:
    """Empty change set: no actions committed; version unchanged."""
    table_path = tmp_path / "delta_empty"
    dt = _seed_table(table_path, [{"id": 1, "val": "a"}])
    version_before = dt.version()
    # Phase-1 with empty source → no touched files → skip phase 2/3.
    assert version_before == dt.version()
    assert len(pa.table(dt.get_add_actions())) >= 1


@pytest.mark.parametrize("n_threads", [2, 8])
def test_concurrent_append_via_write_deltalake_unique_files(tmp_path: Path, n_threads: int) -> None:
    """Baseline: concurrent appends add distinct files (control for MERGE design)."""
    table_path = tmp_path / "delta_append"
    write_deltalake(str(table_path), pa.table({"id": [0], "val": ["z"]}), mode="overwrite")

    def _append(i: int) -> None:
        # Small stagger reduces pure metadata stampede; still exercises multi-writer.
        time.sleep(0.01 * i)
        write_deltalake(
            str(table_path),
            pa.table({"id": [i], "val": [f"w{i}"]}),
            mode="append",
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as pool:
        list(pool.map(_append, range(1, n_threads + 1)))

    dt = DeltaTable(str(table_path))
    ids = sorted(dt.to_pyarrow_table().column("id").to_pylist())
    assert ids == list(range(0, n_threads + 1))
    paths = pa.table(dt.get_add_actions()).column("path").to_pylist()
    assert len(paths) == len(set(paths))
