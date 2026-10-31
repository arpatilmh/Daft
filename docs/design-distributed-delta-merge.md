# Design: Distributed Delta MERGE in Daft

Status: design spike (updated 2026-11-01).

**North-star goal:** Spark Delta MERGE **semantic and clause parity**, with **row work** on Daft/Ray workers and **transactions** via `deltalake` (delta-rs) — without materializing full source/target tables on the driver, and without using delta-rs `TableMerger.execute()` as the distributed engine.

API landings are staged **v1 → v2 → v3** on `DataFrame.write_deltalake` (and, for v3, a fluent builder). v1 is implemented as a prototype; v2/v3 are planned.

## What we learned

### 1. Daft `write_deltalake` today (append / overwrite)

| Stage | Where it runs | What moves |
|-------|---------------|------------|
| Plan sink | Driver | Logical `DeltaLakeCatalogInfo` |
| Write Parquet | Workers (native threads or Ray) | Row batches → unique `{uuid}-{idx}.parquet` (`daft-writers` UUID naming) |
| Collect `AddAction`s | Driver | **Metadata only** (`path`, `size`, stats) via `write_df.to_pydict()["add_action"]` |
| Commit | Driver (single process) | `DeltaTable.create_write_transaction(...)` or `create_table_with_add_actions` |

Key files:

- `daft/dataframe/dataframe.py` — `write_deltalake`
- `daft/io/delta_lake/delta_lake_write.py` — `AddAction` helpers
- `src/daft-writers/src/utils.rs` — `generate_filename` → UUID paths

S3: modern `deltalake` uses conditional writes for the **log**; data files rely on unique names, not put-if-absent as the primary safety net.

Checkpoint / idempotent append (`checkpoint=`) still assumes **one Python process per logical commit**.

### 2. Production pattern in GOMMT (today)

`DPT-GOMMT-Dags/deployment_script/daft_etl/delta_ops.py` → `DeltaLakeMergeDataSinkBase.write`:

- Each Ray micropartition calls `DeltaTable.merge(...).execute()` (delta-rs / **DataFusion**, single process per call).
- OCC conflicts → retry with jitter.
- This is **not** Spark-style distributed rewrite; it is many local merges + log conflicts.

Fine for modest CDC batches with partition predicates; not the long-term Daft MERGE engine.

### 3. Spark MERGE (`delta-io/delta`)

`spark/.../commands/MergeIntoCommand.scala` + `ClassicMergeExecutor.scala`:

1. **findTouchedFiles** — distributed join; collect **file paths** (accumulator), not full rows; reject multi-match unless delete-only.
2. **writeAllChanges** — re-read touched files + source; outer join; write **new** Parquet with unique names.
3. **Commit** — atomically `Remove` touched + `Add` new via optimistic transaction.

Driver holds: touched `AddFile` list + commit actions. Row data stays distributed.

### 4. delta-rs APIs we reuse for phase 3

- **Do use for commit:** `DeltaTable.create_write_transaction([AddAction, ..., RemoveAction, ...], mode="append", schema=...)`
  Proven in `delta-rs/python/tests/test_writer.py` (`test_create_write_transaction_add_and_remove_compaction`).
- **Do not use as the distributed engine:** `TableMerger.execute()` (in-process DataFusion).

`RemoveAction` requires `mode="append"` when mixed with adds for rewrite-style commits (see `table.py` docstring).

**Version gate (critical):** `RemoveAction` in `create_write_transaction` landed in **deltalake python-v1.6.4+** (delta-rs commit `5836f7cf`). Prefer **`deltalake>=1.6.6`** (1.6.5 was yanked on PyPI). GOMMT EMR pin is still **`deltalake==1.4.2`** (uses `dt.merge()` today). Distributed MERGE in Daft therefore needs a deliberate **deltalake bump** on Daft + EMR before phase-3 can ship.

Spike status (2026-10-31): `tests/io/delta_lake/test_distributed_merge_design_spike.py` — **7 passed** on `deltalake==1.6.6` with multi-thread local runs (`--noconftest` if the local Daft native extension is out of sync).

## Proposed Daft MERGE architecture

```
Source DF (Daft) ─┐
                  ├─ Phase 1: join on predicate → distinct target file paths
Target scan ──────┘         (collect paths on driver / reduce)
                            validate 1:1 match cardinality

Phase 2: scan only touched files ⋈ source
         apply MATCHED / NOT MATCHED / NOT MATCHED BY SOURCE
         write new Parquet on workers (UUID names → AddActions)

Phase 3: driver create_write_transaction(adds + removes)
         OCC retry on CommitFailedError
```

**Decision (locked for this design):** distributed Daft compute + delta-rs commit only.
Do **not** wrap `dt.merge()` inside `write_deltalake(mode="merge")` as the default path.

### Driver vs worker memory

| Object | On driver? | OOM risk |
|--------|------------|----------|
| Source / target row data | No | Low if joins stay distributed |
| Touched file path set | Yes | Medium if millions of tiny files |
| Add/Remove action list | Yes | Same |
| Full source DF `.collect()` | Forbidden | High |

### Concurrency

- Unique data paths (UUID) → two workers never “own” the same object key by design.
- Optional S3 `If-None-Match: *` is belt-and-suspenders, not the protocol.
- Two concurrent MERGEs that remove the same file → `CommitFailedError` → retry (re-run phases or conflict resolution policy TBD).

## API roadmap: v1 → v2 → v3 (Spark parity)

Merge is exposed on
[`DataFrame.write_deltalake`](https://docs.daft.ai/en/stable/api/io/#daft.dataframe.DataFrame.write_deltalake)
via **`mode="merge"`**. There is **no** separate `merge_deltalake` method.

| Concept | Meaning |
|---------|---------|
| `mode="merge"` | Row-level MERGE (find touched → rewrite → Add+Remove) |
| `schema_mode="merge"` | Schema evolution — **unsupported until v3**; unrelated to `mode="merge"` |

**Ultimate acceptance:** a user can express the same MERGE as Spark SQL / `DeltaTable.merge` (including multiple ordered clauses with predicates, partial updates, deletes, and not-matched-by-source) and get equivalent table state after `execute` / write, with distributed Daft compute.

### Spark / delta-rs feature matrix (target)

| Capability | Spark Delta | delta-rs `TableMerger` | Daft v1 | Daft v2 | Daft v3 |
|------------|-------------|------------------------|---------|---------|---------|
| Equality upsert on keys | Yes | Yes (`predicate`) | **Yes** | Yes | Yes |
| Arbitrary `ON` / SQL predicate | Yes | Yes | No (`merge_keys` only) | **Yes** (`merge_predicate` eq) | Yes |
| Matched update all columns | Yes | `when_matched_update_all` | **Yes** | Yes | Yes |
| Matched update subset + exprs | Yes | `when_matched_update(updates=)` | No | **Yes** | Yes |
| Matched update with extra condition | Yes | `predicate=` on clause | No | **Yes** | Yes |
| Matched delete (+ condition) | Yes | `when_matched_delete` | No | **Yes** | Yes |
| Not matched insert all | Yes | `when_not_matched_insert_all` | **Yes** | Yes | Yes |
| Not matched insert map / condition | Yes | `when_not_matched_insert` | No | **Yes** | Yes |
| Multiple ordered matched clauses | Yes | Chainable clauses | No | Partial | **Yes** |
| Not matched by source update/delete | Yes | `when_not_matched_by_source_*` | No | No | **Yes** |
| Schema evolution on merge | Yes | `merge_schema=` | No | No | **Yes** |
| Distributed find/rewrite | Spark | No (DataFusion local) | No | **Yes** | **Yes** |
| Commit via Add+Remove OCC | Yes | Yes (inside merge) | **Yes** | Yes | Yes |

Reference: Spark fluent API and SQL; delta-rs `DeltaTable.merge` + `TableMerger` in `delta-rs/python/deltalake/table.py`.

---

### v1 — Simple upsert (implemented, prototype)

**Goal:** Common CDC path — match on equality keys, replace matched rows, insert new keys.

```python
summary = df.write_deltalake(
    "s3://bucket/table",
    mode="merge",
    merge_keys=["id"],              # required
    when_matched="update",          # full-row replace from source
    when_not_matched="insert",      # or "ignore"
    merge_max_retries=3,
)
# summary: version, committed, num_updated, num_inserted, num_copied,
#          num_files_rewritten, commit_attempts
```

| In scope | Out of scope |
|----------|----------------|
| `merge_keys` equality join | Arbitrary `ON` SQL |
| Full-row matched update | Column `SET` maps / SQL exprs |
| Insert-all or ignore for source-only | Matched delete |
| Single implicit clause each side | Multiple clauses / clause conditions |
| Protocol-correct Add+Remove commit | Not matched by source |
| | Distributed phases 1–2 (uses `to_arrow()` today) |

**Execution today:** in-process PyArrow for phases 1–2; deltalake for phase 3. Correctness prototype, not scale.

---

### v2 — CDC-complete clauses (**implemented**)

**Goal:** Cover most production CDC / SCD patterns without full Spark SQL surface.

```python
df.write_deltalake(
    table,
    mode="merge",
    merge_predicate="target.id = source.id",  # or merge_keys=["id"]
    when_matched_update={"status": "source.status", "updated_at": "source.updated_at"},
    when_matched_update_condition="target.status != source.status",
    when_matched_delete_condition="source.delete_flag = true",
    when_not_matched_insert="all",  # or column map
    when_not_matched_insert_condition=None,
)
```

| v2 capability | Status |
|---------------|--------|
| `merge_predicate` (`target.col = source.col` equalities) | Done |
| Partial column updates | Done |
| Matched delete + condition | Done |
| Insert map + insert condition | Done |
| Matched-update condition | Done |
| Distributed phases 1–2 | Done (prior) |

v1 kwargs remain as sugar. Module: `daft/io/delta_lake/merge_plan.py`.

---

### v3 — Spark parity (**implemented**)

**Goal:** Express Spark `DeltaTable.merge` / `MERGE INTO` clause shapes with distributed Daft execution.

```python
# Option A — structured clauses on write_deltalake
from daft.io.delta_lake.merge_plan import (
    MatchedUpdate, MatchedDelete, NotMatchedInsertAll, NotMatchedBySourceDelete,
)
df.write_deltalake(
    table,
    mode="merge",
    merge_predicate="target.id = source.id",
    merge_clauses=[
        MatchedUpdate(set={"status": "source.status"}, condition="target.status != source.status"),
        MatchedDelete(condition="source.delete_flag = true"),
        NotMatchedInsertAll(),
        NotMatchedBySourceDelete(condition="target.expired = true"),
    ],
    merge_schema=True,  # or schema_mode="merge"
)

# Option B — fluent builder
(
    df.merge_into_deltalake(table)
    .on("target.id = source.id")
    .when_matched_update(set={"status": "source.status"}, condition="...")
    .when_matched_delete(condition="source.delete_flag = true")
    .when_not_matched_insert_all()
    .when_not_matched_by_source_delete(condition="target.expired = true")
    .with_schema_evolution()
    .execute()
)
```

Spark SQL equivalent that v3 must support:

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED AND target.status != source.status THEN
  UPDATE SET status = source.status
WHEN MATCHED AND source.delete_flag = true THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT *
WHEN NOT MATCHED BY SOURCE AND target.created_at < CURRENT_DATE() - 30 THEN
  DELETE
```

| v3 must include | Notes |
|-----------------|--------|
| All clause kinds | Matched update/delete; not-matched insert; not-matched-by-source update/delete |
| Ordered multi-clause evaluation | Same precedence rules as Spark |
| Per-clause predicates | SQL strings (align with delta-rs) |
| `update` / `insert` expr maps | Column → SQL expression |
| `*_all` / `except_cols` | Match delta-rs helpers |
| Schema evolution | Opt-in; maps to Spark / `merge_schema` |
| Distributed find + rewrite | No full-table gather on driver |
| Metrics parity | Align with Spark / delta-rs merge metrics where practical |

**Non-goals even at v3 (unless separately scheduled):** deletion vectors, CDF generation, Databricks-only extensions.

---

### Compatibility rules across versions

1. `mode="merge"` stays the entry on `write_deltalake` for v1/v2 shorthand.
2. v1 call shapes keep working when v2/v3 land (deprecated aliases OK).
3. v3 builder / `merge_clauses` is the Spark-parity path; shorthand never blocks it.
4. Engine remains: **Daft distributed rewrite + deltalake commit** — not `TableMerger.execute()` as default.

## Test plan

### A. Local unit / building-block tests (multi-thread native runner)

Location: `tests/io/delta_lake/test_distributed_merge_design_spike.py`

| Test | Asserts |
|------|---------|
| Concurrent UUID writers | N threads write distinct paths; no overwrite collisions |
| Add+Remove transaction | Rewrite commit via `create_write_transaction` leaves correct logical rows |
| Empty source | No removes; no-op or empty adds |
| Multi-match detection | Duplicate merge keys → error (Spark parity) |
| Commit conflict | Two threads commit overlapping removes → one succeeds, one `CommitFailedError` |

Run:

```bash
cd /Users/mmt12963/Repos/Daft
source .venv/bin/activate  # or make .venv
python -m pip install 'deltalake>=1.6.6'
DAFT_RUNNER=native python -m pytest --noconftest \
  tests/io/delta_lake/test_distributed_merge_design_spike.py -v
```

### B. Local “distributed-ish” with threads / Ray local

```bash
DAFT_RUNNER=native  # multi-threaded on one machine
# or
DAFT_RUNNER=ray ray start --head --num-cpus=4
DAFT_RUNNER=ray pytest tests/io/delta_lake/test_distributed_merge_design_spike.py -v
```

Scale: 10–100 target files, source hitting a subset; assert only those files are removed.

### C. EMR / Ray cluster (integration)

1. Seed target Delta on S3 (many partitions / files).
2. Submit Ray job: Daft source (or MySQL via existing worker) → `write_deltalake(mode="merge")`.
3. Until MERGE lands in Daft: A/B compare
   - **Baseline:** today’s `DeltaLakeMergeDataSink` (`dt.merge` per micropartition)
   - **Candidate:** phase-1/2/3 prototype script packaged in worker zip
4. Metrics: wall time, peak driver RSS, `# files rewritten`, commit retries, row counts vs Spark MERGE on same fixture.
5. Failure injection: kill a worker mid-phase-2 → orphans ok; rerun should OCC-clean or vacuum.

### D. Correctness matrix

**v1:** matched update-all; insert-only; update+insert; multi-match error; empty source; OCC conflict

**v2+:** matched delete; conditional update; partial SET; predicate `ON`

**v3:** multi-clause ordering; not-matched-by-source; schema evolution; Spark SQL fixture parity

Also: partition pruning; schema mismatch fail; `delta.appendOnly=true` rejects data-changing removes

## Implementation order

1. Spike tests — lock commit + UUID assumptions. **Done**
2. `find_touched_files` (phase 1). **Done**
3. Rewrite → `AddAction`s (phase 2). **Done**
4. `create_write_transaction` Add+Remove (phase 3). **Done**
5. `write_deltalake(mode="merge")` v1 API + docs. **Done**
6. Distribute phase 1 on Daft/Ray (`find_touched_files_daft`). **Done**
7. Distribute phase 2 rewrite on Daft (`rewrite_touched_files_daft`). **Done**
8. v2 clause shorthand (`merge_predicate`, partial update, matched delete). **Done**
9. v3 Spark-parity builder / `merge_clauses` + not-matched-by-source + schema evolution. **Done**
10. EMR / Ray bake-off vs GOMMT `dt.merge` + `deltalake>=1.6.6` pin. **Next**

### Phase 1 status: `find_touched_files` (+ Daft path)

Modules:

* `daft/io/delta_lake/merge_touched_files.py` — PyArrow (reference / OCC fallback)
* `daft/io/delta_lake/merge_touched_daft.py` — Daft `read_parquet` + join

```python
from daft.io.delta_lake.merge_touched_daft import find_touched_files_daft
import daft

result = find_touched_files_daft(
    source=daft.from_pydict({"id": [1, 2], "val": ["x", "y"]}),
    table="s3://bucket/table",  # or local path / DeltaTable
    merge_keys=["id"],
)
# result.relative_paths -> tuple[str, ...]  (Delta-log relative paths for RemoveAction)
```

Behavior (Spark parity for classic merge):

- Inner-join source ⋈ target on `merge_keys`
- Assign stable target row ids (`monotonically_increasing_id`); raise `DeltaMergeMultipleMatchesError` if multiple source rows hit one target row (`allow_multiple_matches=True` reserved for delete-only)
- Return distinct **relative** paths only (not full URIs)
- Collect only path / cardinality aggregates — not full merge output

`write_deltalake(mode="merge")` calls `merge_into_dataframe`, which uses Daft phase-1 then Arrow phase-2 rewrite (until distributed rewrite lands).

Tests:

```bash
python -m pytest --noconftest tests/io/delta_lake/test_find_touched_files.py -v
DAFT_RUNNER=native python -m pytest tests/io/delta_lake/test_find_touched_files_daft.py -v
```

### Phase 2 status: `rewrite_touched_files` (+ Daft path)

Modules:

* `daft/io/delta_lake/merge_rewrite.py` — PyArrow reference rewrite
* `daft/io/delta_lake/merge_rewrite_daft.py` — Daft joins + `write_parquet`

```python
from daft.io.delta_lake.merge_rewrite_daft import plan_merge_rewrite_daft
import daft

touched, rewritten = plan_merge_rewrite_daft(
    source=daft.from_pydict({"id": [5, 7], "val": ["E", "G"]}),
    table="/path/to/delta",
    merge_keys=["id"],
)
# rewritten.add_actions — new UUID parquet on disk (from write_parquet)
# rewritten.remove_relative_paths — old files for phase-3 RemoveAction
# rewritten.num_updated / num_inserted / num_copied
```

v1 semantics: matched → update (full source row), unmatched target in touched files → copy, unmatched source → insert (`when_not_matched="ignore"` to skip).

Daft path: inner join (updates) + anti joins (copies / inserts); writes UUID Parquet via `write_parquet` into the table root; driver only builds `AddAction`s from path/size/stats. No source `to_arrow()` gather.

Tests: PyArrow `test_merge_rewrite.py` + Daft `test_merge_rewrite_daft.py`

```bash
python -m pytest --noconftest tests/io/delta_lake/test_merge_rewrite.py -v
DAFT_RUNNER=native python -m pytest tests/io/delta_lake/test_merge_rewrite_daft.py -v
```

### Phase 3 status: `merge_into` commit

Module: `daft/io/delta_lake/merge_commit.py`

```python
from daft.io.delta_lake.merge_commit import merge_into
import pyarrow as pa

result = merge_into(
    source=pa.table({"id": [2, 99], "val": ["B", "NEW"]}),
    table="/path/to/delta",
    merge_keys=["id"],
)
# result.version, result.committed, result.rewritten.num_updated / num_inserted
```

- Builds `RemoveAction`s for touched paths + commits with new `AddAction`s via `create_write_transaction(..., mode="append")`
- OCC: on `CommitFailedError`, **re-runs phase-1+2** then retries (not blind recommit)
- Requires `deltalake>=1.6.6` for removes; insert-only can commit adds alone
- Local `file://` table URIs are normalized to absolute paths before Parquet IO

Tests: **5 passed** — `tests/io/delta_lake/test_merge_commit.py`
Full suite (phase 1–3): **19 passed**

```bash
python -m pytest --noconftest \
  tests/io/delta_lake/test_find_touched_files.py \
  tests/io/delta_lake/test_merge_rewrite.py \
  tests/io/delta_lake/test_merge_commit.py -v
```

### Public API status: `write_deltalake(mode="merge")`

Implemented as `_write_deltalake_merge` → `merge_into_dataframe` (Daft phase-1 **and** phase-2).
Tests: `tests/io/delta_lake/test_merge_deltalake_api.py`

```bash
DAFT_RUNNER=native python -m pytest \
  tests/io/delta_lake/test_merge_deltalake_api.py \
  tests/io/delta_lake/test_find_touched_files_daft.py \
  tests/io/delta_lake/test_merge_rewrite_daft.py -v
```

## Non-goals

| Scope | Deferred |
|-------|----------|
| v1 | Anything beyond simple equality upsert (see v2/v3) |
| v2 | Not-matched-by-source; unlimited multi-clause; schema evolution |
| v3+ / later | Deletion vectors; change data feed; Databricks-only MERGE extensions |
| Ops | Replacing GOMMT `dt.merge` sink before EMR bake-off passes |
