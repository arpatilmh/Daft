# Release notes — distributed Delta MERGE phase-1 (Daft)

## Summary

Phase-1 touched-file discovery for `write_deltalake(mode="merge")` now runs on
the Daft runner (`read_parquet` + join) instead of gathering both sides into
PyArrow first. Only file paths and cardinality aggregates are collected to the
driver.

## Changes

- **New:** `daft.io.delta_lake.merge_touched_daft.find_touched_files_daft`
- **New:** `merge_into_dataframe` — Daft phase-1 + Arrow rewrite + commit (OCC)
- **Wire:** `_write_deltalake_merge` uses `merge_into_dataframe`
- **API:** `plan_merge_rewrite(..., touched=...)` accepts precomputed phase-1
- **Tests:** `tests/io/delta_lake/test_find_touched_files_daft.py`
- **Docs:** `docs/design-distributed-delta-merge.md` updated

## Still deferred

- Distributed phase-2 rewrite (source still `to_arrow()` after phase-1)
- v2/v3 clause parity and EMR bake-off
