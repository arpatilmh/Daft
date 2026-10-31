# Release notes — distributed Delta MERGE phase-2 (Daft)

## Summary

Phase-2 rewrite for `write_deltalake(mode="merge")` now runs on the Daft runner
(inner + anti joins, `write_parquet` UUID files). The driver no longer gathers
the source with `to_arrow()` for rewrite; it only collects `AddAction` metadata.

## Changes

- **New:** `daft.io.delta_lake.merge_rewrite_daft`
  (`rewrite_touched_files_daft`, `plan_merge_rewrite_daft`, `build_rewrite_dataframe`)
- **Wire:** `merge_into_dataframe` uses `plan_merge_rewrite_daft` end-to-end
- **Tests:** `tests/io/delta_lake/test_merge_rewrite_daft.py`
- **Docs:** design doc, `docs/spec-rewrite-touched-files-daft.md`

## Still deferred

- v2/v3 clause parity
- EMR bake-off vs GOMMT `dt.merge` + `deltalake>=1.6.6` pin
- Single-pass metrics (counts still execute join branches before write)
