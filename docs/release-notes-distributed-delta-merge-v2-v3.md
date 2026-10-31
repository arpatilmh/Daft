# Release notes — Delta MERGE v2 + v3 (Spark clause parity)

## Summary

`write_deltalake(mode="merge")` now supports Spark-style clause shapes on the
distributed Daft engine: predicate ON, partial updates, matched deletes,
insert maps/conditions, ordered multi-clauses, not-matched-by-source, fluent
builder, and opt-in schema evolution.

## v2

- `merge_predicate` (`target.col = source.col` equalities)
- `when_matched_update` / `_condition`
- `when_matched_delete_condition`
- `when_not_matched_insert` / `_condition`
- Metrics include `num_deleted`

## v3

- `merge_clauses=[MatchedUpdate(...), MatchedDelete(...), ...]`
- `DataFrame.merge_into_deltalake(...).on(...).when_*().execute()`
- `NotMatchedBySourceUpdate` / `NotMatchedBySourceDelete` (scans all target files)
- `merge_schema=True` / `schema_mode="merge"` via `alter.add_columns` before commit

## Modules

- `daft/io/delta_lake/merge_plan.py`
- `daft/io/delta_lake/merge_builder.py`
- Clause-aware rewrite in `merge_rewrite_daft.py`

## Tests

`tests/io/delta_lake/test_merge_plan.py`, `test_merge_v2_v3_clauses.py`

## Next

EMR / Ray bake-off vs GOMMT `dt.merge` with `deltalake>=1.6.6`.
