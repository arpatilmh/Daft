# Spec: `find_touched_files_daft`

## Purpose

Spark-style phase-1 MERGE discovery using Daft DataFrames so scan/join can run
on the native or Ray runner without gathering the full target table to the
driver.

## Signature

```python
find_touched_files_daft(
    source: DataFrame,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    io_config: IOConfig | None = None,
    allow_multiple_matches: bool = False,
) -> TouchedFilesResult
```

## Behavior

1. List active Delta data-file relative paths via `list_delta_data_files`.
2. If none: return empty `relative_paths` with `num_source_rows = source.count_rows()`.
3. `daft.read_parquet(full_paths, file_path_column=...)` then
   `monotonically_increasing_id()` as target row id.
4. Inner-join source keys with target keys + path + row id.
5. If any target row id has match count > 1 and `allow_multiple_matches` is
   false: raise `DeltaMergeMultipleMatchesError`.
6. Collect distinct file paths; map full → Delta relative; return
   `TouchedFilesResult`.

## Contract

Same `TouchedFilesResult` as PyArrow `find_touched_files`. Relative paths must
match for identical source/target inputs.

## Non-goals

Does not rewrite files or commit. Does not gather full join output.
