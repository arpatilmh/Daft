# Spec: `rewrite_touched_files_daft` / `plan_merge_rewrite_daft`

## Purpose

Spark-style phase-2 MERGE rewrite on the Daft runner: join source with only the
touched target files, write new UUID Parquet files, return `AddAction`s for
phase-3 without gathering the full source/target to the driver.

## Signatures

```python
rewrite_touched_files_daft(
    source: DataFrame,
    table: DeltaTable | str,
    merge_keys: Sequence[str],
    touched_relative_paths: Sequence[str],
    *,
    storage_options: dict[str, str] | None = None,
    io_config: IOConfig | None = None,
    when_matched: Literal["update"] = "update",
    when_not_matched: Literal["insert", "ignore"] = "insert",
) -> RewriteResult

plan_merge_rewrite_daft(...) -> tuple[TouchedFilesResult, RewriteResult]
```

## Behavior

1. Read touched Parquet via `daft.read_parquet` (or empty frame if no paths).
2. Build branches:
   - **update** = source ⋈ᵢₙₙₑᵣ target_keys → source columns
   - **copy** = target ⋈ₐₙₜᵢ source_keys
   - **insert** = source ⋈ₐₙₜᵢ target_keys (if `when_not_matched="insert"`)
3. Concat non-empty branches; `write_parquet(table_uri, write_mode="append")`.
4. Convert written absolute paths → relative `AddAction`s (size + Parquet stats).
5. `remove_relative_paths` = input touched paths.

## Contract

Same `RewriteResult` metrics as PyArrow `rewrite_touched_files` for identical
logical inputs. Files are written under the table root before commit (orphans
OK on failure; vacuum / OCC retry cleans up).

## Non-goals

Does not commit. Does not support partial SET, matched delete, or predicates
beyond equality keys (v2+).
