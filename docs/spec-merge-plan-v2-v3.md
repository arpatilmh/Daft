# Spec: Merge plan + builder (v2/v3)

## `resolve_merge_plan`

Resolves v1 sugar, v2 shorthand kwargs, or explicit `merge_clauses` into a
`MergePlan` (`merge_keys`, ordered `MergeClause`s, multi-match policy,
`merge_schema`).

- Prefer `merge_predicate` over `merge_keys` when both are set.
- Predicate must be equalities `target.X = source.X` (same name).
- Delete-only matched clauses auto-set `allow_multiple_matches=True`.

## Clause evaluation order (phase-2)

1. Matched update/delete clauses in list order (first hit wins; remainder copied).
2. Not-matched-by-source update/delete on target-only rows in touched files.
3. Not-matched insert clauses on source-only rows.

SQL conditions use `source.` / `target.` qualifiers; rewritten to `source_*` /
`target_*` for `sql_expr`.

## `DeltaMergeBuilder`

Fluent API ending in `.execute()` → `write_deltalake(mode="merge", merge_clauses=...)`.

## Schema evolution

When `merge_schema=True`, rewrite may add source-only columns; phase-3 calls
`DeltaTable.alter.add_columns` for missing fields before Add+Remove commit.
