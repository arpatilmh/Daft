"""Fluent Spark/delta-rs-shaped builder for ``mode='merge'`` (v3).

Example::

    (
        df.merge_into_deltalake(table)
        .on("target.id = source.id")
        .when_matched_update(set={"status": "source.status"}, condition="...")
        .when_matched_delete(condition="source.delete_flag = true")
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(condition="target.expired = true")
        .execute()
    )
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from daft.io.delta_lake.merge_plan import (
    MatchedDelete,
    MatchedUpdate,
    MergeClause,
    MergePlan,
    NotMatchedBySourceDelete,
    NotMatchedBySourceUpdate,
    NotMatchedInsert,
    NotMatchedInsertAll,
    parse_merge_predicate_keys,
)

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


class DeltaMergeBuilder:
    """Accumulate ordered MERGE clauses and execute via the Daft engine."""

    def __init__(
        self,
        source: DataFrame,
        table: Any,
        *,
        io_config: IOConfig | None = None,
        dynamo_table_name: str | None = None,
        allow_unsafe_rename: bool = False,
        merge_max_retries: int = 3,
    ) -> None:
        """Create a builder bound to ``source`` and destination ``table``."""
        self._source = source
        self._table = table
        self._io_config = io_config
        self._dynamo_table_name = dynamo_table_name
        self._allow_unsafe_rename = allow_unsafe_rename
        self._merge_max_retries = merge_max_retries
        self._predicate: str | None = None
        self._keys: list[str] | None = None
        self._clauses: list[MergeClause] = []
        self._allow_multiple_matches: bool | None = None
        self._merge_schema: bool = False

    def on(self, predicate: str) -> DeltaMergeBuilder:
        """Set the SQL ``ON`` predicate (``target.col = source.col`` equalities)."""
        self._predicate = predicate
        self._keys = parse_merge_predicate_keys(predicate)
        return self

    def on_keys(self, keys: Sequence[str]) -> DeltaMergeBuilder:
        """Set equality merge keys (v1 sugar)."""
        self._keys = list(keys)
        self._predicate = None
        return self

    def when_matched_update(
        self,
        set: Mapping[str, str] | None = None,
        condition: str | None = None,
        *,
        updates: Mapping[str, str] | None = None,
    ) -> DeltaMergeBuilder:
        """Append a matched UPDATE clause."""
        self._clauses.append(MatchedUpdate(set=set, condition=condition, updates=updates))
        return self

    def when_matched_update_all(self, condition: str | None = None) -> DeltaMergeBuilder:
        """Append matched UPDATE * (all columns from source)."""
        self._clauses.append(MatchedUpdate(set=None, condition=condition))
        return self

    def when_matched_delete(self, condition: str | None = None) -> DeltaMergeBuilder:
        """Append a matched DELETE clause."""
        self._clauses.append(MatchedDelete(condition=condition))
        return self

    def when_not_matched_insert_all(self, condition: str | None = None) -> DeltaMergeBuilder:
        """Append NOT MATCHED INSERT *."""
        self._clauses.append(NotMatchedInsertAll(condition=condition))
        return self

    def when_not_matched_insert(
        self,
        values: Mapping[str, str],
        condition: str | None = None,
    ) -> DeltaMergeBuilder:
        """Append NOT MATCHED INSERT with a column map."""
        self._clauses.append(NotMatchedInsert(values=values, condition=condition))
        return self

    def when_not_matched_by_source_update(
        self,
        set: Mapping[str, str] | None = None,
        condition: str | None = None,
        *,
        updates: Mapping[str, str] | None = None,
    ) -> DeltaMergeBuilder:
        """Append NOT MATCHED BY SOURCE UPDATE."""
        self._clauses.append(NotMatchedBySourceUpdate(set=set, condition=condition, updates=updates))
        return self

    def when_not_matched_by_source_delete(self, condition: str | None = None) -> DeltaMergeBuilder:
        """Append NOT MATCHED BY SOURCE DELETE."""
        self._clauses.append(NotMatchedBySourceDelete(condition=condition))
        return self

    def with_schema_evolution(self, enabled: bool = True) -> DeltaMergeBuilder:
        """Allow source-only columns to be added to the target schema."""
        self._merge_schema = enabled
        return self

    def allow_multiple_matches(self, enabled: bool = True) -> DeltaMergeBuilder:
        """Override multi-match policy (default: auto for delete-only)."""
        self._allow_multiple_matches = enabled
        return self

    def _to_plan(self) -> MergePlan:
        """Materialize a ``MergePlan`` from builder state."""
        from daft.io.delta_lake.merge_plan import resolve_merge_plan

        if not self._keys and not self._predicate:
            raise ValueError("DeltaMergeBuilder requires .on(...) or .on_keys(...)")
        if not self._clauses:
            raise ValueError("DeltaMergeBuilder requires at least one when_* clause")
        return resolve_merge_plan(
            merge_keys=self._keys,
            merge_predicate=self._predicate,
            merge_clauses=self._clauses,
            allow_multiple_matches=self._allow_multiple_matches,
            merge_schema=self._merge_schema,
        )

    def execute(self) -> DataFrame:
        """Run the merge and return the summary DataFrame."""
        plan = self._to_plan()
        return self._source.write_deltalake(
            self._table,
            mode="merge",
            merge_keys=list(plan.merge_keys),
            merge_predicate=plan.merge_predicate,
            merge_clauses=list(plan.clauses),
            allow_multiple_matches=plan.allow_multiple_matches,
            merge_schema=plan.merge_schema,
            io_config=self._io_config,
            dynamo_table_name=self._dynamo_table_name,
            allow_unsafe_rename=self._allow_unsafe_rename,
            merge_max_retries=self._merge_max_retries,
        )


__all__ = ["DeltaMergeBuilder"]
