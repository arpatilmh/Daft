"""Merge clause model + v1/v2/v3 plan resolution for Delta MERGE.

Resolves ``write_deltalake(mode="merge")`` kwargs (and optional ``merge_clauses``)
into an ordered ``MergePlan`` consumed by phase-1/2. SQL strings use
``source.`` / ``target.`` qualifiers (Spark / delta-rs style); they are rewritten
to prefixed column names for Daft ``sql_expr`` evaluation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

ClauseKind = Literal[
    "matched_update",
    "matched_delete",
    "not_matched_insert",
    "not_matched_by_source_update",
    "not_matched_by_source_delete",
]


@dataclass(frozen=True)
class MergeClause:
    """One ordered MERGE clause (Spark WHEN … THEN …).

    Attributes:
        kind: Clause category.
        condition: Optional SQL predicate with ``source.`` / ``target.`` qualifiers.
        updates: Column → SQL expr for update clauses (``None`` = update all from source).
        inserts: Column → SQL expr for insert, or ``"all"`` for insert-all.
    """

    kind: ClauseKind
    condition: str | None = None
    updates: Mapping[str, str] | None = None
    inserts: Mapping[str, str] | Literal["all"] | None = None


@dataclass(frozen=True)
class MergePlan:
    """Resolved merge configuration for phases 1–3.

    Attributes:
        merge_keys: Equality join columns (same name on source and target).
        merge_predicate: Original ON SQL (informational; keys drive the join).
        clauses: Ordered clauses (Spark evaluation order).
        allow_multiple_matches: Multi-match policy for phase-1.
        merge_schema: If True, allow adding source-only columns (v3).
    """

    merge_keys: tuple[str, ...]
    merge_predicate: str | None
    clauses: tuple[MergeClause, ...]
    allow_multiple_matches: bool = False
    merge_schema: bool = False


_EQ_PAIR = re.compile(
    r"target\.(?P<t>\w+)\s*=\s*source\.(?P<s>\w+)"
    r"|source\.(?P<s2>\w+)\s*=\s*target\.(?P<t2>\w+)",
    re.IGNORECASE,
)


def parse_merge_predicate_keys(predicate: str) -> list[str]:
    """Parse equality ``ON`` predicate into same-named merge keys.

    Args:
        predicate: SQL like ``target.id = source.id AND target.sk = source.sk``.

    Returns:
        Ordered unique key column names (requires ``target.x = source.x`` same name).

    Raises:
        ValueError: If no pairs found or sides use different column names.
    """
    if not predicate or not predicate.strip():
        raise ValueError("merge_predicate must be a non-empty SQL string")

    keys: list[str] = []
    seen: set[str] = set()
    for match in _EQ_PAIR.finditer(predicate):
        t = match.group("t") or match.group("t2")
        s = match.group("s") or match.group("s2")
        assert t is not None and s is not None
        # lowercase comparison for key name equality across sides
        if t.lower() != s.lower():
            raise ValueError(
                f"merge_predicate equality requires same column names on both sides; "
                f"got target.{t} = source.{s}. Use merge_keys for renames (not yet supported)."
            )
        key = t
        # lowercase comparison for de-dupe
        if key.lower() not in seen:
            seen.add(key.lower())
            keys.append(key)

    if not keys:
        raise ValueError(
            "merge_predicate must contain at least one equality of the form "
            "`target.col = source.col` (same column name on both sides)"
        )
    return keys


def rewrite_merge_sql(sql: str) -> str:
    """Rewrite ``source.col`` / ``target.col`` to ``source_col`` / ``target_col``."""
    out = re.sub(r"\bsource\.(\w+)\b", r"source_\1", sql, flags=re.IGNORECASE)
    out = re.sub(r"\btarget\.(\w+)\b", r"target_\1", out, flags=re.IGNORECASE)
    return out


def prefix_column(side: Literal["source", "target"], name: str) -> str:
    """Build a prefixed column name for the join frame."""
    return f"{side}_{name}"


def _matched_delete_only(clauses: Sequence[MergeClause]) -> bool:
    """Return True when every matched clause is a delete (Spark multi-match rule)."""
    matched = [c for c in clauses if c.kind.startswith("matched_")]
    return bool(matched) and all(c.kind == "matched_delete" for c in matched)


def resolve_merge_plan(
    *,
    merge_keys: Sequence[str] | None = None,
    merge_predicate: str | None = None,
    when_matched: Literal["update", "delete"] | None = "update",
    when_not_matched: Literal["insert", "ignore"] | None = "insert",
    when_matched_update: Mapping[str, str] | Literal["all"] | None = None,
    when_matched_update_condition: str | None = None,
    when_matched_delete_condition: str | None = None,
    when_not_matched_insert: Mapping[str, str] | Literal["all"] | None = None,
    when_not_matched_insert_condition: str | None = None,
    merge_clauses: Sequence[MergeClause] | None = None,
    allow_multiple_matches: bool | None = None,
    merge_schema: bool = False,
) -> MergePlan:
    """Resolve v1/v2 kwargs or explicit ``merge_clauses`` into a ``MergePlan``.

    Args:
        merge_keys: Equality join columns (v1).
        merge_predicate: SQL ON predicate (v2); preferred when both set.
        when_matched: v1 sugar — ``update`` or ``delete``.
        when_not_matched: v1 sugar — ``insert`` or ``ignore``.
        when_matched_update: v2 partial/all update map (implies matched update clause).
        when_matched_update_condition: Optional condition on the update clause.
        when_matched_delete_condition: If set (or ``when_matched="delete"``), add delete clause.
        when_not_matched_insert: v2 insert map or ``"all"``.
        when_not_matched_insert_condition: Optional insert condition.
        merge_clauses: v3 explicit ordered clauses (overrides shorthand when set).
        allow_multiple_matches: Phase-1 policy; auto-True when matched clauses are delete-only.
        merge_schema: Opt-in schema evolution (v3).

    Returns:
        Resolved ``MergePlan``.
    """
    if merge_predicate:
        keys = parse_merge_predicate_keys(merge_predicate)
    elif merge_keys:
        keys = list(merge_keys)
    else:
        raise ValueError("mode='merge' requires merge_keys or merge_predicate")

    if not keys:
        raise ValueError("merge_keys / merge_predicate must yield a non-empty key list")

    if merge_clauses is not None:
        clauses = tuple(merge_clauses)
    else:
        clauses = _clauses_from_shorthand(
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            when_matched_update=when_matched_update,
            when_matched_update_condition=when_matched_update_condition,
            when_matched_delete_condition=when_matched_delete_condition,
            when_not_matched_insert=when_not_matched_insert,
            when_not_matched_insert_condition=when_not_matched_insert_condition,
        )

    if allow_multiple_matches is None:
        allow_multiple_matches = _matched_delete_only(clauses)

    return MergePlan(
        merge_keys=tuple(keys),
        merge_predicate=merge_predicate,
        clauses=clauses,
        allow_multiple_matches=bool(allow_multiple_matches),
        merge_schema=bool(merge_schema),
    )


def _clauses_from_shorthand(
    *,
    when_matched: Literal["update", "delete"] | None,
    when_not_matched: Literal["insert", "ignore"] | None,
    when_matched_update: Mapping[str, str] | Literal["all"] | None,
    when_matched_update_condition: str | None,
    when_matched_delete_condition: str | None,
    when_not_matched_insert: Mapping[str, str] | Literal["all"] | None,
    when_not_matched_insert_condition: str | None,
) -> tuple[MergeClause, ...]:
    """Build ordered clauses from v1/v2 shorthand kwargs."""
    clauses: list[MergeClause] = []
    # lowercase comparison for matched action sugar
    matched = (when_matched or "update").lower()

    if matched == "delete" and when_matched_update is None:
        clauses.append(MergeClause(kind="matched_delete", condition=when_matched_delete_condition))
    else:
        if when_matched_update is None or (
            isinstance(when_matched_update, str) and when_matched_update.lower() == "all"
        ):
            updates: Mapping[str, str] | None = None  # update-all from source
        elif isinstance(when_matched_update, Mapping):
            updates = dict(when_matched_update)
        else:
            updates = None
        clauses.append(
            MergeClause(
                kind="matched_update",
                condition=when_matched_update_condition,
                updates=updates,
            )
        )
        if when_matched_delete_condition is not None:
            clauses.append(MergeClause(kind="matched_delete", condition=when_matched_delete_condition))

    # lowercase comparison for not-matched sugar
    not_matched = (when_not_matched or "insert").lower()
    if not_matched != "ignore":
        if when_not_matched_insert is None or (
            isinstance(when_not_matched_insert, str) and when_not_matched_insert.lower() == "all"
        ):
            inserts: Mapping[str, str] | Literal["all"] = "all"
        else:
            inserts = dict(when_not_matched_insert)  # type: ignore[arg-type]
        clauses.append(
            MergeClause(
                kind="not_matched_insert",
                condition=when_not_matched_insert_condition,
                inserts=inserts,
            )
        )

    if not clauses:
        raise ValueError("merge produced no clauses; check when_matched / when_not_matched options")
    return tuple(clauses)


# Fluent builder helpers (v3)
def MatchedUpdate(
    set: Mapping[str, str] | None = None,
    condition: str | None = None,
    *,
    updates: Mapping[str, str] | None = None,
) -> MergeClause:
    """Matched UPDATE clause (``set`` or ``updates`` = column → SQL expr; None = all)."""
    mapping = set if set is not None else updates
    return MergeClause(kind="matched_update", condition=condition, updates=mapping)


def MatchedDelete(condition: str | None = None) -> MergeClause:
    """Matched DELETE clause."""
    return MergeClause(kind="matched_delete", condition=condition)


def NotMatchedInsertAll(condition: str | None = None) -> MergeClause:
    """NOT MATCHED INSERT *."""
    return MergeClause(kind="not_matched_insert", condition=condition, inserts="all")


def NotMatchedInsert(
    values: Mapping[str, str],
    condition: str | None = None,
) -> MergeClause:
    """NOT MATCHED INSERT with column map."""
    return MergeClause(kind="not_matched_insert", condition=condition, inserts=dict(values))


def NotMatchedBySourceUpdate(
    set: Mapping[str, str] | None = None,
    condition: str | None = None,
    *,
    updates: Mapping[str, str] | None = None,
) -> MergeClause:
    """NOT MATCHED BY SOURCE UPDATE."""
    mapping = set if set is not None else updates
    return MergeClause(kind="not_matched_by_source_update", condition=condition, updates=mapping)


def NotMatchedBySourceDelete(condition: str | None = None) -> MergeClause:
    """NOT MATCHED BY SOURCE DELETE."""
    return MergeClause(kind="not_matched_by_source_delete", condition=condition)


def merge_plan_to_dict(plan: MergePlan) -> dict[str, Any]:
    """Serialize plan for debugging / tests."""
    return {
        "merge_keys": list(plan.merge_keys),
        "merge_predicate": plan.merge_predicate,
        "allow_multiple_matches": plan.allow_multiple_matches,
        "merge_schema": plan.merge_schema,
        "clauses": [
            {
                "kind": c.kind,
                "condition": c.condition,
                "updates": dict(c.updates) if c.updates is not None else None,
                "inserts": (dict(c.inserts) if isinstance(c.inserts, Mapping) else c.inserts),
            }
            for c in plan.clauses
        ],
    }


__all__ = [
    "ClauseKind",
    "MatchedDelete",
    "MatchedUpdate",
    "MergeClause",
    "MergePlan",
    "NotMatchedBySourceDelete",
    "NotMatchedBySourceUpdate",
    "NotMatchedInsert",
    "NotMatchedInsertAll",
    "merge_plan_to_dict",
    "parse_merge_predicate_keys",
    "prefix_column",
    "resolve_merge_plan",
    "rewrite_merge_sql",
]
