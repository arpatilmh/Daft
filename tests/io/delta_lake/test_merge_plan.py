"""Unit tests for merge plan resolution (v1/v2/v3)."""

from __future__ import annotations

import pytest

from daft.io.delta_lake.merge_plan import (
    MatchedDelete,
    MatchedUpdate,
    NotMatchedBySourceDelete,
    NotMatchedInsertAll,
    parse_merge_predicate_keys,
    resolve_merge_plan,
    rewrite_merge_sql,
)


def test_parse_merge_predicate_keys_and() -> None:
    keys = parse_merge_predicate_keys("target.id = source.id AND target.sk = source.sk")
    assert keys == ["id", "sk"]


def test_parse_merge_predicate_rejects_renames() -> None:
    with pytest.raises(ValueError, match="same column names"):
        parse_merge_predicate_keys("target.id = source.user_id")


def test_rewrite_merge_sql() -> None:
    assert rewrite_merge_sql("source.delete_flag = true AND target.status != source.status") == (
        "source_delete_flag = true AND target_status != source_status"
    )


def test_resolve_v1_defaults() -> None:
    plan = resolve_merge_plan(merge_keys=["id"])
    assert plan.merge_keys == ("id",)
    assert [c.kind for c in plan.clauses] == ["matched_update", "not_matched_insert"]


def test_resolve_v2_partial_update_and_delete() -> None:
    plan = resolve_merge_plan(
        merge_predicate="target.id = source.id",
        when_matched_update={"status": "source.status"},
        when_matched_update_condition="target.status != source.status",
        when_matched_delete_condition="source.delete_flag = true",
        when_not_matched_insert="all",
    )
    assert plan.merge_keys == ("id",)
    assert [c.kind for c in plan.clauses] == [
        "matched_update",
        "matched_delete",
        "not_matched_insert",
    ]
    assert plan.clauses[0].updates == {"status": "source.status"}
    assert plan.clauses[1].condition == "source.delete_flag = true"


def test_resolve_delete_only_allows_multiple_matches() -> None:
    plan = resolve_merge_plan(merge_keys=["id"], when_matched="delete", when_not_matched="ignore")
    assert [c.kind for c in plan.clauses] == ["matched_delete"]
    assert plan.allow_multiple_matches is True


def test_resolve_explicit_clauses_v3() -> None:
    plan = resolve_merge_plan(
        merge_keys=["id"],
        merge_clauses=[
            MatchedUpdate(set={"status": "source.status"}),
            MatchedDelete(condition="source.delete_flag = true"),
            NotMatchedInsertAll(),
            NotMatchedBySourceDelete(condition="target.expired = true"),
        ],
    )
    assert len(plan.clauses) == 4
    assert plan.clauses[-1].kind == "not_matched_by_source_delete"
