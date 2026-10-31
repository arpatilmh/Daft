"""Delta Lake IO helpers for Daft.

MERGE phases (Spark-style):

* Plan: ``merge_plan`` (v1–v3 clauses) + ``merge_builder`` (fluent v3)
* Phase-1 discovery: ``merge_touched_files`` / ``merge_touched_daft``
* Phase-2 rewrite: ``merge_rewrite`` / ``merge_rewrite_daft``
* Phase-3 commit: ``merge_commit`` (delta-rs Add+Remove)

``write_deltalake(mode="merge")`` and ``DataFrame.merge_into_deltalake`` use the
Daft paths via ``merge_into_dataframe``.
"""
