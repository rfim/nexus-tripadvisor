"""Snowflake and BigQuery SQL from one contract: committed, masked, and the merge semantics checked."""
from helpers import site
from nexus import warehouse
from nexus.paths import ROOT


def test_the_committed_sql_is_what_the_contract_renders():
    for path, sql in warehouse.render().items():
        assert (ROOT / "warehouse" / path).read_text() == sql, path


def test_every_personal_field_is_masked_in_both_warehouses():
    checks = [c for c in site()["warehouse"]["checks"] if c["check"].startswith("pii_masked")]
    assert len(checks) == 3 and all(c["status"] == "pass" for c in checks)


def test_reviews_merge_never_lets_an_older_version_win():
    for d in ("snowflake", "bigquery"):
        sql = (ROOT / "warehouse" / d / "reviews_current.sql").read_text()
        assert "s.erased AND s.version > t.version THEN DELETE" in sql
        assert "NOT s.erased AND s.version > t.version THEN UPDATE" in sql


def test_quality_and_erasure_pass_in_the_loaded_warehouse():
    assert all(c["status"] == "pass" for c in site()["warehouse"]["checks"])
    assert site()["warehouse"]["erased_rows_removed"] >= 4
