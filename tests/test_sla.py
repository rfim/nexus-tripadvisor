"""SLAs measured from published completeness: the incident is the only breach, and the record says why."""
from helpers import site


def test_the_incident_is_the_only_breach_all_week():
    by = {r["product"]: r for r in site()["sla"]}
    assert by["location_views_hourly"]["breaches"] == [] and by["reviews_current"]["breaches"] == []
    (b,) = by["partner_revenue_15m"]["breaches"]
    assert b["from"].startswith("2026-10-09T14:3") and b["to"] == "2026-10-09T15:20:00.000Z"


def test_detection_mitigation_and_recovery_are_in_order():
    i = site()["incident"]
    assert i["started"] < i["detected"] < i["acknowledged"] < i["mitigated"] < i["recovered"]
    assert i["minutes"]["would_detect_with_restart_alert"] < i["minutes"]["to_detect"]


def test_nothing_but_the_bad_record_is_lost():
    i = site()["incident"]
    r = site()["reconcile"]["partner_clicks"]
    assert i["dead_letters"] == 1 and r["unexplained"] == 0 and r["dead_letters"] == 1
