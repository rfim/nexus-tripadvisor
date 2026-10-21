"""The stream job's rules, on records written by hand, then on the week."""
import pytest

from helpers import pv, recs, site
from nexus.generate import parse
from nexus.stream import Job, Poison


def run(*rows, **kw):
    return Job("page_views", **{k: v for k, v in kw.items() if k == "dlq"}).run(recs(*rows), **{k: v for k, v in kw.items() if k != "dlq"})


def test_a_window_fires_once_the_watermark_passes_its_end():
    j = run(("2026-10-05T10:01:00Z", 0, 0, pv("a", "2026-10-05T10:00:30.000Z")),
            ("2026-10-05T11:12:00Z", 0, 1, pv("b", "2026-10-05T11:11:00.000Z")))
    row = j.rows()[0]
    assert (row["window_start"], row["n"], row["emits"]) == ("2026-10-05T10:00:00.000Z", 1, 1)
    # fired at the first record whose event time took the watermark past 11:00 (+10 min out of orderness)
    assert row["first_emit"] == "2026-10-05T11:12:00.000Z"


def test_the_same_event_id_twice_counts_once():
    j = run(("2026-10-05T10:00:01Z", 0, 0, pv("a", "2026-10-05T10:00:00.000Z")),
            ("2026-10-05T10:00:09Z", 0, 1, pv("a", "2026-10-05T10:00:00.000Z")))
    assert j.s.ledger["duplicates"] == 1 and j.rows()[0]["n"] == 1


def test_late_within_allowed_lateness_updates_and_refires():
    j = run(("2026-10-05T10:00:01Z", 0, 0, pv("a", "2026-10-05T10:00:00.000Z")),
            ("2026-10-05T11:20:00Z", 0, 1, pv("b", "2026-10-05T11:20:00.000Z")),
            ("2026-10-05T11:40:00Z", 0, 2, pv("c", "2026-10-05T10:59:00.000Z")))
    row = j.rows()[0]
    assert (row["n"], row["emits"], j.s.ledger["late_updates"]) == (2, 2, 1)


def test_past_allowed_lateness_goes_to_the_side_output_not_the_window():
    j = run(("2026-10-05T10:00:01Z", 0, 0, pv("a", "2026-10-05T10:00:00.000Z")),
            ("2026-10-05T12:30:00Z", 0, 1, pv("b", "2026-10-05T12:30:00.000Z")),
            ("2026-10-05T12:31:00Z", 0, 2, pv("c", "2026-10-05T10:30:00.000Z")))
    assert j.rows()[0]["n"] == 1 and list(j.late) == ["c"] and j.s.ledger["late_dropped"] == 1


def test_an_idle_partition_does_not_hold_the_watermark_back():
    # partition 1 says something at 10:00 and then nothing; partition 0 keeps going. Without idleness the watermark
    # would stay at 09:50 (partition 1's 10:00 less 10 minutes) and the 10:00 window would wait.
    minute = lambda m: f"2026-10-05T{10 + m // 60}:{m % 60:02d}:00"  # noqa: E731
    j = run(("2026-10-05T10:00:01Z", 1, 0, pv("x", "2026-10-05T10:00:00.000Z")),
            *[(minute(m) + "Z", 0, m, pv(f"p{m}", minute(m) + ".000Z")) for m in range(1, 80)])
    assert j.rows()[0]["first_emit"] == "2026-10-05T11:10:00.000Z"


def test_a_poison_pill_without_a_dead_letter_topic_stops_the_job():
    j = Job("page_views", dlq=False)
    with pytest.raises(Poison):
        j._loop(recs(("2026-10-05T10:00:00Z", 0, 0, '{"event_id":')), parse("2026-10-05T10:00:00Z"),
                parse("2026-10-05T10:05:00Z"), -1, None)


def test_with_a_dead_letter_topic_the_job_keeps_going():
    j = run(("2026-10-05T10:00:00Z", 0, 0, '{"event_id":'),
            ("2026-10-05T10:00:01Z", 0, 1, pv("a", "2026-10-05T10:00:00.000Z")))
    assert j.dlq == {(0, 0): "not JSON: Expecting value (char 12)"} and j.s.ledger["accepted"] == 1


def test_a_lost_taskmanager_changes_nothing_downstream():
    s = site()
    assert s["replay"]["identical"] and s["replay"]["re_emitted"] > 0


def test_the_crash_loop_lasts_until_the_fix():
    inc = site()["incident"]
    assert inc["restarts"] == inc["minutes"]["to_mitigate"]
    assert site()["streams"]["partner_clicks"]["dead_letters"][0]["partition"] == 3


def test_a_stale_version_never_resurrects_an_erased_review():
    s = site()
    assert s["streams"]["reviews"]["ledger"]["stale_versions"] == 1
    assert [d["decision"] for d in s["trace"]["reviews"]][-2:] == ["applied", "stale: version 4 kept"]


def test_the_ledger_balances():
    for name, st in site()["streams"].items():
        led = st["ledger"]
        assert led["read"] == led["accepted"] + led["duplicates"] + led["late_dropped"] + led["dead_letters"] + led["stale_versions"], name
