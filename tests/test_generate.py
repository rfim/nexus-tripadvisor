"""The simulated topics: deterministic, and every planted problem really is in the log."""
import hashlib
import json

from helpers import site
from nexus.contracts import BadRecord, upcast
from nexus.generate import generate, parse
from nexus.paths import LANDING
from nexus.stream import read_topic


def _digest():
    return hashlib.sha256(b"".join(p.read_bytes() for p in sorted((LANDING / "kafka").rglob("*.jsonl")))).hexdigest()


def test_deterministic():
    site()
    first = _digest()
    generate()
    assert _digest() == first


def test_offsets_follow_arrival_within_a_partition():
    site()
    for name in ("page_views", "partner_clicks", "reviews"):
        by_part = {}
        for r in read_topic(name):
            by_part.setdefault(r.partition, []).append(r)
        for rows in by_part.values():
            assert [r.offset for r in sorted(rows, key=lambda r: (r.ts, r.offset))] == list(range(len(rows)))


def test_retries_resend_the_same_event_id():
    site()
    ids = [json.loads(r.raw)["event_id"] for r in read_topic("page_views")]
    dups = len(ids) - len(set(ids))
    assert dups == json.loads((LANDING / "truth.json").read_text())["retried"] > 50


def test_the_offline_upload_arrives_hours_after_it_happened():
    site()
    late = [r for r in read_topic("page_views") if json.loads(r.raw)["session_id"].startswith("s9")]
    assert len(late) == site()["generator"]["late_offline"]
    assert min(r.ts - parse(json.loads(r.raw)["event_time"]) for r in late) > 2 * 3600_000


def test_schema_v2_only_from_the_74_app():
    site()
    v2 = [json.loads(r.raw) for r in read_topic("partner_clicks") if '"price"' in r.raw]
    assert v2 and min(parse(v["event_time"]) for v in v2) >= parse("2026-10-07T09:00:00Z")
    assert {v["price"]["currency"] for v in v2} == {"GBP", "EUR", "USD"}


def test_exactly_one_poison_pill_where_planted():
    site()
    bad = []
    for r in read_topic("partner_clicks"):
        try:
            upcast("partner_clicks", r.raw)
        except BadRecord:
            bad.append(r)
    assert [(r.partition) for r in bad] == [3]


def test_the_erasure_is_followed_by_a_stale_version():
    site()
    rid = json.loads((LANDING / "truth.json").read_text())["reviews"]["erased"][0]
    seq = [json.loads(r.raw) for r in read_topic("reviews") if r.key == rid]
    assert [(v["version"], v["op"]) for v in seq][-3:] == [(3, "edited"), (4, "erased"), (3, "edited")]
