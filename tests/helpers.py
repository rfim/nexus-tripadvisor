"""One pipeline run per test session, shared by every test file (also in the browser, where there are no fixtures)."""
from functools import lru_cache

from nexus.contracts import BadRecord, upcast  # noqa: F401  (re-exported for tests)
from nexus.generate import parse
from nexus.stream import Record


@lru_cache(maxsize=1)
def site() -> dict:
    from nexus.pipeline import run
    return run()


def recs(*rows) -> list[Record]:
    """Tiny topics by hand: (arrival, partition, offset, value-json-or-raw)."""
    import json
    out = []
    for ts, p, off, v in rows:
        out.append(Record(parse(ts), p, off, f"s{p}", v if isinstance(v, str) else json.dumps(v)))
    return sorted(out, key=lambda r: (r.ts, r.partition, r.offset))


def pv(eid: str, event_time: str, loc: str = "d1") -> dict:
    return {"event_id": eid, "event_time": event_time, "session_id": "s", "user_hash": "u", "location_id": loc,
            "page_type": "hotel", "platform": "web", "app_version": "web"}
