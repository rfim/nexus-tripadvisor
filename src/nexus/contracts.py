"""The topic contracts (contracts/sources.yml): schemas by version, keys, personal data, products and their SLAs.

upcast() is the one reader every job uses, so the stream, the nightly batch and the Java twin agree on what a
record means whatever schema version the producer sent.
"""
import json
from functools import lru_cache

import yaml

from nexus.paths import ROOT

CONTRACTS = ROOT / "contracts"


class BadRecord(ValueError):
    """A record that can't be read: not JSON, or missing what its schema requires. It goes to the dead-letter topic."""


@lru_cache(maxsize=1)
def _load() -> list[dict]:
    out = yaml.safe_load((CONTRACTS / "sources.yml").read_text())
    for t in out:                       # schema versions are ints; YAML readers in the browser give "1", "2"
        if "schemas" in t:
            t["schemas"] = {int(k): v for k, v in t["schemas"].items()}
    return out


def topics() -> list[dict]:
    return [t for t in _load() if "topic" in t]


def fx() -> dict:
    return next(t for t in _load() if "fx_to_gbp" in t)["fx_to_gbp"]


def topic(name: str) -> dict:
    return next(t for t in topics() if t["topic"] == name) | {"_fx": fx()}


def load_quality() -> dict:
    return yaml.safe_load((CONTRACTS / "quality.yml").read_text())


def schema_version(value: dict) -> int:
    return 2 if "price" in value else 1


def upcast(topic_name: str, raw: str) -> dict:
    """Parse one record's value and bring it to the current shape. Raises BadRecord for a poison pill."""
    try:
        v = json.loads(raw)
    except json.JSONDecodeError as e:
        raise BadRecord(f"not JSON: {e.msg} (char {e.pos})") from None
    if not isinstance(v, dict) or "event_time" not in v:
        raise BadRecord("no event_time")
    if topic_name == "partner_clicks":
        if "price" in v:                                   # v2: amount in the partner's currency
            rate = fx().get(v["price"].get("currency"))
            if rate is None:
                raise BadRecord(f"unknown currency {v['price'].get('currency')}")
            v["revenue_gbp"] = round(v["price"]["amount"] * rate, 4)
        elif "cpc_gbp" in v:
            v["revenue_gbp"] = v["cpc_gbp"]
        else:
            raise BadRecord("no price")
    return v


def compatible(old: dict, new: dict) -> list[str]:
    """BACKWARD compatibility, as a schema registry checks it: a consumer on `new` can read data written with `old`
    only if every field `new` requires was already in `old`. Returns the problems; empty means compatible."""
    return [f"field '{f}' is new in v2 and absent from v1 data" for f in new if f not in old] + \
           [f"field '{f}' was removed" for f in old if f not in new]
