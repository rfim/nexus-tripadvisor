"""The nightly batch: recompute every window from the whole topic archive, with nothing ever late.

The stream answers within minutes; the batch answers once the day is complete. Where they differ, the difference
must be exactly the records the stream sent to its side output or its dead-letter topic. reconcile() proves that,
window by window. spark_batch.py is the same computation in PySpark; tests/test_spark_batch.py checks they agree.
"""
from collections import defaultdict

from nexus.contracts import BadRecord, topic, upcast
from nexus.generate import iso, parse
from nexus.stream import MIN, Record


def recompute(name: str, records: list[Record], exclude_users: set = frozenset()) -> dict:
    """Window → {"n", "sum"} from every record, de-duplicated on id (first arrival wins), bad records counted.
    exclude_users drops the users an investigation found to be bots (Gold only; the stream comparison keeps them)."""
    t = topic(name)
    prod = t["product"]
    size = prod["window_minutes"] * MIN
    sum_field = next((m for m, how in prod["measures"].items() if how == "sum"), None)
    seen, out, bad, dups, dropped = set(), defaultdict(lambda: {"n": 0, "sum": 0.0}), 0, 0, 0
    for rec in sorted(records, key=lambda r: (r.ts, r.partition, r.offset)):
        try:
            v = upcast(name, rec.raw)
        except BadRecord:
            bad += 1
            continue
        if v[t["id"]] in seen:
            dups += 1
            continue
        seen.add(v[t["id"]])
        if v.get("user_hash") in exclude_users:
            dropped += 1
            continue
        et = parse(v["event_time"])
        w = out[(et - et % size, tuple(v[g] for g in prod["group_by"]))]
        w["n"] += 1
        w["sum"] += v.get(sum_field, 0) if sum_field else 0
    return {"windows": {k: {"n": w["n"], "sum": round(w["sum"], 4)} for k, w in out.items()},
            "bad_records": bad, "duplicates": dups, "events": len(seen) - dropped, "excluded": dropped}


def reconcile(job, batch: dict) -> dict:
    """Stream sink against the batch, window by window. Every difference must be explained by the side output."""
    late = defaultdict(lambda: {"n": 0, "sum": 0.0})
    for e in job.late.values():
        k = (parse(e["window_start"]), tuple(e["group"]))
        late[k]["n"] += 1
        late[k]["sum"] += e["value"]
    keys = set(batch["windows"]) | set(job.sink)
    equal = explained = 0
    unexplained = []
    for k in sorted(keys):
        b = batch["windows"].get(k, {"n": 0, "sum": 0.0})
        s = job.sink[k]["value"] if k in job.sink else {"n": 0, "sum": 0.0}
        if b["n"] == s["n"] and abs(b["sum"] - s["sum"]) < 1e-6:
            equal += 1
        elif b["n"] - s["n"] == late[k]["n"] and abs(b["sum"] - s["sum"] - late[k]["sum"]) < 1e-6:
            explained += 1
        else:
            unexplained.append({"window_start": iso(k[0]), "group": list(k[1]), "batch": b, "stream": s})
    return {"windows": len(keys), "equal": equal, "explained_by_side_output": explained,
            "unexplained": len(unexplained), "unexplained_rows": unexplained[:20],
            "late_events": len(job.late), "dead_letters": len(job.dlq),
            "stream_events": sum(v["value"]["n"] for v in job.sink.values()), "batch_events": batch["events"]}


def hourly_volume(windows: dict) -> dict:
    """Page views per hour, from the batch's location windows."""
    out = defaultdict(int)
    for (start, _), w in windows.items():
        out[start - start % (60 * MIN)] += w["n"]
    return dict(sorted(out.items()))


def volume_holds(hours: dict, sigmas: float = 2, min_history: int = 3, min_ratio: float = 2.0) -> list[dict]:
    """hold(t) ⟺ V(t) − mean > k·σ and V(t) ≥ min_ratio·mean, over the same hour of the day on earlier days.
    With a week of history σ is small and Saturdays look like anomalies; at Lifepal the window was 30 days.
    The ratio keeps a busy weekend from paging anyone while a scraper still stands out by an order of magnitude."""
    holds, by_hod = [], defaultdict(list)
    for t, v in sorted(hours.items()):
        hist = by_hod[(t // (60 * MIN)) % 24]
        if len(hist) >= min_history:
            mean = sum(hist) / len(hist)
            sd = (sum((x - mean) ** 2 for x in hist) / (len(hist) - 1)) ** 0.5
            if v - mean > sigmas * max(sd, 1.0) and v >= min_ratio * mean:
                holds.append({"hour": iso(t), "views": v, "mean": round(mean, 1), "sd": round(sd, 1),
                              "threshold": round(mean + sigmas * max(sd, 1.0), 1), "z": round((v - mean) / max(sd, 1.0), 1)})
        hist.append(v)
    return holds


def bot_users(records: list[Record], hour_iso: str, max_per_hour: int) -> dict:
    """Users with more views than a person makes in the held hour: the investigation behind a volume hold."""
    h0 = parse(hour_iso)
    per_user, seen = defaultdict(int), set()
    for rec in records:
        try:
            v = upcast("page_views", rec.raw)
        except BadRecord:
            continue
        et = parse(v["event_time"])
        if h0 <= et < h0 + 60 * MIN and v["event_id"] not in seen:
            seen.add(v["event_id"])
            per_user[v["user_hash"]] += 1
    top = sorted(per_user.items(), key=lambda kv: -kv[1])
    return {"users_in_hour": len(per_user), "top": [{"user_hash": u, "views": n} for u, n in top[:6]],
            "bots": [u for u, n in top if n > max_per_hour], "max_per_hour": max_per_hour}
