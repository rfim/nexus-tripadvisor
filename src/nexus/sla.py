"""SLAs, measured the way a consumer feels them, and the incident record when one is missed.

Freshness of a windowed product is how far behind its published completeness is:  age(t) = t − complete_up_to(t),
where the job publishes "complete up to" (its watermark, floored to a window) with the data. Measuring from the newest
non-empty window instead would page every quiet night, when a fifteen-minute window can simply have no clicks.
Freshness of a state product (reviews_current) is consumer lag: how long the oldest unread record has been waiting.
The monitor samples every five minutes; an SLO turns the samples into an error budget.
"""
import heapq

from nexus.generate import iso
from nexus.stream import MIN

STEP = 5 * MIN


def _samples(start: int, end: int):
    t = start - start % STEP + STEP
    while t <= end:
        yield t
        t += STEP


def age_series(job, start: int, end: int) -> list[tuple[int, float]]:
    emits = sorted(job.progress)
    out, i, visible = [], 0, None
    for t in _samples(start, end):
        while i < len(emits) and emits[i][0] <= t:
            visible = emits[i][1] if visible is None else max(visible, emits[i][1])
            i += 1
        if visible is not None:
            out.append((t, round((t - visible) / MIN, 1)))
    return out


def lag_series(job, start: int, end: int) -> list[tuple[int, float]]:
    """Minutes the oldest record that has arrived but isn't processed yet has been waiting (0 when caught up)."""
    recs = sorted(zip((r.ts for r in job.records), job.done_at))
    out, i, waiting = [], 0, []
    for t in _samples(start, end):
        while i < len(recs) and recs[i][0] <= t:
            heapq.heappush(waiting, recs[i])
            i += 1
        while waiting and waiting[0][1] <= t:
            heapq.heappop(waiting)
        # a record processed later than an older one still waiting stays in the heap: drop finished ones lazily
        live = [w for w in waiting if w[1] > t]
        out.append((t, round((t - min(w[0] for w in live)) / MIN, 1) if live else 0.0))
    return out


def lag_by_partition(job, t: int) -> dict:
    """Records that have arrived by t and are not processed by t, per partition: what `kafka-consumer-groups` shows."""
    out = {}
    for r, done in zip(job.records, job.done_at):
        if r.ts <= t and done > t:
            out[r.partition] = out.get(r.partition, 0) + 1
    return {p: out.get(p, 0) for p in range(max(r.partition for r in job.records) + 1)}


def report(name: str, product: dict, series: list, kind: str) -> dict:
    sla = product["sla"]
    limit = sla["freshness_minutes"]
    bad = [(t, a) for t, a in series if a > limit]
    breaches, cur = [], None
    for t, a in series:
        if a > limit:
            if cur is None:
                cur = {"from": iso(t), "to": iso(t), "max_minutes": a}
            cur["to"], cur["max_minutes"] = iso(t), max(cur["max_minutes"], a)
        elif cur:
            breaches.append(cur)
            cur = None
    if cur:
        breaches.append(cur)
    n = len(series)
    allowed = (1 - sla["slo"]) * n
    return {"product": product["name"], "topic": name, "kind": kind, "freshness_minutes": limit, "tier": sla["tier"],
            "slo": sla["slo"], "samples": n, "late_samples": len(bad), "on_time": round(1 - len(bad) / n, 5),
            "budget_samples": round(allowed, 1), "budget_used": round(len(bad) / allowed, 2) if allowed else None,
            "p50": _pct([a for _, a in series], .5), "p95": _pct([a for _, a in series], .95),
            "max": max(a for _, a in series), "breaches": breaches}


def _pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


ACK_AFTER = 4 * MIN           # the on-call engineer acknowledges the page (scripted: a person, not code)
RESTART_RULE = 3              # proposed: page when the job restarts more than 3 times in 10 minutes


def incident(job, series: list, product: dict) -> dict:
    """The incident record for the poison pill, built from the job's own log and the monitor's samples."""
    p = job.poison
    limit = product["sla"]["freshness_minutes"]
    detect = next(t for t, a in series if t > p["at"] and a > limit)
    recovered = next(t for t, a in series if t > p["fix_at"] and a <= limit)
    ooo = product["out_of_orderness_minutes"] * MIN
    size = product["window_minutes"] * MIN
    delayed = [(k, v) for k, v in job.sink.items() if v["first_emit"] - (k[0] + size + ooo) > 10 * MIN]
    rec = next(r for r in job.records if r.partition == p["partition"] and r.offset == p["offset"])
    worst = max(series, key=lambda s: s[1] if p["at"] <= s[0] <= recovered else -1)
    timeline = [
        {"at": iso(p["at"]), "who": "job", "what": f"Record {p['partition']}/{p['offset']} can't be parsed. The job fails, "
                                                  f"restores from its checkpoint, reads the same record, and fails again."},
        {"at": iso(p["at"] + RESTART_RULE * MIN), "who": "would page", "what": f"Restart count passes {RESTART_RULE} in 10 minutes. "
                                                                              "Not alerted on yet: this is action 2."},
        {"at": iso(detect), "who": "SLA monitor", "what": f"{product['name']} is {dict(series)[detect]:.0f} minutes old, "
                                                         f"over its {limit}-minute SLA. Page sent."},
        {"at": iso(detect + ACK_AFTER), "who": "on call", "what": "Acknowledged. Job RESTARTING; exception points to partition "
                                                                 f"{p['partition']} offset {p['offset']}."},
        {"at": iso(p["fix_at"]), "who": "on call", "what": "Mitigation: deploy with the dead-letter topic on. Restore from the "
                                                          "last checkpoint; the bad record goes to the DLQ."},
        {"at": iso(recovered), "who": "SLA monitor", "what": f"Back within SLA. Every delayed window re-computed from the checkpoint."},
    ]
    return {
        "product": product["name"], "topic": job.name, "sla_minutes": limit, "severity": "SEV-2 (tier-1 product late, no data lost)",
        "started": iso(p["at"]), "detected": iso(detect), "acknowledged": iso(detect + ACK_AFTER),
        "mitigated": iso(p["fix_at"]), "recovered": iso(recovered),
        "minutes": {"to_detect": (detect - p["at"]) // MIN, "to_ack": ACK_AFTER // MIN,
                    "to_mitigate": (p["fix_at"] - p["at"]) // MIN, "total": (recovered - p["at"]) // MIN,
                    "would_detect_with_restart_alert": RESTART_RULE},
        "restarts": p["restarts"], "partition": p["partition"], "offset": p["offset"], "reason": p["reason"],
        "raw": rec.raw, "raw_bytes": len(rec.raw.encode()),
        "lag_at_detection": lag_by_partition(job, detect),
        "worst_age_minutes": worst[1],
        "windows_delayed": len(delayed), "revenue_in_delayed_windows_gbp": round(sum(v["value"]["sum"] for _, v in delayed), 2),
        "clicks_in_delayed_windows": sum(v["value"]["n"] for _, v in delayed),
        "dead_letters": len(job.dlq), "data_lost": "1 click: the record itself, truncated by the producer; held in the DLQ",
        "timeline": timeline,
        "series": [(iso(t), a) for t, a in series if p["at"] - 60 * MIN <= t <= recovered + 45 * MIN],
        "actions": [
            {"what": "Dead-letter bad records by default in the shared job template (done: stream.Job(dlq=True)).", "status": "done"},
            {"what": f"Page on restarts > {RESTART_RULE} in 10 minutes: detection in {RESTART_RULE} minutes instead of "
                     f"{(detect - p['at']) // MIN}.", "status": "proposed"},
            {"what": "Alert on DLQ depth > 0 for tier-1 topics, with the record and its producer.", "status": "proposed"},
            {"what": "Producer contract test in the partner SDK's CI: every record must parse under the registered schema.",
             "status": "proposed"},
        ],
    }


def postmortem(inc: dict) -> str:
    m = inc["minutes"]
    lines = [f"# Incident: {inc['product']} late for {m['total']} minutes", "",
             f"**Severity:** {inc['severity']}  ", f"**Started:** {inc['started']} · **Detected:** {inc['detected']} · "
             f"**Mitigated:** {inc['mitigated']} · **Recovered:** {inc['recovered']}", "",
             "## What happened", "",
             f"One record on `{inc['topic']}` partition {inc['partition']}, offset {inc['offset']}, was {inc['raw_bytes']} bytes of "
             f"truncated JSON. The stream job failed on it, restored from its checkpoint, read it again and failed again: "
             f"{inc['restarts']} restarts in {m['to_mitigate']} minutes. Nothing downstream moved, so `{inc['product']}` aged "
             f"to {inc['worst_age_minutes']:.0f} minutes against a {inc['sla_minutes']}-minute SLA.", "",
             "## Impact", "",
             f"- {inc['windows_delayed']} fifteen-minute revenue windows ({inc['clicks_in_delayed_windows']} clicks, "
             f"£{inc['revenue_in_delayed_windows_gbp']:,.2f}) were published late. None were wrong once published.",
             f"- Data lost: {inc['data_lost']}.", "",
             "## Timeline (UTC)", ""]
    lines += [f"- **{t['at'][11:16]}** · {t['who']}: {t['what']}" for t in inc["timeline"]]
    lines += ["", "## Root cause", "",
              "The job treated a record it couldn't parse as a reason to stop. A restart can't fix a bad record: it is "
              "still at the same offset after every restore. The cause upstream is the producer cutting the record short.",
              "", "## Why it took " + str(m["to_detect"]) + " minutes to notice", "",
              "Only freshness was alerted on, and freshness is a lagging signal: it has to exceed the SLA first. "
              "The job's restart count said the same thing within three minutes.", "", "## Actions", ""]
    lines += [f"- [{'x' if a['status'] == 'done' else ' '}] {a['what']}" for a in inc["actions"]]
    return "\n".join(lines) + "\n"
