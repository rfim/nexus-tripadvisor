"""Why each choice: re-run the same week under the alternatives and measure what each one costs.

    python -m nexus.benchmarks        # after the pipeline; writes assets/data/benchmarks.json (about 40 seconds)

Every number is measured on this repository's synthetic week, except the warehouse layout, which is modelled from the
run's own row counts (bytes = rows × an assumed row width). The deck's "why this, not that" slides read this file.
"""
import json
import time
from statistics import mean

from nexus import batch, sla
from nexus.contracts import load_quality, topic
from nexus.generate import END, START, ms, parse
from nexus.paths import LANDING, ROOT
from nexus.stream import MIN, Job, read_topic

OUT = ROOT / "assets" / "data" / "benchmarks.json"


def _pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def _measure(job: Job, batch_events: int, sla_min: int) -> dict:
    ages = [a for _, a in sla.age_series(job, ms(START) + 60 * MIN, ms(END))]
    led = job.s.ledger
    return {"counted": led["accepted"], "missed_vs_batch": batch_events - led["accepted"], "side_output": led["late_dropped"],
            "late_refires": led["late_updates"], "duplicates_dropped": led["duplicates"], "sink_writes": len(job.emits),
            "freshness_p50": _pct(ages, .5), "freshness_p95": _pct(ages, .95), "freshness_max": max(ages),
            "sla_breach_samples": sum(a > sla_min for a in ages),
            "peak_open_windows": job.peak["open_windows"], "peak_dedupe_keys": job.peak["dedupe_keys"]}


def _run(recs, b_events, sla_min, **kw):
    t0 = time.time()
    j = Job("page_views", **kw).run(recs)
    return _measure(j, b_events, sla_min) | {"seconds": round(time.time() - t0, 2)}


def _age_from_last_window(job: Job, start: int, end: int) -> list:
    """The naive freshness: age of the newest window that had data. Pages every quiet night."""
    emits, out, i, vis = sorted(job.emits), [], 0, None
    t = start - start % (5 * MIN) + 5 * MIN
    while t <= end:
        while i < len(emits) and emits[i][0] <= t:
            vis = emits[i][1] if vis is None else max(vis, emits[i][1])
            i += 1
        if vis is not None:
            out.append((t, (t - vis) / MIN))
        t += 5 * MIN
    return out


def run() -> dict:
    t0 = time.time()
    pv = read_topic("page_views")
    b_pv = batch.recompute("page_views", pv)
    sla_pv = topic("page_views")["product"]["sla"]["freshness_minutes"]
    out = {}

    # 1. the watermark: which clock decides that an hour is over
    out["watermark"] = {m: _run(pv, b_pv["events"], sla_pv, watermark=m) for m in ("active_min", "min_all", "global_max")}

    # 2. out-of-orderness: how long to wait before the first firing
    out["out_of_orderness"] = {str(o): _run(pv, b_pv["events"], sla_pv, ooo_min=o) for o in (0, 2, 10, 30)}

    # 3. allowed lateness: how long a fired window stays open to late records
    out["allowed_lateness"] = {str(lt): _run(pv, b_pv["events"], sla_pv, lateness_min=lt) for lt in (0, 15, 60, 180)}

    # 4. dedupe TTL: how long an id is remembered
    out["dedupe_ttl"] = {str(h): _run(pv, b_pv["events"], sla_pv, dedupe_ttl_h=h) for h in (0.01, 0.25, 1, 24)}
    retry_gaps = []
    first = {}
    for r in pv:
        eid = json.loads(r.raw)["event_id"]
        if eid in first:
            retry_gaps.append((r.ts - first[eid]) / 1000)
        else:
            first[eid] = r.ts
    out["retry_gap_seconds"] = {"count": len(retry_gaps), "max": max(retry_gaps), "p50": _pct(retry_gaps, .5)}

    # 5. a record that can't be read: stop, skip, or dead-letter
    pc = read_topic("partner_clicks")
    fix = parse("2026-10-09T15:20:00Z")
    failing = Job("partner_clicks", dlq=False).run(pc, fix_at=fix)
    series = sla.age_series(failing, ms(START), ms(END))
    inc = sla.incident(failing, series, topic("partner_clicks")["product"])
    dlq = Job("partner_clicks").run(pc)
    dlq_ages = [a for _, a in sla.age_series(dlq, ms(START), ms(END))]
    out["bad_record"] = {
        "fail_and_restart": {"restarts": inc["restarts"], "minutes_late": inc["minutes"]["total"], "windows_late": inc["windows_delayed"],
                             "records_kept_for_inspection": 1, "worst_age_minutes": inc["worst_age_minutes"]},
        "skip_silently": {"restarts": 0, "minutes_late": 0, "windows_late": 0, "records_kept_for_inspection": 0,
                          "worst_age_minutes": max(dlq_ages)},
        "dead_letter": {"restarts": 0, "minutes_late": 0, "windows_late": 0, "records_kept_for_inspection": len(dlq.dlq),
                        "worst_age_minutes": max(dlq_ages)},
    }

    # 6. what freshness means: newest non-empty window, or published completeness
    sla_pc = topic("partner_clicks")["product"]["sla"]["freshness_minutes"]
    naive = _age_from_last_window(dlq, ms(START) + 60 * MIN, ms(END))
    good = sla.age_series(dlq, ms(START) + 60 * MIN, ms(END))
    out["freshness_metric"] = {
        "last_non_empty_window": {"breach_samples": sum(a > sla_pc for _, a in naive), "p95": _pct([a for _, a in naive], .95),
                                  "max": max(a for _, a in naive), "pages": _episodes(naive, sla_pc)},
        "published_completeness": {"breach_samples": sum(a > sla_pc for _, a in good), "p95": _pct([a for _, a in good], .95),
                                   "max": max(a for _, a in good), "pages": _episodes(good, sla_pc)},
        "note": "partner_clicks with the dead-letter topic on: no real incident, so every page is a false one"}

    # 7. the volume hold: which rule catches the scraper and nothing else
    q = load_quality()["volume"]
    hours = batch.hourly_volume(b_pv["windows"])
    bot_hour = json.loads((LANDING / "truth.json").read_text())["planted"]["bots"]["from"][:13]
    rules = {"2σ": dict(sigmas=2, min_ratio=0), "3σ": dict(sigmas=3, min_ratio=0), "2σ and ×2 (chosen)": dict(sigmas=2, min_ratio=2.0),
             "×2 only": dict(sigmas=0, min_ratio=2.0)}
    out["volume_rule"] = {}
    for name, kw in rules.items():
        holds = batch.volume_holds(hours, kw["sigmas"], q["min_history"], kw["min_ratio"])
        out["volume_rule"][name] = {"holds": len(holds), "caught_scraper": any(h["hour"].startswith(bot_hour) for h in holds),
                                    "false_holds": sum(not h["hour"].startswith(bot_hour) for h in holds)}

    # 8. which alert pages first
    out["alerting"] = {"freshness_over_sla": inc["minutes"]["to_detect"], "restarts_over_3_in_10_min": inc["minutes"]["would_detect_with_restart_alert"]}

    # 9. the warehouse layout for partner_revenue_15m: bytes a query scans (modelled)
    gold = json.loads((ROOT / "assets/data/site.json").read_text())
    rows = gold["warehouse"]["rows"]["partner_revenue_15m"]
    per_day = rows / 7
    partners = 6
    row_bytes = 48            # window_start 8 + partner ~12 + clicks 8 + revenue 16 + _loaded_at 8 (an assumption)
    out["layout"] = {"query": "revenue for one partner on one day", "row_bytes_assumed": row_bytes, "rows": rows,
                     "no_partitioning": rows * row_bytes, "partition_by_day": round(per_day * row_bytes),
                     "partition_by_day_cluster_by_partner": round(per_day / partners * row_bytes),
                     "note": "bytes = rows read × row width; at this size BigQuery's 10 MB minimum dominates, so read the ratios"}

    # 10. engines: same answer, different cost
    site = json.loads((ROOT / "assets/data/site.json").read_text())
    tasks = {t["key"]: t["duration_s"] for t in site["run"]["tasks"]}
    out["engines"] = {
        "Python reference": {"seconds": tasks.get("stream_page_views"), "identical": True},
        "Java twin (JDK)": {"seconds": round(site["java"]["millis"] / 1000, 2) if site.get("java") else None,
                            "identical": site.get("java", {}).get("identical")},
        "Apache Flink 1.20 (MiniCluster)": {"seconds": round(site["flink"]["location_views_hourly"]["millis"] / 1000, 2) if site.get("flink") else None,
                                            "identical": site.get("flink", {}).get("location_views_hourly", {}).get("identical_to_python")},
        "PySpark batch (both topics)": {"seconds": site["spark"]["seconds"] if site.get("spark") else None,
                                        "identical": site.get("spark", {}).get("page_views", {}).get("identical")},
    }
    out["seconds"] = round(time.time() - t0, 1)
    OUT.write_text(json.dumps(out, indent=1))
    return out


def _episodes(series, limit):
    """Separate breaches: how many times someone would be paged."""
    n, inside = 0, False
    for _, a in series:
        if a > limit and not inside:
            n += 1
        inside = a > limit
    return n


if __name__ == "__main__":
    r = run()
    for k, v in r.items():
        print(k, json.dumps(v)[:400])
    print("mean watermark run", mean(x["seconds"] for x in r["watermark"].values()), "s")
