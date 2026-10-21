"""End to end, in plain Python: a week of Kafka topics → the three stream jobs (with a crash and a poison pill) →
the nightly batch → reconciliation → volume hold and bot filter → Gold → Snowflake and BigQuery SQL → SLAs → incident.

    python -m nexus.pipeline            # writes target/ and assets/data/site.json, about 15 seconds

The Spark batch (spark_batch.py) and the Java twin (java/) are checked against this run by their own tests.
"""
import json
import time
from collections import defaultdict

from nexus import batch, sla, warehouse
from nexus.contracts import load_quality, topic, upcast
from nexus.generate import END, START, generate, iso, ms, parse
from nexus.paths import LANDING, ROOT, TARGET
from nexus.stream import MIN, Job, read_topic

CRASH_AT = parse("2026-10-08T09:17:00Z")      # a TaskManager is lost under the page_views job
FIX_AT = parse("2026-10-09T15:20:00Z")        # the on-call engineer's fix is deployed (scripted)
SITE = ROOT / "assets" / "data" / "site.json"


class Steps:
    """Records each task's duration, like an orchestrator run."""

    def __init__(self):
        self.tasks, self.t0 = [], time.time()

    def run(self, key: str, label: str, fn, *args, **kw):
        start = time.time()
        out = fn(*args, **kw)
        self.tasks.append({"key": key, "label": label, "start_s": round(start - self.t0, 2),
                           "duration_s": round(time.time() - start, 2)})
        return out


def _watch(records: list, name: str) -> set:
    """The records the step-by-step page follows: one of each kind of problem."""
    out, seen, firsts = set(), {}, {}
    p = json.loads((LANDING / "truth.json").read_text())["planted"]
    for r in records:
        try:
            v = upcast(name, r.raw)
        except Exception:  # noqa: BLE001
            out.add((r.partition, r.offset))
            continue
        rid = v.get("event_id") or v.get("click_id")
        if name == "page_views":
            if rid in seen and "dup" not in firsts:
                firsts["dup"] = rid
            seen[rid] = 1
            lag = r.ts - parse(v["event_time"])
            if v["session_id"].startswith("s9") and len([k for k in firsts if k.startswith("off")]) < 3:
                firsts[f"off{len(firsts)}"] = rid
            et = parse(v["event_time"])
            if v["platform"] == "android" and lag < 55 * MIN and r.ts > et - et % (60 * MIN) + 75 * MIN \
                    and "lateok" not in firsts and r.ts > parse("2026-10-06T08:00:00Z"):
                firsts["lateok"] = rid
            if v["user_hash"] in p["bot_users"] and "bot" not in firsts:
                firsts["bot"] = rid
        if name == "partner_clicks":
            if r.ts > parse(p["schema_v2"]["from"]):
                k = "v2" if "price" in v else "v1"
                if "price" in v and v["price"]["currency"] == "EUR":
                    k = "eur"
                firsts.setdefault(k, rid)
    out |= set(firsts.values())
    if name == "reviews":
        out |= set(json.loads((LANDING / "truth.json").read_text())["reviews"]["erased"])
    return out


def run() -> dict:
    steps = Steps()
    truth = steps.run("generate", "Simulate a week of Kafka topics", generate)
    recs = {t: read_topic(t) for t in ("page_views", "partner_clicks", "reviews")}

    # ---- the stream jobs ----
    pv = Job("page_views")
    pv.watch = _watch(recs["page_views"], "page_views")
    steps.run("stream_page_views", "Stream job: page_views (TaskManager lost at 09:17)", pv.run, recs["page_views"], crash_at=CRASH_AT)
    pv_clean = steps.run("replay_check", "Same job, no crash: is the sink identical?", Job("page_views").run, recs["page_views"])
    pc = Job("partner_clicks", dlq=False)                  # the build in production that week: no dead-letter topic
    pc.watch = _watch(recs["partner_clicks"], "partner_clicks")
    steps.run("stream_partner_clicks", "Stream job: partner_clicks (poison pill, crash loop, fix)", pc.run,
              recs["partner_clicks"], fix_at=FIX_AT)
    rv = Job("reviews")
    rv.watch = _watch(recs["reviews"], "reviews")
    steps.run("stream_reviews", "Stream job: reviews (latest version wins, tombstones)", rv.run, recs["reviews"])

    # ---- the nightly batch and the reconciliation ----
    b_pv = steps.run("batch_page_views", "Nightly batch: page_views", batch.recompute, "page_views", recs["page_views"])
    b_pc = steps.run("batch_partner_clicks", "Nightly batch: partner_clicks", batch.recompute, "partner_clicks", recs["partner_clicks"])
    rec_pv = steps.run("reconcile_page_views", "Reconcile stream with batch: page_views", batch.reconcile, pv, b_pv)
    rec_pc = steps.run("reconcile_partner_clicks", "Reconcile stream with batch: partner_clicks", batch.reconcile, pc, b_pc)

    # ---- volume hold, investigation, release ----
    q = load_quality()
    hours = batch.hourly_volume(b_pv["windows"])
    holds = steps.run("volume_check", "Volume check: hold anomalous hours", batch.volume_holds, hours,
                      q["volume"]["sigmas"], q["volume"]["min_history"], q["volume"]["min_ratio"])
    found = [batch.bot_users(recs["page_views"], h["hour"], q["bots"]["max_views_per_user_hour"]) for h in holds]
    bots = {u for f in found for u in f["bots"]}
    gold_pv = steps.run("gold_page_views", "Gold: page views without the bots", batch.recompute, "page_views",
                        recs["page_views"], exclude_users=bots)
    holds_after = batch.volume_holds(batch.hourly_volume(gold_pv["windows"]), q["volume"]["sigmas"],
                                     q["volume"]["min_history"], q["volume"]["min_ratio"])

    # ---- gold and the warehouses ----
    gold = {
        "location_views_hourly": [{"window_start": iso(k[0]), "location_id": k[1][0], "views": w["n"]}
                                  for k, w in sorted(gold_pv["windows"].items())],
        "partner_revenue_15m": [{"window_start": iso(k[0]), "partner": k[1][0], "clicks": w["n"], "revenue_gbp": w["sum"]}
                                for k, w in sorted(b_pc["windows"].items())],
        "reviews_current": [{"review_id": k, **{c: v.get(c) for c in ("location_id", "rating", "status", "lang", "version")}}
                            for k, v in sorted(rv.s.current.items()) if not v["erased"]],
    }
    files = steps.run("render_sql", "Render Snowflake and BigQuery SQL from the contract", warehouse.render)
    raw_reviews = [_raw("reviews", r) for r in recs["reviews"]]
    locs = json.loads((LANDING / "locations.json").read_text())
    TARGET.mkdir(parents=True, exist_ok=True)
    wh = steps.run("load_check", "Load, check quality, apply erasure", warehouse.load, TARGET / "warehouse.db", gold, raw_reviews,
                   locs, truth["reviews"]["erased"])

    # ---- SLAs and the incident ----
    s0, s1 = ms(START) + 60 * MIN, ms(END)
    series = {"page_views": sla.age_series(pv, s0, s1), "partner_clicks": sla.age_series(pc, s0, s1),
              "reviews": sla.lag_series(rv, s0, s1)}
    reports = [sla.report(t, topic(t)["product"], series[t], "age" if t != "reviews" else "lag") for t in series]
    inc = steps.run("incident", "Write the incident record and postmortem", sla.incident, pc, series["partner_clicks"],
                    topic("partner_clicks")["product"])
    md = sla.postmortem(inc)

    site = {
        "run": {"seconds": round(time.time() - steps.t0, 1), "tasks": steps.tasks, "week": [iso(ms(START)), iso(ms(END))]},
        "generator": {"records": {t: sum(c.values()) for t, c in truth["records"].items()},
                      "partitions": {t: len(c) for t, c in truth["records"].items()},
                      "events": {t: sum(truth[t].values()) for t in ("page_views", "partner_clicks")},
                      "reviews": truth["reviews"]["reviews"], "retried": truth["retried"], "late_offline": truth["late_offline"],
                      "bot_views": truth["bot_views"], "planted": truth["planted"],
                      "v2_records": sum('"price"' in r.raw for r in recs["partner_clicks"])},
        "streams": {j.name: {"ledger": j.s.ledger, "windows": len(j.sink), "emits": len(j.emits), "late": len(j.late),
                             "dead_letters": [{"partition": p, "offset": o, "reason": r} for (p, o), r in j.dlq.items()],
                             "timeline": j.timeline, "checkpoints_kept": len(j.checkpoints)}
                    for j in (pv, pc, rv)},
        "replay": {"identical": {k: v["value"] for k, v in pv.sink.items()} == {k: v["value"] for k, v in pv_clean.sink.items()},
                   "windows": len(pv.sink), "re_emitted": len(pv.emits) - len(pv_clean.emits),
                   "restored_from": next(t["detail"] for t in pv.timeline if t["event"] == "restored")},
        "reconcile": {"page_views": rec_pv, "partner_clicks": rec_pc},
        "batch": {"page_views": {k: b_pv[k] for k in ("bad_records", "duplicates", "events")},
                  "partner_clicks": {k: b_pc[k] for k in ("bad_records", "duplicates", "events")}},
        "volume": {"hours": [[iso(t), v] for t, v in hours.items()], "holds": holds, "investigation": found,
                   "bots": sorted(bots), "holds_after_filter": holds_after,
                   "bot_views_removed": b_pv["events"] - gold_pv["events"]},
        "gold": _gold_summary(gold, locs),
        "warehouse": wh | {"files": sorted(files), "dialects": warehouse.dialect_diff()},
        "sla": reports,
        "series": {t: [[iso(a), v] for a, v in s[::3]] for t, s in series.items()},
        "incident": inc,
        "trace": {"page_views": pv.decisions, "partner_clicks": pc.decisions, "reviews": rv.decisions},
        "late_examples": sorted(pv.late.values(), key=lambda e: e["event_time"])[:6],
        "window_examples": _window_examples(pv, b_pv),
    }
    (TARGET / "stream_page_views.json").write_text(json.dumps(
        {"ledger": pv_clean.s.ledger, "windows": sorted([iso(k[0]), k[1][0], v["value"]["n"]] for k, v in pv_clean.sink.items())}))
    site["java"] = steps.run("java_twin", "Java twin of the page_views job: same windows?", _java_twin)
    site["spark"] = steps.run("spark_batch", "PySpark batch: same windows as the plain-Python batch?", _spark_batch, b_pv, b_pc)
    site["run"]["seconds"] = round(time.time() - steps.t0, 1)
    SITE.parent.mkdir(parents=True, exist_ok=True)
    SITE.write_text(json.dumps(site, indent=1, default=str))
    (ROOT / "assets" / "data" / "incident.md").write_text(md)
    (TARGET / "batch_partner_clicks.json").write_text(json.dumps(
        sorted([iso(k[0]), k[1][0], w["n"], w["sum"]] for k, w in b_pc["windows"].items())))
    return site


def _java_twin() -> dict | None:
    """Compile and run java/src/nexus/WindowJob.java on the same topic; compare with the Python job. Skipped without a JDK
    (and in the browser)."""
    import csv
    import shutil
    import subprocess
    if not shutil.which("javac"):
        return None
    out = TARGET / "java_page_views.csv"
    subprocess.run(["javac", "-d", str(TARGET / "java"), *map(str, sorted((ROOT / "java/src/nexus").glob("*.java")))],
                   check=True, capture_output=True)
    subprocess.run(["java", "-cp", str(TARGET / "java"), "nexus.WindowJob", str(LANDING / "kafka/page_views"), "6", str(out)],
                   check=True, capture_output=True)
    py = json.loads((TARGET / "stream_page_views.json").read_text())
    jv = [[r["window_start"], r["location_id"], int(r["views"])] for r in csv.DictReader(out.open())]
    led = json.loads((TARGET / "java_page_views.ledger.json").read_text())
    keys = ("read", "accepted", "duplicates", "late_dropped", "late_updates")
    return {"windows": len(jv), "identical": jv == py["windows"] and all(led[k] == py["ledger"][k] for k in keys),
            "ledger": {k: led[k] for k in keys}, "emits": led["emits"], "millis": led["millis"],
            "java": subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.strip().splitlines()[-3]}


def _spark_batch(b_pv: dict, b_pc: dict) -> dict | None:
    try:
        from nexus import spark_batch
    except ImportError:
        return None
    t0 = time.time()
    spark = spark_batch.get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    out = {}
    for name, b in (("page_views", b_pv), ("partner_clicks", b_pc)):
        a = spark_batch.recompute(spark, name)
        same = a["windows"].keys() == b["windows"].keys() and all(
            a["windows"][k]["n"] == w["n"] and abs(a["windows"][k]["sum"] - w["sum"]) < 1e-6 for k, w in b["windows"].items())
        out[name] = {"windows": len(a["windows"]), "events": a["events"], "bad_records": a["bad_records"], "identical": same}
    return out | {"seconds": round(time.time() - t0, 1), "version": spark.version}


def _raw(name: str, r) -> dict:
    v = upcast(name, r.raw)
    return v | {"_schema_version": 1, "_partition": r.partition, "_offset": r.offset, "_appended_at": iso(r.ts)}


def _gold_summary(gold: dict, locs: list) -> dict:
    name = {x["location_id"]: x for x in locs}
    views = defaultdict(int)
    for r in gold["location_views_hourly"]:
        views[r["location_id"]] += r["views"]
    rating = defaultdict(list)
    for r in gold["reviews_current"]:
        if r["status"] == "published":
            rating[r["location_id"]].append(r["rating"])
    top = sorted(views.items(), key=lambda kv: -kv[1])[:8]
    by_day = defaultdict(lambda: defaultdict(float))
    for r in gold["partner_revenue_15m"]:
        by_day[r["window_start"][:10]][r["partner"]] += r["revenue_gbp"]
    return {"top_locations": [{"location_id": k, "name": name[k]["name"], "views": v,
                               "rating": round(sum(rating[k]) / len(rating[k]), 2) if rating[k] else None,
                               "reviews": len(rating[k])} for k, v in top],
            "revenue_by_day": [{"day": d, **{p: round(x, 2) for p, x in sorted(v.items())}, "total": round(sum(v.values()), 2)}
                               for d, v in sorted(by_day.items()) if d < iso(ms(END))[:10]],
            "rows": {k: len(v) for k, v in gold.items()}}


def _window_examples(job, b) -> list:
    """Windows where stream and batch differ, with the reason."""
    out = []
    late = defaultdict(int)
    for e in job.late.values():
        late[(parse(e["window_start"]), tuple(e["group"]))] += 1
    for k in sorted(late)[:5]:
        out.append({"window_start": iso(k[0]), "location_id": k[1][0], "stream": job.sink.get(k, {"value": {"n": 0}})["value"]["n"],
                    "batch": b["windows"][k]["n"], "side_output": late[k]})
    return out


if __name__ == "__main__":
    s = run()
    print(json.dumps({k: s[k] for k in ("replay", "batch")}, indent=1))
    print("tasks:", [(t["key"], t["duration_s"]) for t in s["run"]["tasks"]], "total", s["run"]["seconds"], "s")
