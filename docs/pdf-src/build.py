"""Write the two downloads (fireside deck, one page) as HTML, filled from the run.

    python docs/pdf-src/build.py          # writes deck.html and one_page.html here
    node scripts/pdfs.js                  # renders them to assets/*.pdf with Chromium

Every run figure comes from assets/data/site.json; every Lifepal figure is from my CV.
"""
import html
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
D = json.loads((ROOT / "assets/data/site.json").read_text())
G, P, S, R, I = D["generator"], D["generator"]["planted"], D["streams"], D["reconcile"], D["incident"]
SITE = "rfim.github.io/nexus-tripadvisor"
n = lambda v: f"{v:,}"   # noqa: E731
hm = lambda iso: iso[11:16]   # noqa: E731
checks = D["warehouse"]["checks"]
passed = sum(c["status"] == "pass" for c in checks)
hold = D["volume"]["holds"][0]
sla = {r["product"]: r for r in D["sla"]}
J, SP, FL = D.get("java"), D.get("spark"), D.get("flink")
_bp = ROOT / "assets/data/benchmarks.json"
B = json.loads(_bp.read_text()) if _bp.exists() else None
KW = r"\b(def|return|if|not|for|in|else|and|or|import|from|with|lambda|while|try|except|raise|class|MERGE|INTO|USING|ON|WHEN|MATCHED|THEN|UPDATE|SET|INSERT|SELECT|FROM|WHERE|DELETE|AND|NOT|BY|GROUP|CREATE|TABLE|WATERMARK|FOR|AS|INTERVAL|long|void|final|private|static|new|public|boolean)\b"


def _hl(line: str) -> str:
    code, com = line, ""
    m = re.search(r"(\s#\s.*|^\s*#.*|\s--\s.*|^\s*--.*|\s//\s.*|^\s*//.*)$", line)
    if m and line.count('"', 0, m.start()) % 2 == 0:
        code, com = line[:m.start()], line[m.start():]
    parts = re.split(r'(f?"[^"]*"|\'[^\']*\')', code)
    out = "".join(f'<span class="s">{html.escape(x)}</span>' if i % 2 else re.sub(KW, r'<span class="k">\1</span>', html.escape(x))
                  for i, x in enumerate(parts))
    return out + (f'<span class="c">{html.escape(com)}</span>' if com else "")


def code(path: str, start: str, end: str | None = None, limit: int = 26) -> str:
    lines = (ROOT / path).read_text().splitlines()
    i = next(k for k, l in enumerate(lines) if start in l)
    j = next((k for k in range(i + 1, len(lines)) if end and end in lines[k]), len(lines))
    body = lines[i:j]
    while body and not body[-1].strip():
        body.pop()
    return "\n".join(_hl(l) for l in body[:limit])


def head(title, css):
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{title}</title>'
            f'<link rel="stylesheet" href="style.css"><style>{(HERE / css).read_text()}</style></head><body>')


_N = [1]


def slide(kicker, title, lede, body):
    _N[0] += 1
    return (f'<section class="slide"><div class="kicker">{kicker}</div><h2>{title}</h2><p class="lede">{lede}</p>{body}'
            f'<div class="foot"><span>Nexus · Lifepal best practices · applied to Tripadvisor</span><span>Vim · {_N[0]:02d}</span></div></section>')


def table(rows, cols):
    th = "".join(f"<th>{c}</th>" for c in cols)
    return "<table><tr>" + th + "</tr>" + "".join("<tr>" + "".join(f'<td class="{"k" if j == 0 else ""}">{v}</td>'
                                                                         for j, v in enumerate(r)) + "</tr>" for r in rows) + "</table>"


def side(*blocks) -> str:
    return "".join(f"<h4>{h}</h4>" + "".join(f"<p>{x}</p>" for x in ps) for h, ps in blocks)


def code_slide(kicker, title, lede, path, tech, src, sd, formula=""):
    left = (f'<div class="formula">{formula}</div>' if formula else "") + f'<div class="fname"><span>{path}</span><span>{tech}</span></div><pre>{src}</pre>'
    return slide(kicker, title, lede, f'<div class="grid split"><div>{left}</div><div class="side">{sd}</div></div>')


PROBLEMS = [
    ("SDK retries", f"{G['retried']} events re-sent, same event_id, new offset", f"Dedupe on id, 24 h checkpointed state: {S['page_views']['ledger']['duplicates']} dropped"),
    ("An offline upload", f"{G['late_offline']} iOS events, 2–8 hours late", f"Side output past an hour's lateness; the batch explains {R['page_views']['explained_by_side_output']} windows"),
    ("A schema change", f"app 7.4: price {{amount, currency}}, {G['v2_records']} records", "One reader upcasts v1 and v2; unknown currency is a dead letter"),
    ("A poison pill", f"partition {I['partition']}, offset {I['offset']}: {I['raw_bytes']} bytes of truncated JSON", f"{I['restarts']} restarts, paged at {hm(I['detected'])}; fixed with the DLQ"),
    ("A scraper", f"{n(hold['views'])} views in an hour that has {hold['mean']}", f"Hour held (2σ and ×2); {len(D['volume']['bots'])} users removed; released"),
    ("Erasure, then a stale version", "an outbox replay re-sends version 3 after the tombstone", f"Version rule: {S['reviews']['ledger']['stale_versions']} rejected; deleted in both warehouses"),
]


def why_slides():
    """"Why this, not that": each decision, the alternatives re-run on the same week, and how the choice was reached."""
    if not B:
        return []
    out = []
    ch = lambda txt: f"<b>{txt} ✓</b>"   # noqa: E731
    METHOD = {"bench": "Benchmark sweep: the same week re-run per option", "trial": "Trial and error: built, measured, changed",
              "replay": "Failure replay: the incident re-run each way", "compare": "Comparison with how other engines do it"}
    tag = lambda *ks: "".join(f'<span class="chip dark">{METHOD[k]}</span>' for k in ks)   # noqa: E731
    out.append(slide("Why this, not that", "How each decision was reached",
        "Every choice below was re-run against its alternatives on the same week of Kafka records, and measured. Where the data says a choice is oversized, the slide says so.",
        table([("Watermark", "min over active partitions, idle skipped", "wait for every partition · one global clock", "bench · compare"),
               ("Out-of-orderness", "10 min", "0 · 2 · 30 min", "bench"),
               ("Allowed lateness", "60 min", "0 · 15 · 180 min", "bench"),
               ("Dedupe state", "24 h TTL", "36 s · 15 min · 1 h", "bench"),
               ("Bad records", "dead-letter topic", "fail and restart · skip silently", "replay · compare"),
               ("Alerting", "restarts and freshness", "freshness only", "replay"),
               ("Freshness metric", "published completeness", "newest non-empty window", "trial"),
               ("Volume hold", "2σ and ×2", "2σ · 3σ · ×2 only", "trial · bench"),
               ("Warehouse layout", "day partitions, cluster by partner", "none · day only", "bench (modelled)"),
               ("Engine", "Flink, checked by Python, Java and Spark", "one engine, unchecked", "bench")],
              ["Decision", "Chosen", "Alternatives re-run", "Method"])
        + f'<div class="proof" style="margin-top:6mm"><span class="kicker">Method</span>{tag("bench", "trial", "replay", "compare")}</div>'))

    w, o = B["watermark"], B["out_of_orderness"]
    out.append(slide("Why · the watermark", "Which clock says an hour is over",
        "Three watermarks on the same week of page_views. The one that waits for every partition stalls whenever one goes quiet.",
        '<div class="grid two"><div>' + table([
            (ch("Min over active partitions") if k == "active_min" else {"min_all": "Min over every partition", "global_max": "One global clock (max)"}[k],
             n(v["counted"]), v["missed_vs_batch"], f'{v["freshness_p95"]:.0f} min', f'{v["freshness_max"]:.0f} min', v["sla_breach_samples"])
            for k, v in w.items()], ["Watermark", "Counted", "Missed", "Fresh p95", "Worst", "SLA breaches"])
        + '<div style="height:4mm"></div>' + table([
            (ch(f"{k} min") if k == "10" else f"{k} min", v["late_refires"], f'{v["freshness_p50"]:.0f} min', f'{v["freshness_p95"]:.0f} min', v["sla_breach_samples"])
            for k, v in o.items()], ["Out-of-orderness", "Late re-fires", "Fresh p50", "Fresh p95", "SLA breaches"])
        + '</div><div class="side">' + side(
            ("How I got here", [tag("bench", "compare")]),
            ("What the numbers say", [f"Waiting for every partition: <b>{w['min_all']['sla_breach_samples']}</b> breach samples, p95 {w['min_all']['freshness_p95']:.0f} min. Quiet partitions hold the whole job back.",
                                      "A global clock costs nothing here because the partitions stay in step. It is the one that drops records when a partition lags, after a restore for example."]),
            ("Elsewhere", ["<b>Flink</b>: per-split watermarks, the minimum wins, <code>withIdleness</code>. <b>Spark Structured Streaming</b>: one watermark from the max event time. <b>Kafka Streams</b>: stream time per task, plus a grace period."]),
            ("What I'd revisit", [f"10 min of out-of-orderness is headroom for phone clocks. The data says 2 min would be {o['10']['freshness_p95'] - o['2']['freshness_p95']:.0f} min fresher at p95 for {o['2']['late_refires'] - o['10']['late_refires']} more re-fires a week."])) + '</div></div>'))

    lt, tt, rg = B["allowed_lateness"], B["dedupe_ttl"], B["retry_gap_seconds"]
    out.append(slide("Why · lateness and dedupe", "How long to wait, how long to remember",
        "Allowed lateness trades events counted in the stream against state held open. Dedupe TTL trades retries caught against keys kept.",
        '<div class="grid two"><div>' + table([
            (ch(f"{k} min") if k == "60" else f"{k} min", n(v["counted"]), v["missed_vs_batch"], v["late_refires"], v["peak_open_windows"])
            for k, v in lt.items()], ["Allowed lateness", "Counted", "Missed (batch has it)", "Re-fires", "Peak open windows"])
        + '<div style="height:4mm"></div>' + table([
            (ch("24 h") if k == "24" else {"0.01": "36 s", "0.25": "15 min", "1": "1 h"}[k], v["duplicates_dropped"], n(v["peak_dedupe_keys"]))
            for k, v in tt.items()], ["Dedupe TTL", "Retries caught", "Peak keys held"])
        + '</div><div class="side">' + side(
            ("How I got here", [tag("bench", "compare")]),
            ("What the numbers say", [f"60 min is the smallest lateness that absorbs all organic lateness: what's left ({lt['60']['missed_vs_batch']}) is the offline upload, hours late. 180 min counts {lt['60']['missed_vs_batch'] - lt['180']['missed_vs_batch']} more but holds {lt['180']['peak_open_windows'] / lt['60']['peak_open_windows']:.1f}× the state; the nightly batch already counts them.",
                                      f"Retries arrive within {rg['max']:.0f} s (p50 {rg['p50']:.0f} s), so 15 minutes would catch all {rg['count']}. 24 h is a deliberate margin for re-delivery after an outage: {n(tt['24']['peak_dedupe_keys'])} keys, well under a megabyte."]),
            ("Elsewhere", ["<b>Flink</b> <code>allowedLateness</code> and keyed state TTL; <b>Kafka Streams</b> <code>grace()</code>; <b>Spark</b> <code>dropDuplicatesWithinWatermark</code>. Kafka's idempotent producer only dedupes broker-level retries, not an app re-sending."]),
            ("What I'd revisit", ["Size the TTL from the longest consumer rewind the team has actually done, not from retries."])) + '</div></div>'))

    br, al = B["bad_record"], B["alerting"]
    out.append(slide("Why · bad records and alerting", "Stop, skip, or keep it aside",
        "The poison pill replayed three ways on the same partner_clicks week, and the two alerts that could have paged.",
        '<div class="grid two"><div>' + table([
            (ch("Dead-letter topic") if k == "dead_letter" else {"fail_and_restart": "Fail and restart", "skip_silently": "Skip silently"}[k],
             v["restarts"], f'{v["minutes_late"]} min', v["windows_late"], v["records_kept_for_inspection"])
            for k, v in br.items()], ["Policy", "Restarts", "Product late", "Windows late", "Record kept"])
        + '<div style="height:4mm"></div>' + table([
            ("Freshness over the SLA", f"{al['freshness_over_sla']} min"), (ch("Restarts > 3 in 10 min, too"), f"{al['restarts_over_3_in_10_min']} min")],
            ["Alert", "Time to page"])
        + '</div><div class="side">' + side(
            ("How I got here", [tag("replay", "compare")]),
            ("What the numbers say", [f"Failing cost {br['fail_and_restart']['restarts']} restarts and {br['fail_and_restart']['minutes_late']} minutes on a tier-1 product for one record. Skipping costs nothing now and loses the evidence. Dead-lettering costs nothing and keeps it.",
                                      f"Freshness is a lagging signal: {al['freshness_over_sla']} minutes. The restart count said the same in {al['restarts_over_3_in_10_min']}."]),
            ("Elsewhere", ["<b>Kafka Connect</b>: <code>errors.tolerance=all</code> with a DLQ topic. <b>Flink</b>: a side output, as here. <b>Spark</b>: permissive parsing with a corrupt-record column."]),
            ("At Lifepal", ["Every incident ended with a root-cause review and a change to the platform; data-trust escalations fell 60%."])) + '</div></div>'))

    fm, vr = B["freshness_metric"], B["volume_rule"]
    out.append(slide("Why · freshness and the volume hold", "Built, measured, changed",
        "Both started as the obvious version. The run showed what it would page for, so both changed. This is the trial and error, kept.",
        '<div class="grid two"><div>' + table([
            ("Newest non-empty window (first version)", fm["last_non_empty_window"]["pages"], fm["last_non_empty_window"]["breach_samples"], f'{fm["last_non_empty_window"]["max"]:.0f} min'),
            (ch("Published completeness"), fm["published_completeness"]["pages"], fm["published_completeness"]["breach_samples"], f'{fm["published_completeness"]["max"]:.0f} min')],
            ["Freshness measured from", "Pages in a week", "Late samples", "Worst"])
        + '<div style="height:4mm"></div>' + table([
            (ch(k.replace(" (chosen)", "")) if "chosen" in k else k + (" (first version)" if k == "2σ" else ""), v["holds"], "yes" if v["caught_scraper"] else "no", v["false_holds"])
            for k, v in vr.items()], ["Volume hold", "Hours held", "Caught the scraper", "False holds"])
        + '</div><div class="side">' + side(
            ("How I got here", [tag("trial", "bench")]),
            ("What the numbers say", [f"With no incident at all, the first freshness metric paged <b>{fm['last_non_empty_window']['pages']}</b> times in a week: a quiet 3 a.m. has no clicks, not a stale product.",
                                      f"2σ alone held <b>{vr['2σ']['false_holds']}</b> innocent hours: a week of history makes σ small and Saturdays look odd. Adding ×2 left only the scraper."]),
            ("Elsewhere", ["<b>dbt source freshness</b> checks the newest <code>loaded_at</code>: the first version here, fine for busy sources. Seasonal anomaly tools compare the same hour on earlier days, as this rule does."]),
            ("What I'd revisit", ["×2 alone also works on this week. σ stays to protect low-volume hours, where a jump from 2 to 5 is noise; at Lifepal the baseline was 30 days."])) + '</div></div>'))

    ly, en = B["layout"], B["engines"]
    out.append(slide("Why · layout and engines", "Read less. Check twice.",
        f"Bytes a query scans in BigQuery, modelled from the run's own row counts ({ly['row_bytes_assumed']} bytes a row, an assumption), and the same week on four engines.",
        '<div class="grid two"><div>' + table([
            ("No partitioning", n(ly["no_partitioning"]), "1×"), ("Partition by day", n(ly["partition_by_day"]), f'{ly["no_partitioning"] / ly["partition_by_day"]:.0f}× less'),
            (ch("Day partitions, cluster by partner"), n(ly["partition_by_day_cluster_by_partner"]), f'{ly["no_partitioning"] / ly["partition_by_day_cluster_by_partner"]:.0f}× less')],
            [ly["query"].capitalize(), "Bytes scanned", "vs none"])
        + '<div style="height:4mm"></div>' + table([
            (ch(k) if "Flink" in k else k, f'{v["seconds"]} s' if v["seconds"] is not None else "—", "identical" if v["identical"] else "—") for k, v in en.items()],
            ["Engine", "The week", "Output"])
        + '</div><div class="side">' + side(
            ("How I got here", [tag("bench", "compare")]),
            ("What the numbers say", [f"Partitioning and clustering cut the scan {ly['no_partitioning'] / ly['partition_by_day_cluster_by_partner']:.0f}×. At this size BigQuery's 10 MB minimum dominates; on a petabyte table, the ratio is the bill.",
                                      "Four engines, one answer. Flink is slowest on a laptop-sized week: start-up dominates. It is chosen for state, checkpoints and per-split watermarks at scale, not for this benchmark."]),
            ("Elsewhere", ["<b>BigQuery</b>: partition and cluster, billed on bytes scanned. <b>Snowflake</b>: micro-partitions and clustering keys, billed on warehouse time."]),
            ("At Lifepal", ["The same discipline took the nightly batch from 6 hours to 90 minutes and kept spend at $11k against a $19k forecast."])) + '</div></div>'))
    return out


def deck():
    s = [head("Nexus for Tripadvisor · Fireside chat", "deck.css")]
    s.append(f'''<section class="slide cover">
  <div class="tag"><div class="t1">PRESENTED TO</div><div class="t2"><img src="tripadvisor-owl.png" alt="" style="height:30px;vertical-align:-5px;margin-right:6px">Tripadvisor</div><div class="t3">DATA PLATFORM · SENIOR DATA ENGINEER</div></div>
  <div class="built"><div><div class="k2">PLATFORM BUILT IN</div><span class="lp"><img src="lifepal.png" alt="Lifepal"></span></div></div>
  <div class="kicker" style="margin-top:14mm">Data engineering · Fireside chat · Nexus best practices from Lifepal</div>
  <h1>Nexus</h1>
  <div class="sub">Kafka in, Flink in the middle, BigQuery out. Every event counted once, on time, and checked</div>
  <p class="desc">Four years leading the data platform at Lifepal, a high-traffic consumer marketplace: Kafka and Debezium streams next to batch sources, a config-driven framework other engineers built on, SLAs, 500+ quality checks and a root-cause review after every incident. This pack applies those practices to a travel site's data on Tripadvisor's listed stack, Kafka → stream processing → Spark → Snowflake and BigQuery, in a runnable repository with synthetic data. Every run figure comes from that run; every Lifepal figure is from my CV.</p>
  <div class="meta"><div><b>PRESENTED BY</b>R. Firman Insan Muhammad (Vim)<br>Senior Data Engineer · London</div><div><b>TO</b>Data Platform<br>Tripadvisor</div>
    <div><b>ROLE</b>Senior Data Engineer<br>London, remote / hybrid</div><div><b>STACK</b>Kafka · Flink · BigQuery<br>Spark · Snowflake · Java · Python</div></div>
  <div class="foot"><span>Nexus · Lifepal best practices · applied to Tripadvisor</span><span>Vim · 01</span></div></section>''')
    s.append(slide("Contents", "What I'd like to cover", "Ten minutes on what I've built, then the parts of the role I'd own first. Every page points to working code.",
        '<table class="toc">' + "".join(f'<tr><td class="n">{a}</td><td><b>{b}</b></td><td>{c}</td></tr>' for a, b, c in [
            ("01", "Measured outcomes", "What changed at Lifepal, in numbers"),
            ("02", "The role, line by line", "Where I've done it before, and where I haven't"),
            ("03", "Six streaming problems", "Planted in the topics, caught in the run"),
            ("04", "The stream job", "Event time, watermarks, lateness, dead letters"),
            ("05", "Stream first, batch checks", "Reconciliation, record for record; Spark agrees"),
            ("06", "Snowflake and BigQuery from one contract", "Governance as generated code"),
            ("06", "Modeling and governance", "Grain, keys, lineage, policy tags, quality gates, erasure"),
            ("07", "SLAs and an incident", "Detection, root cause, postmortem"),
            ("08", "Apache Flink", "The job on Flink 1.20: topology, state, watermarks, production on Kafka"),
            ("08", "Java", "A twin of the job, tested against the reference"),
            ("10", "Why this, not that", "Each decision re-run against its alternatives"),
            ("11", "My first 90 days", "How I'd start in Data Platform")]) + "</table>"))
    s.append(slide("01 · Measured outcomes", "What changed at Lifepal", "Figures from my CV and production work, described at the level I can share publicly.",
        '<div class="grid nums">' + "".join(f'<div class="card"><div class="big">{a}</div><p>{b}</p></div>' for a, b in [
            ("99.7%", "reporting latency cut: 24 h → under 5 min"), ("75%", "nightly batch cut: 6 h → 90 min by tuning Spark and SQL"),
            ("$11k", "monthly platform cost against a $19k forecast"), ("−60%", "data-trust escalations: SLAs, 500+ checks, anomaly holds"),
            ("−40%", "duplicate records")]) + '</div>'
        '<div class="grid two" style="margin-top:7mm"><div class="callout"><b>How:</b> one governed lakehouse out of fragmented batch jobs and legacy ingestion; Kafka + Debezium CDC next to batch from APIs and partner files; a config-driven Python framework; Kimball models and a dbt semantic layer for five teams.</div>'
        '<div class="callout pink"><b>Team:</b> led 3–5 engineers; two promoted to Senior; onboarding halved from 4 weeks to 2. An MCP server for governed agent access and a RAG copilot with 1,000+ outcomes a day.</div></div>'))
    s.append(slide("02 · The role, line by line", "How my experience maps to the role", "The job description's asks, what I've done, and where it shows in this repository.",
        table([("ETL/ELT across Snowflake and BigQuery", "Snowflake + dbt at Deloitte; Databricks + dbt at Lifepal", "warehouse/ · warehouse.py"),
               ("Own pipelines: SLAs, monitoring, root cause", "SLAs, freshness and volume alerts, runbooks, RCA after every incident", "sla.py · oncall.html"),
               ("Streaming and batch: Flink, Spark, Kafka", "Kafka + Debezium with Spark; 6 h → 90 min batch", "flink/job · stream.py · spark_batch.py"),
               ("Production Java and Python", "Python for ten years; Java is my ramp", "java/WindowJob.java"),
               ("Shared frameworks, governance tooling", "Config-driven ingestion; PII masking, RBAC, lineage", "contracts/ · governance.sql"),
               ("GenAI, analytics, ML", "RAG copilot; MCP server; feature pipelines", "Fireside story"),
               ("Flink, specifically", "Not in production. Here: real Flink 1.20 in CI, identical windows to the Python job", "flink/job/")],
              ["The role asks for", "What I've done", "Here"])))
    s.append(slide("03 · Six streaming problems", "What Kafka really delivers", f"A week of a travel site: {n(sum(G['records'].values()))} records on {sum(G['partitions'].values())} partitions. Each problem is planted, handled and tested.",
        table([(a, b, c) for a, b, c in PROBLEMS], ["Problem", "In the run", "What handles it"])))
    s.append(code_slide("04 · The stream job · Python, with a Java twin", "Event time, decided by the watermark",
        "The watermark is the minimum over active partitions, less the out-of-orderness. A quiet partition stops holding it back; when every partition is quiet, processing time carries it.",
        "src/nexus/stream.py", "Flink semantics", code("src/nexus/stream.py", "    def _watermark(", "    def _tick("),
        side(("The rules", ["<b>Fire</b> when the watermark passes a window's end; <b>re-fire</b> on a late record within the hour; past it, the <b>side output</b>.",
                            "<b>Dedupe</b> on event_id, kept 24 hours. <b>Dead letters</b> for what can't be read, with the reason.",
                            "Offsets, windows, dedupe state and the ledger are <b>checkpointed together</b>; sinks are keyed, so a restore re-sends without changing anything."]),
             ("In the run", [f"A TaskManager lost at 09:17: restored from the 09:10 checkpoint, <b>{D['replay']['re_emitted']}</b> windows re-sent, sink <b>{'identical' if D['replay']['identical'] else 'different'}</b>.",
                             f"Ledger, page_views: read {n(S['page_views']['ledger']['read'])} = {n(S['page_views']['ledger']['accepted'])} counted + {S['page_views']['ledger']['duplicates']} duplicates + {S['page_views']['ledger']['late_dropped']} side output."])),
        formula="wm = max(wm, min<sub>active p</sub> max_event(p) − ooo) · fire ⟺ end ≤ wm · drop ⟺ end + lateness ≤ wm"))
    s.append(code_slide("04 · The stream job · the rules", "Dedupe, lateness, side output: the code",
        "One function decides every record: read it, drop a retry, count it, re-fire a late window, or set it aside for the batch.",
        "src/nexus/stream.py", "Python · the reference", code("src/nexus/stream.py", "    def _process(", "    def _apply_version(", limit=29),
        side(("What it decides", [f"<b>{S['page_views']['ledger']['duplicates']}</b> retries dropped by id; <b>{S['page_views']['ledger']['late_updates']}</b> late re-fires; <b>{S['page_views']['ledger']['late_dropped']}</b> sent to the side output.",
                                  "A record that can't be read raises <b>Poison</b> unless the dead-letter topic is on: the incident in one line."]),
             ("Checked three ways", ["The Flink job and the Java twin reach the same windows; the batch explains every difference."]))))
    s.append(code_slide("05 · The nightly batch · PySpark", "The batch that checks the stream",
        "Every record from the topic archive: parse both schema versions, keep the first arrival of each id, window by event time.",
        "src/nexus/spark_batch.py", f"PySpark {SP['version'] if SP else '3.5'}", code("src/nexus/spark_batch.py", "def recompute(", limit=34),
        side(("In the run", [f"page_views: <b>{n(SP['page_views']['windows'])}</b> windows · partner_clicks: <b>{n(SP['partner_clicks']['windows'])}</b> · identical to the plain-Python batch." if SP else "Runs in CI.",
                             "The poison pill is a parse failure here too: counted, never guessed."]),
             ("At Lifepal", ["Spark and SQL tuning took the nightly batch from 6 hours to 90 minutes and kept spend at $11k against a $19k forecast."]))))
    s.append(slide("05 · Stream first, batch checks", "Fast first. Right by morning.", "The nightly batch recomputes every window from the whole topic. Every difference must be explained by the stream's side output.",
        table([(t, n(r["windows"]), n(r["equal"]), r["explained_by_side_output"], f"<b>{r['unexplained']}</b>", n(r["stream_events"]), n(r["batch_events"]), r["late_events"],
                ("identical" if SP and SP[t]["identical"] else "—")) for t, r in R.items()],
              ["Topic", "Windows", "Equal", "Explained", "Unexplained", "Stream", "Batch", "Side output", "Spark agrees"])
        + f'''<div class="grid two" style="margin-top:6mm"><div class="callout"><b>The equation the run checks:</b> batch events − stream events = side output, for every topic and every window. The PySpark batch{f" (Spark {SP['version']})" if SP else ""} produces the same windows as the plain-Python one.</div>
        <div class="callout pink"><b>Volume hold:</b> {hold['hour'][:13].replace('T', ' ')}:00 had {n(hold['views'])} views where {hold['mean']} is usual (z = {hold['z']}). Held from Gold; three users over 120 views an hour removed; re-checked; released.</div></div>'''))
    s.append(code_slide("06 · Snowflake and BigQuery from one contract", "Write it once. Mask it everywhere.",
        "contracts/sources.yml names the personal fields. The DDL, MERGE, masking and erasure for both warehouses are generated from it and committed; CI fails if they drift.",
        "warehouse/snowflake/reviews_current.sql", "generated", code("warehouse/snowflake/reviews_current.sql", "MERGE INTO"),
        side(("Same product, two dialects", [f"<b>{r['concern']}:</b> {html.escape(r['snowflake'])} · {html.escape(r['bigquery'])}" for r in D["warehouse"]["dialects"][3:6]]),
             ("Checked, not only rendered", [f"Gold loaded with the same upsert semantics; <b>{passed}/{len(checks)}</b> checks pass, including masking for every personal field and the erasure ({D['warehouse']['erased_rows_removed']} rows removed)."]),
             ("At Lifepal", ["PII masking, role-based access and lineage in one governed layer; certified models behind an MCP server for agents."]))))
    M = D["warehouse"]["model"]
    s.append(slide("06 · Modeling and governance", "Grain first. Rules travel with the data.",
        "Kafka topics land in raw tables that keep every field and where it came from; Gold products state their grain and key; one contract names the personal fields, the rules and the SLAs.",
        table([(f"<b>{g['table']}</b>", g["grain"], ", ".join(g["key"]), f"PARTITION BY DATE({g['partition']}) CLUSTER BY {', '.join(g['cluster'])}" if g["partition"] else f"CLUSTER BY {', '.join(g['cluster'])}",
                n(D["warehouse"]["rows"][g["table"]]), f"tier {g['tier']} · {g['sla_minutes']} min") for g in M["gold"]],
              ["Gold product", "Grain", "Key (the MERGE)", "BigQuery layout", "Rows", "SLA"])
        + f'''<div class="grid three" style="margin-top:6mm">
          <div class="card"><h3>Personal data</h3><p>{", ".join(c for r in M["raw"] for c in (r["table"] + "." + x["name"] for x in r["columns"] if x["pii"]))}: a policy tag in BigQuery and a masking policy in Snowflake, from one line of the contract; a check per field.</p></div>
          <div class="card"><h3>Quality gates</h3><p>{passed}/{len(checks)} checks pass: rules per product from quality.yml, masking, and the erasure. A volume gate held the scraper hour before Gold.</p></div>
          <div class="card"><h3>Erasure and schema change</h3><p>A tombstone deletes the review in raw and Gold in both warehouses ({D["warehouse"]["erased_rows_removed"]} rows). Schema v2 is flattened into raw; Gold keeps its grain.</p></div></div>'''))
    s.append(code_slide("06 · Modeling · SQL from the contract", "One MERGE, generated for both warehouses",
        "The key from the contract becomes the MERGE condition; a late re-fire updates one row; a tombstone deletes.",
        "src/nexus/warehouse.py", "renders BigQuery + Snowflake", code("src/nexus/warehouse.py", "def _merge(", "def _governance(", limit=24),
        side(("Generated, not hand-written", [f"<b>{len(D['warehouse']['files'])}</b> SQL files from one contract; CI fails if they drift."]),
             ("Governance in the same file", ["Personal fields get a policy tag in BigQuery and a masking policy in Snowflake; erasure SQL is generated for both."]))))
    s.append(slide("07 · SLAs and an incident", f"{I['product']} late for {I['minutes']['total']} minutes", "Freshness is measured from the completeness the job publishes, so a quiet night is not an incident. One thing was all week.",
        f'''<div class="grid two"><div>''' + table([(f"<b>{hm(t['at'])}</b>", t["who"], html.escape(t["what"])) for t in I["timeline"]], ["UTC", "Who", "What"]) + f'''</div>
        <div><div class="grid two" style="grid-template-columns:1fr 1fr">
          <div class="card"><div class="big">{I['minutes']['to_detect']} min</div><p>to detect: freshness had to cross {I['sla_minutes']} minutes first</p></div>
          <div class="card"><div class="big p">{I['minutes']['would_detect_with_restart_alert']} min</div><p>with a restart alert: {I['restarts']} restarts said it early</p></div></div>
          <div class="callout pink" style="margin-top:4mm"><b>Root cause:</b> a record the job couldn't parse was treated as a reason to stop; a restart reads the same offset again. <b>Fix:</b> dead-letter by default in the shared template (done), page on restarts, alert on DLQ depth, a producer contract test.</div>
          <p class="src" style="margin-top:3mm">Error budget for the week: {sla['partner_revenue_15m']['budget_samples']} late samples allowed at 99.9%; the incident used {sla['partner_revenue_15m']['late_samples']}. {I['windows_delayed']} windows late, none wrong; one record in the DLQ.</p></div></div>'''))
    s.append(code_slide("07 · SLAs · the monitor", "Freshness, measured like a consumer feels it",
        "Age is now minus the completeness the job publishes. Sampled every five minutes; the incident record is built from the same samples.",
        "src/nexus/sla.py", "Python", code("src/nexus/sla.py", "def age_series(", "def lag_by_partition(", limit=30),
        side(("In the run", [f"partner_revenue_15m: on time <b>{sla['partner_revenue_15m']['on_time']:.2%}</b>; one breach, {I['minutes']['total']} minutes.",
                             f"Paged at {hm(I['detected'])}; a restart alert would have paged at {hm(I['timeline'][1]['at'])}."]),
             ("Why completeness", ["Measuring from the newest non-empty window would page every quiet night."]))))
    fv, fp = (FL or {}).get("location_views_hourly", {}), (FL or {}).get("partner_revenue_15m", {})
    s.append(code_slide("08 · Apache Flink 1.20 · DataStream API", "The same job, on Flink",
        "Parse with a dead-letter output, dedupe in keyed state, per-partition watermarks, event-time windows with allowed lateness, a side output for what's too late.",
        "flink/job/.../NexusJobs.java", "Flink 1.20.1 · Java 17", code("flink/job/src/main/java/nexus/flink/NexusJobs.java", "    public static Outputs build(", "    }\n}", limit=24),
        side(("On a MiniCluster, in CI", [f"<b>{n(fv.get('windows', 0))}</b> windows, <b>{'identical' if fv.get('identical_to_python') else 'different'}</b> to the Python job: {fv.get('duplicates')} duplicates, {fv.get('late_dropped')} too late, {n(fv.get('firings', 0))} firings.",
                                          f"partner_clicks: the poison pill <b>dead-lettered</b> (partition 3, offset 171), revenue windows <b>{'identical' if fp.get('identical_to_batch') else 'different'}</b> to the batch."]),
             ("One honest difference", ["Python carries the watermark on processing time when every partition is quiet; Flink's idleness doesn't. So at 3 a.m. Flink kept one late click the Python job sent to the side output. Both are explained."]),
             ("Stated plainly", ["Real Flink, run locally, not on a production cluster. At Lifepal my streaming was Kafka + Debezium with Spark."])),
        formula="records → Parse ⟶ DLQ · keyBy(id) → Dedupe(TTL 24 h) · keyBy(group) → Tumbling(1 h) → allowedLateness(1 h) ⟶ late"))
    s.append(code_slide("08 · Apache Flink · production", "On Kafka, with the incident's lessons built in",
        "NexusKafka runs the same topology from KafkaSource to transactional Kafka sinks. Compiled in CI; there is no cluster behind a portfolio.",
        "flink/job/.../NexusKafka.java", "KafkaSource · KafkaSink", code("flink/job/src/main/java/nexus/flink/NexusKafka.java", "        StreamExecutionEnvironment env", "        env.execute(", limit=26),
        side(("What the incident changed", ["<b>Dead letters</b> to <code>dlq.partner_clicks</code>, never a crash loop.",
                                            "<b>failureRateRestart(3, 10 min)</b>: three restarts, then fail and page. 74 restarts can't happen again.",
                                            "<b>Exactly-once</b> checkpoints every 10 minutes and transactional sinks: a restore re-sends nothing twice."]),
             ("Watermarks", ["Per split from the source: <b>forBoundedOutOfOrderness(ooo).withIdleness(5 min)</b>, so a quiet partition can't hold the job back."]))))
    s.append(code_slide("08 · Java", "Python first. Java, checked.",
        "The page_views job, in plain JDK 17 with no framework. CI compiles it, runs it on the same week, and fails if a single window differs from the Python job.",
        "java/src/nexus/WindowJob.java", "JDK 17 · no dependencies", code("java/src/nexus/WindowJob.java", "    void process(", "    void tick("),
        side(("In the run", [f"<b>{n(J['windows'])}</b> windows, <b>{'identical' if J['identical'] else 'different'}</b> to the Python job, in {J['millis']} ms." if J else "Runs in CI."]),
             ("Stated plainly", ["Kafka, Spark and Python are production experience. At Lifepal the streaming was Kafka + Debezium with Spark, not Flink. Snowflake is from Deloitte; BigQuery I haven't run in production, so its SQL here is mapped from Snowflake and Databricks.",
                                 "Java is the part I'd ramp up on. This is how I'd start: the same semantics, tested against a reference I trust."]))))
    s.extend(why_slides())
    s.append(slide("09 · My first 90 days", "How I'd start in Data Platform", "Listen first, then own something small end to end, then make the template better for everyone.",
        '<div class="grid three">' + "".join(f'<div class="card"><div class="kicker">{a}</div><h3>{b}</h3><ul>{"".join(f"<li>{x}</li>" for x in c)}</ul></div>' for a, b, c in [
            ("Days 1–30", "Learn the estate", ["Read the SLAs and the last ten incidents", "Shadow on-call; map pipelines to owners", "Ship a small fix in Java and one in Python"]),
            ("Days 31–60", "Own a pipeline", ["Take one tier-1 pipeline end to end", "Freshness from completeness, not last write", "A runbook a new joiner can follow"]),
            ("Days 61–90", "Improve the template", ["Dead letters and restart alerts by default", "Contract → Snowflake and BigQuery SQL", "Stream/batch reconciliation as a standard check"])]) + '</div>'
        f'<div class="proof" style="margin-top:8mm"><span class="kicker">Questions I\'d bring</span><span>Which pipelines page most, and why?</span><span>How are SLAs measured today?</span><span>Where do Snowflake and BigQuery each fit?</span><span>How much of streaming is Flink vs Spark?</span></div>'))
    s.append('</body></html>')
    return "".join(s)


def one_page():
    return head("Nexus for Tripadvisor · One page", "one_page.css") + f'''<div class="page">
<div class="kicker">One page · prepared for Tripadvisor Data Platform · Senior Data Engineer</div>
<div class="top"><div><h1>Every event counted once, <b>on time, and checked.</b></h1>
  <div class="tl">Kafka in, Flink in the middle, BigQuery out: NEXUS, the platform I built at Lifepal, applied to a travel site's events.</div>
  <p>Kafka topics → an event-time stream job (Python, with a Java twin) → a nightly Spark batch that checks it → one contract rendered to Snowflake and BigQuery → SLAs with an incident and its postmortem. Synthetic data; everything runs from <b>{SITE}</b>.</p></div>
  <div><div class="built"><div><div class="k2">BUILT IN</div><img src="lifepal.png" alt="Lifepal" style="height:30px"></div><span class="arrow">→</span><div><div class="k2">PRESENTED TO</div><span class="codat" style="font-size:16pt"><img src="tripadvisor-owl.png" alt="" style="height:24px;display:inline;vertical-align:-5px;margin-right:5px">Tripadvisor</span></div></div></div></div>
<div class="sec"><div class="sh"><b>Proven at Lifepal</b><i>from my CV</i></div><div class="proof"><span><b>24h → &lt;5m</b> reporting latency</span><span><b>6h → 90m</b> nightly batch</span><span><b>$11k</b> vs $19k forecast</span><span><b>500+</b> quality checks</span><span><b>−60%</b> data-trust escalations</span><span><b>3–5</b> engineers led, 2 promoted</span></div></div>
<div class="sec"><div class="sh"><b>The last run</b><i>a week, 5–11 October</i></div><div class="grid four">
  <div class="card"><div class="big">{n(sum(G['records'].values()))}</div><p>Kafka records, {sum(G['partitions'].values())} partitions, three topics</p></div>
  <div class="card"><div class="big">0</div><p>stream/batch differences unexplained, of {n(R['page_views']['windows'] + R['partner_clicks']['windows'])} windows</p></div>
  <div class="card"><div class="big">{passed}/{len(checks)}</div><p>warehouse checks pass, masking and erasure included</p></div>
  <div class="card"><div class="big">{I['minutes']['total']} min</div><p>the one SLA breach; {I['restarts']} restarts; postmortem written</p></div></div></div>
<div class="sec"><div class="sh"><b>Six problems, planted and caught</b><i>each tested</i></div>''' + table(PROBLEMS, ["Problem", "In the run", "What handles it"]) + f'''</div>
<div class="sec cols"><div><div class="sh"><b>The role, and where it shows</b><i>stated plainly</i></div>''' + table([
        ("Snowflake · BigQuery", "warehouse/ from one contract"), ("SLAs · RCA", "sla.py, the incident page"), ("Kafka · Spark", "stream.py, spark_batch.py"),
        ("Java", "WindowJob.java, identical to Python"), ("Flink", "flink/job: real Flink 1.20 in CI, identical windows; not in production, said so")], ["Ask", "Here"]) + '''</div>
<div><div class="sh"><b>Questions I'd bring</b><i>for Data Platform</i></div><div class="plain"><b>1.</b> Which pipelines page most, and why?<br><b>2.</b> How are freshness SLAs measured today?<br><b>3.</b> Where do Snowflake and BigQuery each fit?<br><b>4.</b> How is streaming split between Flink and Spark?<br><b>5.</b> What would a great first 90 days look like?</div></div></div>
<p class="src" style="margin-top:5mm">R. Firman Insan Muhammad (Vim) · Senior Data Engineer · London · right to work in the UK, no sponsorship · firman.insan.m@gmail.com · ''' + SITE + '''. Synthetic data; an independent application, not affiliated with or endorsed by Tripadvisor.</p>
</div></body></html>'''


def companion():
    v = (FL or {}).get("location_views_hourly", {})
    led = S["page_views"]["ledger"]
    return head("Nexus for Tripadvisor · Companion note", "companion.css") + f"""<div class="page">

<div class="kicker">Companion note · prepared for Data Platform · Senior Data Engineer</div>
<div class="top">
  <div>
    <h1>Different market, <b>same job.</b></h1>
    <div class="tl">Why Lifepal's insurance marketplace maps onto Tripadvisor's travel marketplace. In Lifepal, the other side was the insurers; at Tripadvisor, it's hotels, restaurants, experiences and the booking partners behind every "check prices" click.</div>
    <p>Both businesses sit in the middle. Lifepal helped people compare insurance and sent them on to the insurers that cover them; Tripadvisor helps travellers compare places and sends them on to the partners that sell the stay. In both, events arrive late or twice, partners describe the same thing in their own shapes, records change after the fact, people can ask to be forgotten, and partner revenue depends on every click being counted once. The data problems that follow are the ones NEXUS was built to solve.</p>
  </div>
  <div>
    <div class="tiles"><span>Kafka</span><span>Flink</span><span>Spark</span><span>BigQuery</span><span>Snowflake</span></div>
    <div class="built"><div><div class="k2">Built in</div><img src="lifepal.png" alt="Lifepal"></div><span class="arrow">→</span><div><div class="k2" style="text-align:right">Presented to</div><span class="codat"><img src="tripadvisor-owl.png" alt="" style="height:22px;display:inline;vertical-align:-3px;margin-right:4px">Tripadvisor</span></div></div>
    <div class="k2" style="margin-top:5px;text-align:right;color:var(--violet)">Nexus · Lifepal best practices</div>
  </div>
</div>

<div class="sec"><div class="sh"><b>One flow, two businesses</b><i>Tripadvisor side from public material</i></div>
  <div class="spheres" style="grid-template-columns:repeat(5,1fr)">
    <div class="sph"><div class="ball"><small>01</small><b>Visit</b></div><div class="lbl"><b>Lifepal</b> · a customer comes to compare cover</div><div class="lbl c"><b>Tripadvisor</b> · a traveller searches a city</div></div>
    <div class="sph"><div class="ball"><small>02</small><b>Compare</b></div><div class="lbl"><b>Lifepal</b> · products, prices, insurer terms</div><div class="lbl c"><b>Tripadvisor</b> · places, reviews, rankings</div></div>
    <div class="sph"><div class="ball"><small>03</small><b>Click</b></div><div class="lbl"><b>Lifepal</b> · a quote or application to an insurer</div><div class="lbl c"><b>Tripadvisor</b> · "check prices" to a booking partner</div></div>
    <div class="sph"><div class="ball"><small>04</small><b>Change</b></div><div class="lbl"><b>Lifepal</b> · CDC and late insurer statuses</div><div class="lbl c"><b>Tripadvisor</b> · late app events, review edits, schema changes</div></div>
    <div class="sph"><div class="ball"><small>05</small><b>Earn</b></div><div class="lbl"><b>Lifepal</b> · the insurer binds; commission</div><div class="lbl c"><b>Tripadvisor</b> · the partner pays for the click</div></div>
  </div>
</div>

<div class="sec"><div class="sh"><b>The closest parallel: partner clicks</b><i>a marketplace sends people to partners</i></div>
  <div class="b2b">
    <div>
      <div class="who" style="color:var(--violet)">Lifepal · person → insurer</div>
      <div class="lane">
        <div><b>Visitor</b>Comes to compare insurance, on web or app.</div>
        <div><b>Compares</b>Products and prices from many insurers.</div>
        <div><b>Applies</b>A quote or application goes to one insurer.</div>
        <div><b>Decision</b>Quote, bind, renew. Statuses arrive late, for days.</div>
      </div>
      <div class="who" style="color:var(--pink);margin-top:8px">Tripadvisor · traveller → partner</div>
      <div class="lane ta">
        <div><b>Traveller</b>Searches, reads, comes back on another device.</div>
        <div><b>Reads</b>Reviews and rankings; every view is an event.</div>
        <div><b>Clicks</b>"Check prices" sends them to a partner.</div>
        <div><b>Revenue</b>The partner pays; app events can land hours late.</div>
      </div>
    </div>
    <div>
      <table class="map b2bt">
        <tr><th>The same problem</th><th>How I handled it at Lifepal</th><th>Where it shows here</th></tr>
        <tr><td><b>Events twice, or late</b></td><td>Kafka + Debezium CDC, merged so the newest wins and every reload is safe to rerun.</td><td>Dedupe on id, watermarks, a side output: {led['duplicates']} retries dropped, {led['late_dropped']} late events explained by the batch.</td></tr>
        <tr><td><b>Partners in their own shapes</b></td><td>A config-driven framework: a new source was configuration, not code.</td><td>One contract reads schema v1 and v2; an unknown currency is a dead letter, never a guess.</td></tr>
        <tr><td><b>Pipelines people depend on</b></td><td>SLAs, 500+ checks, a root-cause review after every incident; escalations −60%.</td><td>Freshness from completeness; the incident found in {I['minutes']['to_detect']} minutes, {I['minutes']['would_detect_with_restart_alert']} with the new alert.</td></tr>
        <tr><td><b>One number for every team</b></td><td>Kimball models and a dbt semantic layer; ad-hoc requests −60%.</td><td>Gold products with a stated grain and key, rendered to BigQuery and Snowflake from one file.</td></tr>
      </table>
      <p class="src" style="margin-top:5px">Described at the level I can share publicly; I'm happy to walk through the details in conversation.</p>
    </div>
  </div>
</div>

<div class="sec"><div class="sh"><b>What Tripadvisor runs</b><i>as the Data Platform roles list it · data here is synthetic</i></div>
  <div class="ints">
    <div class="card"><div class="kicker">Streaming</div><p><span class="chip">Kafka</span><span class="chip">Flink</span><span class="chip">event time</span><span class="chip">SLAs</span></p><p style="margin-top:4px">Here: three Kafka topics, six planted problems, and a Flink 1.20 job that reaches {n(v.get('windows', 0))} windows identical to the Python reference.</p></div>
    <div class="card"><div class="kicker">Batch and warehouses</div><p><span class="chip">Spark</span><span class="chip">BigQuery</span><span class="chip">Snowflake</span><span class="chip">Hive</span></p><p style="margin-top:4px">Here: a PySpark batch that checks the stream; SQL for BigQuery and Snowflake from one contract.</p></div>
    <div class="card"><div class="kicker">Languages</div><p><span class="chip">Java</span><span class="chip">Python</span><span class="chip">SQL</span></p><p style="margin-top:4px">Here: the Flink job and a plain-Java twin in Java 17, the reference in Python.</p></div>
    <div class="card"><div class="kicker" style="color:var(--violet2)">Scale</div><div class="big" style="color:#fff;font-size:20pt">Petabytes</div><p>of data behind GenAI, analytics and ML, as the role describes the platform.</p></div>
  </div>
</div>

<div class="sec"><div class="sh"><b>Concept map</b><i>same grain, different names</i></div>
  <table class="map">
    <tr><th>Lifepal · insurance marketplace</th><th>Tripadvisor · travel marketplace (my reading)</th><th>Grain</th><th>Shared data problem</th></tr>
    <tr><td><b>Insurer partner</b><span class="chip">consumer insurers</span><span class="chip">partner files</span></td><td><b>Booking partner</b><span class="chip dark">check-prices clicks</span><span class="chip dark">CPC</span></td><td class="g">one row per click</td><td>Count every click once; revenue per partner reconciles</td></tr>
    <tr><td><b>Product page views</b><span class="chip">web and app</span></td><td><b>Location page views</b><span class="chip dark">hotel</span><span class="chip dark">restaurant</span><span class="chip dark">attraction</span></td><td class="g">location × hour</td><td>Late and duplicated app events</td></tr>
    <tr><td><b>Application status</b><span class="chip">late insurer updates</span></td><td><b>Review versions</b><span class="chip dark">submitted</span><span class="chip dark">moderated</span><span class="chip dark">edited</span></td><td class="g">one row per review, latest version</td><td>Newest version wins; a tombstone can't be undone</td></tr>
    <tr><td><b>Insurer format changes</b><span class="chip">new fields mid-year</span></td><td><b>App release changes a field</b><span class="chip dark">price {{amount, currency}}</span></td><td class="g">raw record × schema version</td><td>Read both versions as one, before anyone reads it</td></tr>
    <tr><td><b>PII masking, role-based access</b><span class="chip">customer data</span></td><td><b>Reviewer and user identifiers</b><span class="chip dark">policy tags</span><span class="chip dark">erasure</span></td><td class="g">column × role</td><td>Personal data never in the clear; erasure everywhere</td></tr>
    <tr><td><b>Insurer and finance reporting</b><span class="chip">one bind rate</span></td><td><b>Partner and internal reporting</b><span class="chip dark">revenue</span><span class="chip dark">views</span></td><td class="g">one product per audience</td><td>Many audiences, one number</td></tr>
    <tr><td><b>RAG copilot, MCP agents</b><span class="chip">1,000+ outcomes a day</span></td><td><b>GenAI on the platform</b><span class="chip dark">certified products</span></td><td class="g">one certified metric</td><td>Agents read the same truth as people, with permissions</td></tr>
  </table>
  <p class="src">Lifepal examples are from my own work. Tripadvisor examples come from the role, public information, and the synthetic data in this project.</p>
</div>

<div class="sec"><div class="sh"><b>What carries over directly</b><i>proven at Lifepal</i></div>
  <div class="carry">
    <div class="card"><h3>Exactly once, from at-least-once</h3><p><b>Lifepal:</b> CDC merged so the newest wins and reloads are safe to rerun.</p><div class="here"><b>Here:</b> a lost TaskManager restored from a checkpoint; the sink {'identical' if D['replay']['identical'] else 'different'}.</div></div>
    <div class="card"><h3>Reliability as a feature</h3><p><b>Lifepal:</b> SLAs, runbooks, RCA after every incident; escalations −60%.</p><div class="here"><b>Here:</b> a postmortem written by the run, and a restart alert that pages in {I['minutes']['would_detect_with_restart_alert']} minutes.</div></div>
    <div class="card"><h3>Spark, fast and cheap</h3><p><b>Lifepal:</b> nightly batch 6 h → 90 min; $11k a month against $19k.</p><div class="here"><b>Here:</b> a PySpark batch, identical to the reference, that checks the stream.</div></div>
    <div class="card"><h3>One definition per metric</h3><p><b>Lifepal:</b> a dbt semantic layer for five teams; ad-hoc requests −60%.</p><div class="here"><b>Here:</b> one contract, rendered to BigQuery and Snowflake, checked in CI.</div></div>
    <div class="card"><h3>Tests before trust</h3><p><b>Lifepal:</b> 500+ checks in production.</p><div class="here"><b>Here:</b> {json.loads((ROOT / 'assets/data/tests.json').read_text())['passed']} tests, Flink and Java parity included; {passed}/{len(checks)} warehouse checks pass.</div></div>
  </div>
</div>

<div class="sec"><div class="sh"><b>What is different, and how I would adapt</b><i></i></div>
  <table>
    <tr><th>Difference</th><th>Lifepal</th><th>Tripadvisor</th><th>How I would adapt</th></tr>
    <tr><td class="k">Stream engine</td><td>Kafka + Debezium with Spark</td><td>Kafka with Flink</td><td>The semantics are already written on Flink 1.20 here; learn the team's operators, state backend and deployment</td></tr>
    <tr><td class="k">Language</td><td>Python in production</td><td>Java and Python in production</td><td>Start where a Java twin tested against a reference is enough; pair early on existing services</td></tr>
    <tr><td class="k">Warehouse</td><td>Databricks and Delta with dbt; Snowflake at Deloitte</td><td>Snowflake, BigQuery and Hive</td><td>One contract for all of them; learn BigQuery's cost model (bytes scanned, slots) first</td></tr>
    <tr><td class="k">Scale</td><td>A high-traffic national marketplace</td><td>Petabyte scale, every data user in the company</td><td>Same contracts and tests; partitioning, clustering and incremental builds from day one</td></tr>
    <tr><td class="k">Who reads it</td><td>Internal teams and insurer partners</td><td>Analytics, ML, GenAI and every product team</td><td>Products with an owner, a grain, a published freshness and an SLA; breaking changes need approval</td></tr>
  </table>
</div>

<div class="sec plain"><b>From NEXUS, built in Lifepal.</b> The practices on this page are NEXUS best practices from my production work at Lifepal. <b>Stated plainly.</b> Lifepal outcomes come from my own record of the work. The Tripadvisor side is built from outside, from the role and public information; every page view, click, review, partner and location in the demo is synthetic. No Tripadvisor systems or data were used. My production streaming was Kafka with Spark, not Flink; BigQuery I haven't run in production.<br><b>Open questions for week one:</b> which pipelines page most and why, how freshness SLAs are measured today, how Snowflake, BigQuery and Hive split across teams, and how streaming splits between Flink and Spark.</div>
<div class="sec footline"><span>R. Firman Insan Muhammad (Vim) · Senior Data Engineer · London · firman.insan.m@gmail.com</span><span>Companion to <b>Nexus for Tripadvisor</b> · 21 October 2026</span></div>
</div></body></html>"""


for name, fn in (("deck.html", deck), ("one_page.html", one_page), ("companion.html", companion)):
    (HERE / name).write_text(fn())
print("wrote deck.html, one_page.html, companion.html")
