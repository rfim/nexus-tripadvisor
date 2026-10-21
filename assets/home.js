// Home: fill every section from the last run (assets/data/site.json).
(async () => {
  const d = await NX.site();
  const $ = (id) => document.getElementById(id);
  const G = d.generator, P = G.planted, S = d.streams, R = d.reconcile, I = d.incident;
  const sum = (o) => Object.values(o).reduce((a, b) => a + b, 0);
  const hm = (iso) => iso.slice(11, 16);
  const day = (iso) => new Date(iso).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: "UTC" });
  const ok = (b, yes = "pass", no = "fail") => `<span class="verdict ${b ? "ok" : "bad"}">${b ? yes : no}</span>`;

  // ---------- the path ----------
  const FV = d.flink && d.flink.location_views_hourly;
  const k = { partitions: sum(G.partitions), records: NX.int(sum(G.records)),
              flink_line: FV ? `Flink ${d.flink.flink}: ${NX.int(FV.windows)} windows, identical to the Python job` : "Flink 1.20 job, checked in CI",
              kafka_line: `${NX.int(sum(G.records))} records · ${sum(G.partitions)} partitions · six planted problems`,
              unexplained: R.page_views.unexplained + R.partner_clicks.unexplained };
  document.querySelectorAll("[data-k]").forEach((el) => (el.textContent = k[el.dataset.k]));

  // ---------- six problems ----------
  const hold = d.volume.holds[0];
  const problems = [
    ["page_views", "SDK retries", `${day(P.retries.from)}, ${hm(P.retries.from)}–${hm(P.retries.to)}`,
      `The web SDK timed out and re-sent: <b>${G.retried}</b> events arrived twice.`,
      `Dedupe on event_id, 24 h of checkpointed state: <b>${S.page_views.ledger.duplicates}</b> dropped.`],
    ["page_views", "An offline upload", `${day(P.offline_upload.delivered_at)}, ${hm(P.offline_upload.delivered_at)}`,
      `${P.offline_upload.sessions} iOS sessions from a flight flushed at once: <b>${G.late_offline}</b> events, 2–8 hours late.`,
      `Past the lateness hour, so to the side output. The batch counts them: <b>${R.page_views.explained_by_side_output}</b> windows differ, all explained.`],
    ["partner_clicks", "A schema change", `${day(P.schema_v2.from)}, ${hm(P.schema_v2.from)}`,
      `App ${P.schema_v2.app_version} sends <code>price {amount, currency}</code> instead of <code>cpc_gbp</code>: <b>${G.v2_records}</b> records, in GBP, EUR and USD.`,
      `A registry would reject it; one reader upcasts both to GBP. Unknown currency → dead letter.`],
    ["partner_clicks", "A poison pill", `${day(P.poison.at)}, ${hm(P.poison.at)}`,
      `One truncated record (${I.partition}/${I.offset}). With no dead-letter topic the job crash-looped <b>${I.restarts}</b> times.`,
      `Paged at ${hm(I.detected)}; the ${hm(I.mitigated)} fix dead-letters it. Nothing else lost.`],
    ["page_views", "A scraper", `${day(P.bots.from)}, ${hm(P.bots.from)}`,
      `Three users, <b>${NX.int(hold.views)}</b> views in the quietest hour, where ${hold.mean} is usual.`,
      `Held from Gold (threshold ${hold.threshold}); ${d.volume.bots.length} users over ${d.volume.investigation[0].max_per_hour} views an hour removed; released.`],
    ["reviews", "Erasure, then a stale version", `${day(P.erasure.at)}, ${hm(P.erasure.at)}`,
      `A reviewer is erased; two minutes later a replay re-sends their old edit with a new id.`,
      `Version 3 can't overwrite the version-4 tombstone (<b>${S.reviews.ledger.stale_versions}</b> rejected). Deleted in both warehouses.`],
  ];
  $("problem-cards").innerHTML = problems.map(([t, title, when, what, how], i) => `<div class="card ${i % 2 ? "top-p" : "top-v"}">
    <div class="kicker" style="color:${i % 2 ? "var(--pink)" : "var(--violet2)"}">${i + 1} · ${t} · ${when}</div>
    <h3 style="margin:.35rem 0;color:#fff">${title}</h3><p>${what}</p><p class="how" style="margin-top:.6rem;padding-top:.6rem;border-top:1px dashed #2e5f55;color:#e8f5f1">${how}</p></div>`).join("");

  const rv = d.trace.reviews;
  $("story").innerHTML = NX.table(rv.map((x) => ({ arrived: x.arrived.replace("T", " ").slice(0, 19), version: x.version, op: x.op,
    rating: x.rating ?? "—", event_id: x.event_id, decision: x.decision })), null,
    { decision: (v) => `<span class="verdict ${v.startsWith("stale") ? "warn" : "ok"}">${NX.esc(v)}</span>` });

  // ---------- the last run ----------
  const tiles = [
    [NX.int(sum(G.records)), "Kafka records in a week", `${Object.keys(G.records).length} topics · ${sum(G.partitions)} partitions`],
    [d.replay.identical ? "identical" : "differs", "sink after a lost TaskManager", `${d.replay.restored_from}; ${d.replay.re_emitted} windows re-sent, none changed`],
    [`${R.page_views.unexplained + R.partner_clicks.unexplained}`, "stream/batch differences unexplained",
      `${NX.int(R.page_views.windows + R.partner_clicks.windows)} windows compared`],
    [`${I.minutes.total} min`, "the one SLA breach, start to recovery", `${I.restarts} restarts · detected after ${I.minutes.to_detect} min`],
  ];
  $("run-tiles").innerHTML = tiles.map(([n, l, s], i) => `<div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="big ${i % 2 ? "p" : ""}">${n}</div>
    <h3 style="margin:.4rem 0 .2rem">${l}</h3><p>${NX.esc(s)}</p></div>`).join("");

  const L = [["accepted", "counted"], ["duplicates", "duplicates"], ["late_dropped", "side output"], ["dead_letters", "dead letters"], ["stale_versions", "stale versions"]];
  $("ledgers").innerHTML = Object.entries(S).map(([t, s]) => `<div style="margin-top:.8rem"><b class="mono" style="font-size:.82rem">${t}</b>
    <div class="ledger"><div><b>${NX.int(s.ledger.read)}</b><small>read</small></div>${L.map(([key, lbl], i) =>
      `<div><b>${i ? "+ " : "= "}${NX.int(s.ledger[key])}</b><small>${lbl}</small></div>`).join("")}</div></div>`).join("");

  const checks = d.warehouse.checks;
  $("checks").innerHTML = `<div class="tablewrap wrap"><table><tr><th>table</th><th>check</th><th class="num">failing</th><th></th></tr>${checks.map((c) =>
    `<tr><td class="mono" style="font-size:.78rem">${NX.esc(c.table)}</td><td class="mono" style="font-size:.78rem">${NX.esc(c.check)}</td>
     <td class="num">${c.failing}</td><td>${ok(c.status === "pass")}</td></tr>`).join("")}</table></div>`;

  $("incident").innerHTML = `<div class="kicker" style="color:var(--pink)">Incident · ${NX.esc(I.severity)}</div>
    <h3 style="margin:.35rem 0">${I.product} late for ${I.minutes.total} minutes</h3>
    <dl class="kv" style="margin:.6rem 0"><dt>started</dt><dd>${hm(I.started)} · record ${I.partition}/${I.offset}, ${I.raw_bytes} bytes of truncated JSON</dd>
      <dt>detected</dt><dd>${hm(I.detected)} by the SLA monitor (${I.minutes.to_detect} min)</dd>
      <dt>mitigated</dt><dd>${hm(I.mitigated)} · dead-letter topic on, restore from checkpoint</dd>
      <dt>recovered</dt><dd>${hm(I.recovered)} · ${I.windows_delayed} windows published late, none wrong</dd>
      <dt>lost</dt><dd>${NX.esc(I.data_lost)}</dd></dl>
    <p><a class="linkish" href="oncall.html">Replay it, with the postmortem →</a></p>`;

  const max = Math.max(...d.run.tasks.map((t) => t.start_s + t.duration_s));
  $("tasks").innerHTML = d.run.tasks.map((t) => `<div class="task"><span>${NX.esc(t.label)}</span>
    <span class="bar"><i class="${t.key === "java_twin" ? "java" : t.key === "spark_batch" ? "spark" : ""}" style="left:${t.start_s / max * 100}%;width:${Math.max(.6, t.duration_s / max * 100)}%"></i></span>
    <span class="num mono">${t.duration_s}s</span></div>`).join("") + `<p style="font-size:.8rem;margin-top:.5rem">${d.run.seconds}s end to end on a laptop-sized runner; Java and Spark steps in orange.</p>`;

  // ---------- reconciliation and volume ----------
  $("recon").innerHTML = `<div class="tablewrap"><table><tr><th>topic</th><th class="num">windows</th><th class="num">equal</th><th class="num">explained by side output</th>
    <th class="num">unexplained</th><th class="num">stream events</th><th class="num">batch events</th><th class="num">side output</th><th class="num">dead letters</th><th>Spark agrees</th></tr>
    ${Object.entries(R).map(([t, r]) => `<tr><td class="mono">${t}</td><td class="num">${NX.int(r.windows)}</td><td class="num">${NX.int(r.equal)}</td>
      <td class="num">${r.explained_by_side_output}</td><td class="num"><b>${r.unexplained}</b></td><td class="num">${NX.int(r.stream_events)}</td>
      <td class="num">${NX.int(r.batch_events)}</td><td class="num">${r.late_events}</td><td class="num">${r.dead_letters}</td>
      <td>${d.spark ? ok(d.spark[t].identical, "identical", "differs") : "—"}</td></tr>`).join("")}</table></div>
    <p style="font-size:.84rem;margin-top:.5rem">batch events − stream events = side output, for every topic. ${d.spark ? `The PySpark batch (Spark ${d.spark.version}) produced the same windows in ${d.spark.seconds}s.` : ""}</p>`;
  $("win-examples").innerHTML = NX.table(d.window_examples.map((w) => ({ window: w.window_start.slice(5, 16).replace("T", " "),
    location: w.location_id, stream: w.stream, batch: w.batch, "side output": w.side_output })));

  const hrs = d.volume.hours, vmax = Math.max(...hrs.map((h) => h[1])), W = 900, H = 200, bw = W / hrs.length;
  const held = new Set(d.volume.holds.map((h) => h.hour));
  $("volume").innerHTML = `<svg class="chart" viewBox="0 0 ${W} ${H + 24}" role="img" aria-label="Page views per hour for the week; one hour held">
    ${[0, .5, 1].map((f) => `<line class="grid" x1="0" x2="${W}" y1="${H - f * H}" y2="${H - f * H}"/><text class="ax" x="2" y="${H - f * H - 3}">${Math.round(f * vmax)}</text>`).join("")}
    ${hrs.map(([t, v], i) => `<rect x="${i * bw}" y="${H - v / vmax * H}" width="${bw - .6}" height="${v / vmax * H}" fill="${held.has(t) ? "var(--pink)" : "var(--violet2)"}" opacity="${held.has(t) ? 1 : .75}"><title>${t.slice(0, 13)}: ${v}</title></rect>`).join("")}
    ${hrs.filter((_, i) => i % 24 === 0).map(([t], j) => `<text class="ax" x="${j * 24 * bw + 2}" y="${H + 16}">${day(t)}</text>`).join("")}</svg>`;
  const inv = d.volume.investigation[0];
  $("bots").innerHTML = `<p style="font-size:.84rem;margin-top:.4rem">Held: ${hold.hour.slice(0, 13).replace("T", " ")}:00, ${NX.int(hold.views)} views (z = ${hold.z}). ${inv.users_in_hour} users in that hour; ${inv.top.slice(0, 3).map((u) => `<code>${u.user_hash}</code> ${u.views}`).join(", ")}. After removing them: ${d.volume.holds_after_filter.length} holds.</p>`;

  // ---------- warehouses ----------
  $("dialects").innerHTML = `<div class="tablewrap wrap"><table><tr><th>concern</th><th>BigQuery</th><th>Snowflake</th></tr>${d.warehouse.dialects.map((r) =>
    `<tr><td class="k">${r.concern}</td><td class="mono" style="font-size:.76rem"><b>${NX.esc(r.bigquery)}</b></td><td class="mono" style="font-size:.76rem">${NX.esc(r.snowflake)}</td></tr>`).join("")}</table></div>`;
  const files = ["partner_revenue_15m", "reviews_current", "governance", "erasure", "raw_reviews"];
  let cur = ["bigquery", "partner_revenue_15m"];
  const showSql = async () => {
    $("sql-tabs").querySelectorAll("button").forEach((b) => b.classList.toggle("on", b.dataset.d === cur[0] || b.dataset.f === cur[1]));
    $("sql").textContent = await fetch(`warehouse/${cur[0]}/${cur[1]}.sql`).then((r) => r.text());
  };
  $("sql-tabs").innerHTML = ["bigquery", "snowflake"].map((x) => `<button data-d="${x}">${x === "snowflake" ? "Snowflake" : "BigQuery"}</button>`).join("") +
    `<span class="sep"></span>` + files.map((f) => `<button data-f="${f}">${f}</button>`).join("");
  $("sql-tabs").addEventListener("click", (e) => { const b = e.target.closest("button"); if (!b) return;
    if (b.dataset.d) cur[0] = b.dataset.d; else cur[1] = b.dataset.f; showSql(); });
  showSql();
  const pass = checks.filter((c) => c.status === "pass").length;
  $("wh-sum").innerHTML = `Last run: <b>${pass}/${checks.length}</b> checks pass; ${d.warehouse.erased_rows_removed} rows of the erased review removed.`;

  // ---------- SLAs ----------
  $("sla-table").innerHTML = `<div class="tablewrap"><table><tr><th>product</th><th>tier</th><th class="num">SLA</th><th>measured as</th><th class="num">p50</th><th class="num">p95</th>
    <th class="num">worst</th><th class="num">on time</th><th class="num">SLO</th><th class="num">error budget used</th><th>breaches</th></tr>
    ${d.sla.map((r) => `<tr><td class="mono">${r.product}</td><td>${r.tier}</td><td class="num">${r.freshness_minutes} min</td><td>${r.kind === "age" ? "age of completeness" : "consumer lag"}</td>
      <td class="num">${r.p50}</td><td class="num">${r.p95}</td><td class="num">${r.max}</td><td class="num">${(r.on_time * 100).toFixed(2)}%</td>
      <td class="num">${(r.slo * 100).toFixed(1)}%</td><td class="num"><span class="verdict ${r.budget_used > 1 ? "bad" : "ok"}">${Math.round(r.budget_used * 100)}%</span></td>
      <td>${r.breaches.length ? r.breaches.map((b) => `${day(b.from)} ${hm(b.from)}–${hm(b.to)}`).join(", ") : "none"}</td></tr>`).join("")}</table></div>
    <p style="font-size:.84rem;margin-top:.5rem">A tier-1 product with a 99.9% SLO can be late for ${d.sla[1].budget_samples} five-minute samples a week. The incident used ${d.sla[1].late_samples}: the budget is spent ${d.sla[1].budget_used} times over, which is what puts the restart alert at the top of the action list.</p>`;
  const ser = I.series, F = 1100, FH = 190, fmax = Math.max(60, ...ser.map((s) => s[1])) * 1.1;
  const x = (i) => i / (ser.length - 1) * F, y = (v) => FH - v / fmax * FH;
  const line = ser.map((s, i) => `${i ? "L" : "M"}${x(i).toFixed(1)} ${y(s[1]).toFixed(1)}`).join(" ");
  const at = (iso) => ser.findIndex((s) => s[0] >= iso);
  $("fresh").innerHTML = `<svg class="chart" viewBox="0 0 ${F} ${FH + 24}" role="img" aria-label="Freshness of partner revenue during the incident">
    <line x1="0" x2="${F}" y1="${y(I.sla_minutes)}" y2="${y(I.sla_minutes)}" stroke="var(--pink)" stroke-dasharray="5 5"/><text class="ax" x="4" y="${y(I.sla_minutes) - 4}">SLA ${I.sla_minutes} min</text>
    ${[["poison", I.started], ["page", I.detected], ["fix", I.mitigated]].map(([l, t]) => `<line x1="${x(at(t))}" x2="${x(at(t))}" y1="0" y2="${FH}" stroke="var(--muted)" stroke-dasharray="2 4"/><text class="ax" x="${x(at(t)) + 3}" y="12">${l} ${hm(t)}</text>`).join("")}
    <path d="${line}" fill="none" stroke="var(--violet)" stroke-width="2.4"/>
    ${ser.filter((_, i) => i % 12 === 0).map((s, j) => `<text class="ax" x="${x(j * 12)}" y="${FH + 16}">${hm(s[0])}</text>`).join("")}</svg>`;

  // ---------- Java and Flink ----------
  const J = d.java;
  $("java-result").innerHTML = J ? `<div class="ledger" style="grid-template-columns:repeat(3,minmax(0,1fr))">
      <div><b>${NX.int(J.windows)}</b><small>windows, Java</small></div><div><b>${ok(J.identical, "identical", "differs")}</b><small>to the Python job</small></div>
      <div><b>${J.millis} ms</b><small>for the week</small></div></div>
    <p style="margin-top:.7rem;font-size:.86rem">Ledger: read ${NX.int(J.ledger.read)} = ${NX.int(J.ledger.accepted)} counted + ${J.ledger.duplicates} duplicates + ${J.ledger.late_dropped} side output; ${J.ledger.late_updates} late re-fires. Ran here on ${NX.esc(J.java)}; CI runs it on Temurin 17.</p>
    <p style="font-size:.86rem">Records, keys and windows are Java records and a <code>TreeMap</code>; the JSON reader is fifty lines, so the job has no dependencies to explain. <a class="linkish" href="run.html#code=java%2Fsrc%2Fnexus%2FWindowJob.java">Read it →</a></p>`
    : `<p>The Java twin runs in CI; this build had no JDK.</p>`;
  NXFlink.mount({ tabs: "flink-tabs", why: "flink-why", code: "flink-code", tiles: "flink-tiles", map: "flink-map" });
})();
