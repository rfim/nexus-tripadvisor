// Streaming & step by step: each problem's records, through the job, with the decisions the run recorded.
(async () => {
  const d = await NX.site();
  const $ = (id) => document.getElementById(id);
  const T = d.trace, P = d.generator.planted, I = d.incident, V = d.volume;
  const t = (iso) => iso ? iso.slice(5, 19).replace("T", " ") : "–";
  const pick = (topic, f) => T[topic].filter(f);
  const one = (topic, f) => T[topic].find(f);

  // a step: caption, which nodes light, which wires carry the packet, the log entry
  const S = (title, text, nodes, wires, log, extra = {}) => ({ title, text, nodes, wires, log, ...extra });
  const rec = (x) => ({ partition: x.partition, offset: x.offset, id: x.id, arrived: x.arrived, event_time: x.event_time,
                        watermark: x.wm, window: x.window, decision: x.decision });

  const dup = pick("page_views", (x) => x.decision === "duplicate")[0];
  const dupFirst = one("page_views", (x) => x.id === dup.id && x.decision === "counted");
  const lateOk = one("page_views", (x) => x.decision.startsWith("late:"));
  const tooLate = pick("page_views", (x) => x.decision.startsWith("too late"));
  const v1 = one("partner_clicks", (x) => x.schema === 1), v2s = pick("partner_clicks", (x) => x.schema === 2);
  const fails = one("partner_clicks", (x) => x.decision === "job fails"), dl = one("partner_clicks", (x) => x.decision === "dead letter");
  const bot = one("page_views", (x) => P.bot_users && x.window && x.window.startsWith(P.bots.from.slice(0, 13)));
  const hold = V.holds[0], inv = V.investigation[0];

  const SCEN = {
    retries: { label: "1 · SDK retries", topic: "page_views", steps: [
      S("A page view arrives", `Event <b>${dupFirst.id}</b> lands on partition ${dupFirst.partition}, offset ${dupFirst.offset}, two seconds after it happened.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(dupFirst), { part: dupFirst.partition, wm: dupFirst.wm }),
      S("The SDK times out and sends it again", `Same event_id, new offset <b>${dup.offset}</b>, ${((Date.parse(dup.arrived) - Date.parse(dupFirst.arrived)) / 1000).toFixed(0)} s later. To Kafka these are two records.`,
        ["kafka", "read", "dedupe"], ["w-kr", "w-rd"], rec(dup), { part: dup.partition, hot: ["dedupe"] }),
      S("Dropped, not counted twice", `The id is in the dedupe state (first seen ${t(dup.first_seen)}). The ledger counts it as a duplicate: <b>${d.streams.page_views.ledger.duplicates}</b> this week.`,
        ["dedupe"], [], { duplicates: d.streams.page_views.ledger.duplicates, ttl: "24 h, checkpointed" }),
    ] },
    late: { label: "2 · Late, and too late", topic: "page_views", steps: [
      S("Late, but inside the hour", `An Android event from ${t(lateOk.event_time)} arrives at ${t(lateOk.arrived)}. Its window has already fired; the watermark is ${t(lateOk.wm)}.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(lateOk), { part: lateOk.partition, wm: lateOk.wm }),
      S("The window is updated and fires again", `Within the hour of allowed lateness: the count goes to <b>${lateOk.n}</b> and the sink upserts the window. ${d.streams.page_views.ledger.late_updates} re-fires this week.`,
        ["window", "sink"], ["w-ws"], { window: lateOk.window, n: lateOk.n, decision: lateOk.decision }),
      S("An offline upload, hours later", `${P.offline_upload.sessions} iOS sessions from a flight flush at ${t(P.offline_upload.delivered_at)}. This one happened at ${t(tooLate[0].event_time)}.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(tooLate[0]), { part: tooLate[0].partition, wm: tooLate[0].wm }),
      S("Too late for the stream", `Its window closed for good at ${t(tooLate[0].closes)}. It goes to the side output, not into a window someone already read.`,
        ["window", "side"], ["w-wl"], { decision: tooLate[0].decision, window: tooLate[0].window, closed: tooLate[0].closes }, { hot: ["side"] }),
      S("The batch counts it", `The nightly batch has no lateness. It differs from the stream in <b>${d.reconcile.page_views.explained_by_side_output}</b> windows, and the side output explains every one: ${d.reconcile.page_views.unexplained} unexplained.`,
        ["side", "batch", "sink"], ["w-lb", "w-sb"], { stream: d.reconcile.page_views.stream_events, batch: d.reconcile.page_views.batch_events,
          side_output: d.reconcile.page_views.late_events, unexplained: d.reconcile.page_views.unexplained }),
    ] },
    schema: { label: "3 · A schema change", topic: "partner_clicks", steps: [
      S("Version 1: cpc_gbp", `A web click at ${t(v1.event_time)}: <code>cpc_gbp</code>, a number in pounds.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(v1) && { ...rec(v1), schema: 1, revenue_gbp: v1.revenue_gbp }, { part: v1.partition, wm: v1.wm }),
      S("Version 2: price {amount, currency}", `App ${P.schema_v2.app_version} from ${t(P.schema_v2.from)}. A backward check fails: <code>cpc_gbp</code> removed, <code>price</code> new.`,
        ["kafka", "read"], ["w-kr"], { ...rec(v2s[0]), schema: 2, revenue_gbp: v2s[0].revenue_gbp }, { part: v2s[0].partition, hot: ["read"] }),
      S("One reader, both versions", `The reader converts with the contract's rates: this one is <b>£${v2s[v2s.length - 1].revenue_gbp}</b>. An unknown currency would be a dead letter, never a guess.`,
        ["read", "dedupe", "window", "sink"], ["w-rd", "w-dw", "w-ws"], { ...rec(v2s[v2s.length - 1]), schema: 2, revenue_gbp: v2s[v2s.length - 1].revenue_gbp }),
    ] },
    poison: { label: "4 · A poison pill", topic: "partner_clicks", steps: [
      S("57 bytes of truncated JSON", `Partition ${fails.partition}, offset ${fails.offset}, at ${t(fails.arrived)}. The producer cut the record short.`,
        ["kafka", "read"], ["w-kr"], { partition: fails.partition, offset: fails.offset, raw: fails.raw }, { part: fails.partition, hot: ["read"] }),
      S("The job fails, restarts, fails again", `Without a dead-letter topic the only thing it can do is stop. Restored from the 14:00 checkpoint, it reads the same record: <b>${I.restarts}</b> restarts.`,
        ["read"], [], { decision: fails.decision, reason: fails.reason, restarts: I.restarts }, { hot: ["read", "window"] }),
      S(`Paged at ${t(I.detected)}`, `Nothing downstream moves, so partner revenue ages past its 45-minute SLA. The restart count said so at ${t(I.timeline[1].at)}.`,
        ["sink"], [], { sla: "45 min", detected: I.detected, worst_age_minutes: I.worst_age_minutes }, { hot: ["sink"] }),
      S("Fix: dead-letter it", `Deployed at ${t(I.mitigated)}: the record goes to the dead-letter topic with its reason, and the job restores and catches up. Back within SLA at ${t(I.recovered)}.`,
        ["read", "dlq", "dedupe", "window", "sink"], ["w-rq", "w-rd", "w-dw", "w-ws"], { decision: dl.decision, processed: dl.processed, reason: dl.reason }),
    ] },
    bots: { label: "5 · A scraper", topic: "page_views", steps: [
      S("Three users, a flood of views", `${t(P.bots.from)}: <b>${hold.views}</b> views in an hour that usually has ${hold.mean}. To the stream these are valid page views.`,
        ["kafka", "read", "dedupe", "window", "sink"], ["w-kr", "w-rd", "w-dw", "w-ws"], bot ? rec(bot) : { hour: hold.hour, views: hold.views }, { part: bot ? bot.partition : 0 }),
      S("The hour is held", `${hold.views} is more than 2σ over the same hour on earlier days (threshold ${hold.threshold}), and more than double. Gold doesn't publish it.`,
        ["batch"], ["w-sb"], { hour: hold.hour, views: hold.views, mean: hold.mean, sd: hold.sd, threshold: hold.threshold, z: hold.z }, { hot: ["batch"] }),
      S("Investigated, filtered, released", `${inv.users_in_hour} users in the hour; ${inv.bots.length} made over ${inv.max_per_hour} views each. Removed from Gold (${V.bot_views_removed} views), re-checked: ${V.holds_after_filter.length} holds.`,
        ["batch", "sink"], ["w-sb"], { top: inv.top.slice(0, 4), removed: V.bot_views_removed, holds_after: V.holds_after_filter.length }),
    ] },
    erasure: { label: "6 · Erasure, then a stale version", topic: "reviews", steps: T.reviews.map((x, i, a) => S(
      i === a.length - 1 ? "A stale version arrives" : x.op === "erased" ? "Erased: a tombstone" : `Version ${x.version}: ${x.op}`,
      i === a.length - 1 ? `An outbox replay re-sends version 3 with a new event id, so de-duplication can't catch it. The version rule does: <b>${x.decision}</b>.`
        : x.op === "erased" ? "The reviewer asks to be erased. Version 4 is a tombstone: no fields, and nothing older can overwrite it. Both warehouses delete the review."
        : `Review ${x.id} ${x.op === "submitted" ? "is submitted" : x.op === "moderated" ? "passes moderation" : "is edited"}${x.rating ? `, rating ${x.rating}` : ""}.`,
      i === a.length - 1 ? ["kafka", "read", "dedupe", "window"] : ["kafka", "read", "dedupe", "window", "sink"],
      i === a.length - 1 ? ["w-kr", "w-rd", "w-dw"] : ["w-kr", "w-rd", "w-dw", "w-ws"],
      { review: x.id, version: x.version, op: x.op, event_id: x.event_id, arrived: x.arrived, decision: x.decision },
      { part: x.partition, hot: i === a.length - 1 ? ["window"] : [] })) },
    crash: { label: "+ A lost TaskManager", topic: "page_views", steps: [
      S("09:17: the job loses a TaskManager", "Mid-morning on Thursday. Everything since the last checkpoint is in flight.",
        ["window"], [], { at: "2026-10-08 09:17", event: "job lost" }, { hot: ["window", "dedupe", "read"] }),
      S("Restore from the checkpoint", `${d.replay.restored_from}. Offsets, open windows, dedupe state and the ledger come back together.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], { restored: d.replay.restored_from }),
      S("Re-sent, and nothing changed", `${d.replay.re_emitted} windows were sent again. The sink upserts by window, so it is <b>${d.replay.identical ? "identical" : "different"}</b> to a run with no crash.`,
        ["window", "sink"], ["w-ws"], { re_emitted: d.replay.re_emitted, identical: d.replay.identical, windows: d.replay.windows }),
    ] },
  };

  // ---------- rendering ----------
  const NODES = ["kafka", "read", "dedupe", "window", "sink", "side", "batch", "dlq"];
  const WIRES = [...document.querySelectorAll(".wire")].map((w) => w.id);
  let key = "retries", i = -1, timer = null;
  $("views").innerHTML = Object.entries(SCEN).map(([k, s]) => `<button role="tab" data-k="${k}" aria-selected="${k === key}">${s.label}</button>`).join("");
  const parts = (topic) => {
    const n = d.generator.partitions[topic];
    $("k-topic").textContent = topic;
    $("k-parts").innerHTML = Array.from({ length: n }, (_, p) => `<g transform="translate(16,${66 + p * (84 / n)})"><rect id="kp${p}" width="138" height="${84 / n - 3}" rx="3" style="fill:#0a2a21;stroke:#2e5f55"/>
      <text class="sm" x="6" y="${(84 / n - 3) / 2 + 4}" style="font-size:9.5px">p${p}</text></g>`).join("");
  };
  const packet = (wire, hot) => new Promise((res) => {
    const path = $(wire); if (!path) return res();
    const len = path.getTotalLength(), c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    c.setAttribute("r", 7); c.setAttribute("fill", hot ? "#ff6b4a" : "#34e0a1"); c.setAttribute("class", "packet");
    $("packets").appendChild(c);
    const t0 = performance.now(), dur = +$("speed").value / 3;
    const step = (now) => { const f = Math.min(1, (now - t0) / dur), p = path.getPointAtLength(f * len);
      c.setAttribute("cx", p.x); c.setAttribute("cy", p.y); if (f < 1) requestAnimationFrame(step); else { c.remove(); res(); } };
    requestAnimationFrame(step);
  });
  const render = async () => {
    const sc = SCEN[key], st = sc.steps[i];
    $("steps").innerHTML = sc.steps.map((s, k) => `<li class="${k < i ? "done" : k === i ? "now" : ""}" data-i="${k}"><span class="dot">${k + 1}</span><span><b>${s.title}</b></span></li>`).join("");
    if (!st) { NODES.forEach((n) => $("n-" + n).setAttribute("class", "node" + (n === "kafka" ? "" : " off"))); WIRES.forEach((w) => $(w).setAttribute("class", "wire"));
      $("caption").innerHTML = `<b>${sc.label.replace(/^[\d+]+ · /, "")}.</b> Press Play, or Next to step through it.`; $("log").innerHTML = ""; return; }
    NODES.forEach((n) => $("n-" + n).setAttribute("class", "node " + ((st.hot || []).includes(n) ? "hot" : st.nodes.includes(n) ? "on" : "off")));
    WIRES.forEach((w) => $(w).setAttribute("class", "wire" + (st.wires.includes(w) ? ((st.hot || []).length ? " hot" : " on") : "")));
    document.querySelectorAll("[id^=kp]").forEach((r) => (r.style.fill = r.id === `kp${st.part}` ? "#0b6b53" : "#0a2a21"));
    if (st.wm) $("wm-label").textContent = `watermark ${t(st.wm).slice(6)}`;
    $("caption").innerHTML = `<b>${st.title}.</b> ${st.text}`;
    const m = document.createElement("div");
    m.className = `msg ${(st.hot || []).length ? "err" : "res"}`;
    m.innerHTML = `<span class="dir">step ${i + 1} · ${sc.topic}</span>${NX.esc(JSON.stringify(st.log, null, 1).replace(/\n\s*/g, " "))}`;
    $("log").prepend(m);
    for (const w of st.wires) await packet(w, (st.hot || []).length);
  };
  const go = (k) => { const n = SCEN[key].steps.length; i = Math.max(-1, Math.min(n - 1, k)); render(); };
  const stop = () => { clearInterval(timer); timer = null; $("play").textContent = "▶ Play"; };
  const select = (k) => { stop(); key = k; parts(SCEN[k].topic); $("log").innerHTML = "";
    document.querySelectorAll("#views [role=tab]").forEach((b) => b.setAttribute("aria-selected", b.dataset.k === k)); go(-1); };
  $("views").addEventListener("click", (e) => { const b = e.target.closest("[data-k]"); if (b) { select(b.dataset.k); history.replaceState(null, "", "#" + b.dataset.k); } });
  $("next").addEventListener("click", () => { stop(); go(i + 1); });
  $("prev").addEventListener("click", () => { stop(); go(i - 1); });
  $("steps").addEventListener("click", (e) => { const li = e.target.closest("li"); if (li) { stop(); go(+li.dataset.i); } });
  $("play").addEventListener("click", () => {
    if (timer) return stop();
    if (i >= SCEN[key].steps.length - 1) go(-1);
    $("play").textContent = "❚❚ Pause"; go(i + 1);
    timer = setInterval(() => { if (i >= SCEN[key].steps.length - 1) { const ks = Object.keys(SCEN), nx = ks[ks.indexOf(key) + 1];
      if (nx && document.body.dataset.tour) { select(nx); $("play").click(); } else stop(); return; } go(i + 1); }, +$("speed").value * 2.2);
  });
  const start = location.hash.slice(1);
  select(SCEN[start] ? start : key);
  if (new URLSearchParams(location.search).has("tour")) { document.body.dataset.tour = 1; $("play").click(); }
})();
