// The incident & replayed: the afternoon of 9 October at a minute a tick, from site.json and the postmortem.
(async () => {
  const d = await NX.site();
  const I = d.incident;
  const $ = (id) => document.getElementById(id);
  const hm = (iso) => iso.slice(11, 16);
  const ms = (iso) => Date.parse(iso);

  $("o-tiles").innerHTML = [
    [`${I.minutes.to_detect} min`, "to detect", "freshness had to cross 45 minutes first"],
    [`${I.restarts}`, "restarts", "the same record, every time"],
    [`${I.minutes.total} min`, "start to recovery", `${I.windows_delayed} windows late, none wrong`],
    [`${I.minutes.would_detect_with_restart_alert} min`, "with the restart alert", "action 2 in the postmortem"],
  ].map(([n, l, s], i) => `<div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="big ${i % 2 ? "p" : ""}">${n}</div><h3 style="margin:.4rem 0 .2rem;color:#fff">${l}</h3><p>${s}</p></div>`).join("");

  // chart
  const ser = I.series, W = 980, H = 220, max = Math.max(...ser.map((s) => s[1])) * 1.15;
  const t0 = ms(ser[0][0]), t1 = ms(ser[ser.length - 1][0]);
  const x = (iso) => (ms(iso) - t0) / (t1 - t0) * W, y = (v) => H - v / max * H;
  const marks = [["poison", I.started], ["would page", I.timeline[1].at], ["paged", I.detected], ["fix", I.mitigated], ["recovered", I.recovered]];
  $("o-chart").innerHTML = `<svg class="chart" viewBox="0 0 ${W} ${H + 26}" role="img" aria-label="Freshness of partner revenue during the incident">
    <line x1="0" x2="${W}" y1="${y(I.sla_minutes)}" y2="${y(I.sla_minutes)}" stroke="#ff6b4a" stroke-dasharray="6 6"/><text class="ax" x="4" y="${y(I.sla_minutes) - 5}">SLA ${I.sla_minutes} min</text>
    ${marks.map(([l, t], k) => `<g class="mk" data-t="${t}" opacity=".15"><line x1="${x(t)}" x2="${x(t)}" y1="0" y2="${H}" stroke="#9cc0b7" stroke-dasharray="2 4"/>
      <text class="ax" x="${x(t) + 3}" y="${12 + (k % 2) * 14}">${l} ${hm(t)}</text></g>`).join("")}
    <path id="o-line" fill="none" stroke="#34e0a1" stroke-width="2.6" d=""/>
    <line id="o-cursor" x1="0" x2="0" y1="0" y2="${H}" stroke="#fff" stroke-width="1"/>
    ${ser.filter((_, i) => i % 6 === 0).map((s) => `<text class="ax" x="${x(s[0])}" y="${H + 18}">${hm(s[0])}</text>`).join("")}</svg>`;

  // timeline
  $("o-timeline").innerHTML = I.timeline.map((e) => `<div class="tl" data-t="${e.at}"><div class="t">${hm(e.at)} · ${NX.esc(e.who)}</div><div>${NX.esc(e.what)}</div></div>`).join("");

  // root cause
  $("o-lag").innerHTML = `<div class="tablewrap"><table><tr><th>partition</th><th class="num">records waiting at ${hm(I.detected)}</th><th></th></tr>${Object.entries(I.lag_at_detection).map(([p, n]) =>
    `<tr><td class="mono">partner_clicks-${p}</td><td class="num">${n}</td><td>${+p === I.partition ? '<span class="verdict bad">offset ' + I.offset + "</span>" : ""}</td></tr>`).join("")}</table></div>
    <p style="font-size:.84rem;margin-top:.5rem">Every partition waits, not just ${I.partition}: one job, so one bad record stops all of it. That is why a restart can't help, and why the fix is in the job, not the consumer group.</p>`;
  $("o-raw").textContent = `// partner_clicks-${I.partition} @ ${I.offset}, ${I.raw_bytes} bytes\n${I.raw}\n// ${I.reason}`;
  $("o-diff").innerHTML = [
    ["del", `-pc = Job("partner_clicks", dlq=False)    # a bad record stops the job`],
    ["add", `+pc = Job("partner_clicks")               # dlq=True is the template's default now`],
    ["", ` `],
    ["", ` # alerts.yml (proposed)`],
    ["add", `+- alert: StreamJobRestarting`],
    ["add", `+  expr: increase(job_restarts_total{tier="1"}[10m]) > 3`],
    ["add", `+  for: 0m                                  # would have paged at ${hm(I.timeline[1].at)}, not ${hm(I.detected)}`],
    ["add", `+- alert: DeadLettersOnTier1`],
    ["add", `+  expr: dlq_records_total{tier="1"} > 0     # with the record and its producer attached`],
  ].map(([c, l]) => `<span class="${c}">${NX.esc(l)}</span>`).join("\n");

  // postmortem (small markdown)
  const md = await fetch("assets/data/incident.md").then((r) => r.text());
  $("o-pm").innerHTML = md.split("\n").map((l) => {
    const inline = (s) => NX.esc(s).replace(/\*\*(.+?)\*\*/g, "<b>$1</b>").replace(/`(.+?)`/g, "<code>$1</code>");
    if (l.startsWith("# ")) return `<h3 style="font-size:1.4rem;margin-bottom:.4rem">${inline(l.slice(2))}</h3>`;
    if (l.startsWith("## ")) return `<h3 style="margin:1rem 0 .3rem">${inline(l.slice(3))}</h3>`;
    if (l.startsWith("- [x] ")) return `<p>✅ ${inline(l.slice(6))}</p>`;
    if (l.startsWith("- [ ] ")) return `<p>⬜ ${inline(l.slice(6))}</p>`;
    if (l.startsWith("- ")) return `<p>• ${inline(l.slice(2))}</p>`;
    return l.trim() ? `<p>${inline(l)}</p>` : "";
  }).join("");

  // replay clock
  let timer = null, now = t0;
  const paint = (t) => {
    now = t;
    $("clock").textContent = new Date(t).toISOString().slice(11, 16);
    const seen = ser.filter((s) => ms(s[0]) <= t);
    $("o-line").setAttribute("d", seen.map((s, i) => `${i ? "L" : "M"}${x(s[0]).toFixed(1)} ${y(s[1]).toFixed(1)}`).join(" "));
    $("o-line").setAttribute("stroke", seen.length && seen[seen.length - 1][1] > I.sla_minutes ? "#ff6b4a" : "#34e0a1");
    const cx = Math.min(W, (t - t0) / (t1 - t0) * W);
    $("o-cursor").setAttribute("x1", cx); $("o-cursor").setAttribute("x2", cx);
    document.querySelectorAll(".tl").forEach((e) => e.classList.toggle("on", ms(e.dataset.t) <= t));
    document.querySelectorAll(".mk").forEach((e) => e.setAttribute("opacity", ms(e.dataset.t) <= t ? 1 : .15));
  };
  const stop = () => { clearInterval(timer); timer = null; $("replay").textContent = "▶ Replay"; };
  $("replay").addEventListener("click", () => {
    if (timer) return stop();
    if (now >= t1) now = t0;
    $("replay").textContent = "❚❚ Pause";
    timer = setInterval(() => { if (now >= t1) return stop(); paint(now + 60_000); }, 90);
  });
  paint(matchMedia("(prefers-reduced-motion: reduce)").matches || location.hash === "#end" ? t1 : t0);
  if (new URLSearchParams(location.search).has("play")) $("replay").click();
})();
