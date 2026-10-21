// "How it was chosen": one small diagram under each step's caption, on the streaming and modeling pages.
// A bars choice is measured (alternatives re-run on this week); a matrix choice is a design call scored on criteria.
//   bars:   { q, unit, low: true|false, o: [[label, value, picked, note]], why }
//   matrix: { q, crit: [...], o: [[label, [1 | .5 | 0, ...], picked]], why }
window.NXChoice = (() => {
  const esc = (s) => String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  const num = (v) => typeof v === "number" ? v.toLocaleString("en-GB") : v;
  const bars = (c) => {
    const max = Math.max(...c.o.map((x) => +x[1] || 0), 1);
    return `<div class="ch-bars">${c.o.map(([l, v, pick, note], k) => `<div class="ch-row ${pick ? "pick" : ""}">
      <span class="ch-l">${c.o.some((x) => x[2]) ? `<em class="ch-n">${k + 1}</em>` : ""}${esc(l)}</span>
      <span class="ch-track"><i style="width:${Math.max(2, 100 * (+v || 0) / max)}%"></i></span>
      <span class="ch-v">${esc(num(v))}${note ? `<small>${esc(note)}</small>` : ""}</span></div>`).join("")}</div>
      <div class="ch-unit">${esc(c.unit)}${c.low === false ? " · higher is better" : c.low === null ? "" : " · lower is better"}</div>`;
  };
  const mark = (m) => m === 1 ? `<b class="y">✓</b>` : m === 0 ? `<b class="n">✗</b>` : `<b class="h">~</b>`;
  const matrix = (c) => `<table class="ch-mx"><tr><th></th>${c.crit.map((k) => `<th>${esc(k)}</th>`).join("")}</tr>${c.o.map(([l, ms, pick], k) =>
    `<tr class="${pick ? "pick" : ""}"><td><em class="ch-n">${k + 1}</em>${esc(l)}</td>${ms.map((m) => `<td>${mark(m)}</td>`).join("")}</tr>`).join("")}</table>`;
  // one model from The Decision Book (Krogerus & Tschäppeler) per choice:
  //   three criteria → the project triangle (an option sits nearer the corners it satisfies; all three → the centre)
  //   two criteria   → a 2×2 matrix · measured → the rubber band (where each option sits between the extremes)
  const G = { pick: "#2fb487", dot: "#4c6e61", ink: "#b9cfc7", dim: "#7f9c92", line: "#34584b", gold: "#d9b866" };
  const T = (x, y, t, o = {}) => `<text x="${x}" y="${y}" fill="${o.c || G.ink}" font-size="${o.s || 10.5}" font-weight="${o.w || 500}" text-anchor="${o.a || "middle"}" font-family="Inter,system-ui,sans-serif">${esc(t)}</text>`;
  const dot = (x, y, k, pick) => `<circle cx="${x}" cy="${y}" r="${pick ? 10 : 8}" fill="${pick ? G.pick : G.dot}" stroke="#081713" stroke-width="2"/>` + T(x, y + 3.5, k + 1, { c: "#081713", w: 800, s: 10 });
  const spread = (pts) => { pts.forEach((p, i) => pts.slice(0, i).forEach((q) => { if (Math.hypot(p[0] - q[0], p[1] - q[1]) < 18) p[0] += 20; })); return pts; };
  const short = (t) => t.length > 22 ? t.slice(0, 21) + "…" : t;
  const triangle = (c) => {
    const V = [[130, 28], [28, 168], [232, 168]];
    const pts = spread(c.o.map(([, ms]) => { const w = ms.map((m) => m + .12), n = w.reduce((a, b) => a + b, 0);
      return [0, 1].map((ax) => w.reduce((a, wi, i) => a + wi * V[i][ax], 0) / n); }));
    return `<svg viewBox="0 0 260 200" role="img" aria-label="Project triangle of the options">
      <path d="M${V[0]} L${V[1]} L${V[2]} Z" fill="rgba(255,255,255,.025)" stroke="${G.line}" stroke-width="1.5"/>
      ${T(130, 16, short(c.crit[0]), { c: G.gold, w: 700 })}${T(28, 188, short(c.crit[1]), { c: G.gold, w: 700, a: "start" })}${T(232, 188, short(c.crit[2]), { c: G.gold, w: 700, a: "end" })}
      ${pts.map(([x, y], k) => dot(x, y, k, c.o[k][2])).join("")}</svg>`;
  };
  const square = (c) => {
    const X = (m) => 52 + m * 170, Y = (m) => 160 - m * 120;
    const pts = spread(c.o.map(([, ms]) => [X(ms[0]), Y(ms[1])]));
    return `<svg viewBox="0 0 260 200" role="img" aria-label="Two-by-two matrix of the options">
      <rect x="40" y="28" width="194" height="144" fill="rgba(255,255,255,.025)" stroke="${G.line}"/>
      <rect x="137" y="28" width="97" height="72" fill="rgba(47,180,135,.12)"/>
      <line x1="137" y1="28" x2="137" y2="172" stroke="${G.line}" stroke-dasharray="3 3"/><line x1="40" y1="100" x2="234" y2="100" stroke="${G.line}" stroke-dasharray="3 3"/>
      ${T(137, 190, short(c.crit[0]) + " →", { c: G.gold, w: 700 })}
      <g transform="translate(18 100) rotate(-90)">${T(0, 0, short(c.crit[1]) + " →", { c: G.gold, w: 700 })}</g>
      ${pts.map(([x, y], k) => dot(x, y, k, c.o[k][2])).join("")}</svg>`;
  };
  const band = (c) => {
    const vs = c.o.map((x) => +x[1] || 0), lo = Math.min(...vs), hi = Math.max(...vs), span = hi - lo || 1;
    const pts = spread(c.o.map((x, k) => [30 + 200 * (vs[k] - lo) / span, 96]));
    const pk = c.o.findIndex((x) => x[2]), px = pk >= 0 ? pts[pk][0] : 130;
    return `<svg viewBox="0 0 260 200" role="img" aria-label="Rubber band between the two extremes">
      <path d="M30 96 Q ${px} ${132} 230 96" fill="none" stroke="${G.gold}" stroke-width="3" opacity=".75"/>
      <circle cx="30" cy="96" r="4" fill="${G.gold}"/><circle cx="230" cy="96" r="4" fill="${G.gold}"/>
      ${T(30, 66, "least", { c: G.gold, w: 700 })}${T(30, 80, num(lo), { c: G.dim })}${T(230, 66, "most", { c: G.gold, w: 700 })}${T(230, 80, num(hi), { c: G.dim })}
      ${pts.map(([x, y], k) => dot(x, y + (k === pk ? 0 : 0), k, c.o[k][2])).join("")}
      ${T(130, 160, c.unit.length > 40 ? c.unit.slice(0, 39) + "…" : c.unit, { c: G.dim, s: 9.5 })}</svg>`;
  };
  const model = (c) => {
    if (c.unit) return c.o.some((x) => x[2]) ? ["Rubber band", band(c)] : null;
    return c.crit.length === 3 ? ["Project triangle", triangle(c)] : c.crit.length === 2 ? ["2×2 matrix", square(c)] : null;
  };
  // open or closed is the reader's choice, kept across steps (and visits, where storage works)
  let open = false;
  try { open = localStorage.getItem("nx-choice-v2") === "open"; } catch (e) {}
  const pickOf = (c) => (c.o.find((x) => x[2]) || [])[0];
  const rival = (c) => (c.o.find((x) => !x[2]) || [])[0];
  const ask = (c) => c.ask || (pickOf(c) && rival(c) ? `Why “${pickOf(c)}” and not “${rival(c)}”?` : `${c.q.charAt(0).toUpperCase()}${c.q.slice(1)}`);
  const render = (el, c) => {
    if (!el) return;
    if (!c) { el.innerHTML = ""; el.hidden = true; return; }
    el.hidden = false;
    const measured = !!c.unit;
    let m;
    el.innerHTML = `<details class="ch-box" ${open ? "open" : ""}><summary>
        <span class="ch-tag ${measured ? "m" : "d"}">${esc(c.tag || (measured ? "Measured on this week" : "Design call · scored on criteria"))}</span>
        <span class="ch-q ch-when-open">How it was chosen: ${esc(c.q)}</span>
        <span class="ch-q ch-when-closed">${esc(ask(c))}</span>
        <span class="ch-toggle"><span class="ch-when-open">Hide ↑</span><span class="ch-when-closed">See how it was chosen ↓</span></span></summary>
      <div class="ch-body"><div class="ch-main">${measured ? bars(c) : matrix(c)}${c.why ? `<p class="ch-why">${c.why}</p>` : ""}</div>${
        (m = model(c)) ? `<figure class="ch-model">${m[1]}<figcaption>${m[0]} · The Decision Book</figcaption></figure>` : ""}</div></details>`;
    el.querySelector("details").addEventListener("toggle", (e) => {
      open = e.target.open;
      try { localStorage.setItem("nx-choice-v2", open ? "open" : "closed"); } catch (err) {}
    });
  };
  return { render };
})();
