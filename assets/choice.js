// "How it was chosen": one small diagram under each step's caption, on the streaming and modeling pages.
// A bars choice is measured (alternatives re-run on this week); a matrix choice is a design call scored on criteria.
//   bars:   { q, unit, low: true|false, o: [[label, value, picked, note]], why }
//   matrix: { q, crit: [...], o: [[label, [1 | .5 | 0, ...], picked]], why }
window.NXChoice = (() => {
  const esc = (s) => String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  const num = (v) => typeof v === "number" ? v.toLocaleString("en-GB") : v;
  const bars = (c) => {
    const max = Math.max(...c.o.map((x) => +x[1] || 0), 1);
    return `<div class="ch-bars">${c.o.map(([l, v, pick, note]) => `<div class="ch-row ${pick ? "pick" : ""}">
      <span class="ch-l">${esc(l)}</span>
      <span class="ch-track"><i style="width:${Math.max(2, 100 * (+v || 0) / max)}%"></i></span>
      <span class="ch-v">${esc(num(v))}${note ? `<small>${esc(note)}</small>` : ""}</span></div>`).join("")}</div>
      <div class="ch-unit">${esc(c.unit)} · ${c.low === false ? "higher is better" : c.low === null ? "" : "lower is better"}</div>`;
  };
  const mark = (m) => m === 1 ? `<b class="y">✓</b>` : m === 0 ? `<b class="n">✗</b>` : `<b class="h">~</b>`;
  const matrix = (c) => `<table class="ch-mx"><tr><th></th>${c.crit.map((k) => `<th>${esc(k)}</th>`).join("")}</tr>${c.o.map(([l, ms, pick]) =>
    `<tr class="${pick ? "pick" : ""}"><td>${esc(l)}</td>${ms.map((m) => `<td>${mark(m)}</td>`).join("")}</tr>`).join("")}</table>`;
  const render = (el, c) => {
    if (!el) return;
    if (!c) { el.innerHTML = ""; el.hidden = true; return; }
    el.hidden = false;
    const measured = !!c.unit;
    el.innerHTML = `<div class="ch-head"><span class="ch-tag ${measured ? "m" : "d"}">${esc(c.tag || (measured ? "Measured on this week" : "Design call · scored on criteria"))}</span>
      <span class="ch-q">How it was chosen: ${esc(c.q)}</span></div>
      ${measured ? bars(c) : matrix(c)}${c.why ? `<p class="ch-why">${c.why}</p>` : ""}`;
  };
  return { render };
})();
