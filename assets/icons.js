// Technology icons: one SVG sprite for every page, simplified drawings (not the projects' official logo files).
// Text that names a technology in a heading, label, tab or bold phrase gets its icon in front, also when a page
// re-renders (captions, tabs). Diagrams use the same symbols directly: <use href="#ti-kafka" .../>.
(() => {
  const S = {
    kafka: `<g fill="none" stroke="#d9e2de" stroke-width="1.8"><path d="M9 4.5V19.5M9 12L16.5 7.5M9 12L16.5 16.5"/></g>
      <g fill="#081713" stroke="#d9e2de" stroke-width="1.8"><circle cx="9" cy="4" r="2.4"/><circle cx="9" cy="20" r="2.4"/><circle cx="9" cy="12" r="3"/><circle cx="17" cy="7" r="2.4"/><circle cx="17" cy="17" r="2.4"/></g>`,
    flink: `<circle cx="12" cy="12" r="10.5" fill="#e6526f"/><path d="M6.5 14.5c2-4.5 6.5-6.5 11-5.5M6.5 18c2.5-3 6-4 9.5-3.5M8 10.5c1.5-2.5 4-3.8 7-3.8" fill="none" stroke="#fff" stroke-width="1.8" stroke-linecap="round"/>`,
    spark: `<path d="M12 1.5l2.6 7.1 7.4.4-5.8 4.6 2 7.2L12 16.7l-6.2 4.1 2-7.2L2 9l7.4-.4z" fill="#e25a1c"/>`,
    bigquery: `<path d="M12 1.5l9 5.2v10.6l-9 5.2-9-5.2V6.7z" fill="#4285f4"/><circle cx="11" cy="11" r="4.6" fill="none" stroke="#fff" stroke-width="1.9"/>
      <path d="M14.4 14.4l3.6 3.6" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/><path d="M9.2 12.6v-1.8M11 12.6V9M12.8 12.6v-1.2" stroke="#fff" stroke-width="1.3" stroke-linecap="round"/>`,
    snowflake: `<g stroke="#29b5e8" stroke-width="2" stroke-linecap="round" fill="none">${[0, 60, 120].map((r) =>
      `<g transform="rotate(${r} 12 12)"><path d="M12 1.5v21"/><path d="M8.6 4.8L12 7.6l3.4-2.8M8.6 19.2L12 16.4l3.4 2.8"/></g>`).join("")}</g>`,
    python: `<path d="M11.8 2C7 2 7.3 4.1 7.3 4.1v2.2h4.6V7H5.4S2 6.6 2 11.9s3 5.1 3 5.1h1.8v-2.5s-.1-3 2.9-3h4.9s2.8 0 2.8-2.7V4.5S17.8 2 11.8 2zM9.2 3.5a.9.9 0 110 1.8.9.9 0 010-1.8z" fill="#3776ab"/>
      <path d="M12.2 22c4.8 0 4.5-2.1 4.5-2.1v-2.2h-4.6V17h6.5s3.4.4 3.4-4.9-3-5.1-3-5.1h-1.8v2.5s.1 3-2.9 3H9.4s-2.8 0-2.8 2.7v4.3S6.2 22 12.2 22zm2.6-1.5a.9.9 0 110-1.8.9.9 0 010 1.8z" fill="#ffd43b"/>`,
    java: `<path d="M5 12.5h11v4a4.5 4.5 0 01-4.5 4.5h-2A4.5 4.5 0 015 16.5z" fill="#5382a1"/><path d="M16 13.5h1.4a2 2 0 010 4H16" fill="none" stroke="#5382a1" stroke-width="1.6"/>
      <path d="M9 10.5c-1.5-1.8 1.5-2.8 0-5M12.5 10.5c-1.5-1.8 1.5-2.8 0-5" fill="none" stroke="#e76f00" stroke-width="1.6" stroke-linecap="round"/>`,
    airflow: `<path d="M12 12L12 2.5c3 0 4.5 4 0 9.5z" fill="#017cee"/><path d="M12 12L21.5 12c0 3-4 4.5-9.5 0z" fill="#00ad46"/>
      <path d="M12 12L12 21.5c-3 0-4.5-4 0-9.5z" fill="#e43921"/><path d="M12 12L2.5 12c0-3 4-4.5 9.5 0z" fill="#ffd261"/>`,
    dbt: `<path d="M4 4l16 16M20 4L4 20" stroke="#ff694b" stroke-width="4" stroke-linecap="round"/><circle cx="12" cy="12" r="2.6" fill="#081713"/>`,
    databricks: `<path d="M12 3l9 5-9 5-9-5z" fill="#ff3621"/><path d="M3 12l9 5 9-5M3 16l9 5 9-5" fill="none" stroke="#ff3621" stroke-width="2"/>`,
    db: `<ellipse cx="12" cy="5.5" rx="7.5" ry="3" fill="#8fb3c9"/><path d="M4.5 5.5v13c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-13" fill="none" stroke="#8fb3c9" stroke-width="1.8"/>
      <path d="M4.5 12c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3" fill="none" stroke="#8fb3c9" stroke-width="1.8"/>`,
  };
  const sprite = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  sprite.setAttribute("aria-hidden", "true");
  sprite.style.cssText = "position:absolute;width:0;height:0;overflow:hidden";
  sprite.innerHTML = Object.entries(S).map(([k, v]) => `<symbol id="ti-${k}" viewBox="0 0 24 24">${v}</symbol>`).join("");
  document.body.prepend(sprite);

  const style = document.createElement("style");
  style.textContent = `.ti{display:inline-block;width:1.05em;height:1.05em;vertical-align:-.17em;margin-right:.32em;flex:none}
    .ti + .ti{margin-left:-.12em}
    h1 .ti,h2 .ti{width:.62em;height:.62em;vertical-align:.08em;margin-right:.22em}`;
  document.head.appendChild(style);

  // which words mean which icon; the first match in the text wins its place, at most two icons per label
  const RULES = [[/BigQuery/, "bigquery"], [/Snowflake/, "snowflake"], [/Flink/, "flink"], [/Kafka/, "kafka"], [/PySpark|\bSpark\b/, "spark"],
    [/Databricks/, "databricks"], [/Airflow/, "airflow"], [/\bdbt\b/, "dbt"], [/Python/, "python"], [/\bJava\b/, "java"], [/PostgreSQL|SQLite/, "db"]];
  const SEL = "nav a, h1, h2, h3, h4, b, strong, .kicker, .card-kind, .eyebrow, .flink-tab, [role=tab], #flink-tabs button, #cb-groups button, #cb-tabs button";
  const SKIP = "pre, code, svg, script, .term, .brand, .skip, .skip-link, .ch-box, .dq-line, .no-ti";
  const icon = (k) => `<svg class="ti" aria-hidden="true" focusable="false"><use href="#ti-${k}"/></svg>`;
  const decorate = (root) => {
    root.querySelectorAll(SEL).forEach((el) => {
      if (el.dataset.ti || el.closest(SKIP) || el.querySelector(".ti") || el.parentElement.closest('[data-ti="2"]')) return;
      const text = el.textContent;
      if (!text || text.length > 90) return;
      const hits = RULES.map(([re, k]) => [text.search(re), k]).filter(([i]) => i >= 0).sort((a, b) => a[0] - b[0]).slice(0, 2);
      el.dataset.ti = hits.length ? "2" : "1";
      if (hits.length) el.insertAdjacentHTML("afterbegin", hits.map(([, k]) => icon(k)).join(""));
    });
  };
  const run = () => decorate(document.body);
  run();
  let t = null;
  new MutationObserver(() => { clearTimeout(t); t = setTimeout(run, 60); }).observe(document.body, { childList: true, subtree: true });
  window.NXIcon = { html: icon };
})();
