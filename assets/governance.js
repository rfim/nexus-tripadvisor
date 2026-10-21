// Modeling & governance, step by step: the model and its rules, drawn from site.json and the repository's own files.
(async () => {
  const d = await NX.site();
  const $ = (id) => document.getElementById(id);
  const W = d.warehouse, M = W.model, Q = W.queries, C = W.checks;
  const SRC = {};
  const file = (f) => (SRC[f] ??= fetch(f).then((r) => r.ok ? r.text() : `(${f} not found)`));

  // ---------- the stage: nodes and wires ----------
  const N = {
    contract: [470, 10, 260, 54, "contracts/sources.yml", "topics · schemas · pii · products · SLAs"],
    "t-page_views": [24, 104, 170, 62, "page_views", "Kafka · 6 partitions"],
    "t-partner_clicks": [24, 196, 170, 62, "partner_clicks", "Kafka · 4 partitions"],
    "t-reviews": [24, 288, 170, 62, "reviews", "Kafka · 3 partitions"],
    "raw_page_views": [250, 104, 200, 62, "raw_page_views", ""],
    "raw_partner_clicks": [250, 196, 200, 62, "raw_partner_clicks", ""],
    "raw_reviews": [250, 288, 200, 62, "raw_reviews", ""],
    "location_views_hourly": [506, 104, 230, 62, "location_views_hourly", ""],
    "partner_revenue_15m": [506, 196, 230, 62, "partner_revenue_15m", ""],
    "reviews_current": [506, 288, 230, 62, "reviews_current", ""],
    dim_location: [506, 386, 230, 54, "dim_location", "96 locations · city · kind"],
    quality: [792, 104, 180, 62, "Quality gates", "contracts/quality.yml"],
    pii: [792, 196, 180, 62, "Policy tags · masking", "from pii: in the contract"],
    erasure: [792, 288, 180, 62, "Erasure", "tombstone → DELETE"],
    warehouse: [1012, 150, 172, 170, "BigQuery + Snowflake", "one contract · two dialects"],
  };
  const rows = W.rows, sub = (t) => {
    const g = M.gold.find((x) => x.table === t);
    if (g) return `${NX.int(rows[t])} rows · ${g.key.join(", ")}`;
    const r = M.raw.find((x) => x.table === t);
    return r ? `${r.columns.length} columns · ${r.columns.filter((c) => c.pii).length} personal` : "";
  };
  const mid = (k, side) => { const [x, y, w, h] = N[k]; return side === "r" ? [x + w, y + h / 2] : side === "l" ? [x, y + h / 2] : side === "b" ? [x + w / 2, y + h] : [x + w / 2, y]; };
  const curve = (a, b) => { const [x1, y1] = a, [x2, y2] = b, mx = (x1 + x2) / 2; return `M${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`; };
  const WIRES = {};
  const wire = (id, a, b) => (WIRES[id] = curve(a, b));
  ["page_views", "partner_clicks", "reviews"].forEach((t) => wire(`w-t-${t}`, mid(`t-${t}`, "r"), mid(`raw_${t}`, "l")));
  [["page_views", "location_views_hourly"], ["partner_clicks", "partner_revenue_15m"], ["reviews", "reviews_current"]]
    .forEach(([t, g]) => wire(`w-r-${g}`, mid(`raw_${t}`, "r"), mid(g, "l")));
  wire("w-dim-lv", mid("dim_location", "r"), [736, 135]); wire("w-dim-rc", [736, 413], [736, 319]);
  WIRES["w-dim-lv"] = "M736 413 C 770 413, 770 135, 736 135"; WIRES["w-dim-rc"] = "M736 413 C 760 413, 760 319, 736 319";
  ["raw_page_views", "raw_partner_clicks", "raw_reviews"].forEach((r) => (WIRES[`w-c-${r}`] = `M600 64 C 600 84, ${N[r][0] + 100} 80, ${N[r][0] + 100} 104`));
  WIRES["w-c-pii"] = "M730 37 C 860 37, 882 120, 882 196"; WIRES["w-c-q"] = "M730 30 C 840 30, 882 60, 882 104";
  ["location_views_hourly", "partner_revenue_15m", "reviews_current"].forEach((g) => wire(`w-g-q-${g}`, mid(g, "r"), mid("quality", "l")));
  wire("w-raw-pii", [450, 319], mid("pii", "l")); wire("w-rc-er", mid("reviews_current", "r"), mid("erasure", "l"));
  wire("w-q-wh", mid("quality", "r"), [1012, 200]); wire("w-p-wh", mid("pii", "r"), [1012, 235]); wire("w-e-wh", mid("erasure", "r"), [1012, 270]);
  $("g-wires").innerHTML = Object.entries(WIRES).map(([id, dd]) => `<path id="${id}" class="wire" d="${dd}"/>`).join("");
  $("g-nodes").innerHTML = Object.entries(N).map(([k, [x, y, w, h, t, s]]) => `<g class="node off" id="n-${k}" transform="translate(${x},${y})">
    <rect width="${w}" height="${h}" rx="12"/><text x="${w / 2}" y="${h / 2 - 3}" text-anchor="middle" style="font-size:${t.length > 18 ? 12.5 : 14}px">${t}</text>
    <text class="sm" x="${w / 2}" y="${h / 2 + 15}" text-anchor="middle">${NX.esc(s || sub(k))}</text></g>`).join("") +
    `<text x="109" y="92" text-anchor="middle" class="flink-label" style="fill:#c3caff">KAFKA</text><text x="350" y="92" text-anchor="middle" class="flink-label" style="fill:#9cc0b7">RAW</text>
     <text x="621" y="92" text-anchor="middle" class="flink-label" style="fill:#34e0a1">GOLD</text><text x="882" y="92" text-anchor="middle" class="flink-label" style="fill:#ffd27a">GOVERNANCE</text>
     <text x="1098" y="138" text-anchor="middle" class="flink-label" style="fill:#a9d0ff">WAREHOUSE</text>
     <g transform="translate(1012,150)"><text x="86" y="124" text-anchor="middle" style="font:500 10.5px var(--mono);fill:#a9d0ff">BigQuery: day partitions</text>
       <text x="86" y="140" text-anchor="middle" style="font:500 10.5px var(--mono);fill:#cde3dd">Snowflake: masking policy</text>
       <text x="86" y="156" text-anchor="middle" style="font:500 10.5px var(--mono);fill:#9cc0b7">same MERGE · same key</text></g>`;

  // ---------- helpers for the terminal ----------
  const J = (o) => JSON.stringify(o);
  const table = (q) => {
    const w = q.cols.map((c, i) => Math.max(c.length, ...q.rows.map((r) => String(r[i]).length)));
    return [q.cols.map((c, i) => c.padEnd(w[i])).join("  "), w.map((n) => "-".repeat(n)).join("  "),
            ...q.rows.map((r) => r.map((v, i) => String(v).padEnd(w[i])).join("  "))];
  };
  const sql = (q) => [`sqlite> ${q.sql};`, ...table(q)];
  const SQLITE = "$ sqlite3 -header -column target/warehouse.db     # in the build: Gold, loaded with the same upsert as the MERGE";
  const qy = (needle) => Q.find((x) => x.sql.includes(needle));
  const cat = async (f, from, n = 14) => { const ls = (await file(f)).split("\n"); const i = Math.max(0, ls.findIndex((l) => l.includes(from)));
    return [`$ sed -n '${i + 1},${i + n}p' ${f}`, ...ls.slice(i, i + n)]; };
  const chk = (name) => C.find((c) => c.check === name);
  const piiRaw = M.raw.flatMap((r) => r.columns.filter((c) => c.pii).map((c) => `${r.table}.${c.name}`));
  const gold = Object.fromEntries(M.gold.map((g) => [g.table, g]));

  // ---------- the six parts ----------
  const S = (title, text, nodes, wires, log, term, py, hot = []) => ({ title, text, nodes, wires, log, term, py, hot });
  const SCEN = {
    model: { label: "1 · The model", steps: [
      S("Three topics", `Kafka keeps what producers send: page views, partner clicks and reviews. Nothing is modelled yet; every record is kept as sent.`,
        ["t-page_views", "t-partner_clicks", "t-reviews"], [], { topics: d.generator.records },
        async () => ["$ jq -c '.generator.records' assets/data/site.json", J(d.generator.records)], ["src/nexus/generate.py", "class Log:"]),
      S("Raw: every field, typed, with where it came from", `One raw table per topic: every field of every schema version, plus _partition, _offset and _appended_at, so any row can be traced back to its Kafka record.`,
        ["t-page_views", "t-partner_clicks", "t-reviews", "raw_page_views", "raw_partner_clicks", "raw_reviews"], ["w-t-page_views", "w-t-partner_clicks", "w-t-reviews"],
        { raw: M.raw.map((r) => ({ table: r.table, columns: r.columns.length })) },
        async () => cat("warehouse/bigquery/raw_partner_clicks.sql", "CREATE", 18), ["src/nexus/warehouse.py", "def raw_columns("]),
      S("Gold: a stated grain and a key", `${M.gold.map((g) => `<b>${g.table}</b>: ${g.grain}`).join("; ")}. The key is what the MERGE matches on, so a late re-fire updates one row.`,
        ["raw_page_views", "raw_partner_clicks", "raw_reviews", "location_views_hourly", "partner_revenue_15m", "reviews_current"],
        ["w-r-location_views_hourly", "w-r-partner_revenue_15m", "w-r-reviews_current"], M.gold.map((g) => ({ table: g.table, grain: g.grain, key: g.key })),
        async () => [SQLITE, ...sql(qy("FROM location_views_hourly ORDER BY views"))], ["src/nexus/warehouse.py", "PRODUCTS = {"]),
      S("A conformed dimension", "dim_location joins views and reviews on the same location_id, so 'reviews per city' and 'views per city' mean the same city.",
        ["dim_location", "location_views_hourly", "reviews_current"], ["w-dim-lv", "w-dim-rc"], { dimension: "dim_location", joins: ["location_views_hourly", "reviews_current"] },
        async () => [SQLITE, ...sql(qy("JOIN dim_location"))], ["src/nexus/warehouse.py", "CREATE TABLE dim_location"]),
      S("Answers a person can use", `Revenue by partner for the week, from partner_revenue_15m: one query, on a table built for it.`,
        ["partner_revenue_15m", "bigquery", "snowflake"], ["w-q-bq", "w-q-sf"], { rows: W.rows },
        async () => [SQLITE, ...sql(qy("GROUP BY partner"))], ["src/nexus/pipeline.py", "\"partner_revenue_15m\": [{"]),
    ] },
    contract: { label: "2 · One contract, two warehouses", steps: [
      S("The contract", "What a topic is, its key and id, its schema versions, its personal fields, and the product built from it with its SLA. One file, reviewed like code.",
        ["contract"], [], { file: "contracts/sources.yml" }, async () => cat("contracts/sources.yml", "- topic: partner_clicks", 22), ["src/nexus/contracts.py", "def _load("]),
      S("Rendered, never hand-written", `<b>${W.files.length}</b> SQL files, BigQuery and Snowflake, generated from the contract on every run.`,
        ["contract", "raw_page_views", "raw_partner_clicks", "raw_reviews"], ["w-c-raw_page_views", "w-c-raw_partner_clicks", "w-c-raw_reviews"], { files: W.files },
        async () => ["$ PYTHONPATH=src python -m nexus.warehouse", J(W.files)], ["src/nexus/warehouse.py", "def render("]),
      S("BigQuery", "Partitioned by day on window_start and clustered by partner: a query for one day and one partner scans one partition's blocks. You pay for bytes scanned.",
        ["partner_revenue_15m", "bigquery"], ["w-q-bq"], W.dialects.slice(0, 4).map((x) => ({ [x.concern]: x.bigquery })),
        async () => cat("warehouse/bigquery/partner_revenue_15m.sql", "CREATE", 22), ["src/nexus/warehouse.py", "def _ddl("]),
      S("Snowflake", "Clustered by date and partner; the same MERGE, the same key, in Snowflake's types and names.",
        ["partner_revenue_15m", "snowflake"], ["w-q-sf"], W.dialects.slice(0, 4).map((x) => ({ [x.concern]: x.snowflake })),
        async () => cat("warehouse/snowflake/partner_revenue_15m.sql", "CREATE", 22), ["src/nexus/warehouse.py", "def _merge("]),
      S("CI holds the line", "The committed SQL must be exactly what the contract renders. Change the contract without the SQL, or the SQL by hand, and the build fails.",
        ["contract", "bigquery", "snowflake"], [], { test: "tests/test_warehouse.py::test_the_committed_sql_is_what_the_contract_renders", status: "passed" },
        async () => ["$ ./scripts/build.sh && git diff --exit-code -- warehouse/ && echo 'warehouse SQL is current'", "warehouse SQL is current",
          "$ PYTHONPATH=src python -m pytest -q tests/test_warehouse.py", "....", "4 passed"], ["tests/test_warehouse.py", "def test_the_committed_sql_is_what_the_contract_renders"]),
    ] },
    pii: { label: "3 · Personal data", steps: [
      S("Named once", `The contract lists the personal fields: <b>${piiRaw.join(", ")}</b>.`, ["contract", "raw_page_views", "raw_reviews"], ["w-c-raw_page_views", "w-c-raw_reviews"],
        { pii: piiRaw }, async () => cat("contracts/sources.yml", "- topic: reviews", 14), ["src/nexus/contracts.py", "def topic("]),
      S("Still in raw", "raw_reviews keeps them so the platform can work, but nobody without the role sees them in the clear.",
        ["raw_reviews", "pii"], ["w-raw-pii"], { columns: qy("pragma_table_info").rows }, async () => [SQLITE, ...sql(qy("pragma_table_info"))], ["src/nexus/warehouse.py", "def raw_columns("], ["pii"]),
      S("BigQuery: a policy tag", "Each personal column gets the pii.restricted policy tag and a SHA-256 data masking rule for every role without Fine-Grained Reader.",
        ["pii", "bigquery"], ["w-c-pii", "w-p-bq"], { bigquery: "policy tag pii.restricted + masking rule" }, async () => cat("warehouse/bigquery/governance.sql", "--", 8), ["src/nexus/warehouse.py", "def _governance("]),
      S("Snowflake: a masking policy", "One masking policy, SHA2 unless the session has PII_READER, attached to every personal column.",
        ["pii", "snowflake"], ["w-p-sf"], { snowflake: "MASKING POLICY PII_STRING" }, async () => cat("warehouse/snowflake/governance.sql", "CREATE MASKING", 7), ["src/nexus/warehouse.py", "def _governance("]),
      S("Checked, every build", `A check per personal field: masking in Snowflake and a tag in BigQuery, or the build fails. ${C.filter((c) => c.check.startsWith("pii_masked")).length} checks, all pass.`,
        ["pii", "bigquery", "snowflake", "quality"], ["w-p-bq", "w-p-sf"], C.filter((c) => c.check.startsWith("pii_masked")).map((c) => ({ check: c.check, status: c.status })),
        async () => ["$ jq -c '.warehouse.checks[] | select(.check | startswith(\"pii\")) | {table, check, status}' assets/data/site.json",
          ...C.filter((c) => c.check.startsWith("pii_masked")).map((c) => J({ table: c.table, check: c.check, status: c.status }))], ["src/nexus/warehouse.py", "for c in t[\"pii\"]:"]),
    ] },
    quality: { label: "4 · Quality gates", steps: [
      ...["location_views_hourly", "partner_revenue_15m", "reviews_current"].map((t) => S(`${t}: ${C.filter((c) => c.table === t).length} rules`,
        `${C.filter((c) => c.table === t).map((c) => `<code>${NX.esc(c.check)}</code>`).join(", ")}: ${C.filter((c) => c.table === t).reduce((a, c) => a + c.failing, 0)} failing rows of ${NX.int(rows[t])}.`,
        [t, "quality"], [`w-g-q-${t}`], C.filter((c) => c.table === t).map((c) => ({ check: c.check, failing: c.failing, status: c.status })),
        async () => [SQLITE, ...C.filter((c) => c.table === t).flatMap((c) => [`sqlite> SELECT count(*) AS failing FROM ${t} WHERE NOT (${c.rule});`, "failing", "-------", String(c.failing)])],
        ["src/nexus/warehouse.py", "for table, rules in load_quality()"])),
      S("And a volume gate before Gold", `An hour more than 2σ above the same hour on earlier days, and at least double, is held: <b>${d.volume.holds.length}</b> held (the scraper), then released clean.`,
        ["quality", "location_views_hourly"], ["w-g-q-location_views_hourly"], d.volume.holds[0],
        async () => ["$ jq -c '.volume.holds[0]' assets/data/site.json", J(d.volume.holds[0])], ["src/nexus/batch.py", "def volume_holds("], ["quality"]),
      S("Publish only what passed", `${C.filter((c) => c.status === "pass").length}/${C.length} checks pass, so Gold goes to both warehouses. A failing rule would hold the product and keep the rows as evidence.`,
        ["quality", "bigquery", "snowflake"], ["w-q-bq", "w-q-sf"], { passed: C.filter((c) => c.status === "pass").length, total: C.length },
        async () => cat("contracts/quality.yml", "rules:", 16), ["contracts/quality.yml", "rules:"]),
    ] },
    erasure: { label: "5 · Erasure", steps: [
      S("A review, version by version", "Submitted, moderated, edited: every version is in raw_reviews with the reviewer's hash.",
        ["raw_reviews", "reviews_current"], ["w-r-reviews_current"], { rows: qy("version, op, reviewer_hash").rows },
        async () => [SQLITE + "   (before the erasure)", ...sql(qy("version, op, reviewer_hash"))], ["src/nexus/warehouse.py", "def load("]),
      S("The tombstone", "The stream applies version 4, an erasure; a stale version 3 that arrives after it is rejected (see the streaming page).",
        ["reviews_current", "erasure"], ["w-rc-er"], d.trace.reviews.slice(-2).map((x) => ({ version: x.version, op: x.op, decision: x.decision })),
        async () => ["$ jq -c '.trace.reviews[-2:][] | {version, op, decision}' assets/data/site.json", ...d.trace.reviews.slice(-2).map((x) => J({ version: x.version, op: x.op, decision: x.decision }))],
        ["src/nexus/stream.py", "def _apply_version("], ["erasure"]),
      S("DELETE, in both warehouses", "Generated from the contract: delete from raw and from Gold, with the ids as a parameter.",
        ["erasure", "bigquery", "snowflake"], ["w-e-bq", "w-e-sf"], { bigquery: "IN UNNEST(@review_ids)", snowflake: "FLATTEN(PARSE_JSON(:review_ids))" },
        async () => [...(await cat("warehouse/bigquery/erasure.sql", "DELETE", 3)), "", ...(await cat("warehouse/snowflake/erasure.sql", "DELETE", 3))], ["src/nexus/warehouse.py", "def _erasure("]),
      S("Gone, and checked", `${W.erased_rows_removed} raw rows removed; ${chk("erased_reviews_gone").failing} left. Time travel keeps deleted rows for a while: the SQL says how long, so the erasure has a completion date.`,
        ["raw_reviews", "reviews_current", "quality"], [], { check: "erased_reviews_gone", status: chk("erased_reviews_gone").status },
        async () => [SQLITE + "   (after)", ...sql(qy("rows_left"))], ["src/nexus/warehouse.py", "erased_reviews_gone"]),
    ] },
    schema: { label: "6 · A schema change", steps: [
      S("App 7.4 changes the price", "partner_clicks v2 sends price {amount, currency} instead of cpc_gbp.",
        ["t-partner_clicks", "contract"], [], { v1: "cpc_gbp: decimal", v2: "price: {amount, currency}" }, async () => cat("contracts/sources.yml", "schemas:", 8, ), ["contracts/sources.yml", "schemas:"]),
      S("A registry would refuse it", "BACKWARD compatibility: a reader on v2 can't read v1 data. Found before it ships, not after.",
        ["contract"], [], { compatible: false }, async () => ["$ PYTHONPATH=src python -c \"from nexus.contracts import compatible, topic; s = topic('partner_clicks')['schemas']; print(compatible(s[1], s[2]))\"",
          "[\"field 'price' is new in v2 and absent from v1 data\", \"field 'cpc_gbp' was removed\"]"], ["src/nexus/contracts.py", "def compatible("], ["contract"]),
      S("Raw takes both", `raw_partner_clicks gets price_amount and price_currency next to cpc_gbp, plus revenue_gbp and _schema_version: ${M.raw.find((r) => r.table === "raw_partner_clicks").columns.length} columns, no rewrite of old rows.`,
        ["raw_partner_clicks", "contract"], ["w-c-raw_partner_clicks"], { columns: M.raw.find((r) => r.table === "raw_partner_clicks").columns.map((c) => c.name) },
        async () => cat("warehouse/bigquery/raw_partner_clicks.sql", "CREATE", 18), ["src/nexus/warehouse.py", "def raw_columns("]),
      S("Gold doesn't notice", "One reader upcasts both versions to revenue in GBP, so partner_revenue_15m keeps its grain, key and meaning across the change.",
        ["raw_partner_clicks", "partner_revenue_15m", "quality"], ["w-r-partner_revenue_15m", "w-g-q-partner_revenue_15m"], { v2_records: d.generator.v2_records },
        async () => [SQLITE, ...sql(qy("revenue_gbp / clicks BETWEEN"))], ["src/nexus/contracts.py", "def upcast("]),
    ] },
  };

  // one warehouse node stands for both: map the steps' names onto it
  const WH = (x) => (x === "bigquery" || x === "snowflake" ? "warehouse" : x);
  const WHW = (w) => w.replace(/-(bq|sf)$/, "-wh");
  Object.values(SCEN).forEach((sc) => sc.steps.forEach((st) => {
    st.nodes = [...new Set(st.nodes.map(WH))]; st.hot = [...new Set(st.hot.map(WH))]; st.wires = [...new Set(st.wires.map(WHW))]; }));

  // ---------- rendering (same controls as the streaming page) ----------
  const NODES = Object.keys(N), WIRE_IDS = Object.keys(WIRES);
  let key = "model", i = -1, timer = null, pane = "term";
  $("views").innerHTML = Object.entries(SCEN).map(([k, s]) => `<button role="tab" data-k="${k}" aria-selected="${k === key}">${s.label}</button>`).join("");
  const packet = (w) => new Promise((res) => {
    const path = $(w); if (!path) return res();
    const len = path.getTotalLength(), c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    c.setAttribute("r", 6); c.setAttribute("fill", "#34e0a1"); c.setAttribute("class", "packet"); $("packets").appendChild(c);
    const t0 = performance.now(), dur = +$("speed").value / 3;
    const step = (now) => { const f = Math.min(1, (now - t0) / dur), p = path.getPointAtLength(f * len);
      c.setAttribute("cx", p.x); c.setAttribute("cy", p.y); if (f < 1) requestAnimationFrame(step); else { c.remove(); res(); } };
    requestAnimationFrame(step);
  });
  const paint = async (st) => {
    document.querySelectorAll("#term-tabs button").forEach((b) => b.classList.toggle("on", b.dataset.p === pane));
    const box = $("term-out");
    if (!st) { box.innerHTML = `<span class="t-dim">Step through a part: its SQL, files and code appear here.</span>`; return; }
    if (pane === "term") {
      const lines = await st.term();
      box.innerHTML = lines.map((l) => `<span class="${l.startsWith("$") || l.startsWith("sqlite>") ? "t-cmd" : l.startsWith("#") ? "t-dim" : ""}">${NX.esc(l)}</span>`).join("\n");
    } else {
      const [f, mark] = st.py, ls = (await file(f)).split("\n"), at = Math.max(0, ls.findIndex((l) => l.includes(mark)));
      const from = Math.max(0, at - 2), to = Math.min(ls.length, at + 18);
      box.innerHTML = `<span class="t-dim"># ${f}, lines ${from + 1}–${to}</span>\n` + ls.slice(from, to).map((l, k) =>
        `<span class="${from + k === at ? "t-hit" : ""}"><i>${String(from + k + 1).padStart(4)}</i>  ${NX.esc(l)}</span>`).join("\n");
    }
  };
  const render = async () => {
    const sc = SCEN[key], st = sc.steps[i];
    $("steps").innerHTML = sc.steps.map((s, k) => `<li class="${k < i ? "done" : k === i ? "now" : ""}" data-i="${k}"><span class="dot">${k + 1}</span><span><b>${s.title}</b></span></li>`).join("");
    if (!st) { NODES.forEach((n) => $("n-" + n).setAttribute("class", "node off")); WIRE_IDS.forEach((w) => $(w).setAttribute("class", "wire"));
      $("caption").innerHTML = `<b>${sc.label.replace(/^\d · /, "")}.</b> Press Play, or Next to step through it.`; $("log").innerHTML = ""; paint(null); return; }
    NODES.forEach((n) => $("n-" + n).setAttribute("class", "node " + (st.hot.includes(n) ? "hot" : st.nodes.includes(n) ? "on" : "off")));
    WIRE_IDS.forEach((w) => $(w).setAttribute("class", "wire" + (st.wires.includes(w) ? " on" : "")));
    $("caption").innerHTML = `<b>${st.title}.</b> ${st.text}`;
    const m = document.createElement("div"); m.className = `msg ${st.hot.length ? "err" : "res"}`;
    m.innerHTML = `<span class="dir">step ${i + 1} · ${sc.label.replace(/^\d · /, "")}</span>${NX.esc(JSON.stringify(st.log).slice(0, 700))}`;
    $("log").prepend(m);
    paint(st);
    await Promise.all(st.wires.map(packet));
  };
  const go = (k) => { i = Math.max(-1, Math.min(SCEN[key].steps.length - 1, k)); render(); };
  const stop = () => { clearInterval(timer); timer = null; $("play").textContent = "▶ Play"; };
  const select = (k) => { stop(); key = k; $("log").innerHTML = "";
    document.querySelectorAll("#views [role=tab]").forEach((b) => b.setAttribute("aria-selected", b.dataset.k === k)); go(-1); };
  $("views").addEventListener("click", (e) => { const b = e.target.closest("[data-k]"); if (b) { select(b.dataset.k); history.replaceState(null, "", "#" + b.dataset.k); } });
  $("next").addEventListener("click", () => { stop(); go(i + 1); });
  $("prev").addEventListener("click", () => { stop(); go(i - 1); });
  $("steps").addEventListener("click", (e) => { const li = e.target.closest("li"); if (li) { stop(); go(+li.dataset.i); } });
  $("term-tabs").addEventListener("click", (e) => { const b = e.target.closest("button"); if (!b) return; pane = b.dataset.p; paint(SCEN[key].steps[i]); });
  $("play").addEventListener("click", () => {
    if (timer) return stop();
    if (i >= SCEN[key].steps.length - 1) go(-1);
    $("play").textContent = "❚❚ Pause"; go(i + 1);
    timer = setInterval(() => { if (i >= SCEN[key].steps.length - 1) { const ks = Object.keys(SCEN), nx = ks[ks.indexOf(key) + 1];
      if (nx && document.body.dataset.tour) { select(nx); $("play").click(); } else stop(); return; } go(i + 1); }, +$("speed").value * 2.4);
  });

  // ---------- the model as cards ----------
  $("gold-cards").innerHTML = M.gold.map((g, k) => `<div class="card ${k % 2 ? "top-p" : "top-v"}"><div class="kicker" style="color:#34e0a1">Gold · tier ${g.tier} · SLA ${g.sla_minutes} min</div>
    <h3 style="color:#fff;margin:.3rem 0">${g.table}</h3><p><b>Grain:</b> ${g.grain}<br><b>Key:</b> <code>${g.key.join(", ")}</code><br>
    <b>BigQuery:</b> <code>${g.partition ? `PARTITION BY DATE(${g.partition}) ` : ""}CLUSTER BY ${g.cluster.join(", ")}</code><br><b>Rows:</b> ${NX.int(rows[g.table])}</p>
    <p class="mono" style="font-size:.74rem;margin-top:.4rem">${g.columns.map((c) => `${c.name} ${c.type}`).join(" · ")}</p></div>`).join("");
  $("raw-cards").innerHTML = M.raw.map((r) => `<div class="card"><div class="kicker" style="color:#9cc0b7">Raw · from Kafka ${r.topic} · owner ${r.owner}</div>
    <h3 style="color:#fff;margin:.3rem 0">${r.table}</h3><p class="mono" style="font-size:.74rem">${r.columns.map((c) => c.pii ? `<span style="color:#ff9db2">${c.name} ⚑</span>` : c.name).join(" · ")}</p>
    <p style="font-size:.8rem;margin-top:.3rem">⚑ personal: policy tag in BigQuery, masking policy in Snowflake</p></div>`).join("");

  const start = location.hash.slice(1);
  select(SCEN[start] ? start : key);
  if (new URLSearchParams(location.search).has("tour")) { document.body.dataset.tour = 1; $("play").click(); }
})();
