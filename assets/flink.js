// The Flink job on the page: tabs over its real source files, the run's parity result, and the Python ↔ Flink map.
// Used by the home page and the streaming page.
const NXFlink = (() => {
  const FILES = [
    ["NexusJobs.java", "flink/job/src/main/java/nexus/flink/NexusJobs.java", "The topology: parse → dedupe → watermarks → keyed tumbling windows with allowed lateness, and three side outputs."],
    ["Parse.java", "flink/job/src/main/java/nexus/flink/Parse.java", "Both schema versions into one shape; what can't be read goes to the dead-letter output. The 9 October lesson, in code."],
    ["Dedupe.java", "flink/job/src/main/java/nexus/flink/Dedupe.java", "Keyed by id, a ValueState flag with a 24-hour TTL: checkpointed with the offsets, so a restore can't let a retry through."],
    ["PartitionWatermarks.java", "flink/job/src/main/java/nexus/flink/PartitionWatermarks.java", "min over active partitions − out-of-orderness, idle partitions left out: what KafkaSource does per split, for a local replay."],
    ["Windows.java", "flink/job/src/main/java/nexus/flink/Windows.java", "An incremental AggregateFunction plus a ProcessWindowFunction that emits on every firing, late re-fires included."],
    ["NexusKafka.java", "flink/job/src/main/java/nexus/flink/NexusKafka.java", "Production: KafkaSource with withIdleness, exactly-once checkpoints, a failure-rate restart strategy, transactional Kafka sinks for gold, DLQ and late."],
    ["NexusLocal.java", "flink/job/src/main/java/nexus/flink/NexusLocal.java", "The same topology on a MiniCluster, replaying the week in arrival order; writes what the upsert sinks would hold."],
    ["pom.xml", "flink/job/pom.xml", "Flink 1.20.1, the Kafka connector 3.4, Java 17."],
    ["Flink SQL", "flink/page_views_hourly.sql", "The same product as Flink SQL: idle timeout, allowed lateness and late firing as table options."],
  ];
  const MAP = [
    ["Job(...).run()", "StreamExecutionEnvironment + NexusJobs.build()"],
    ["upcast() + dead letters", "ProcessFunction + OutputTag<DeadLetter>"],
    ["state.seen, 24 h", "ValueState<Boolean> with StateTtlConfig"],
    ["watermark: min over active partitions − ooo", "forBoundedOutOfOrderness(ooo).withIdleness(5 min)"],
    ["window by event time", "TumblingEventTimeWindows.of(1 h)"],
    ["re-fire within the hour", "allowedLateness(1 h)"],
    ["side output past lateness", "sideOutputLateData(TOO_LATE)"],
    ["checkpoint every 10 min", "enableCheckpointing(10 min, EXACTLY_ONCE)"],
    ["keyed upsert sink", "KafkaSink, DeliveryGuarantee.EXACTLY_ONCE"],
    ["74 restarts (the incident)", "failureRateRestart(3, 10 min): fail and page"],
  ];
  const ok = (b, y, n) => `<span class="verdict ${b ? "ok" : "bad"}">${b ? y : n}</span>`;
  async function mount({ tabs, why, code, tiles, map, start = 0 }) {
    const d = await NX.site();
    const F = d.flink, $ = (id) => document.getElementById(id);
    if (tiles && $(tiles)) {
      const v = F && F.location_views_hourly, p = F && F.partner_revenue_15m;
      $(tiles).innerHTML = F ? [
        [NX.int(v.windows), "windows on Flink " + F.flink, ok(v.identical_to_python, "identical to the Python job", "differs"), `${NX.int(v.firings)} firings · ${v.duplicates} duplicates · ${v.late_dropped} too late`],
        [p.dead_letters, "poison pill, dead-lettered", ok(p.dead_letters === 1, "no crash loop", "check"), NX.esc(p.dead_letter_records[0] || "")],
        [NX.int(p.windows), "revenue windows", ok(p.identical_to_batch, "identical to the batch", "differs"), "per-split idleness kept the 3 a.m. click the Python heartbeat dropped"],
      ].map(([n, l, v2, s], i) => `<div class="card ${i % 2 ? "top-p" : "top-v"}"><div class="big ${i % 2 ? "p" : ""}">${n}</div>
        <h3 style="margin:.4rem 0 .3rem;color:#fff">${l}</h3><p>${v2}</p><p style="font-size:.8rem;margin-top:.3rem">${s}</p></div>`).join("")
        : `<p>The Flink job runs in CI; this build had no Maven.</p>`;
    }
    if (map && $(map)) $(map).innerHTML = `<div class="tablewrap wrap"><table><tr><th>stream.py</th><th>Flink</th></tr>${MAP.map(([a, b]) =>
      `<tr><td class="mono" style="font-size:.76rem">${NX.esc(a)}</td><td class="mono" style="font-size:.76rem">${NX.esc(b)}</td></tr>`).join("")}</table></div>`;
    const show = async (k) => {
      $(tabs).querySelectorAll("button").forEach((b, i) => b.classList.toggle("on", i === k));
      $(why).textContent = FILES[k][2] + "  ·  " + FILES[k][1];
      $(code).textContent = await fetch(FILES[k][1]).then((r) => r.text());
    };
    $(tabs).innerHTML = FILES.map(([n]) => `<button>${n}</button>`).join("");
    $(tabs).addEventListener("click", (e) => { const b = e.target.closest("button"); if (b) show([...$(tabs).children].indexOf(b)); });
    show(start);
  }
  return { mount };
})();
