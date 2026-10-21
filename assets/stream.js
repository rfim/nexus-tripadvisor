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
      S("The SDK times out and sends it again", `Same event_id, new offset <b>${dup.offset}</b>, ${((Date.parse(dup.arrived) - Date.parse(dupFirst.arrived)) / 1000).toFixed(0)} s later: two records to Kafka.`,
        ["kafka", "read", "dedupe"], ["w-kr", "w-rd"], rec(dup), { part: dup.partition, hot: ["dedupe"] }),
      S("Dropped, not counted twice", `The id is already in dedupe state (first seen ${t(dup.first_seen)}): <b>${d.streams.page_views.ledger.duplicates}</b> duplicates this week.`,
        ["dedupe"], [], { duplicates: d.streams.page_views.ledger.duplicates, ttl: "24 h, checkpointed" }),
    ] },
    late: { label: "2 · Late, and too late", topic: "page_views", steps: [
      S("Late, but inside the hour", `An Android event from ${t(lateOk.event_time)} arrives at ${t(lateOk.arrived)}, after its window fired (watermark ${t(lateOk.wm)}).`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(lateOk), { part: lateOk.partition, wm: lateOk.wm }),
      S("The window is updated and fires again", `Inside the hour of lateness: the count becomes <b>${lateOk.n}</b> and the sink upserts. ${d.streams.page_views.ledger.late_updates} re-fires this week.`,
        ["window", "sink"], ["w-ws"], { window: lateOk.window, n: lateOk.n, decision: lateOk.decision }),
      S("An offline upload, hours later", `${P.offline_upload.sessions} iOS sessions from a flight flush at ${t(P.offline_upload.delivered_at)}. This one is from ${t(tooLate[0].event_time)}.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(tooLate[0]), { part: tooLate[0].partition, wm: tooLate[0].wm }),
      S("Too late for the stream", `Its window closed at ${t(tooLate[0].closes)}, so it goes to the side output.`,
        ["window", "side"], ["w-wl"], { decision: tooLate[0].decision, window: tooLate[0].window, closed: tooLate[0].closes }, { hot: ["side"] }),
      S("The batch counts it", `The batch has no lateness. It differs in <b>${d.reconcile.page_views.explained_by_side_output}</b> windows; the side output explains all (${d.reconcile.page_views.unexplained} unexplained).`,
        ["side", "batch", "sink"], ["w-lb", "w-sb"], { stream: d.reconcile.page_views.stream_events, batch: d.reconcile.page_views.batch_events,
          side_output: d.reconcile.page_views.late_events, unexplained: d.reconcile.page_views.unexplained }),
    ] },
    schema: { label: "3 · A schema change", topic: "partner_clicks", steps: [
      S("Version 1: cpc_gbp", `A web click at ${t(v1.event_time)}: <code>cpc_gbp</code>, a number in pounds.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], rec(v1) && { ...rec(v1), schema: 1, revenue_gbp: v1.revenue_gbp }, { part: v1.partition, wm: v1.wm }),
      S("Version 2: price {amount, currency}", `App ${P.schema_v2.app_version} from ${t(P.schema_v2.from)}. A backward check fails: <code>cpc_gbp</code> removed, <code>price</code> new.`,
        ["kafka", "read"], ["w-kr"], { ...rec(v2s[0]), schema: 2, revenue_gbp: v2s[0].revenue_gbp }, { part: v2s[0].partition, hot: ["read"] }),
      S("One reader, both versions", `Converted at the contract's rates: <b>£${v2s[v2s.length - 1].revenue_gbp}</b>. An unknown currency is a dead letter.`,
        ["read", "dedupe", "window", "sink"], ["w-rd", "w-dw", "w-ws"], { ...rec(v2s[v2s.length - 1]), schema: 2, revenue_gbp: v2s[v2s.length - 1].revenue_gbp }),
    ] },
    poison: { label: "4 · A poison pill", topic: "partner_clicks", steps: [
      S("57 bytes of truncated JSON", `Partition ${fails.partition}, offset ${fails.offset}, at ${t(fails.arrived)}. The producer cut it short.`,
        ["kafka", "read"], ["w-kr"], { partition: fails.partition, offset: fails.offset, raw: fails.raw }, { part: fails.partition, hot: ["read"] }),
      S("The job fails, restarts, fails again", `With no dead-letter topic it can only stop. Each restore reads the same record: <b>${I.restarts}</b> restarts.`,
        ["read"], [], { decision: fails.decision, reason: fails.reason, restarts: I.restarts }, { hot: ["read", "window"] }),
      S(`Paged at ${t(I.detected)}`, `Nothing downstream moves, so partner revenue ages past its 45-minute SLA. The restart count said so at ${t(I.timeline[1].at)}.`,
        ["sink"], [], { sla: "45 min", detected: I.detected, worst_age_minutes: I.worst_age_minutes }, { hot: ["sink"] }),
      S("Fix: dead-letter it", `Deployed at ${t(I.mitigated)}: the record is dead-lettered and the job catches up. Within SLA at ${t(I.recovered)}.`,
        ["read", "dlq", "dedupe", "window", "sink"], ["w-rq", "w-rd", "w-dw", "w-ws"], { decision: dl.decision, processed: dl.processed, reason: dl.reason }),
    ] },
    bots: { label: "5 · A scraper", topic: "page_views", steps: [
      S("Three users, a flood of views", `${t(P.bots.from)}: <b>${hold.views}</b> views where ${hold.mean} is usual. To the stream, valid page views.`,
        ["kafka", "read", "dedupe", "window", "sink"], ["w-kr", "w-rd", "w-dw", "w-ws"], bot ? rec(bot) : { hour: hold.hour, views: hold.views }, { part: bot ? bot.partition : 0 }),
      S("The hour is held", `${hold.views} is over 2σ and double the usual (threshold ${hold.threshold}). Gold holds it.`,
        ["batch"], ["w-sb"], { hour: hold.hour, views: hold.views, mean: hold.mean, sd: hold.sd, threshold: hold.threshold, z: hold.z }, { hot: ["batch"] }),
      S("Investigated, filtered, released", `${inv.users_in_hour} users; ${inv.bots.length} made over ${inv.max_per_hour} views each. ${V.bot_views_removed} views removed; re-checked: ${V.holds_after_filter.length} holds.`,
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
      S("Restore from the checkpoint", `${d.replay.restored_from}. Offsets, windows, dedupe state and ledger come back together.`,
        ["kafka", "read", "dedupe", "window"], ["w-kr", "w-rd", "w-dw"], { restored: d.replay.restored_from }),
      S("Re-sent, and nothing changed", `${d.replay.re_emitted} windows re-sent; the sink upserts, so it's <b>${d.replay.identical ? "identical" : "different"}</b> to a run without the crash.`,
        ["window", "sink"], ["w-ws"], { re_emitted: d.replay.re_emitted, identical: d.replay.identical, windows: d.replay.windows }),
    ] },
  };

  // ---------- the same step on Flink (flink/job): the API call that makes this decision ----------
  const FL = {
    retries: ['records.process(new Parse("page_views"))', 'parsed.keyBy(e -> e.id).process(new Dedupe())   // ValueState<Boolean>, TTL 24 h',
              'if (seen.value() != null) { ctx.output(DUPLICATES, e); return; }'],
    late: ['.window(TumblingEventTimeWindows.of(Duration.ofHours(1))).allowedLateness(Duration.ofHours(1))',
           '// EventTimeTrigger fires again on each late element inside allowedLateness; the KafkaSink upserts on (start, group)',
           'WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(10)).withIdleness(Duration.ofMinutes(5))',
           '.sideOutputLateData(TOO_LATE)   →   windows.getSideOutput(TOO_LATE).sinkTo(sink(brokers, "late.page_views"))',
           '// the nightly batch counts late.page_views; Flink on the MiniCluster: 192 too late, same as the Python job'],
    schema: ['e.amount = v.get("cpc_gbp").asDouble();   // v1', 'Double rate = FX_TO_GBP.get(v.path("price").path("currency").asText());   // v2',
             'if (rate == null) { ctx.output(DEAD_LETTERS, new DeadLetter(r, "unknown currency")); return; }'],
    poison: ['try { v = json.readTree(r.value); } catch (Exception e) { ctx.output(DEAD_LETTERS, new DeadLetter(r, "not JSON: ...")); return; }',
             '// without that catch: an exception, a restore, the same offset, the same exception',
             'env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.seconds(30)));   // fail and page',
             'o.deadLetters().map(NexusKafka::dead).sinkTo(sink(brokers, "dlq.partner_clicks"))'],
    bots: ['// the stream counts them: a valid page view is a valid page view', '// the hold is a batch rule on Gold, not a stream rule', '// (Spark: hourly volume vs the same hour on earlier days)'],
    erasure: ['// not in flink/job yet: reviews_current runs in the Python job only. On Flink it would be:',
              'reviews.keyBy(v -> v.reviewId).process(new LatestVersion())   // ValueState<Integer> version',
              'if (version.value() != null && version.value() >= v.version) { ctx.output(STALE, v); return; }'],
    crash: ['env.enableCheckpointing(Duration.ofMinutes(10).toMillis(), CheckpointingMode.EXACTLY_ONCE);',
            '// restore: Kafka offsets + window state + dedupe state, from one checkpoint', 'KafkaSink ... .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)'],
  };
  Object.entries(FL).forEach(([k, xs]) => SCEN[k].steps.forEach((st, i) => { st.flink = xs[Math.min(i, xs.length - 1)]; }));

  // ---------- how each step's decision was chosen (assets/choice.js): measured on this week, or scored on criteria ----------
  const B = await fetch("assets/data/benchmarks.json").then((r) => r.json()).catch(() => null);
  if (B) {
    const o = B.out_of_orderness, lt = B.allowed_lateness, tt = B.dedupe_ttl, rg = B.retry_gap_seconds, br = B.bad_record,
          al = B.alerting, vr = B.volume_rule, ld = B.late_distribution, nDup = d.streams.page_views.ledger.duplicates;
    const ttlName = { "0.01": "remember 36 s", "0.25": "remember 15 min", "1": "remember 1 h", "24": "remember 24 h" };
    const policy = { fail_and_restart: "Fail and restart", skip_silently: "Skip silently", dead_letter: "Dead-letter topic" };
    const versionRule = { q: "which version of a review wins?", ask: "Why can't dedupe stop a stale version?", crit: ["Stale replay blocked", "Erasure sticks", "No extra state"],
      o: [["Last arrival wins", [0, 0, 1]], ["Dedupe by event_id", [0, 0, 0]], ["Highest version wins · tombstone is final", [1, 1, 0], 1]],
      why: "A replay re-sends version 3 with a new event_id, so dedupe can't see it. Only the version number can. The cost: one integer of state per review." };
    const CH = {
      retries: [
        { q: "what makes two records the same page view?", ask: "Why isn't the Kafka offset enough to spot a duplicate?", unit: "retries recognised", low: false,
          o: [["Kafka partition + offset", 0], ["event_id stamped by the SDK", nDup, 1]],
          why: "A re-send lands at a new offset. Only the id the SDK stamps survives it." },
        { q: "how long to remember an id?", ask: "Why remember ids for 24 hours when every retry comes within 20 seconds?", unit: "ids held in memory at peak",
          o: ["0.01", "0.25", "1", "24"].map((k) => [ttlName[k], tt[k].peak_dedupe_keys, k === "24", `catches ${tt[k].duplicates_dropped} of ${rg.count}`]),
          why: `Every option catches all ${rg.count} retries: the slowest came ${Math.round(rg.max)} s later. 24 h is margin for a consumer rewind after an outage, and costs only ${tt["24"].peak_dedupe_keys.toLocaleString("en-GB")} ids.` },
        { q: "count every record, or dedupe in the stream?", ask: "Why can't Kafka's idempotent producer stop these duplicates?", unit: "page views counted twice",
          o: [["Count every record", nDup], ["Dedupe in the stream", 0, 1]],
          why: "Kafka's idempotent producer can't help here: the app re-sent the event, not the broker." },
      ],
      late: [
        { q: "how long to wait for out-of-order events?", ask: "Why wait 10 minutes when 2 would be fresher?", unit: "p95 freshness, minutes",
          o: Object.entries(o).map(([k, v]) => [`wait ${k} min`, v.freshness_p95, k === "10", `${v.late_refires} re-fires`]),
          why: `2 min would be ${o["10"].freshness_p95 - o["2"].freshness_p95} min fresher. 10 min is headroom for phone clocks, well inside the 90-min SLA.` },
        { q: "how long to keep a fired window open?", ask: "Why not just wait 3 hours and catch everything?", unit: "late page views the stream misses (the batch still counts them)", low: null,
          o: Object.entries(lt).map(([k, v]) => [`keep ${k} min`, v.missed_vs_batch, k === "60", `${v.peak_open_windows} open windows`]),
          why: `180 min catches ${lt["60"].missed_vs_batch - lt["180"].missed_vs_batch} more but holds ${(lt["180"].peak_open_windows / lt["60"].peak_open_windows).toFixed(1)}× the windows. The batch counts the rest anyway.` },
        { q: "how late are late page views?", unit: "page views that missed their first firing", low: null,
          o: ld.buckets.map((b) => [b.bucket + " late", b.records, false]),
          why: `${Math.round(100 * ld.buckets[3].records / ld.records_missing_first_firing)}% arrive more than 3 h late: offline uploads. No stream wait catches them cheaply.` },
        { q: "drop a too-late record, or send it somewhere?", ask: "Why keep records that are too late to count?", unit: "stream-vs-batch differences nobody can explain",
          o: [["Drop it silently", d.reconcile.page_views.explained_by_side_output], ["Side output", d.reconcile.page_views.unexplained, 1]],
          why: "The side output is the receipt: every window where stream and batch differ is explained by it." },
        { q: "who has the final say on the count?", ask: "Why run two engines on the same data?", crit: ["Fresh within the hour", "Counts every record", "Checks the other"],
          o: [["Stream only", [1, 0, 0]], ["Batch only", [0, 1, 0]], ["Stream now · batch checks nightly", [1, 1, 1], 1]],
          why: "Two engines on the same records, reconciled every night. A difference nobody can explain is a bug." },
      ],
      schema: [
        { q: "how does v2 ship?", ask: "Why not just refuse v2 at the schema registry?", crit: ["App ships on time", "Consumers unchanged", "One history"],
          o: [["Refuse v2 at the registry", [0, 1, 1]], ["A new topic for v2", [1, 0, 0]], ["One reader upcasts both", [1, 1, 1], 1]],
          why: "The app team ships. Every consumer still reads one topic and one meaning: revenue in GBP." },
        { q: "which rate converts EUR and USD?", ask: "Why not use a live exchange rate?", crit: ["A replay gives the same £", "No network call in the job", "£ at once"],
          o: [["Live FX API", [0, 0, 1]], ["Convert later in the warehouse", [1, 1, 0]], ["Weekly rate in the contract", [1, 1, 1], 1]],
          why: "A replay must give the same answer as the first run. A live rate can't promise that." },
        { q: "an unknown currency arrives. Then what?", crit: ["No wrong revenue", "Evidence kept", "Job keeps running"],
          o: [["Assume GBP", [0, 0, 1]], ["Drop it", [1, 0, 1]], ["Dead-letter it", [1, 1, 1], 1]] },
      ],
      poison: [
        { q: "what does the job do with a record it can't read?", ask: "Why not let the job restart until it works?", unit: "restarts in the replayed week",
          o: Object.entries(br).map(([k, v]) => [policy[k], v.restarts, k === "dead_letter", `${v.records_kept_for_inspection} kept for inspection`]),
          why: "Skipping is free but loses the evidence. Dead-lettering is free and keeps it." },
        { q: "what does each policy cost the product?", unit: "minutes partner revenue was late",
          o: Object.entries(br).map(([k, v]) => [policy[k], v.minutes_late, k === "dead_letter"]),
          why: "A restart can't fix a bad record: it is still at the same offset after every restore." },
        { q: "which alert pages first?", ask: "Why didn't the freshness alert catch it sooner?", unit: "minutes from the first failure to a page",
          o: [["Freshness over the SLA", al.freshness_over_sla], ["Restarts > 3 in 10 min", al.restarts_over_3_in_10_min, 1]],
          why: "Freshness has to exceed the SLA before it says anything. The restart count says it in minutes." },
        { q: "after the fix: how old does the product get?", unit: "worst age of partner revenue, minutes",
          o: [["Fail and restart", br.fail_and_restart.worst_age_minutes], ["Dead-letter topic", br.dead_letter.worst_age_minutes, 1]] },
      ],
      bots: [
        { q: "where do we catch a scraper?", ask: "Why can't the stream itself spot a scraper?", crit: ["Stream stays simple", "History to compare", "Rows kept as evidence"],
          o: [["Ignore it", [1, 0, 1]], ["Filter in the stream", [0, 0, 0]], ["Hold the hour before Gold", [1, 1, 1], 1]],
          why: "To the stream a scraper's view is a valid view. Only a comparison with earlier days shows it." },
        { q: "which rule holds the scraper and nothing else?", ask: "Why isn't 2σ enough to flag a scraper?", unit: "false holds in the week",
          o: Object.entries(vr).map(([k, v]) => [k.replace(" (chosen)", ""), v.false_holds, k.includes("chosen"), v.caught_scraper ? "caught the scraper" : "missed it"]),
          why: "A week of history makes σ small and Saturdays look odd. Requiring at least double left only the scraper." },
        { q: "who made the views in the held hour?", unit: `views per user in the hour · over ${inv.max_per_hour} is not a person`, low: null,
          o: inv.top.slice(0, 6).map((u) => [u.user_hash, u.views, false, u.views > inv.max_per_hour ? "removed" : "kept"]),
          why: `${inv.bots.length} users over ${inv.max_per_hour} views an hour are removed. Everyone else stays.` },
      ],
      erasure: [versionRule],
      crash: [
        { q: "how often to checkpoint?", ask: "Why every 10 minutes and not every minute?", crit: ["Little replay after a crash", "Low overhead"],
          o: [["Every 1 min", [1, .5]], ["Every 10 min", [.5, 1], 1], ["Every hour", [0, 1]]],
          why: "10 minutes of records to replay is small next to a 90-minute SLA. Not benchmarked here: a design call." },
        { q: "what comes back on restore?", crit: ["Counts right", "No duplicates", "No gaps"],
          o: [["Offsets only", [0, 0, 1]], ["State only", [0, 1, 0]], ["Offsets + state from one checkpoint", [1, 1, 1], 1]],
          why: "Offsets and state are saved together, so the job resumes exactly where its state says it was." },
        { q: "how does the sink take re-sent windows?", ask: "Why does a crash not double-count anything?", unit: "duplicate rows after the restore",
          o: [["Plain INSERT", d.replay.re_emitted], ["MERGE on the window key", 0, 1]],
          why: `${d.replay.re_emitted} windows were sent twice. Keyed upserts make that harmless: the result is identical to a run without the crash.` },
      ],
    };
    Object.entries(CH).forEach(([k, xs]) => SCEN[k].steps.forEach((st, i) => { st.choice = xs[Math.min(i, xs.length - 1)]; }));
  }


  // ---------- the checkpoint node lights where state is saved or restored ----------
  const ck = (st) => { st.nodes = [...st.nodes, "ckpt"]; st.wires = [...st.wires, "w-ck"]; };
  ck(SCEN.crash.steps[1]); ck(SCEN.poison.steps[1]); ck(SCEN.poison.steps[3]);
  SCEN.crash.steps[0].hot = [...SCEN.crash.steps[0].hot];

  // ---------- the terminal: what an operator would type, with output from this run ----------
  // "$ jq ..." lines run on this repository's own files; "# on a cluster" blocks are the commands you'd type on a real
  // one, with output built from the run's records and decisions (no cluster sits behind a portfolio).
  const J = (o) => JSON.stringify(o);
  const CLUSTER = "# on a cluster · the output below is this run's own records and decisions";
  const kcat = (topic, recs) => [CLUSTER,
    `$ kafka-console-consumer.sh --bootstrap-server $KAFKA --topic ${topic} --partition ${recs[0].partition} --offset ${recs[0].offset} \\`,
    `    --max-messages ${recs.length} --property print.offset=true --property print.partition=true`,
    ...recs.map((r) => `Partition:${r.partition}\tOffset:${r.offset}\t${r.raw}`)];
  const jlog = (job, x) => `${x.processed.slice(0, 19).replace("T", " ")} INFO  [${job}] p${x.partition}/${x.offset} ${x.id || ""} → ${x.decision}`;
  const jq = (expr, val) => [`$ jq -c '${expr}' assets/data/site.json`, J(val)];
  const fcmd = (job) => [`$ flink list -r`, `${"08.10.2026 09:00:12"} : 7f3c…a91e : nexus ${job} (RUNNING)`];
  const S_ = d.streams, Rp = d.reconcile;
  const TERM = {
    retries: [
      [...kcat("page_views", [dupFirst, dup])],
      [CLUSTER, "$ flink logs nexus-location-views-hourly | grep -E 'duplicate|counted' | tail -2", jlog("location_views_hourly", dupFirst), jlog("location_views_hourly", dup)],
      [...jq(".streams.page_views.ledger", S_.page_views.ledger), "", ...jq(".flink.location_views_hourly | {duplicates, identical_to_python}",
        d.flink ? { duplicates: d.flink.location_views_hourly.duplicates, identical_to_python: d.flink.location_views_hourly.identical_to_python } : null)],
    ],
    late: [
      [...kcat("page_views", [lateOk])],
      [CLUSTER, `$ flink logs nexus-location-views-hourly | grep ${lateOk.id}`, jlog("location_views_hourly", lateOk) + ` (window ${lateOk.window.slice(11, 16)}, n=${lateOk.n})`],
      [...kcat("page_views", [tooLate[0]])],
      [CLUSTER, "$ kafka-console-consumer.sh --topic late.page_views --from-beginning --max-messages 3",
        ...tooLate.slice(0, 3).map((x) => J({ id: x.id, event_time: x.event_time, window: x.window, closed: x.closes }))],
      [...jq(".reconcile.page_views | {windows, equal, explained_by_side_output, unexplained, late_events}",
        { windows: Rp.page_views.windows, equal: Rp.page_views.equal, explained_by_side_output: Rp.page_views.explained_by_side_output,
          unexplained: Rp.page_views.unexplained, late_events: Rp.page_views.late_events })],
    ],
    schema: [
      [...kcat("partner_clicks", [v1])],
      [...kcat("partner_clicks", [v2s[0]]), "", "$ curl -s $SCHEMA_REGISTRY/compatibility/subjects/partner_clicks-value/versions/latest -d @v2.avsc",
        J({ is_compatible: false, messages: ["field 'cpc_gbp' was removed", "field 'price' is new in v2"] })],
      ["$ PYTHONPATH=src python examples/02_read_a_record.py | head -2",
        `${v1.revenue_gbp} GBP from {"click_id":"${v1.id}",…cpc_gbp…}`, `${v2s[v2s.length - 1].revenue_gbp} GBP from {"click_id":"${v2s[v2s.length - 1].id}",…price…}`],
    ],
    poison: [
      [...kcat("partner_clicks", [fails]), "# 57 bytes: the JSON stops in the middle of event_time"],
      [CLUSTER, ...fcmd("partner_revenue_15m").map((l) => l.replace("RUNNING", "RESTARTING")),
        `2026-10-09 14:05:12 INFO  ExecutionGraph - Job nexus partner_revenue_15m switched from state RUNNING to RESTARTING.`,
        `java.io.IOException: ${fails.reason} at partition ${fails.partition} offset ${fails.offset}`,
        `2026-10-09 14:06:12 INFO  CheckpointCoordinator - Restoring job from Checkpoint 851 @ 2026-10-09 14:00:00`,
        `… ${I.restarts} restarts until 15:20`],
      [CLUSTER, "$ kafka-consumer-groups.sh --bootstrap-server $KAFKA --describe --group nexus-partner-revenue",
        "GROUP                  TOPIC           PARTITION  LAG",
        ...Object.entries(I.lag_at_detection).map(([p_, n]) => `nexus-partner-revenue  partner_clicks  ${p_.padEnd(9)}  ${n}`),
        "", ...jq(".incident | {detected, worst_age_minutes, restarts}", { detected: I.detected, worst_age_minutes: I.worst_age_minutes, restarts: I.restarts })],
      ["$ grep 'dlq: bool = True' src/nexus/stream.py   # the fix: dead letters are the template's default", "    def __init__(self, topic_name: str, dlq: bool = True):",
        CLUSTER, "$ flink run -s s3://nexus/checkpoints/partner-revenue/chk-851 -c nexus.flink.NexusKafka nexus-flink.jar partner_clicks",
        "Job has been submitted with JobID 2b7e…c04d",
        "$ kafka-console-consumer.sh --topic dlq.partner_clicks --from-beginning --max-messages 1",
        J({ partition: dl.partition, offset: dl.offset, reason: dl.reason, raw: dl.raw })],
    ],
    bots: [
      [...jq(".volume.holds[0]", hold)],
      ["$ PYTHONPATH=src python -c 'from nexus import batch, stream; …bot_users(…, \"" + hold.hour.slice(0, 13) + "\", 120)'", J(inv.top.slice(0, 4))],
      [...jq(".volume | {bots, bot_views_removed, holds_after_filter}", { bots: V.bots, bot_views_removed: V.bot_views_removed, holds_after_filter: V.holds_after_filter })],
    ],
    erasure: T.reviews.map((x, i, a) => i === a.length - 1
      ? [...kcat("reviews", [x]), "", CLUSTER, `$ flink logs nexus-reviews-current | grep ${x.event_id}`, jlog("reviews_current", x)]
      : x.op === "erased"
        ? [...kcat("reviews", [x]), "", "$ cat warehouse/bigquery/erasure.sql | head -2", "DELETE FROM `nexus-data.raw.reviews` WHERE review_id IN UNNEST(@review_ids);",
           "DELETE FROM `nexus-data.gold.reviews_current` WHERE review_id IN UNNEST(@review_ids);"]
        : [...kcat("reviews", [x])]),
    crash: [
      [CLUSTER, "$ flink list -r", "08.10.2026 09:00:12 : 7f3c…a91e : nexus location_views_hourly (RESTARTING)",
        "2026-10-08 09:17:00 WARN  TaskManager tm-2 lost (heartbeat timeout); tasks failed: window-1, dedupe-1",
        "2026-10-08 09:17:00 INFO  ExecutionGraph - Job switched from state RUNNING to RESTARTING."],
      [CLUSTER, `2026-10-08 09:18:00 INFO  CheckpointCoordinator - Restoring job from Checkpoint (${d.replay.restored_from.split(",")[0]})`,
        "2026-10-08 09:18:00 INFO  KafkaSourceReader - Seeking 6 partitions to checkpointed offsets",
        "2026-10-08 09:18:00 INFO  ExecutionGraph - Job switched from state RESTARTING to RUNNING."],
      [...jq(".replay", d.replay)],
    ],
  };
  Object.entries(TERM).forEach(([k, xs]) => SCEN[k].steps.forEach((st, i) => { st.term = xs[Math.min(i, xs.length - 1)]; }));

  // ---------- the Python that made the decision: the lines of the repository's own files ----------
  const PY = {
    retries: [["src/nexus/stream.py", "def read_topic("], ["src/nexus/stream.py", "if rid in st.seen:"], ["src/nexus/stream.py", "def _tick("]],
    late: [["src/nexus/stream.py", "def _watermark("], ["src/nexus/stream.py", "if w[\"fired\"]:"], ["src/nexus/stream.py", "def _watermark("],
           ["src/nexus/stream.py", "if start + self.size + self.lateness <= st.wm:"], ["src/nexus/batch.py", "def reconcile("]],
    schema: [["src/nexus/contracts.py", "def upcast("], ["src/nexus/contracts.py", "def compatible("], ["src/nexus/contracts.py", "if \"price\" in v:"]],
    poison: [["src/nexus/contracts.py", "except json.JSONDecodeError"], ["src/nexus/stream.py", "except Poison as p:"],
             ["src/nexus/sla.py", "def incident("], ["src/nexus/stream.py", "except BadRecord as e:"]],
    bots: [["src/nexus/batch.py", "def volume_holds("], ["src/nexus/batch.py", "def bot_users("], ["src/nexus/batch.py", "exclude_users"]],
    erasure: [["src/nexus/stream.py", "def _apply_version("]],
    crash: [["src/nexus/stream.py", "except _Crash as c:"], ["src/nexus/stream.py", "def _restore("], ["src/nexus/stream.py", "def _emit("]],
  };
  Object.entries(PY).forEach(([k, xs]) => SCEN[k].steps.forEach((st, i) => { st.py = xs[Math.min(i, xs.length - 1)]; }));
  const SRC = {};
  const src = async (f) => (SRC[f] ??= fetch(f).then((r) => r.text()));
  let pane = "term";
  const paint = async (st) => {
    const box = $("term-out");
    document.querySelectorAll("#term-tabs button").forEach((b) => b.classList.toggle("on", b.dataset.p === pane));
    if (!st) { box.innerHTML = `<span class="t-dim">Step through a problem: its commands and its code appear here.</span>`; return; }
    if (pane === "term") {
      box.innerHTML = st.term.map((l) => `<span class="${l.startsWith("$") ? "t-cmd" : l.startsWith("#") ? "t-dim" : /WARN|RESTARTING|Exception|IOException/.test(l) ? "t-err" : ""}">${NX.esc(l)}</span>`).join("\n");
    } else {
      const [file, mark] = st.py, lines = (await src(file)).split("\n"), at = Math.max(0, lines.findIndex((l) => l.includes(mark)));
      const from = Math.max(0, at - 3), to = Math.min(lines.length, at + 16);
      box.innerHTML = `<span class="t-dim"># ${file}, lines ${from + 1}–${to}</span>\n` + lines.slice(from, to).map((l, i) =>
        `<span class="${from + i === at ? "t-hit" : ""}"><i>${String(from + i + 1).padStart(4)}</i>  ${NX.esc(l)}</span>`).join("\n");
    }
  };
  $("term-tabs").addEventListener("click", (e) => { const b = e.target.closest("button"); if (!b) return; pane = b.dataset.p; paint(SCEN[key].steps[i]); });

  // ---------- the server, as it ran: the build's own console ----------
  fetch("assets/data/build_log.txt").then((r) => r.ok ? r.text() : "").then((txt) => {
    if (!txt) return;
    $("server-log").innerHTML = txt.trim().split("\n").map((l) => `<span class="${l.startsWith("$") ? "t-cmd" : /^\[(flink|java|spark)/.test(l) ? "t-hit" : /^\[(ledger|reconcile|sla|incident)/.test(l) ? "t-ok" : ""}">${NX.esc(l)}</span>`).join("\n");
  });
  NX.get("assets/data/tests.json").then((r) => { $("server-tests").innerHTML = `<span class="t-cmd">$ PYTHONPATH=src python -m pytest -q</span>\n` +
    r.suites.map((x) => `${x.file.padEnd(28)} ${x.tests.map((t) => t.outcome === "passed" ? "." : t.outcome === "skipped" ? "s" : "F").join("")}`).join("\n") +
    `\n<span class="t-ok">${r.passed} passed</span>${r.failed ? `, <span class="t-err">${r.failed} failed</span>` : ""} in ${r.seconds}s`; }).catch(() => {});

  // ---------- rendering ----------
  const NODES = ["kafka", "read", "dedupe", "window", "sink", "side", "batch", "dlq", "ckpt"];
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
      $("caption").innerHTML = `<b>${sc.label.replace(/^[\d+]+ · /, "")}.</b> Press Play, or Next to step through it.`; $("log").innerHTML = ""; NXChoice.render($("choice"), sc.steps[0] && sc.steps[0].choice); return; }
    NODES.forEach((n) => $("n-" + n).setAttribute("class", "node " + ((st.hot || []).includes(n) ? "hot" : st.nodes.includes(n) ? "on" : "off")));
    WIRES.forEach((w) => $(w).setAttribute("class", "wire" + (st.wires.includes(w) ? ((st.hot || []).length ? " hot" : " on") : "")));
    document.querySelectorAll("[id^=kp]").forEach((r) => (r.style.fill = r.id === `kp${st.part}` ? "#0b6b53" : "#0a2a21"));
    if (st.wm) $("wm-label").textContent = `watermark ${t(st.wm).slice(6)}`;
    $("caption").innerHTML = `<b>${st.title}.</b> ${st.text}` +
      (st.flink ? `<code class="on-flink"><span>On Flink</span>${NX.esc(st.flink)}</code>` : "");
    const m = document.createElement("div");
    m.className = `msg ${(st.hot || []).length ? "err" : "res"}`;
    m.innerHTML = `<span class="dir">step ${i + 1} · ${sc.topic}</span>${NX.esc(JSON.stringify(st.log, null, 1).replace(/\n\s*/g, " "))}`;
    $("log").prepend(m);
    NXChoice.render($("choice"), st.choice);
    paint(st);
    for (const w of st.wires) await packet(w, (st.hot || []).length);
  };
  const go = (k) => { const n = SCEN[key].steps.length; i = Math.max(-1, Math.min(n - 1, k)); render(); };
  const stop = () => { clearInterval(timer); timer = null; $("play").textContent = "▶ Play"; };
  const select = (k) => { stop(); key = k; parts(SCEN[k].topic); $("log").innerHTML = ""; paint(null);
    $("flink-job").textContent = "nexus " + { page_views: "location_views_hourly", partner_clicks: "partner_revenue_15m", reviews: "reviews_current" }[SCEN[k].topic];
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
