package nexus.flink;

import java.time.Duration;
import nexus.flink.Model.DeadLetter;
import nexus.flink.Model.Event;
import nexus.flink.Model.KafkaRecord;
import nexus.flink.Model.WindowRow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

/**
 * The two windowed products as one Flink topology, the same for a local replay (NexusLocal) and for Kafka (NexusKafka):
 *
 *   records → Parse ──dead letters──▶ DLQ
 *               │
 *               ▼
 *            Dedupe (keyed by id, 24 h TTL) ──duplicates──▶ counted, dropped
 *               │
 *               ▼  watermarks: min over active partitions − out-of-orderness
 *            keyBy(group) → tumbling event-time window → allowed lateness ──too late──▶ side output (the batch counts it)
 *               │
 *               ▼
 *            upsert sink keyed on (window start, group)
 */
public final class NexusJobs {
    private NexusJobs() {}

    /** What a product is: window size, out-of-orderness and allowed lateness, as contracts/sources.yml says. */
    public record Product(String topic, String name, Duration window, Duration outOfOrderness, Duration lateness) {}

    public static final Product LOCATION_VIEWS_HOURLY =
            new Product("page_views", "location_views_hourly", Duration.ofHours(1), Duration.ofMinutes(10), Duration.ofHours(1));
    public static final Product PARTNER_REVENUE_15M =
            new Product("partner_clicks", "partner_revenue_15m", Duration.ofMinutes(15), Duration.ofMinutes(5), Duration.ofMinutes(30));

    public static final OutputTag<Event> TOO_LATE = new OutputTag<>("too-late") {};

    /** The streams a product produces: the windows, and the three places a record can go instead. */
    public record Outputs(DataStream<WindowRow> windows, DataStream<DeadLetter> deadLetters,
                          DataStream<Event> duplicates, DataStream<Event> tooLate) {}

    /** Build the product on a stream of records. replayWatermarks: compute per-partition watermarks here (local replay);
     *  false when the source already assigns them per split (KafkaSource). */
    public static Outputs build(DataStream<KafkaRecord> records, Product p, boolean replayWatermarks) {
        SingleOutputStreamOperator<Event> parsed = records.process(new Parse(p.topic())).name("parse " + p.topic());
        SingleOutputStreamOperator<Event> unique = parsed.keyBy(e -> e.id).process(new Dedupe()).name("dedupe on id");
        final long ooo = p.outOfOrderness().toMillis();       // a long, not the record: the lambda is serialized
        DataStream<Event> timed = replayWatermarks
                ? unique.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forGenerator(ctx -> new PartitionWatermarks(ooo))
                        .withTimestampAssigner((e, ts) -> e.eventTime)).name("watermarks")
                : unique;
        SingleOutputStreamOperator<WindowRow> windows = timed
                .keyBy(e -> e.group)
                .window(TumblingEventTimeWindows.of(p.window()))
                .allowedLateness(p.lateness())
                .sideOutputLateData(TOO_LATE)
                .aggregate(new Windows.CountSum(), new Windows.Emit())
                .name(p.name());
        return new Outputs(windows, parsed.getSideOutput(Parse.DEAD_LETTERS), unique.getSideOutput(Dedupe.DUPLICATES),
                windows.getSideOutput(TOO_LATE));
    }
}
