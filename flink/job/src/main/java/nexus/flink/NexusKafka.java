package nexus.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import nexus.flink.Model.DeadLetter;
import nexus.flink.Model.KafkaRecord;
import nexus.flink.Model.WindowRow;
import nexus.flink.NexusJobs.Outputs;
import nexus.flink.NexusJobs.Product;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Collector;

/**
 * The production entry point: the same topology on Kafka. Written to be read and compiled in CI; there is no Kafka
 * cluster behind a portfolio, so NexusLocal is the one that runs.
 *
 *   flink run -c nexus.flink.NexusKafka nexus-flink.jar page_views|partner_clicks
 *
 * What the incident taught is all here: dead letters to a topic instead of a crash loop, exactly-once checkpoints,
 * a restart strategy that gives up (and pages) instead of looping forever, and upsert sinks keyed on the window.
 */
public final class NexusKafka {
    private NexusKafka() {}

    public static void main(String[] args) throws Exception {
        Product p = args.length > 0 && args[0].equals("partner_clicks") ? NexusJobs.PARTNER_REVENUE_15M : NexusJobs.LOCATION_VIEWS_HOURLY;
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(Duration.ofMinutes(10).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofMinutes(1).toMillis());
        // 3 restarts in 10 minutes, then fail and page: a poison pill never crash-loops for 74 minutes again
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.seconds(30)));

        KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(brokers)
                .setTopics(p.topic())
                .setGroupId("nexus-" + p.name())
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new RecordSchema()))
                .build();
        // per-partition watermarks and idleness come from the source itself, one generator per split
        WatermarkStrategy<KafkaRecord> wm = WatermarkStrategy.<KafkaRecord>forBoundedOutOfOrderness(p.outOfOrderness())
                .withIdleness(Duration.ofMinutes(5))
                .withTimestampAssigner((r, kafkaTs) -> eventTime(r, kafkaTs));

        Outputs o = NexusJobs.build(env.fromSource(source, wm, "kafka " + p.topic()), p, false);
        o.windows().map(NexusKafka::row).sinkTo(sink(brokers, "gold." + p.name())).name("upsert gold." + p.name());
        o.deadLetters().map(NexusKafka::dead).sinkTo(sink(brokers, "dlq." + p.topic())).name("dlq." + p.topic());
        o.tooLate().map(e -> e.id + "," + e.eventTime).sinkTo(sink(brokers, "late." + p.topic())).name("late." + p.topic());
        env.execute("nexus " + p.name());
    }

    /** Event time from the value, or the broker time if the value can't be read (it will be dead-lettered anyway). */
    static long eventTime(KafkaRecord r, long kafkaTs) {
        int i = r.value.indexOf("\"event_time\":\"");
        try {
            return Instant.parse(r.value.substring(i + 14, r.value.indexOf('"', i + 14))).toEpochMilli();
        } catch (RuntimeException e) {
            return kafkaTs;
        }
    }

    static String row(WindowRow r) throws Exception {
        return new ObjectMapper().writeValueAsString(r);
    }

    static String dead(DeadLetter d) throws Exception {
        return new ObjectMapper().writeValueAsString(d);
    }

    static KafkaSink<String> sink(String brokers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("nexus-" + topic)
                .build();
    }

    /** Keeps the partition, offset and append time with the value, as the replay does. */
    static class RecordSchema implements org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema<KafkaRecord> {
        @Override public boolean isEndOfStream(KafkaRecord r) { return false; }
        @Override public KafkaRecord deserialize(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r) {
            return new KafkaRecord(r.timestamp(), r.partition(), r.offset(), r.value() == null ? "" : new String(r.value()));
        }
        @Override public void deserialize(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r, Collector<KafkaRecord> out) {
            out.collect(deserialize(r));
        }
        @Override public org.apache.flink.api.common.typeinfo.TypeInformation<KafkaRecord> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(KafkaRecord.class);
        }
    }
}
