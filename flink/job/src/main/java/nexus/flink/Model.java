package nexus.flink;

import java.io.Serializable;

/** The records the jobs pass between operators. Public fields and no-arg constructors: Flink treats them as POJOs. */
public final class Model {
    private Model() {}

    /** One Kafka record as the brokers keep it: arrival (append) time, partition, offset, the producer's value. */
    public static class KafkaRecord implements Serializable {
        public long arrival;
        public int partition;
        public long offset;
        public String value;
        public KafkaRecord() {}
        public KafkaRecord(long arrival, int partition, long offset, String value) {
            this.arrival = arrival; this.partition = partition; this.offset = offset; this.value = value;
        }
    }

    /** A parsed event: id, event time, the group it is counted under, and the amount it adds (revenue for clicks). */
    public static class Event implements Serializable {
        public String id;
        public long eventTime;
        public String group;
        public double amount;
        public int schema;
        public int partition;
        public long arrival;
        public Event() {}
    }

    /** A record that could not be read, kept with its reason: the dead-letter topic. */
    public static class DeadLetter implements Serializable {
        public int partition;
        public long offset;
        public String reason;
        public String raw;
        public DeadLetter() {}
        public DeadLetter(KafkaRecord r, String reason) {
            this.partition = r.partition; this.offset = r.offset; this.reason = reason; this.raw = r.value;
        }
    }

    /** One firing of a window. A late record within allowed lateness fires it again; the sink upserts on (start, group). */
    public static class WindowRow implements Serializable {
        public long windowStart;
        public String group;
        public long n;
        public double sum;
        public long firedAt;     // the watermark when it fired
        public WindowRow() {}
    }
}
