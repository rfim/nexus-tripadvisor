package nexus.flink;

import java.util.HashMap;
import java.util.Map;
import nexus.flink.Model.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * The watermark the stream job uses: the minimum over active Kafka partitions of their newest event time, less the
 * out-of-orderness; a partition quiet for five minutes is left out.
 *
 * In production KafkaSource does this itself, per split: WatermarkStrategy.forBoundedOutOfOrderness(ooo)
 * .withIdleness(5 min) (see NexusKafka). This generator does the same thing on a replay of the topic in arrival order,
 * so a local run reaches the decisions the production job would. Punctuated: it emits after every event.
 */
public class PartitionWatermarks implements WatermarkGenerator<Event> {
    static final long IDLE = 5 * 60_000L;
    private final long ooo;
    private final Map<Integer, Long> maxEvent = new HashMap<>(), lastArrival = new HashMap<>();
    private long current = Long.MIN_VALUE;

    public PartitionWatermarks(long outOfOrdernessMillis) {
        this.ooo = outOfOrdernessMillis;
    }

    @Override
    public void onEvent(Event e, long ts, WatermarkOutput out) {
        maxEvent.merge(e.partition, e.eventTime, Math::max);
        lastArrival.put(e.partition, e.arrival);
        long min = Long.MAX_VALUE;
        for (var p : lastArrival.entrySet()) {
            if (e.arrival - p.getValue() < IDLE) min = Math.min(min, maxEvent.get(p.getKey()));
        }
        long wm = min - ooo;
        if (wm > current) {
            current = wm;
            out.emitWatermark(new Watermark(wm));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput out) {}
}
