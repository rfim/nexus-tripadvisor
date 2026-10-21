package nexus.flink;

import nexus.flink.Model.Event;
import nexus.flink.Model.WindowRow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Count and sum per window, incrementally (one accumulator per key and window, not a buffer of events). */
public final class Windows {
    private Windows() {}

    public static class CountSum implements AggregateFunction<Event, Tuple2<Long, Double>, Tuple2<Long, Double>> {
        @Override public Tuple2<Long, Double> createAccumulator() { return Tuple2.of(0L, 0.0); }
        @Override public Tuple2<Long, Double> add(Event e, Tuple2<Long, Double> a) { return Tuple2.of(a.f0 + 1, a.f1 + e.amount); }
        @Override public Tuple2<Long, Double> getResult(Tuple2<Long, Double> a) { return a; }
        @Override public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) { return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1); }
    }

    /** Emits a row on every firing: once when the watermark passes the end, again for each late record within lateness. */
    public static class Emit extends ProcessWindowFunction<Tuple2<Long, Double>, WindowRow, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<Long, Double>> in, Collector<WindowRow> out) {
            Tuple2<Long, Double> a = in.iterator().next();
            WindowRow r = new WindowRow();
            r.windowStart = ctx.window().getStart();
            r.group = key;
            r.n = a.f0;
            r.sum = Math.round(a.f1 * 10_000) / 10_000.0;
            r.firedAt = ctx.currentWatermark();
            out.collect(r);
        }
    }
}
