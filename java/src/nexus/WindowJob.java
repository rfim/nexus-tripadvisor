package nexus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The page_views stream job in Java: the same rules as src/nexus/stream.py, record for record.
 *
 * <p>Event-time hourly windows per location, a watermark that is the minimum over active partitions minus ten
 * minutes of out-of-orderness, idle partitions (nothing for five minutes) left out of that minimum, sixty minutes of
 * allowed lateness, de-duplication on event_id for 24 hours, and a side output for records past the lateness.
 *
 * <p>Plain JDK 17, no Flink: it shows the semantics, and tests/test_java_twin.py checks it reaches exactly the
 * Python job's windows and ledger. On Flink the same job is a KeyedProcessFunction, or flink/page_views_hourly.sql.
 *
 * <pre>
 *   javac -d target/java java/src/nexus/*.java
 *   java -cp target/java nexus.WindowJob landing/kafka/page_views 6 target/java_page_views.csv
 * </pre>
 */
public final class WindowJob {
    static final long MIN = 60_000L, TICK = MIN, IDLE = 5 * MIN, DEDUPE_TTL = 24 * 60 * MIN, NEG = -1_000_000_000_000_000L;
    static final long SIZE = 60 * MIN, OOO = 10 * MIN, LATENESS = 60 * MIN;
    static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    record Rec(long ts, int partition, long offset, String value) {}

    record Key(long start, String location) implements Comparable<Key> {
        public int compareTo(Key o) {
            int c = Long.compare(start, o.start);
            return c != 0 ? c : location.compareTo(o.location);
        }
    }

    static final class Window {
        long n;
        boolean fired;
    }

    long wm = NEG;
    final Map<Integer, Long> partMax = new HashMap<>(), partSeen = new HashMap<>();
    final TreeMap<Key, Window> windows = new TreeMap<>();
    Map<String, Long> seen = new HashMap<>();
    final Map<Key, Long> sink = new TreeMap<>();
    final Map<String, Long> ledger = new LinkedHashMap<>();
    long emits;

    WindowJob() {
        for (String k : new String[] {"read", "accepted", "duplicates", "late_dropped", "late_updates"}) ledger.put(k, 0L);
    }

    private void count(String k) {
        ledger.merge(k, 1L, Long::sum);
    }

    long watermark(long now) {
        long min = Long.MAX_VALUE;
        boolean any = false;
        for (var e : partSeen.entrySet()) {
            if (now - e.getValue() < IDLE) {
                any = true;
                min = Math.min(min, partMax.get(e.getKey()));
            }
        }
        long w = any ? min - OOO : !partMax.isEmpty() ? now - OOO - IDLE : NEG;
        return Math.max(wm, w);
    }

    void advance(long now) {
        long w = watermark(now);
        if (w <= wm && wm != NEG) return;
        wm = w;
        for (var e : windows.entrySet()) {
            if (!e.getValue().fired && e.getKey().start() + SIZE <= wm) {
                e.getValue().fired = true;
                emit(e.getKey(), e.getValue());
            }
        }
        windows.keySet().removeIf(k -> k.start() + SIZE + LATENESS <= wm);
    }

    void emit(Key k, Window w) {
        sink.put(k, w.n);
        emits++;
    }

    void process(Rec r, long now) {
        count("read");
        Map<String, Object> v = Json.object(r.value());
        String id = (String) v.get("event_id");
        if (seen.containsKey(id)) {
            count("duplicates");
            return;
        }
        seen.put(id, now);
        long et = Instant.parse((String) v.get("event_time")).toEpochMilli();
        partMax.merge(r.partition(), et, Math::max);
        partSeen.put(r.partition(), now);
        long start = et - Math.floorMod(et, SIZE);
        Key key = new Key(start, (String) v.get("location_id"));
        if (start + SIZE + LATENESS <= wm) {
            count("late_dropped");
        } else {
            Window w = windows.computeIfAbsent(key, x -> new Window());
            w.n++;
            count("accepted");
            if (w.fired) {
                count("late_updates");
                emit(key, w);
            }
        }
        advance(now);
    }

    void tick(long now) {
        advance(now);
        if (now % (60 * MIN) == 0) seen.entrySet().removeIf(e -> now - e.getValue() >= DEDUPE_TTL);
    }

    void run(List<Rec> recs) {
        int pos = 0;
        long now = recs.get(0).ts() - Math.floorMod(recs.get(0).ts(), TICK);
        long end = recs.get(recs.size() - 1).ts() + 3 * 60 * MIN;
        while (now <= end) {
            long next = now + TICK;
            while (pos < recs.size() && recs.get(pos).ts() < next) {
                process(recs.get(pos), recs.get(pos).ts());
                pos++;
            }
            tick(next);
            now = next;
        }
    }

    static List<Rec> read(Path dir, int partitions) throws IOException {
        List<Rec> out = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            for (String line : Files.readAllLines(dir.resolve("p" + p + ".jsonl"))) {
                Map<String, Object> env = Json.object(line);
                out.add(new Rec(Instant.parse((String) env.get("ts")).toEpochMilli(), p,
                        ((Number) env.get("offset")).longValue(), (String) env.get("value")));
            }
        }
        out.sort(Comparator.comparingLong(Rec::ts).thenComparingInt(Rec::partition).thenComparingLong(Rec::offset));
        return out;
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : "landing/kafka/page_views");
        int partitions = args.length > 1 ? Integer.parseInt(args[1]) : 6;
        Path out = Path.of(args.length > 2 ? args[2] : "target/java_page_views.csv");
        long t0 = System.nanoTime();
        WindowJob job = new WindowJob();
        List<Rec> recs = read(dir, partitions);
        job.run(recs);
        StringBuilder sb = new StringBuilder("window_start,location_id,views\n");
        job.sink.forEach((k, n) -> sb.append(ISO.format(Instant.ofEpochMilli(k.start()))).append(',').append(k.location())
                .append(',').append(n).append('\n'));
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, sb.toString());
        StringBuilder led = new StringBuilder("{");
        job.ledger.forEach((k, v) -> led.append(led.length() > 1 ? ", " : "").append('"').append(k).append("\": ").append(v));
        led.append(", \"windows\": ").append(job.sink.size()).append(", \"emits\": ").append(job.emits)
                .append(", \"millis\": ").append((System.nanoTime() - t0) / 1_000_000).append('}');
        Files.writeString(Path.of(out.toString().replace(".csv", ".ledger.json")), led + "\n");
        System.out.println(led);
    }
}
