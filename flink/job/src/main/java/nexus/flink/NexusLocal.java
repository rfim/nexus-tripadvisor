package nexus.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import nexus.flink.Model.KafkaRecord;
import nexus.flink.NexusJobs.Outputs;
import nexus.flink.NexusJobs.Product;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/**
 * Run both products on Flink's local MiniCluster, replaying the week in landing/kafka in arrival order, and write what
 * the upsert sinks would hold to target/flink_*.csv with a ledger per topic. tests compare it with the Python job.
 *
 *   java ... nexus.flink.NexusLocal <repo root>
 */
public final class NexusLocal {
    private NexusLocal() {}

    static List<KafkaRecord> replay(Path root, String topic) throws Exception {
        ObjectMapper json = new ObjectMapper();
        List<KafkaRecord> out = new ArrayList<>();
        for (Path p : Files.list(root.resolve("landing/kafka/" + topic)).sorted().toList()) {
            int partition = Integer.parseInt(p.getFileName().toString().replaceAll("\\D", ""));
            for (String line : Files.readAllLines(p)) {
                JsonNode r = json.readTree(line);
                out.add(new KafkaRecord(Instant.parse(r.get("ts").asText()).toEpochMilli(), partition, r.get("offset").asLong(), r.get("value").asText()));
            }
        }
        out.sort(Comparator.comparingLong((KafkaRecord r) -> r.arrival).thenComparingInt(r -> r.partition).thenComparingLong(r -> r.offset));
        return out;
    }

    public static Map<String, Object> run(Path root, Product p) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);    // streaming semantics on a bounded replay: lateness is real
        env.setParallelism(1);                                 // one replayed "consumer", so arrival order is kept
        List<KafkaRecord> records = replay(root, p.topic());
        Outputs o = NexusJobs.build(env.fromData(records), p, true);
        DataStream<String> all = o.windows().map(r -> "W\t" + r.windowStart + "\t" + r.group + "\t" + r.n + "\t" + r.sum)
                .union(o.deadLetters().map(d -> "D\t" + d.partition + "\t" + d.offset + "\t" + d.reason),
                       o.duplicates().map(e -> "U\t" + e.id),
                       o.tooLate().map(e -> "L\t" + e.id));
        Map<String, String> sink = new TreeMap<>();            // the upsert sink: last firing per (window, group) wins
        Map<String, Long> ledger = new LinkedHashMap<>(Map.of("read", (long) records.size(), "firings", 0L,
                "dead_letters", 0L, "duplicates", 0L, "late_dropped", 0L));
        List<String> dead = new ArrayList<>();
        long t0 = System.nanoTime();
        try (CloseableIterator<String> it = all.executeAndCollect("nexus " + p.name())) {
            while (it.hasNext()) {
                String[] f = it.next().split("\t");
                switch (f[0]) {
                    case "W" -> { sink.put(Instant.ofEpochMilli(Long.parseLong(f[1])) + "\t" + f[2], f[3] + "\t" + f[4]); ledger.merge("firings", 1L, Long::sum); }
                    case "D" -> { ledger.merge("dead_letters", 1L, Long::sum); dead.add("partition " + f[1] + " offset " + f[2] + ": " + f[3]); }
                    case "U" -> ledger.merge("duplicates", 1L, Long::sum);
                    default -> ledger.merge("late_dropped", 1L, Long::sum);
                }
            }
        }
        ledger.put("windows", (long) sink.size());
        ledger.put("events_in_windows", sink.values().stream().mapToLong(v -> Long.parseLong(v.split("\t")[0])).sum());
        ledger.put("millis", (System.nanoTime() - t0) / 1_000_000);
        StringBuilder csv = new StringBuilder("window_start,group,n,sum\n");
        sink.forEach((k, v) -> csv.append(k.replace('\t', ',')).append(',').append(v.replace('\t', ',')).append('\n'));
        Path target = root.resolve("target");
        Files.createDirectories(target);
        Files.writeString(target.resolve("flink_" + p.name() + ".csv"), csv.toString());
        Map<String, Object> out = new LinkedHashMap<>(ledger);
        out.put("dead_letter_records", dead);
        return out;
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args.length > 0 ? args[0] : "../..").toAbsolutePath().normalize();
        Map<String, Object> result = new LinkedHashMap<>();
        for (Product p : List.of(NexusJobs.LOCATION_VIEWS_HOURLY, NexusJobs.PARTNER_REVENUE_15M)) result.put(p.name(), run(root, p));
        result.put("flink", org.apache.flink.runtime.util.EnvironmentInformation.getVersion());
        String s = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result);
        Files.writeString(root.resolve("target/flink_ledger.json"), s);
        System.out.println(s);
    }
}
