package engine.bus.bench;

import engine.bus.RedisStreamsEventPublisher;
import engine.bus.RetentionPolicy;
import engine.core.event.InstrumentId;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Instant;
import java.util.List;

/**
 * NEG-22 end-to-end smoke and throughput harness — <em>not</em> a JUnit test, a plain {@code main} run
 * via {@code ./gradlew :bus:e2eBench}, never wired into {@code build}/{@code check}. It drives the real
 * path end to end: {@code RedisStreamsEventPublisher} → docker-compose Redis → N
 * {@code RedisStreamsEventSubscriber} consumer groups, and judges the result against an explicit target
 * (≥ the configured rate sustained, per-group p99 publish→handler under {@link BenchReport#P99_BUDGET}).
 * A missed target exits non-zero, so the Gradle task fails — the harness is a gate, not a suggestion.
 *
 * <p>Distinct from {@code PublishBench}, which measures publish-side service latency only. Requires the
 * docker-compose Redis ({@code docker compose up -d}), and must not run concurrently with
 * {@code :bus:integrationTest} — both loads share one Redis and would poison each other's numbers.
 *
 * <p>Exit codes: {@code 0} pass, {@code 1} target missed, {@code 2} bad usage.
 */
public final class EndToEndBench {

    static final String REDIS_URI = "redis://localhost:6379";

    private EndToEndBench() {}

    public static void main(String[] args) {
        if (BenchConfig.isHelp(args)) {
            System.out.println(BenchConfig.USAGE);
            return;
        }

        BenchConfig config;
        try {
            config = BenchConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("e2eBench: " + e.getMessage());
            System.err.println();
            System.err.println(BenchConfig.USAGE);
            System.exit(2);
            return;
        }

        BenchReport report = new BenchReport(config, readRedisVersion(), Instant.now());
        List<InstrumentId> universe = TickGenerator.universe(config.instruments());
        // Reset before anything subscribes: DEL drops each stream with its consumer groups, so a
        // previous run's entries can neither be redelivered into this one's accounting nor inflate
        // the soak's XLEN evidence.
        resetBenchStreams(universe);

        try (RedisStreamsEventPublisher publisher = new RedisStreamsEventPublisher(
                REDIS_URI, new JsonEventCodec(PayloadRegistry.standard()), RetentionPolicy.standard())) {
            TickGenerator generator = new TickGenerator(publisher, config);
            generator.warmup();
            report.addGenerator(generator.measure());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("e2eBench: interrupted");
            System.exit(2);
            return;
        }

        // Steps 3-5 append the per-group and memory sections here.
        System.out.println(report.render());
    }

    /** Drops every bench stream (and with it every bench consumer group) so each run starts clean. */
    static void resetBenchStreams(List<InstrumentId> universe) {
        RedisClient client = RedisClient.create(REDIS_URI);
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            List<String> streams = TickGenerator.streams(universe);
            connection.sync().del(streams.toArray(String[]::new));
        } finally {
            client.shutdown();
        }
    }

    /** Reads {@code redis_version} from {@code INFO server}; also proves Redis is reachable before the run. */
    static String readRedisVersion() {
        RedisClient client = RedisClient.create(REDIS_URI);
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            String info = connection.sync().info("server");
            for (String line : info.split("\r?\n")) {
                if (line.startsWith("redis_version:")) {
                    return line.substring("redis_version:".length()).trim();
                }
            }
            return "unknown";
        } finally {
            client.shutdown();
        }
    }
}
