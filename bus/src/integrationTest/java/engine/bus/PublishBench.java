package engine.bus;

import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Side;
import engine.core.event.TradeTick;
import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Step 7 throughput harness — <em>not</em> a JUnit test (no annotations, no assertions), a plain
 * {@code main} run via {@code ./gradlew :bus:publishBench}. It measures the async, pipelined publish
 * path so the numbers can go in the PR as the baseline that decides whether a batch API is needed.
 *
 * <p>Two measured phases, because the plan's gate ("≥5,000 events/s sustained with p99 ≤ 5 ms")
 * asks two different questions of two different load shapes:
 *
 * <ul>
 *   <li><b>Phase 1 — saturated:</b> unthrottled behind the 4096-deep in-flight window; reports max
 *       sustained throughput. Its latencies are queueing-dominated (an event completes only after
 *       the ~4096 commands ahead of it drain), so they measure depth, not service time.
 *   <li><b>Phase 2 — paced at 5,000 events/s:</b> one publish every 200 µs on a nanoTime schedule;
 *       its p50/p99/max are the service latencies the plan's 5 ms gate actually judges.
 * </ul>
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}. Writes only to the bench-only
 * stream {@code md.tick.trade.BENCH-A.ITEST} — a stream real data can never collide with — and
 * {@code DEL}s it at the end. It never touches {@code dlq.*} or {@code replay.*}.
 */
public final class PublishBench {

    private static final String REDIS_URI = "redis://localhost:6379";

    /** Bench-only instrument: routes to {@code md.tick.trade.BENCH-A.ITEST}, never real data. */
    private static final InstrumentId BENCH_A = InstrumentId.parse("BENCH-A.ITEST");

    private static final String BENCH_STREAM = "md.tick.trade.BENCH-A.ITEST";

    /** Constant payload; a fresh {@link Event} per publish keeps eventId/timestamps encoding-live. */
    private static final TradeTick TICK = new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY);

    private static final int SATURATED_WARMUP = 10_000;
    private static final int PACED_WARMUP = 2_000; // connection is already warm after phase 1
    private static final int N = 100_000;

    /** Phase 2 cadence: 200 µs between publishes = the plan's 5,000 events/s gate rate. */
    private static final long PACE_NANOS = 200_000;

    /** Matches the publisher's Lettuce request-queue bound so queue-full failures stay out of the numbers. */
    private static final int IN_FLIGHT_WINDOW = 4096;

    private PublishBench() {}

    /** One measured phase's results; {@code latenciesNanos} arrives sorted. */
    private record PhaseResult(long[] latenciesNanos, double eventsPerSecond, int failures, Throwable firstFailure) {}

    public static void main(String[] args) throws Exception {
        EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());
        String redisVersion = readRedisVersion();

        // try-with-resources: close() drains in-flight publishes; an unclosed client leaks
        // non-daemon event-loop threads (the NEG-15 lesson).
        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher(REDIS_URI, codec, RetentionPolicy.standard())) {

            // Phase 1 — saturated. Warm up JIT + connection ramp with unmeasured publishes
            // (awaited before the clock starts), then measure.
            runPhase(publisher, SATURATED_WARMUP, 0, null, null, null, null);
            PhaseResult saturated = measure(publisher, 0);

            // Phase 2 — paced at 5,000 events/s. Short unmeasured paced warmup, then measure.
            runPhase(publisher, PACED_WARMUP, PACE_NANOS, null, null, null, null);
            PhaseResult paced = measure(publisher, PACE_NANOS);

            printHeader(redisVersion);
            printPhase("phase 1: saturated (max throughput)", "unthrottled, in-flight window 4096", saturated);
            printPhase("phase 2: paced at 5,000 events/s", "one publish per 200 us (nanoTime schedule)", paced);
        } finally {
            deleteBenchStream();
        }
    }

    /** Runs one measured phase of {@code N} publishes; {@code paceNanos} 0 = saturated. */
    private static PhaseResult measure(RedisStreamsEventPublisher publisher, long paceNanos)
            throws InterruptedException {
        long[] latenciesNanos = new long[N];
        AtomicInteger index = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        long startNanos = System.nanoTime();
        runPhase(publisher, N, paceNanos, latenciesNanos, index, failures, firstFailure);
        long wallNanos = System.nanoTime() - startNanos;

        Arrays.sort(latenciesNanos);
        double eventsPerSecond = N / (wallNanos / 1_000_000_000.0);
        return new PhaseResult(latenciesNanos, eventsPerSecond, failures.get(), firstFailure.get());
    }

    /**
     * Publishes {@code count} fresh events through the bounded in-flight window and blocks until
     * every stage completes. When {@code paceNanos} is positive, publish {@code i} is released at
     * {@code start + i * paceNanos} on a {@link LockSupport#parkNanos} schedule — not {@code
     * Thread.sleep}, whose granularity would distort the cadence. When {@code out} is non-null, each
     * completion records its wall latency (ns).
     */
    private static void runPhase(
            RedisStreamsEventPublisher publisher,
            int count,
            long paceNanos,
            long[] out,
            AtomicInteger index,
            AtomicInteger failures,
            AtomicReference<Throwable> firstFailure)
            throws InterruptedException {
        Semaphore window = new Semaphore(IN_FLIGHT_WINDOW);
        CountDownLatch done = new CountDownLatch(count);
        long scheduleStart = System.nanoTime();

        for (int i = 0; i < count; i++) {
            if (paceNanos > 0) {
                long target = scheduleStart + i * paceNanos;
                long now;
                while ((now = System.nanoTime()) < target) {
                    LockSupport.parkNanos(target - now);
                }
            }
            window.acquire();
            long t0 = System.nanoTime();
            publisher.publish(freshEvent()).whenComplete((v, err) -> {
                long delta = System.nanoTime() - t0;
                if (out != null) {
                    out[index.getAndIncrement()] = delta;
                }
                if (err != null && failures != null) {
                    failures.incrementAndGet();
                    firstFailure.compareAndSet(null, err);
                }
                window.release();
                done.countDown();
            });
        }
        done.await();
    }

    /** Fresh envelope every call: new eventId and current timestamps, so encoding is never amortized away. */
    private static Event freshEvent() {
        return Event.of("bench-feed", BENCH_A, Instant.now(), TICK);
    }

    private static String readRedisVersion() {
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

    private static void deleteBenchStream() {
        RedisClient client = RedisClient.create(REDIS_URI);
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> sync = connection.sync();
            sync.del(BENCH_STREAM);
        } finally {
            client.shutdown();
        }
    }

    private static void printHeader(String redisVersion) {
        StringBuilder header = new StringBuilder();
        header.append("======== NEG-18 publish throughput bench ========\n");
        header.append(String.format(
                "machine       : %s %s, %d cores, java %s%n",
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(),
                System.getProperty("java.version")));
        header.append(String.format("redis         : %s%n", redisVersion));
        header.append(String.format(
                "events        : N=%d per phase (warmups: %d saturated, %d paced)", N, SATURATED_WARMUP, PACED_WARMUP));
        System.out.println(header);
    }

    private static void printPhase(String title, String load, PhaseResult result) {
        long[] sorted = result.latenciesNanos();
        double p50Ms = sorted[(int) (N * 0.50)] / 1_000_000.0;
        double p99Ms = sorted[(int) (N * 0.99)] / 1_000_000.0;
        double maxMs = sorted[N - 1] / 1_000_000.0;

        StringBuilder report = new StringBuilder();
        report.append(String.format("-------- %s --------%n", title));
        report.append(String.format("load          : %s%n", load));
        report.append(String.format("throughput    : %,.0f events/s achieved%n", result.eventsPerSecond()));
        report.append(String.format("latency p50   : %.3f ms%n", p50Ms));
        report.append(String.format("latency p99   : %.3f ms%n", p99Ms));
        report.append(String.format("latency max   : %.3f ms%n", maxMs));
        report.append(String.format("failures      : %d%n", result.failures()));
        if (result.failures() > 0) {
            report.append(String.format("first failure : %s%n", result.firstFailure()));
        }
        report.append("=================================================");
        System.out.println(report);
    }
}
