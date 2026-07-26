package engine.bus.monitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

import engine.bus.RedisStreamsEventPublisher;
import engine.bus.RedisStreamsEventSubscriber;
import engine.bus.RetentionPolicy;
import engine.bus.SubscriberTuning;
import engine.bus.XInfoReplies;
import engine.bus.monitor.MetricNames.LatencyPercentile;
import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Metric;
import engine.core.event.OrderIntent;
import engine.core.event.Side;
import engine.core.event.TradeTick;
import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import engine.core.serde.SampleEvents;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-21 Step 6 — the acceptance criteria as integration tests against the docker-compose Redis
 * ({@code docker compose up -d}). Every assertion reads the monitor's output the honest way: by
 * subscribing to the {@code Metric} events it publishes on the bus (proving the dogfooding path end
 * to end), with the endpoint asserted where it is the deliverable (the DLQ last-error string).
 *
 * <p>Sweeps are driven manually via {@link BusMonitor#runOnce()} wherever a test needs a controlled
 * drain boundary (exact percentiles, the failure counter, rates) — the scheduler's interval edges
 * would otherwise split a sample set across intervals. Aggressive tuning keeps the suite quick; a
 * test-only instrument keeps market-data stream names off any real feed on the shared dev Redis.
 */
class BusObservabilityIntegrationTest {

    private static final String REDIS = "redis://localhost:6379";
    private static final InstrumentId INSTRUMENT = InstrumentId.parse("MON-A.ITEST");
    private static final String TICKS = "md.tick.trade.MON-A.ITEST";
    private static final String INTENTS = "orders.intents";
    private static final String METRICS = "metrics";
    private static final List<String> STREAMS = List.of(TICKS, INTENTS, METRICS, "dlq." + INTENTS, "dlq." + TICKS);

    private static final MonitorTuning MON = new MonitorTuning(Duration.ofMillis(100), 0);
    private static final Clock CLOCK = Clock.systemUTC();

    /** Reader-ish subscriber tuning: fast poll, no aggressive claim/park interference. */
    private static final SubscriberTuning READER = new SubscriberTuning(
            Duration.ofMillis(100), 200, Duration.ofSeconds(10), Duration.ofSeconds(10), 100, 100_000);

    /** Poison tuning: park after 2 deliveries in well under a second. */
    private static final SubscriberTuning POISON = new SubscriberTuning(
            Duration.ofMillis(100), 10, Duration.ofMillis(100), Duration.ofMillis(150), 2, 100_000);

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    @BeforeEach
    void connect() {
        client = RedisClient.create(REDIS);
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
        reader.del(STREAMS.toArray(new String[0])); // DEL removes each stream and all its consumer groups
    }

    @AfterEach
    void cleanUp() {
        reader.del(STREAMS.toArray(new String[0]));
        connection.close();
        client.shutdown();
    }

    // Criterion 1: a slowed consumer's lag visibly increases, then recovers.
    @Test
    void consumerLagRisesThenRecovers() throws Exception {
        MetricSink sink = new MetricSink();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstSeen = new CountDownLatch(1);
        String lag = MetricNames.groupLag(TICKS);

        try (RedisStreamsEventSubscriber metrics = metricsReader(sink);
                RedisStreamsEventSubscriber slow =
                        new RedisStreamsEventSubscriber(REDIS, codec, "itest-slow", "a", READER);
                RedisStreamsEventPublisher publisher = publisher()) {

            slow.subscribe(ticks(), latest(), e -> {
                firstSeen.countDown();
                awaitLatch(release); // block on the first batch so later ticks pile up undelivered
            });

            // First batch delivered and stuck in the handler.
            publishTicks(publisher, 10);
            assertThat(firstSeen.await(10, TimeUnit.SECONDS)).isTrue();

            BusMonitor monitor = monitor(publisher);
            try {
                List<Long> rising = new ArrayList<>();
                rising.add(sampleLag(monitor, sink, lag, "itest-slow"));
                for (int batch = 0; batch < 3; batch++) {
                    publishTicks(publisher, 10); // each batch adds ~10 undelivered
                    Thread.sleep(150);
                    rising.add(sampleLag(monitor, sink, lag, "itest-slow"));
                }

                // Lag climbed as backlog grew: a clear rise to a substantial peak.
                long peak = rising.stream().mapToLong(Long::longValue).max().orElse(0);
                assertThat(peak)
                        .as("lag should climb well above zero: %s", rising)
                        .isGreaterThanOrEqualTo(20);
                assertThat(rising.get(rising.size() - 1))
                        .as("lag should end far above where it started: %s", rising)
                        .isGreaterThan(rising.get(0) + 15);

                // Release the handler: the backlog drains and lag falls back to ~0.
                release.countDown();
                long recovered = Long.MAX_VALUE;
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (System.nanoTime() < deadline) {
                    recovered = sampleLag(monitor, sink, lag, "itest-slow");
                    if (recovered <= 2) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertThat(recovered)
                        .as("lag should recover to ~0 after the handler is released")
                        .isLessThanOrEqualTo(2);
            } finally {
                release.countDown();
                monitor.close();
            }
        }
    }

    // Criterion 2: dead-letter depth increments observably, and the endpoint surfaces the last error.
    @Test
    void deadLetterDepthAndLastErrorAreObservable() throws Exception {
        MetricSink sink = new MetricSink();
        String dlqDepth = MetricNames.dlqDepth(INTENTS);

        try (RedisStreamsEventSubscriber metrics = metricsReader(sink);
                RedisStreamsEventSubscriber poison =
                        new RedisStreamsEventSubscriber(REDIS, codec, "itest-poison", "a", POISON);
                RedisStreamsEventPublisher publisher = publisher()) {

            poison.subscribe(intents(), latest(), e -> {
                throw new IllegalStateException("poison itest");
            });

            BusMonitor monitor = monitor(publisher);
            monitor.start(); // scheduler + endpoint; DLQ appearance is a trend, not an exact-drain assertion
            try {
                publishOne(publisher, SampleEvents.orderIntent());

                // Depth goes absent → ≥1 once the entry crosses maxDeliveries and parks.
                assertThat(await(
                                () -> sink.latest(dlqDepth, "bus")
                                        .map(v -> v.longValue() >= 1)
                                        .orElse(false),
                                Duration.ofSeconds(20)))
                        .as("bus.dlq.depth should become observable once the poison parks")
                        .isTrue();

                // The endpoint's info-metric carries the thrown exception's class and the failing group.
                HttpClient http = HttpClient.newHttpClient();
                String body = awaitEndpoint(http, monitor.endpointPort(), "bus_dlq_last_error", Duration.ofSeconds(20));
                assertThat(body).contains("bus_dlq_last_error");
                assertThat(body).contains("stream=\"" + INTENTS + "\"");
                assertThat(body).contains("group=\"itest-poison\"");
                assertThat(body).contains("IllegalStateException");
            } finally {
                monitor.close();
            }
        }
    }

    // Criterion 3: feed-latency percentiles per source are exact.
    @Test
    void feedLatencyPercentilesAreExact() throws Exception {
        MetricSink sink = new MetricSink();
        String source = "itest-feed";

        try (RedisStreamsEventSubscriber metrics = metricsReader(sink);
                RedisStreamsEventPublisher publisher = publisher()) {

            // occurredAt sits a known distance behind ingestedAt: latencies {10,20,…,100} ms.
            Instant base = Instant.now();
            for (long offset : new long[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}) {
                publishOne(
                        publisher,
                        new Event(UUID.randomUUID(), source, INSTRUMENT, base.minusMillis(offset), base, tradeTick()));
            }

            BusMonitor monitor = monitor(publisher);
            try {
                monitor.runOnce(); // one drain over exactly the ten samples

                assertThat(await(
                                () -> sink.size(MetricNames.feedLatency(LatencyPercentile.P50), source) > 0,
                                Duration.ofSeconds(10)))
                        .isTrue();
                // Nearest-rank over [10..100]: p50=50, p90=90, p99=100, max=100, count=10.
                assertThat(sink.latest(MetricNames.feedLatency(LatencyPercentile.P50), source))
                        .hasValueSatisfying(v -> assertThat(v).isEqualByComparingTo("50"));
                assertThat(sink.latest(MetricNames.feedLatency(LatencyPercentile.P90), source))
                        .hasValueSatisfying(v -> assertThat(v).isEqualByComparingTo("90"));
                assertThat(sink.latest(MetricNames.feedLatency(LatencyPercentile.P99), source))
                        .hasValueSatisfying(v -> assertThat(v).isEqualByComparingTo("100"));
                assertThat(sink.latest(MetricNames.feedLatency(LatencyPercentile.MAX), source))
                        .hasValueSatisfying(v -> assertThat(v).isEqualByComparingTo("100"));
                assertThat(sink.latest(MetricNames.FEED_EVENT_COUNT, source))
                        .hasValueSatisfying(v -> assertThat(v).isEqualByComparingTo("10"));
            } finally {
                monitor.close();
            }
        }
    }

    // Publisher error/buffer metrics: failed rises during an outage, published resumes on recovery.
    @Test
    void publisherFailureCounterRisesThenRecovers() throws Exception {
        MetricSink sink = new MetricSink();
        String failed = MetricNames.PUBLISHER_FAILED;
        String published = MetricNames.PUBLISHER_PUBLISHED;

        RedisClient blockerClient = RedisClient.create(REDIS);
        StatefulRedisConnection<String, byte[]> blocker =
                blockerClient.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));

        try (RedisStreamsEventSubscriber metrics = metricsReader(sink);
                RedisStreamsEventPublisher publisher = publisher()) {

            BusMonitor monitor = monitor(publisher);
            try {
                monitor.runOnce(); // baseline
                Thread.sleep(300); // let baseline metric publishes complete (they increment `published`)

                // Freeze the whole single-threaded server; clientPause returns before the pause engages.
                blocker.sync().clientPause(3_000);
                Throwable thrown = catchThrowable(() -> publisher
                        .publish(new Event(
                                UUID.randomUUID(), "itest-feed", INSTRUMENT, Instant.now(), Instant.now(), tradeTick()))
                        .toCompletableFuture()
                        .get(2, TimeUnit.SECONDS));
                assertThat(thrown)
                        .as("a publish against a frozen server must fail")
                        .isNotNull();

                Thread.sleep(3_200); // let the pause lapse — the server is responsive again

                // The next sweep drains failed ≥ 1.
                awaitMetric(monitor, sink, failed, "bus", v -> v >= 1, Duration.ofSeconds(15));

                // Recovery: a successful publish, and published keeps advancing.
                publishOne(publisher, tickEvent());
                awaitMetric(monitor, sink, published, "bus", v -> v >= 1, Duration.ofSeconds(15));
            } finally {
                monitor.close();
            }
        } finally {
            blocker.close();
            blockerClient.shutdown();
        }
    }

    // Per-stream publish rate tracks reality, and oldest-entry age grows on a quiet stream.
    @Test
    void ratesAndWindowAgeTrackReality() throws Exception {
        MetricSink sink = new MetricSink();
        String rate = MetricNames.streamRate(TICKS);
        String age = MetricNames.streamOldestAgeSeconds(TICKS);
        String depth = MetricNames.streamDepth(TICKS);

        try (RedisStreamsEventSubscriber metrics = metricsReader(sink);
                RedisStreamsEventPublisher publisher = publisher()) {

            BusMonitor monitor = monitor(publisher);
            try {
                publishOne(publisher, tickEvent()); // establishes the stream and its first-entry

                long before1 = System.currentTimeMillis();
                monitor.runOnce(); // baseline: entries-added=1, no rate yet (no prior sweep)
                long after1 = System.currentTimeMillis();
                assertThat(await(() -> sink.size(depth, "bus") > 0, Duration.ofSeconds(5)))
                        .isTrue();

                Thread.sleep(300);
                int extra = 50;
                for (int i = 0; i < extra; i++) {
                    publishOne(publisher, tickEvent());
                }

                int rateSamplesBefore = sink.size(rate, "bus");
                long before2 = System.currentTimeMillis();
                monitor.runOnce(); // second sweep: entries-added delta = 50
                long after2 = System.currentTimeMillis();
                assertThat(await(() -> sink.size(rate, "bus") > rateSamplesBefore, Duration.ofSeconds(5)))
                        .isTrue();

                // Rate = 50 / (takenAt2 − takenAt1); bound the elapsed by the wall clock around each sweep.
                double observed = sink.latest(rate, "bus").orElseThrow().doubleValue();
                double rateMin = extra * 1000.0 / (after2 - before1); // widest elapsed → lowest rate
                double rateMax = extra * 1000.0 / (before2 - after1); // narrowest elapsed → highest rate
                assertThat(observed)
                        .as("rate %.2f should sit within [%.2f, %.2f]", observed, rateMin, rateMax)
                        .isBetween(rateMin * 0.99, rateMax * 1.01);

                // Oldest-entry age on the (now quiet) stream grew sweep-over-sweep.
                List<BigDecimal> ages = sink.values(age, "bus");
                assertThat(ages).hasSizeGreaterThanOrEqualTo(2);
                assertThat(ages.get(ages.size() - 1).doubleValue())
                        .as("oldest age should grow as the stream sits quiet: %s", ages)
                        .isGreaterThan(ages.get(0).doubleValue());
            } finally {
                monitor.close();
            }
        }
    }

    // The pure-reader promise: the monitor creates no group, acks nothing, and leaves no connection.
    @Test
    void monitorNeverMutatesWhatItObserves() throws Exception {
        CountDownLatch release = new CountDownLatch(1);

        try (RedisStreamsEventPublisher publisher = publisher();
                RedisStreamsEventSubscriber consumer =
                        new RedisStreamsEventSubscriber(REDIS, codec, "itest-pure", "a", READER)) {

            consumer.subscribe(intents(), latest(), e -> awaitLatch(release)); // holds one entry pending
            publishOne(publisher, SampleEvents.orderIntent());
            assertThat(await(() -> reader.xpending(INTENTS, "itest-pure").getCount() >= 1, Duration.ofSeconds(10)))
                    .isTrue();

            long pendingBefore = reader.xpending(INTENTS, "itest-pure").getCount();
            int connectionsBefore = clientCount();

            BusMonitor monitor = monitor(publisher);
            try {
                monitor.runOnce();
                monitor.runOnce();
                monitor.runOnce();

                // Nothing the monitor touched changed: the pending entry is still pending (no ack),
                // and the observed stream carries only the test's group (no monitor group created).
                assertThat(reader.xpending(INTENTS, "itest-pure").getCount()).isEqualTo(pendingBefore);
                assertThat(groupNames(INTENTS)).containsExactly("itest-pure");
            } finally {
                monitor.close();
            }

            // close() leaves no dangling connection: the client count returns to its pre-monitor baseline.
            assertThat(await(() -> clientCount() <= connectionsBefore, Duration.ofSeconds(10)))
                    .as("monitor.close() must leave no connection behind")
                    .isTrue();
            release.countDown();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private RedisStreamsEventPublisher publisher() {
        return new RedisStreamsEventPublisher(REDIS, codec, RetentionPolicy.standard());
    }

    private BusMonitor monitor(RedisStreamsEventPublisher publisher) {
        return new BusMonitor(REDIS, publisher, publisher.stats(), MON, CLOCK);
    }

    private RedisStreamsEventSubscriber metricsReader(MetricSink sink) {
        RedisStreamsEventSubscriber subscriber =
                new RedisStreamsEventSubscriber(REDIS, codec, "itest-mon-reader", "a", READER);
        subscriber.subscribe(List.of(EventSelector.of(Metric.class)), earliest(), sink::accept);
        return subscriber;
    }

    /** Runs one sweep and returns the lag value it emitted for the group (waits for it to arrive on the bus). */
    private long sampleLag(BusMonitor monitor, MetricSink sink, String lagName, String group) throws Exception {
        int before = sink.size(lagName, group);
        monitor.runOnce();
        assertThat(await(() -> sink.size(lagName, group) > before, Duration.ofSeconds(5)))
                .as("lag metric for %s should be emitted", group)
                .isTrue();
        return sink.latest(lagName, group).orElseThrow().longValue();
    }

    /** Sweeps repeatedly until the metric's latest value satisfies the predicate, or fails at the deadline. */
    private void awaitMetric(
            BusMonitor monitor, MetricSink sink, String name, String owner, LongPredicate predicate, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            int before = sink.size(name, owner);
            monitor.runOnce();
            await(() -> sink.size(name, owner) > before, Duration.ofSeconds(3));
            if (sink.latest(name, owner).map(v -> predicate.test(v.longValue())).orElse(false)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("metric %s{owner=%s} never satisfied the predicate within %s", name, owner, timeout);
    }

    private String awaitEndpoint(HttpClient http, int port, String contains, Duration timeout) throws Exception {
        URI uri = URI.create("http://localhost:" + port + "/metrics");
        long deadline = System.nanoTime() + timeout.toNanos();
        String body = "";
        while (System.nanoTime() < deadline) {
            HttpResponse<String> response =
                    http.send(HttpRequest.newBuilder().uri(uri).build(), HttpResponse.BodyHandlers.ofString());
            body = response.body();
            if (response.statusCode() == 200 && body.contains(contains)) {
                return body;
            }
            Thread.sleep(100);
        }
        return body;
    }

    private Set<String> groupNames(String stream) {
        Set<String> names = new HashSet<>();
        for (Object entry : reader.xinfoGroups(stream)) {
            if (entry instanceof List<?> fields) {
                names.add(XInfoReplies.asString(XInfoReplies.asFieldMap(fields).get("name")));
            }
        }
        return names;
    }

    private int clientCount() {
        String list = reader.clientList();
        return (int) list.lines().filter(line -> !line.isBlank()).count();
    }

    private void publishTicks(RedisStreamsEventPublisher publisher, int n) throws Exception {
        for (int i = 0; i < n; i++) {
            publishOne(publisher, tickEvent());
        }
    }

    private static void publishOne(RedisStreamsEventPublisher publisher, Event event) throws Exception {
        publisher.publish(event).toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static Event tickEvent() {
        return SampleEvents.event(INSTRUMENT, tradeTick());
    }

    private static TradeTick tradeTick() {
        return new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY);
    }

    private static List<EventSelector> ticks() {
        return List.of(EventSelector.of(TradeTick.class, INSTRUMENT));
    }

    private static List<EventSelector> intents() {
        return List.of(EventSelector.of(OrderIntent.class));
    }

    private static SubscribeOptions earliest() {
        return SubscribeOptions.of(StartPosition.EARLIEST);
    }

    private static SubscribeOptions latest() {
        return SubscribeOptions.of(StartPosition.LATEST);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(30);
        }
        return condition.getAsBoolean();
    }

    /** Collects the values of every emitted metric, keyed by (name, owner), preserving arrival order. */
    private static final class MetricSink {
        private final Map<String, List<BigDecimal>> byKey = new ConcurrentHashMap<>();

        void accept(Event event) {
            Metric metric = (Metric) event.payload();
            byKey.computeIfAbsent(key(metric.name(), metric.owner()), k -> new CopyOnWriteArrayList<>())
                    .add(metric.value());
        }

        int size(String name, String owner) {
            return values(name, owner).size();
        }

        List<BigDecimal> values(String name, String owner) {
            return byKey.getOrDefault(key(name, owner), List.of());
        }

        Optional<BigDecimal> latest(String name, String owner) {
            List<BigDecimal> values = values(name, owner);
            return values.isEmpty() ? Optional.empty() : Optional.of(values.get(values.size() - 1));
        }

        private static String key(String name, String owner) {
            return name + ' ' + owner;
        }
    }
}
