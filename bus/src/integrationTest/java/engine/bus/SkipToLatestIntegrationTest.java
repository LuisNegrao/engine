package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.SkipToLatest;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.bus.Subscription;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-19 Step 6 acceptance: a {@link SkipToLatest} subscription whose handler is gated so lag piles
 * up abandons the backlog once lag crosses the threshold, resumes near the tail, and records the
 * skip. Total handled ends far below the number published — the market-data consumer gapped on
 * purpose rather than compute on stale ticks.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 */
class SkipToLatestIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");
    private static final String STREAM = "md.tick.trade.TEST-A.ITEST";
    private static final String GROUP = "itest-skip";
    private static final int PUBLISHED = 5_000;
    private static final long THRESHOLD = 1_000;

    private static final SubscriberTuning FAST = new SubscriberTuning(
            Duration.ofMillis(100), 100, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    @BeforeEach
    void connect() {
        client = RedisClient.create(URI);
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
    }

    @AfterEach
    void cleanUp() {
        try {
            reader.xgroupDestroy(STREAM, GROUP);
        } catch (RuntimeException ignored) {
            // group may not exist if the test failed before subscribe
        }
        reader.del(STREAM);
        connection.close();
        client.shutdown();
    }

    @Test
    void skipsTheBacklogAndResumesNearTheTail() throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger handled = new AtomicInteger();
        Set<String> handledIds = ConcurrentHashMap.newKeySet();

        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, GROUP, "instance-a", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            Subscription subscription = subscriber.subscribe(
                    List.of(EventSelector.of(TradeTick.class, TEST_A)),
                    SubscribeOptions.of(StartPosition.LATEST, new SkipToLatest(THRESHOLD)),
                    event -> {
                        // Every call parks on the gate until released; the first delivered event stalls
                        // the poll thread while the backlog builds behind it.
                        gate.await();
                        handledIds.add(event.eventId().toString());
                        handled.incrementAndGet();
                    });

            for (int i = 0; i < PUBLISHED; i++) {
                publisher.publish(tick()).toCompletableFuture().get(2, TimeUnit.SECONDS);
            }

            // Release the stalled handler; the poll thread finishes its first batch, then the next
            // claim sweep sees ~5000 of undelivered lag and skips to the tail.
            gate.countDown();

            assertThat(awaitSkip(subscription)).isTrue();

            // It resumes live at the tail: markers published after the skip are all delivered.
            long handledBeforeMarkers = handled.get();
            Set<String> markers = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < 5; i++) {
                Event marker = tick();
                markers.add(marker.eventId().toString());
                publisher.publish(marker).toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
            assertThat(awaitAll(handledIds, markers)).isTrue();

            subscription.close();

            assertThat(subscription.skipCount()).isPositive();
            // Far below PUBLISHED: only the first batch (plus the 5 markers) was ever handled.
            assertThat(handled.get()).isLessThan(PUBLISHED / 2);
            assertThat(handledBeforeMarkers).isLessThan(PUBLISHED / 2);
        }
    }

    private boolean awaitSkip(Subscription subscription) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (subscription.skipCount() > 0) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private boolean awaitAll(Set<String> handledIds, Set<String> wanted) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (handledIds.containsAll(wanted)) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private Event tick() {
        return SampleEvents.event(
                TEST_A, new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY));
    }
}
