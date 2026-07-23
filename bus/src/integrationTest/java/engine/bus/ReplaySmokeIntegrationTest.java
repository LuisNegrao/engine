package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.bus.EventSelector;
import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayRange;
import engine.core.event.Event;
import engine.core.event.OrderIntent;
import engine.core.event.OrderType;
import engine.core.event.Side;
import engine.core.event.TimeInForce;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-20 Step 4 happy-path smoke: publish ten events through the real {@link
 * RedisStreamsEventPublisher}, replay the bounded range through {@link RedisStreamsEventSubscriber#replay},
 * and assert all ten equal events arrive in order, {@code done()} completes with 10, and the replay's
 * dedicated connection is gone afterward (no leak).
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 */
class ReplaySmokeIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final String STREAM = "orders.intents";
    private static final String GROUP = "itest-replay-smoke";

    private static final SubscriberTuning FAST =
            new SubscriberTuning(Duration.ofMillis(100), 4, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    @BeforeEach
    void connect() {
        client = RedisClient.create(URI);
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
        reader.del(STREAM);
    }

    @AfterEach
    void cleanUp() {
        reader.del(STREAM);
        connection.close();
        client.shutdown();
    }

    @Test
    void replaysAllTenEventsInOrderThenReleasesItsConnection() throws Exception {
        List<Event> published = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            published.add(orderIntent(i));
        }

        List<Event> received = new CopyOnWriteArrayList<>();

        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, GROUP, "replayer", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            // Batch smaller than 10 so the merge/refill path (advancing the cursor across batches) runs.
            for (Event event : published) {
                publisher.publish(event).toCompletableFuture().get(2, TimeUnit.SECONDS);
            }

            long baselineConnections = clientCount();

            var replay = subscriber.replay(
                    List.of(EventSelector.of(OrderIntent.class)),
                    ReplayRange.from(ReplayPosition.earliest()),
                    received::add);

            Long count = replay.done().toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertThat(count).isEqualTo(10L);
            assertThat(received).containsExactlyElementsOf(published);

            // The replay's dedicated connection is closed by its own thread on completion — the client
            // count returns to the pre-replay baseline, proving no connection was leaked.
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (clientCount() > baselineConnections && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(clientCount()).isEqualTo(baselineConnections);

            // A pure reader creates no consumer group on the stream.
            assertThat(reader.xinfoGroups(STREAM)).isEmpty();
        }
    }

    private long clientCount() {
        return reader.clientList().lines().filter(line -> !line.isBlank()).count();
    }

    private Event orderIntent(int i) {
        return SampleEvents.event(
                SampleEvents.BTC,
                new OrderIntent(
                        "strat-" + i,
                        Side.BUY,
                        new BigDecimal("1.00"),
                        OrderType.LIMIT,
                        new BigDecimal("67000.00"),
                        TimeInForce.GTC,
                        "client-order-" + i));
    }
}
