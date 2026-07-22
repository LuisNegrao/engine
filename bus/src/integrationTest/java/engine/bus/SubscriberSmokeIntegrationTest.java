package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.bus.Subscription;
import engine.core.event.Event;
import engine.core.event.OrderIntent;
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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-19 Step 4 happy-path smoke: subscribe under a consumer group, publish one event through the
 * real {@link RedisStreamsEventPublisher}, and assert the handler receives the decoded, equal event
 * and that the entry is acknowledged (pending count drops to zero).
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}. Aggressive tuning (100 ms
 * block) keeps the suite from sleeping its way to seconds.
 */
class SubscriberSmokeIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final String STREAM = "orders.intents";
    private static final String GROUP = "itest-smoke";

    private static final SubscriberTuning FAST = new SubscriberTuning(
            Duration.ofMillis(100), 256, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);

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
    void deliversAPublishedEventToTheHandlerAndAcksIt() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<Event> received = new AtomicReference<>();

        Event published = SampleEvents.orderIntent();
        assertThat(published.payload()).isInstanceOf(OrderIntent.class);

        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, GROUP, "instance-a", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            Subscription subscription = subscriber.subscribe(
                    java.util.List.of(EventSelector.of(OrderIntent.class)),
                    SubscribeOptions.of(StartPosition.LATEST),
                    event -> {
                        received.set(event);
                        delivered.countDown();
                    });

            publisher.publish(published).toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(received.get()).isEqualTo(published);

            // Give the ack (which follows the handler return on the poll thread) a moment to land.
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (reader.xpending(STREAM, GROUP).getCount() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(reader.xpending(STREAM, GROUP).getCount()).isZero();

            subscription.close();
        }
    }
}
