package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.event.Event;
import engine.core.event.OrderIntent;
import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import engine.core.serde.SampleEvents;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-19 Step 5 acceptance: an entry whose handler always throws is redelivered by the claim sweep
 * until it crosses {@code maxDeliveries}, then parked on {@code dlq.<stream>} with the frozen fields —
 * while a healthy event published right after it is handled long before the poison parks.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}. Aggressive claim timings park
 * the poison in ~1 s instead of minutes.
 */
class PoisonToDlqIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final String STREAM = "orders.intents";
    private static final String DLQ = "dlq.orders.intents";
    private static final String GROUP = "itest-poison";
    private static final int MAX_DELIVERIES = 5;

    private static final SubscriberTuning FAST = new SubscriberTuning(
            Duration.ofMillis(100), 256, Duration.ofMillis(100), Duration.ofMillis(150), MAX_DELIVERIES, 100_000);

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
        reader.del(STREAM, DLQ);
        connection.close();
        client.shutdown();
    }

    @Test
    void parksAPoisonEntryAndKeepsHealthyEventsFlowing() throws Exception {
        Event poison = SampleEvents.orderIntent();
        Event healthy = SampleEvents.orderIntent();
        String poisonId = poison.eventId().toString();
        String healthyId = healthy.eventId().toString();

        CountDownLatch healthyHandled = new CountDownLatch(1);

        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, GROUP, "instance-a", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            subscriber.subscribe(
                    List.of(EventSelector.of(OrderIntent.class)), SubscribeOptions.of(StartPosition.LATEST), event -> {
                        if (event.eventId().toString().equals(poisonId)) {
                            throw new IllegalStateException("poison: refusing " + poisonId);
                        }
                        if (event.eventId().toString().equals(healthyId)) {
                            healthyHandled.countDown();
                        }
                    });

            publisher.publish(poison).toCompletableFuture().get(2, TimeUnit.SECONDS);
            publisher.publish(healthy).toCompletableFuture().get(2, TimeUnit.SECONDS);

            // The healthy event is acked on the first pass, long before the poison exhausts its retries.
            assertThat(healthyHandled.await(5, TimeUnit.SECONDS)).isTrue();

            // The poison lands on the DLQ once it crosses maxDeliveries (~5 × 150 ms claim backoff).
            List<StreamMessage<String, byte[]>> parked = awaitDlqEntry();
            assertThat(parked).hasSize(1);

            var body = parked.get(0).getBody();
            assertThat(codec.decode(body.get(DeadLetter.FIELD_EVENT))).contains(poison);
            assertThat(utf8(body.get(DeadLetter.FIELD_STREAM))).isEqualTo(STREAM);
            assertThat(utf8(body.get(DeadLetter.FIELD_GROUP))).isEqualTo(GROUP);
            assertThat(utf8(body.get(DeadLetter.FIELD_CONSUMER))).isEqualTo("instance-a");
            assertThat(utf8(body.get(DeadLetter.FIELD_DELIVERIES))).isEqualTo(Integer.toString(MAX_DELIVERIES));
            assertThat(utf8(body.get(DeadLetter.FIELD_ERROR))).contains("poison: refusing " + poisonId);
            assertThat(utf8(body.get(DeadLetter.FIELD_FAILED_AT))).isNotBlank();

            // Original acked off the group's PEL — nothing left pending.
            assertThat(reader.xpending(STREAM, GROUP).getCount()).isZero();
        }
    }

    private List<StreamMessage<String, byte[]>> awaitDlqEntry() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            List<StreamMessage<String, byte[]>> entries = reader.xrange(DLQ, Range.unbounded());
            if (!entries.isEmpty()) {
                return entries;
            }
            Thread.sleep(50);
        }
        return List.of();
    }

    private static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
