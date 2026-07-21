package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.event.Event;
import engine.core.event.InstrumentId;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Step 6.2 acceptance: the publisher's {@code MAXLEN ~} cap actually bounds a stream. A test policy
 * caps {@code md.tick.trade.} at 100; after publishing 1000 trade ticks the stream length must sit
 * <em>between 100 and 300</em> — bounded on both sides. The lower bound proves trimming happened at
 * all; the upper bound tolerates {@code ~} approximation, which evicts whole radix-tree nodes and so
 * overshoots the exact cap by design (an equality assertion here would be flaky).
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 */
class MaxLenCapIntegrationTest {

    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");
    private static final String TRADE_STREAM = "md.tick.trade.TEST-A.ITEST";

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    /** Covers every stream this test publishes to — {@code ruleFor} throws on an unmatched stream. */
    private final RetentionPolicy cappedPolicy =
            new RetentionPolicy(List.of(new RetentionPolicy.Rule("md.tick.trade.", Duration.ofHours(12), 100)));

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    @BeforeEach
    void connect() {
        client = RedisClient.create("redis://localhost:6379");
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
    }

    @AfterEach
    void cleanUp() {
        reader.del(TRADE_STREAM);
        connection.close();
        client.shutdown();
    }

    @Test
    void publisherCapsStreamNearMaxLen() throws Exception {
        Event tradeTick = SampleEvents.event(TEST_A, SampleEvents.tradeTick().payload());

        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher("redis://localhost:6379", codec, cappedPolicy)) {
            List<CompletableFuture<Void>> inFlight = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                inFlight.add(publisher.publish(tradeTick).toCompletableFuture());
            }
            CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        }

        assertThat(reader.xlen(TRADE_STREAM)).isBetween(100L, 300L);
    }
}
