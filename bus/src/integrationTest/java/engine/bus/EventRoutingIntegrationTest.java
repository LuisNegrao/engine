package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.event.Event;
import engine.core.event.InstrumentId;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Step 6.1 acceptance: every payload type routes to its ADR 0002 §3 stream and round-trips through
 * the wire codec unchanged. Publishing goes through the real {@link RedisStreamsEventPublisher};
 * read-back is a plain {@code XRANGE}, and the expected stream names are <em>hardcoded ADR 0002 §3
 * string literals</em> — deriving them via {@link StreamNames} would make the test a tautology that
 * could never catch a wrong routing table.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 *
 * <p>Instrument-scoped events use the test-only instrument {@code TEST-A.ITEST} so the per-instrument
 * stream names can never collide with real market data on the shared dev Redis.
 */
class EventRoutingIntegrationTest {

    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");

    /** Every stream this test may write, DELeted after each case regardless of which one ran. */
    private static final List<String> ALL_STREAMS = List.of(
            "md.tick.trade.TEST-A.ITEST",
            "md.tick.quote.TEST-A.ITEST",
            "md.bar.1m.TEST-A.ITEST",
            "signals",
            "orders.intents",
            "orders.fills",
            "metrics",
            "commands");

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    /**
     * The eight routing cases: each pairs a fully-populated sample event with the exact ADR 0002 §3
     * stream literal it must land on. Instrument-scoped payloads are re-wrapped onto {@code
     * TEST-A.ITEST}; single-stream payloads keep the sample's envelope (their stream ignores the
     * instrument).
     */
    static Stream<Arguments> routingCases() {
        return Stream.of(
                Arguments.of(
                        "md.tick.trade.TEST-A.ITEST",
                        SampleEvents.event(TEST_A, SampleEvents.tradeTick().payload())),
                Arguments.of(
                        "md.tick.quote.TEST-A.ITEST",
                        SampleEvents.event(TEST_A, SampleEvents.quoteTick().payload())),
                Arguments.of(
                        "md.bar.1m.TEST-A.ITEST",
                        SampleEvents.event(TEST_A, SampleEvents.bar().payload())),
                Arguments.of("signals", SampleEvents.signal()),
                Arguments.of("orders.intents", SampleEvents.orderIntent()),
                Arguments.of("orders.fills", SampleEvents.fill()),
                Arguments.of("metrics", SampleEvents.metric()),
                Arguments.of("commands", SampleEvents.command()));
    }

    @BeforeEach
    void connect() {
        client = RedisClient.create("redis://localhost:6379");
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
    }

    @AfterEach
    void cleanUp() {
        reader.del(ALL_STREAMS.toArray(new String[0]));
        connection.close();
        client.shutdown();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("routingCases")
    void publishesEachTypeToItsAdrStreamAndRoundTrips(String expectedStream, Event event) throws Exception {
        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher("redis://localhost:6379", codec, RetentionPolicy.standard())) {
            publisher.publish(event).toCompletableFuture().get(2, TimeUnit.SECONDS);
        }

        List<StreamMessage<String, byte[]>> messages = reader.xrange(expectedStream, Range.unbounded());
        assertThat(messages).hasSize(1);

        byte[] bytes = messages.get(0).getBody().get(RedisStreamsEventPublisher.EVENT_FIELD);
        assertThat(bytes).isNotNull();

        Event decoded = codec.decode(bytes).orElseThrow();
        assertThat(decoded).isEqualTo(event);
    }
}
