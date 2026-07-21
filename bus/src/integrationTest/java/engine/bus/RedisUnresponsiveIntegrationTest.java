package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import engine.core.serde.SampleEvents;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Step 6.4 acceptance: a mid-run outage fails the publish stage on a command timeout instead of
 * hanging. A {@code CLIENT PAUSE} issued on a <em>second</em> connection freezes command processing
 * across the whole single-threaded Redis server; a publish issued against that frozen server must
 * have its stage completed exceptionally with a {@link RedisCommandTimeoutException} (the client's
 * 1 s command timeout) well before {@code get(2, SECONDS)} elapses.
 *
 * <p>The plan's original stand-in was {@code DEBUG SLEEP}, but {@code redis:7.4-alpine} ships with
 * {@code enable-debug-command no}, so {@code DEBUG} is rejected outright and never blocks the server.
 * {@code CLIENT PAUSE} is the equivalent deterministic freeze that needs no server reconfiguration:
 * it is issued <em>synchronously</em> and returns {@code OK} <em>before</em> the pause takes effect,
 * so the pause is provably active by the time we publish — no race, unlike an async DEBUG dispatch.
 * The pause auto-expires after its window; the teardown {@code DEL} (itself paused until expiry)
 * then both cleans up and guarantees the server is responsive again for later tests.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 */
class RedisUnresponsiveIntegrationTest {

    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");
    private static final String TRADE_STREAM = "md.tick.trade.TEST-A.ITEST";

    /** Freeze window: comfortably longer than the client's 1 s command timeout and the 2 s get(). */
    private static final long PAUSE_MILLIS = 3_000L;

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient blockerClient;
    private StatefulRedisConnection<String, byte[]> blockerConnection;

    @BeforeEach
    void connectBlocker() {
        blockerClient = RedisClient.create("redis://localhost:6379");
        blockerConnection = blockerClient.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
    }

    @AfterEach
    void cleanUp() {
        // The DEL is paused until the CLIENT PAUSE window lapses, then runs — so returning from it
        // proves the server is responsive again for the next test. The timed-out XADD may still have
        // landed once the pause ended (at-least-once), so DEL sweeps it either way.
        RedisCommands<String, byte[]> reaper = blockerConnection.sync();
        reaper.del(TRADE_STREAM);
        blockerConnection.close();
        blockerClient.shutdown();
    }

    @Test
    void publishStageFailsWithTimeoutWhileServerIsBlocked() {
        Event tradeTick = SampleEvents.event(TEST_A, SampleEvents.tradeTick().payload());

        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher("redis://localhost:6379", codec, RetentionPolicy.standard())) {
            // Freeze the server on the second connection. clientPause returns OK *before* the pause
            // engages, so the freeze is provably active by the time we publish — no race to lose.
            blockerConnection.sync().clientPause(PAUSE_MILLIS);

            Throwable thrown = catchThrowable(
                    () -> publisher.publish(tradeTick).toCompletableFuture().get(2, TimeUnit.SECONDS));

            assertThat(thrown)
                    .as("publish against a frozen server must fail, not hang")
                    .isNotNull();
            assertThat(throwableChain(thrown))
                    .as("the failure must be a command timeout (1 s), surfaced before the 2 s get() deadline")
                    .anyMatch(RedisCommandTimeoutException.class::isInstance);
        }
    }

    private static List<Throwable> throwableChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }
}
