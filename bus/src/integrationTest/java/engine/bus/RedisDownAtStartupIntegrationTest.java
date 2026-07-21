package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import io.lettuce.core.RedisConnectionException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Step 6.3 acceptance: Redis down at startup is fail-fast. Constructing a publisher against a closed
 * port throws (a {@link RedisConnectionException}, possibly wrapped, in the throwable chain) and does
 * so <em>promptly</em> — the eager {@code connect()} with a 1 s connect timeout must surface the
 * failure well under 3 s rather than hang. Promptness is the contract, so the elapsed time is part of
 * the assertion.
 *
 * <p>No Redis is needed for this test: {@code localhost:6390} is deliberately a port nothing listens
 * on.
 */
class RedisDownAtStartupIntegrationTest {

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    @Test
    void constructorFailsFastWhenRedisIsDown() {
        long startNanos = System.nanoTime();

        Throwable thrown = catchThrowable(
                () -> new RedisStreamsEventPublisher("redis://localhost:6390", codec, RetentionPolicy.standard()));

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(thrown)
                .as("constructor must throw when Redis is unreachable")
                .isNotNull();
        assertThat(throwableChain(thrown))
                .as("a RedisConnectionException must appear somewhere in the cause chain")
                .anyMatch(RedisConnectionException.class::isInstance);
        assertThat(elapsedMillis)
                .as("failure must be prompt — the fail-fast contract, not a hang")
                .isLessThan(3_000L);
    }

    private static List<Throwable> throwableChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }
}
