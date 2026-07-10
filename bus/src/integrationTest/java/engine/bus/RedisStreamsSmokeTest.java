package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Wiring proof for the whole toolchain: JVM -> Lettuce -> docker-compose Redis, using XADD/XRANGE
 * deliberately — streams are the primitive the entire bus epic is built on.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 */
class RedisStreamsSmokeTest {

    @Test
    void roundTripsThroughRedisStreams() {
        // try-with-resources is load-bearing: an unclosed client leaves non-daemon
        // threads alive and the test JVM never exits.
        try (RedisClient client = RedisClient.create("redis://localhost:6379")) {
            var commands = client.connect().sync();
            String id = commands.xadd("smoke.test", Map.of("k", "v"));
            var entries = commands.xrange("smoke.test", Range.<String>unbounded());
            assertThat(entries).extracting(StreamMessage::getId).contains(id);
            commands.del("smoke.test");
        }
    }
}
