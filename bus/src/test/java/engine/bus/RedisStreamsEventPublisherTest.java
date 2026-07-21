package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.ClientOptions;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RedisStreamsEventPublisherTest {

    /**
     * Pins every load-bearing client option. Lettuce's defaults are exactly the forbidden behavior:
     * {@code disconnectedBehavior = ACCEPT_COMMANDS} while disconnected and
     * {@code requestQueueSize = Integer.MAX_VALUE}, which together silently buffer unbounded commands
     * during a Redis outage and replay them stale on reconnect. This pin is what stops a refactor
     * from quietly regressing back to that.
     */
    @Test
    void clientOptionsPinTheBoundedFailFastBehavior() {
        ClientOptions options = RedisStreamsEventPublisher.clientOptions();

        assertThat(options.getDisconnectedBehavior()).isEqualTo(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS);
        assertThat(options.getRequestQueueSize()).isEqualTo(4096);
        assertThat(options.getTimeoutOptions().isTimeoutCommands()).isTrue();
        assertThat(options.getSocketOptions().getConnectTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.isAutoReconnect()).isTrue();
    }
}
