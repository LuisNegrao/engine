package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.SkipToLatest;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.event.OrderIntent;
import engine.core.event.TradeTick;
import engine.core.serde.SampleEvents;
import io.lettuce.core.ClientOptions;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class RedisStreamsEventSubscriberTest {

    /**
     * Pins the subscriber's own client options. The load-bearing difference from the publisher is the
     * 5 s command timeout: it MUST exceed the {@code XREADGROUP BLOCK} (1 s), or Lettuce kills every
     * blocking read from the inside. This pin is what stops a refactor from "unifying" the two option
     * sets and silently breaking every read.
     */
    @Test
    void clientOptionsPinTheBlockingReadSafeTimeout() {
        ClientOptions options = RedisStreamsEventSubscriber.clientOptions();

        assertThat(options.getDisconnectedBehavior()).isEqualTo(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS);
        assertThat(options.getTimeoutOptions().isTimeoutCommands()).isTrue();
        assertThat(options.getSocketOptions().getConnectTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.isAutoReconnect()).isTrue();
    }

    @Test
    void resolveStreamsMapsSelectorsToDistinctStreams() {
        List<String> streams = RedisStreamsEventSubscriber.resolveStreams(
                List.of(EventSelector.of(OrderIntent.class), EventSelector.of(OrderIntent.class)),
                SubscribeOptions.of(StartPosition.LATEST));

        assertThat(streams).containsExactly("orders.intents");
    }

    @Test
    void resolveStreamsRejectsInvalidSelector() {
        assertThatThrownBy(() -> RedisStreamsEventSubscriber.resolveStreams(
                        List.of(EventSelector.of(TradeTick.class)), SubscribeOptions.of(StartPosition.LATEST)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TradeTick");
    }

    @Test
    void skipToLatestOnANonMarketDataStreamThrowsAtSubscribe() {
        assertThatThrownBy(() -> RedisStreamsEventSubscriber.resolveStreams(
                        List.of(EventSelector.of(OrderIntent.class)),
                        SubscribeOptions.of(StartPosition.LATEST, new SkipToLatest(1000))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orders.intents");
    }

    @Test
    void skipToLatestOnMarketDataStreamsIsAllowed() {
        List<String> streams = RedisStreamsEventSubscriber.resolveStreams(
                List.of(EventSelector.of(TradeTick.class, SampleEvents.BTC)),
                SubscribeOptions.of(StartPosition.LATEST, new SkipToLatest(1000)));

        assertThat(streams).containsExactly("md.tick.trade.BTC-USDT.BINANCE");
    }

    @Test
    void resolveReplayStreamsMapsSelectorsToDistinctStreams() {
        List<String> streams = RedisStreamsEventSubscriber.resolveReplayStreams(
                List.of(EventSelector.of(TradeTick.class, SampleEvents.BTC), EventSelector.of(OrderIntent.class)));

        assertThat(streams).containsExactly("md.tick.trade.BTC-USDT.BINANCE", "orders.intents");
    }

    @Test
    void resolveReplayStreamsRejectsInvalidSelector() {
        // A partitioned type with no instrument fails at wiring time, exactly as on the subscribe path.
        assertThatThrownBy(() ->
                        RedisStreamsEventSubscriber.resolveReplayStreams(List.of(EventSelector.of(TradeTick.class))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TradeTick");
    }
}
