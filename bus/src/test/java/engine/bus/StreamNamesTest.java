package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import engine.core.event.Bar;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Payload;
import engine.core.serde.SampleEvents;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class StreamNamesTest {

    /**
     * Expected names are hardcoded literals from the ADR 0002 §3 table on purpose — computing them
     * via {@link StreamNames} would make the test a tautology unable to catch a wrong table.
     */
    static Stream<Arguments> adrStreamTable() {
        return Stream.of(
                arguments(SampleEvents.tradeTick(), "md.tick.trade.BTC-USDT.BINANCE"),
                arguments(SampleEvents.quoteTick(), "md.tick.quote.BTC-USDT.BINANCE"),
                arguments(SampleEvents.bar(), "md.bar.1m.BTC-USDT.BINANCE"),
                arguments(SampleEvents.signal(), "signals"),
                arguments(SampleEvents.orderIntent(), "orders.intents"),
                arguments(SampleEvents.fill(), "orders.fills"),
                arguments(SampleEvents.metric(), "metrics"),
                arguments(SampleEvents.command(), "commands"));
    }

    @ParameterizedTest
    @MethodSource("adrStreamTable")
    void routesToTheAdrPrescribedStream(Event event, String expectedStream) {
        assertThat(StreamNames.streamFor(event)).isEqualTo(expectedStream);
    }

    @Test
    void adrStreamTableCoversEveryPayloadType() {
        Set<Class<?>> covered = adrStreamTable()
                .map(args -> ((Event) args.get()[0]).payload().getClass())
                .collect(Collectors.toSet());
        assertThat(covered).containsExactlyInAnyOrder(Payload.class.getPermittedSubclasses());
    }

    static Stream<Payload> instrumentScopedPayloads() {
        return Stream.of(
                SampleEvents.tradeTick().payload(),
                SampleEvents.quoteTick().payload(),
                SampleEvents.bar().payload());
    }

    @ParameterizedTest
    @MethodSource("instrumentScopedPayloads")
    void instrumentScopedPayloadWithoutInstrumentThrows(Payload payload) {
        Event event = event(null, payload);

        assertThatThrownBy(() -> StreamNames.streamFor(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(payload.getClass().getSimpleName());
    }

    @Test
    void singleStreamPayloadsRouteTheSameWithOrWithoutInstrument() {
        assertThat(StreamNames.streamFor(
                        event(SampleEvents.BTC, SampleEvents.metric().payload())))
                .isEqualTo("metrics");
        assertThat(StreamNames.streamFor(event(null, SampleEvents.metric().payload())))
                .isEqualTo("metrics");
    }

    @ParameterizedTest
    @CsvSource({"PT1M, 1m", "PT5M, 5m", "PT15M, 15m", "PT1H, 1h", "PT4H, 4h", "P1D, 1d"})
    void everyVocabularyIntervalMaps(Duration interval, String token) {
        Event event = event(SampleEvents.BTC, barWith(interval));

        assertThat(StreamNames.streamFor(event)).isEqualTo("md.bar." + token + ".BTC-USDT.BINANCE");
    }

    @Test
    void barIntervalOutsideTheVocabularyThrowsNamingTheDuration() {
        Event event = event(SampleEvents.BTC, barWith(Duration.ofSeconds(90)));

        assertThatThrownBy(() -> StreamNames.streamFor(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PT1M30S");
    }

    private static Bar barWith(Duration interval) {
        Bar sample = (Bar) SampleEvents.bar().payload();
        return new Bar(
                sample.intervalStart(),
                interval,
                sample.open(),
                sample.high(),
                sample.low(),
                sample.close(),
                sample.volume());
    }

    private static Event event(InstrumentId instrumentId, Payload payload) {
        return new Event(
                UUID.randomUUID(), "test-feed", instrumentId, SampleEvents.OCCURRED, SampleEvents.INGESTED, payload);
    }
}
