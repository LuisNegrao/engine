package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import engine.bus.RetentionPolicy.Rule;
import engine.core.event.Event;
import engine.core.serde.SampleEvents;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class RetentionPolicyTest {

    /**
     * Expected windows and caps are hardcoded literals from the ADR 0002 §4 table on purpose —
     * reading them back off {@link RetentionPolicy#standard()} would make the test a tautology
     * unable to catch a wrong number.
     */
    static Stream<Arguments> adrRetentionTable() {
        return Stream.of(
                arguments("md.tick.quote.BTC-USDT.BINANCE", Duration.ofHours(12), 2_000_000L),
                arguments("md.tick.trade.ETH-USDT.BINANCE", Duration.ofHours(12), 1_000_000L),
                arguments("md.bar.1m.BTC-USDT.BINANCE", Duration.ofDays(14), 50_000L),
                arguments("signals", Duration.ofDays(14), 500_000L),
                arguments("orders.intents", Duration.ofDays(30), 1_000_000L),
                arguments("orders.fills", Duration.ofDays(30), 1_000_000L),
                arguments("metrics", Duration.ofHours(48), 2_000_000L),
                arguments("commands", Duration.ofDays(30), 100_000L));
    }

    @ParameterizedTest
    @MethodSource("adrRetentionTable")
    void concreteStreamResolvesToItsAdrRule(String stream, Duration window, long maxlen) {
        Rule rule = RetentionPolicy.standard().ruleFor(stream);

        assertThat(rule.window()).isEqualTo(window);
        assertThat(rule.maxlen()).isEqualTo(maxlen);
    }

    /**
     * Guards drift between the two ADR-table encodings: every stream {@link StreamNames} can produce
     * must resolve to a retention rule. Combined with {@code StreamNamesTest}'s coverage test, this
     * turns "new payload type without a retention rule" into a CI failure instead of a production
     * exception.
     */
    @ParameterizedTest
    @MethodSource("everySampleEvent")
    void everyRoutableStreamHasARetentionRule(Event event) {
        String stream = StreamNames.streamFor(event);

        assertThat(RetentionPolicy.standard().ruleFor(stream)).isNotNull();
    }

    static Stream<Event> everySampleEvent() {
        return SampleEvents.all().stream();
    }

    @Test
    void longestPrefixWins() {
        RetentionPolicy policy = new RetentionPolicy(List.of(
                new Rule("md.", Duration.ofHours(1), 10), new Rule("md.tick.trade.", Duration.ofHours(12), 1_000_000)));

        Rule rule = policy.ruleFor("md.tick.trade.X.Y");

        assertThat(rule.prefix()).isEqualTo("md.tick.trade.");
        assertThat(rule.window()).isEqualTo(Duration.ofHours(12));
        assertThat(rule.maxlen()).isEqualTo(1_000_000);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.intentsX", "signals.extra"})
    void exactRulesDoNotPrefixMatch(String stream) {
        assertThatThrownBy(() -> RetentionPolicy.standard().ruleFor(stream))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(stream);
    }

    @Test
    void unknownStreamThrowsNamingTheStream() {
        assertThatThrownBy(() -> RetentionPolicy.standard().ruleFor("bogus.stream"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus.stream");
    }

    @Test
    void standardHasExactlyEightRules() {
        assertThat(RetentionPolicy.standard().rules()).hasSize(8);
    }

    @Test
    void emptyRulesListThrows() {
        assertThatThrownBy(() -> new RetentionPolicy(List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ruleWithBlankPrefixThrows() {
        assertThatThrownBy(() -> new Rule(" ", Duration.ofHours(1), 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("nonPositiveDurations")
    void ruleWithNonPositiveWindowThrows(Duration window) {
        assertThatThrownBy(() -> new Rule("md.", window, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Duration> nonPositiveDurations() {
        return Stream.of(Duration.ZERO, Duration.ofHours(-1));
    }

    @Test
    void ruleWithMaxlenBelowOneThrows() {
        assertThatThrownBy(() -> new Rule("md.", Duration.ofHours(1), 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
