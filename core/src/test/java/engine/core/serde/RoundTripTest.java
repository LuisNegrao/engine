package engine.core.serde;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.event.Event;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Scenario 1: every payload type round-trips {@code decode(encode(e)) == e}, envelope and payload.
 * Because records use {@code equals} and {@link java.math.BigDecimal#equals} is scale-sensitive,
 * this also proves string serialization preserves scale.
 */
class RoundTripTest {

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    static Stream<Arguments> samples() {
        return SampleEvents.all().stream()
                .map(e -> Arguments.of(Named.of(e.payload().getClass().getSimpleName(), e)));
    }

    @ParameterizedTest
    @MethodSource("samples")
    void roundTripsEnvelopeAndPayload(Event original) {
        byte[] bytes = codec.encode(original);
        Optional<Event> decoded = codec.decode(bytes);
        assertThat(decoded).contains(original);
    }
}
