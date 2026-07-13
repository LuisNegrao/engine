package engine.core.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.event.Event;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Scenario 4: an unknown {@code eventType} is a normal forward-compat situation — {@code envelope()}
 * still reads, {@code decode()} returns empty, nothing throws. The negative control: syntactically
 * broken JSON makes both throw, because corrupt data is a bug, not a new event type.
 */
class UnknownEventTypeTest {

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private static final String UNKNOWN_TYPE =
            """
            {"eventId":"5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a","eventType":"tick.futuristic",\
            "schemaVersion":7,"source":"future-feed","instrumentId":"BTC-USDT.BINANCE",\
            "occurredAt":"2026-07-10T14:03:22.113Z","ingestedAt":"2026-07-10T14:03:22.145Z",\
            "payload":{"whatever":"shape"}}""";

    @Test
    void unknownEventTypeReadsEnvelopeButDecodesEmpty() {
        byte[] bytes = UNKNOWN_TYPE.getBytes(StandardCharsets.UTF_8);

        EnvelopeView view = codec.envelope(bytes);
        assertThat(view.eventType()).isEqualTo("tick.futuristic");
        assertThat(view.schemaVersion()).isEqualTo(7);
        assertThat(view.source()).isEqualTo("future-feed");
        assertThat(view.instrumentId()).isEqualTo(SampleEvents.BTC);

        Optional<Event> decoded = codec.decode(bytes);
        assertThat(decoded).isEmpty();
    }

    @Test
    void brokenJsonThrowsFromBothEnvelopeAndDecode() {
        byte[] broken = "{not valid json".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> codec.envelope(broken)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> codec.decode(broken)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void missingRequiredEnvelopeFieldThrows() {
        // No "source" — a missing required envelope field is corrupt data, so it throws.
        String json =
                """
                {"eventId":"5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a","eventType":"metric",\
                "schemaVersion":1,"occurredAt":"2026-07-10T14:03:22.113Z",\
                "ingestedAt":"2026-07-10T14:03:22.145Z","payload":{}}""";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> codec.envelope(bytes)).isInstanceOf(IllegalArgumentException.class);
    }
}
