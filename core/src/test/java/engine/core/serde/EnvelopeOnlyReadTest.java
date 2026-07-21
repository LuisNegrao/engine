package engine.core.serde;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Scenario 5: {@code envelope()} reads envelope fields even when the payload subtree is garbage,
 * proving generic infrastructure (bus, archive, replay, metrics) never pays payload-parsing costs
 * or risks a payload it doesn't understand.
 */
public class EnvelopeOnlyReadTest {

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    @Test
    void envelopeReadsDespiteGarbagePayload() {
        // eventType is a known type, but the payload would fail full decode ("not-a-number").
        String json =
                """
                {"eventId":"5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a","eventType":"tick.trade",\
                "schemaVersion":1,"source":"test-feed","instrumentId":"BTC-USDT.BINANCE",\
                "occurredAt":"2026-07-10T14:03:22.113Z","ingestedAt":"2026-07-10T14:03:22.145Z",\
                "payload":{"price":"not-a-number"}}""";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        EnvelopeView view = codec.envelope(bytes);

        assertThat(view.eventType()).isEqualTo("tick.trade");
        assertThat(view.source()).isEqualTo("test-feed");
        assertThat(view.instrumentId()).isEqualTo(SampleEvents.BTC);
        assertThat(view.occurredAt()).isEqualTo(SampleEvents.OCCURRED);
        assertThat(view.ingestedAt()).isEqualTo(SampleEvents.INGESTED);
    }
}
