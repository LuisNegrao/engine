package engine.core.serde;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Scenario 2: {@code BigDecimal} is on the wire as an exact-scale JSON <em>string</em>, not a
 * number. This is what preserves scale (and dodges binary-float rounding on money).
 */
class BigDecimalWireShapeTest {

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    @Test
    void bigDecimalIsSerializedAsExactScaleString() {
        String json = new String(codec.encode(SampleEvents.tradeTick()), StandardCharsets.UTF_8);

        assertThat(json).contains("\"price\":\"67231.50\"");
        assertThat(json).contains("\"quantity\":\"0.0042\"");
        // Not a bare JSON number.
        assertThat(json).doesNotContain("\"price\":67231.5");
    }

    @Test
    void instantAndDurationAreIso8601Strings() {
        String json = new String(codec.encode(SampleEvents.bar()), StandardCharsets.UTF_8);

        // Duration as ISO-8601 (PT1M), Instant as ISO-8601 — pins JavaTimeModule + no-timestamps.
        assertThat(json).contains("\"interval\":\"PT1M\"");
        assertThat(json).contains("\"intervalStart\":\"2026-07-10T14:03:00Z\"");
        assertThat(json).contains("\"occurredAt\":\"2026-07-10T14:03:22.113Z\"");
    }
}
