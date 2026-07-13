package engine.core.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import engine.core.event.Event;
import engine.core.event.Metric;
import engine.core.event.Signal;
import engine.core.serde.PayloadRegistry.PayloadType;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Scenario 3: forward/backward schema compatibility. Pins the two {@link ObjectMapper} settings that
 * make it work — {@code FAIL_ON_UNKNOWN_PROPERTIES} off (a newer producer's extra field is ignored)
 * and default-null for missing components — plus the registry-injection point that lets the same
 * {@code eventType} resolve at different versions per codec instance.
 *
 * <p>Note: the plan sketches synthetic {@code CompatProbeV1(a)}/{@code CompatProbeV2(a,b)} payload
 * records. {@code Payload} is a sealed interface (a deliberate step-3 requirement so consumer
 * switches stay exhaustive), which forbids test-only implementers, so the two mapper settings are
 * exercised here with standalone probe records on the codec's shared mapper <em>and</em>
 * end-to-end through the real codec using the {@code Signal}/{@code Metric} payloads (whose
 * nullable/extra fields give the same v1↔v2 shape difference).
 */
class SchemaCompatTest {

    /** Older schema of some payload. */
    private record CompatProbeV1(int a) {}

    /** Newer schema: adds field {@code b}. */
    private record CompatProbeV2(int a, Integer b) {}

    private final ObjectMapper mapper = JsonEventCodec.defaultObjectMapper();

    @Test
    void v2BytesReadByV1IgnoreTheExtraField() throws Exception {
        byte[] v2 = mapper.writeValueAsBytes(new CompatProbeV2(1, 2));

        CompatProbeV1 asV1 = mapper.readValue(v2, CompatProbeV1.class);

        assertThat(asV1.a()).isEqualTo(1); // extra "b" ignored -> FAIL_ON_UNKNOWN_PROPERTIES off
    }

    @Test
    void v1BytesReadByV2LeaveTheNewFieldNull() throws Exception {
        byte[] v1 = mapper.writeValueAsBytes(new CompatProbeV1(1));

        CompatProbeV2 asV2 = mapper.readValue(v1, CompatProbeV2.class);

        assertThat(asV2.a()).isEqualTo(1);
        assertThat(asV2.b()).isNull(); // absent field -> null, no failure
    }

    @Test
    void extraPayloadFieldIsIgnoredEndToEndThroughTheCodec() {
        EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());
        // A "v2" producer added an "unit" field to the metric payload the v1 consumer doesn't know.
        String json =
                """
                {"eventId":"5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a","eventType":"metric",\
                "schemaVersion":1,"source":"test","occurredAt":"2026-07-10T14:03:22.113Z",\
                "ingestedAt":"2026-07-10T14:03:22.145Z",\
                "payload":{"name":"pnl.unrealized","value":"10.00","owner":"s1","unit":"USD"}}""";

        Optional<Event> decoded = codec.decode(json.getBytes(StandardCharsets.UTF_8));

        assertThat(decoded).isPresent();
        assertThat(decoded.get().payload()).isInstanceOf(Metric.class);
        assertThat(((Metric) decoded.get().payload()).name()).isEqualTo("pnl.unrealized");
    }

    @Test
    void missingNullablePayloadFieldDecodesToNullEndToEnd() {
        EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());
        // A "v1" producer omits the later-added nullable "confidence".
        String json =
                """
                {"eventId":"5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a","eventType":"signal",\
                "schemaVersion":1,"source":"test","occurredAt":"2026-07-10T14:03:22.113Z",\
                "ingestedAt":"2026-07-10T14:03:22.145Z",\
                "payload":{"signalType":"sentiment","value":"0.75"}}""";

        Optional<Event> decoded = codec.decode(json.getBytes(StandardCharsets.UTF_8));

        assertThat(decoded).isPresent();
        assertThat(((Signal) decoded.get().payload()).confidence()).isNull();
    }

    @Test
    void injectedRegistriesResolveTheSameEventTypeAtDifferentVersions() {
        // The injection point: two codecs, one registry each, same eventType at v1 and v2.
        PayloadRegistry v1 = new PayloadRegistry(List.of(new PayloadType("compat.probe", 1, Metric.class)));
        PayloadRegistry v2 = new PayloadRegistry(List.of(new PayloadType("compat.probe", 2, Metric.class)));
        EventCodec codecV1 = new JsonEventCodec(v1);
        EventCodec codecV2 = new JsonEventCodec(v2);

        assertThat(v1.byEventType("compat.probe"))
                .get()
                .extracting(PayloadType::schemaVersion)
                .isEqualTo(1);
        assertThat(v2.byEventType("compat.probe"))
                .get()
                .extracting(PayloadType::schemaVersion)
                .isEqualTo(2);

        // v2-encoded bytes (schemaVersion 2 on the wire) still decode via the v1-aware codec.
        Event event = new Event(
                UUID.randomUUID(),
                "test",
                null,
                Instant.parse("2026-07-10T14:03:22.113Z"),
                Instant.parse("2026-07-10T14:03:22.145Z"),
                new Metric("pnl.unrealized", new BigDecimal("10.00"), "s1"));
        byte[] v2Bytes = codecV2.encode(event);
        assertThat(codecV2.envelope(v2Bytes).schemaVersion()).isEqualTo(2);

        Optional<Event> viaV1 = codecV1.decode(v2Bytes);
        assertThat(viaV1).contains(event); // version mismatch tolerated; type resolves from registry
    }
}
