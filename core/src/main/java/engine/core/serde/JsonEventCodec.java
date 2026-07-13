package engine.core.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Payload;
import engine.core.serde.PayloadRegistry.PayloadType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The one and only Jackson-importing file. Encodes/decodes {@link Event}s as JSON per the wire
 * format in the NEG-16 plan. Decode is two-phase: read the tree, build the {@link EnvelopeView},
 * then look the {@code eventType} up in the injected {@link PayloadRegistry} and convert the payload
 * subtree — a lookup miss means empty (forward-compat), not an exception.
 */
public final class JsonEventCodec implements EventCodec {

    private final ObjectMapper mapper;
    private final PayloadRegistry registry;

    public JsonEventCodec(PayloadRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.mapper = defaultObjectMapper();
    }

    /**
     * Builds the shared, immutable-config {@link ObjectMapper}. The settings here are load-bearing
     * and pinned by the codec test suite:
     *
     * <ul>
     *   <li>{@link JavaTimeModule} + dates/durations NOT as timestamps ⇒ ISO-8601 {@code Instant}
     *       and {@code Duration} strings.
     *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES} off ⇒ a newer producer's extra payload fields are
     *       ignored by older consumers (forward compat).
     *   <li>{@code USE_BIG_DECIMAL_FOR_FLOATS} on ⇒ any numeric floats become {@code BigDecimal}.
     *   <li>{@code NON_NULL} inclusion ⇒ null components (e.g. an absent {@code limitPrice}) are
     *       omitted rather than written as {@code null}.
     *   <li>{@code BigDecimal} serialized via {@link ToStringSerializer} ⇒ JSON strings that
     *       preserve scale (e.g. {@code "67231.50"}), keeping the records annotation-free.
     * </ul>
     */
    static ObjectMapper defaultObjectMapper() {
        SimpleModule bigDecimalAsString = new SimpleModule();
        bigDecimalAsString.addSerializer(BigDecimal.class, ToStringSerializer.instance);
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .addModule(bigDecimalAsString)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .build();
    }

    @Override
    public byte[] encode(Event event) {
        Objects.requireNonNull(event, "event must not be null");
        PayloadType pt = registry.forPayloadClass(event.payload().getClass());
        ObjectNode root = mapper.createObjectNode();
        root.put("eventId", event.eventId().toString());
        root.put("eventType", pt.eventType());
        root.put("schemaVersion", pt.schemaVersion());
        root.put("source", event.source());
        if (event.instrumentId() != null) {
            root.put("instrumentId", event.instrumentId().toString());
        }
        root.put("occurredAt", event.occurredAt().toString());
        root.put("ingestedAt", event.ingestedAt().toString());
        root.set("payload", mapper.valueToTree(event.payload()));
        try {
            return mapper.writeValueAsBytes(root);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode event", e);
        }
    }

    @Override
    public EnvelopeView envelope(byte[] bytes) {
        return toEnvelope(readTree(bytes));
    }

    @Override
    public Optional<Event> decode(byte[] bytes) {
        JsonNode root = readTree(bytes);
        EnvelopeView env = toEnvelope(root);
        Optional<PayloadType> pt = registry.byEventType(env.eventType());
        if (pt.isEmpty()) {
            return Optional.empty();
        }
        JsonNode payloadNode = root.get("payload");
        if (payloadNode == null || payloadNode.isNull()) {
            throw new IllegalArgumentException("missing required envelope field: payload");
        }
        Payload payload = mapper.convertValue(payloadNode, pt.get().type());
        return Optional.of(new Event(
                env.eventId(), env.source(), env.instrumentId(), env.occurredAt(), env.ingestedAt(), payload));
    }

    private JsonNode readTree(byte[] bytes) {
        try {
            return mapper.readTree(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("malformed event JSON", e);
        }
    }

    private static EnvelopeView toEnvelope(JsonNode root) {
        UUID eventId = UUID.fromString(required(root, "eventId").asText());
        String eventType = required(root, "eventType").asText();
        int schemaVersion = required(root, "schemaVersion").asInt();
        String source = required(root, "source").asText();
        JsonNode instrumentNode = root.get("instrumentId");
        InstrumentId instrumentId = (instrumentNode == null || instrumentNode.isNull())
                ? null
                : InstrumentId.parse(instrumentNode.asText());
        Instant occurredAt = Instant.parse(required(root, "occurredAt").asText());
        Instant ingestedAt = Instant.parse(required(root, "ingestedAt").asText());
        return new EnvelopeView(eventId, eventType, schemaVersion, source, instrumentId, occurredAt, ingestedAt);
    }

    private static JsonNode required(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("missing required envelope field: " + field);
        }
        return node;
    }
}
