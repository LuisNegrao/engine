package engine.core.serde;

import engine.core.event.InstrumentId;
import java.time.Instant;
import java.util.UUID;

/**
 * The envelope fields read without touching the payload — everything generic infrastructure (bus,
 * archive, replay, metrics) ever needs. {@code eventType} stays a raw {@code String} and {@code
 * schemaVersion} a raw {@code int} precisely because infra reads them without knowing the payload
 * type, so parsing never depends on a registry lookup succeeding.
 *
 * @param eventId unique event id
 * @param eventType raw wire discriminator, e.g. {@code "tick.trade"}
 * @param schemaVersion raw wire schema version
 * @param source producing component
 * @param instrumentId the instrument, or {@code null} if absent
 * @param occurredAt source timestamp
 * @param ingestedAt engine-receipt timestamp
 */
public record EnvelopeView(
        UUID eventId,
        String eventType,
        int schemaVersion,
        String source,
        InstrumentId instrumentId,
        Instant occurredAt,
        Instant ingestedAt) {}
