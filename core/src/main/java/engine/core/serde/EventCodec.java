package engine.core.serde;

import engine.core.event.Event;
import java.util.Optional;

/**
 * Wire (de)serialization for {@link Event}s. The interface a binary format would re-implement — the
 * rest of the engine depends only on this, never on Jackson.
 *
 * <p>The return shapes encode a deliberate distinction: an unknown {@code eventType} is a normal
 * forward-compat situation ({@link #decode} returns empty), whereas malformed JSON or a missing
 * required envelope field is corrupt data — a bug — and <strong>throws</strong>.
 */
public interface EventCodec {

    /** Serializes an event to bytes. */
    byte[] encode(Event event);

    /**
     * Reads only the envelope; never fails on an unknown {@code eventType} because it never inspects
     * the payload. Throws on malformed JSON or a missing required envelope field.
     */
    EnvelopeView envelope(byte[] bytes);

    /**
     * Fully decodes an event. Returns empty iff the JSON is well-formed but its {@code eventType} is
     * not registered. Throws on malformed JSON or a missing required envelope field.
     */
    Optional<Event> decode(byte[] bytes);
}
