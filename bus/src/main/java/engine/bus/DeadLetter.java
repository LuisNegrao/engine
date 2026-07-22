package engine.bus;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parks poison entries on the {@code dlq.<stream>} dead-letter stream, per ADR 0002 §3. An entry is
 * poison when its handler has failed {@code maxDeliveries} times, or when its bytes will not decode
 * at all (a version-skewed producer). Either way the evidence is preserved on the DLQ and the
 * original is acknowledged so healthy events keep flowing past it.
 *
 * <p>The DLQ field names below are <strong>frozen wire constants</strong>: NEG-21's monitoring and
 * any operator replaying a DLQ read them by name, so renaming one strands every historical entry.
 * The failing <em>group</em> is a field, not part of the stream name — one DLQ per source stream
 * carries the poison of every group reading it.
 *
 * <p><strong>Park and ack are two commands, not one transaction.</strong> A crash between the
 * {@code XADD} and the {@code XACK} re-parks the entry on its next redelivery — a duplicate DLQ entry
 * carrying the same {@code eventId}, which at-least-once already permits. A Redis transaction here
 * would buy nothing the idempotency contract does not already cover.
 */
public final class DeadLetter {

    /** The original codec-encoded event bytes, verbatim — a DLQ entry is replayable. */
    public static final String FIELD_EVENT = "event";
    /** The source stream the poison entry came from. */
    public static final String FIELD_STREAM = "stream";
    /** The consumer group whose handler failed. */
    public static final String FIELD_GROUP = "group";
    /** The consumer name that parked it. */
    public static final String FIELD_CONSUMER = "consumer";
    /** How many deliveries were attempted before parking (decimal string). */
    public static final String FIELD_DELIVERIES = "deliveries";
    /** The last failure: exception class, message, and first frames (truncated). */
    public static final String FIELD_ERROR = "error";
    /** When it was parked, as an ISO-8601 {@link Instant} string. */
    public static final String FIELD_FAILED_AT = "failedAt";

    /** {@link #FIELD_ERROR} is capped at 2 KB — enough for the class, message, and a few frames. */
    static final int MAX_ERROR_BYTES = 2048;

    private static final String TRUNCATION_MARKER = "…(truncated)";

    private DeadLetter() {}

    /**
     * Writes the poison entry to {@code dlq.<sourceStream>} with a {@code MAXLEN ~ dlqMaxlen} cap
     * (runaway protection — NEG-21 alerts on DLQ depth long before this), then {@code XACK}s the
     * original off the source stream's pending list. Not atomic; see the class note.
     */
    public static void park(
            RedisCommands<String, byte[]> commands,
            String sourceStream,
            String group,
            String consumer,
            String messageId,
            byte[] eventBytes,
            long deliveries,
            String error,
            long dlqMaxlen,
            Instant failedAt) {
        Map<String, byte[]> body = new LinkedHashMap<>();
        body.put(FIELD_EVENT, eventBytes);
        body.put(FIELD_STREAM, utf8(sourceStream));
        body.put(FIELD_GROUP, utf8(group));
        body.put(FIELD_CONSUMER, utf8(consumer));
        body.put(FIELD_DELIVERIES, utf8(Long.toString(deliveries)));
        body.put(FIELD_ERROR, utf8(error));
        body.put(FIELD_FAILED_AT, utf8(failedAt.toString()));

        XAddArgs args = new XAddArgs().maxlen(dlqMaxlen).approximateTrimming();
        commands.xadd(StreamNames.dlqFor(sourceStream), args, body);
        commands.xack(sourceStream, group, messageId);
    }

    /**
     * Renders a throwable as {@code ClassName: message} plus its first five frames, truncated to
     * {@link #MAX_ERROR_BYTES}. Keeping the full history is a non-goal — only the last failure is
     * recorded, which is what NEG-21's monitoring needs.
     */
    static String describeError(Throwable t) {
        StringBuilder sb = new StringBuilder(t.getClass().getName());
        if (t.getMessage() != null) {
            sb.append(": ").append(t.getMessage());
        }
        StackTraceElement[] frames = t.getStackTrace();
        for (int i = 0; i < Math.min(frames.length, 5); i++) {
            sb.append("\n\tat ").append(frames[i]);
        }
        return truncate(sb.toString(), MAX_ERROR_BYTES);
    }

    /** Truncates to at most {@code maxBytes} UTF-8 bytes, appending a marker when it cuts. */
    static String truncate(String value, int maxBytes) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return value;
        }
        // Leave room for the marker and a possible replacement char from a cut multibyte sequence,
        // so the result stays at or under maxBytes even in the worst case.
        int budget = Math.max(0, maxBytes - TRUNCATION_MARKER.getBytes(StandardCharsets.UTF_8).length - 3);
        String head = new String(bytes, 0, budget, StandardCharsets.UTF_8);
        return head + TRUNCATION_MARKER;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
