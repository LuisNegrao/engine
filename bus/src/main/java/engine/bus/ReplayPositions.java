package engine.bus;

import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayPosition.At;
import engine.core.bus.ReplayPosition.Earliest;
import engine.core.bus.ReplayPosition.Offset;
import engine.core.bus.ReplayRange;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Maps a transport-free {@link ReplayPosition} to the Redis stream ID an {@code XRANGE} reads from,
 * and validates what only the transport can. Pure and package-private so the mapping is unit-testable
 * without Redis.
 *
 * <p>Ingest time <em>is</em> the stream ID: {@link At}{@code (t)} maps to {@code t}'s epoch millis
 * because a stream ID's high half is the ingest millisecond (ADR 0002 §5). Bounds are inclusive at
 * millisecond granularity — a start fills the low seq to {@code 0} and an end omits the seq so Redis
 * expands it to the maximum, covering the whole millisecond either way.
 */
final class ReplayPositions {

    /** A Redis stream ID: {@code ms} or {@code ms-seq}, both non-negative decimals. */
    private static final Pattern STREAM_ID = Pattern.compile("\\d+(-\\d+)?");

    private ReplayPositions() {}

    /**
     * The inclusive lower stream ID for a range start: {@link Earliest} → {@code "0-0"}, {@link
     * At}{@code (t)} → {@code "<millis>-0"} (the start of that millisecond), {@link Offset} → the
     * token verbatim after validating its shape.
     */
    static String startId(ReplayPosition start) {
        return switch (start) {
            case Earliest ignored -> "0-0";
            case At at -> at.instant().toEpochMilli() + "-0";
            case Offset offset -> validatedToken(offset.token());
        };
    }

    /**
     * The inclusive upper stream ID for a range end. An absent ({@code null}) end returns {@code
     * tailAtStart} — the stream's last ID sampled when the replay begins, so delivery stops at the
     * retained tail and never follows into live traffic. {@link At}{@code (t)} → {@code "<millis>"}
     * with no seq, which Redis expands to the maximum seq — the whole millisecond, inclusive. {@link
     * Offset} → the token verbatim after validating its shape.
     */
    static String endId(ReplayPosition end, String tailAtStart) {
        return switch (end) {
            case null -> tailAtStart;
            case At at -> Long.toString(at.instant().toEpochMilli());
            case Offset offset -> validatedToken(offset.token());
            case Earliest ignored ->
            // Rejected by ReplayRange, so unreachable — kept for switch exhaustiveness.
            throw new IllegalArgumentException("earliest() is not a valid range end");
        };
    }

    /**
     * An {@link Offset} boundary is an opaque per-stream ID and cannot address a multi-stream merge,
     * so a range using one on either edge must resolve to exactly one stream. Throws at wiring time
     * otherwise.
     */
    static void requireSingleStreamForOffset(ReplayRange range, List<String> streams) {
        boolean usesOffset = range.start() instanceof Offset || range.maybeEnd().orElse(null) instanceof Offset;
        if (usesOffset && streams.size() != 1) {
            throw new IllegalArgumentException(
                    "an offset position addresses a single stream, but these selectors resolved to " + streams);
        }
    }

    private static String validatedToken(String token) {
        if (!STREAM_ID.matcher(token).matches()) {
            throw new IllegalArgumentException(
                    "offset token is not a valid Redis stream ID (expected \"ms\" or \"ms-seq\"): " + token);
        }
        return token;
    }
}
