package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayRetentionException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The pure start-retention decision in {@link RedisReplay#includesAtStart}, exercised against
 * fabricated {@code XINFO STREAM} field maps — no Redis. The trim-overtakes-a-running-replay behavior
 * and the abort semantics land in the Step 6 integration tests.
 */
class RedisReplayTest {

    private static final ReplayPosition EARLIEST = ReplayPosition.earliest();
    private static final ReplayPosition AT = ReplayPosition.at(Instant.parse("2026-07-10T14:03:22.145Z"));

    /** An XINFO map whose oldest retained entry ({@code first-entry}) is the trim floor. */
    private static Map<String, Object> xinfo(String oldestRetainedId) {
        return Map.of("first-entry", List.of(oldestRetainedId, List.of()));
    }

    /** An XINFO map for an existing but empty stream — no {@code first-entry}. */
    private static Map<String, Object> emptyStreamXinfo() {
        return Map.of("length", "0");
    }

    @Test
    void earliestOnAMissingStreamContributesNothing() {
        assertThat(RedisReplay.includesAtStart("md.tick.trade.X", EARLIEST, "0-0", null))
                .isFalse();
    }

    @Test
    void timestampedStartOnAMissingStreamFailsLoudly() {
        assertThatThrownBy(() -> RedisReplay.includesAtStart("md.tick.trade.X", AT, "1783692202145-0", null))
                .isInstanceOf(ReplayRetentionException.class)
                .hasMessageContaining("md.tick.trade.X")
                .hasMessageContaining("does not exist");
    }

    @Test
    void earliestIsExemptEvenWhenEntriesHaveBeenTrimmed() {
        // "oldest retained" is what earliest() means, so the trim floor never rejects it.
        assertThat(RedisReplay.includesAtStart("signals", EARLIEST, "0-0", xinfo("501-0")))
                .isTrue();
    }

    @Test
    void timestampedStartAboveTheOldestRetainedIsAdmitted() {
        assertThat(RedisReplay.includesAtStart("signals", AT, "600-0", xinfo("501-0")))
                .isTrue();
    }

    @Test
    void timestampedStartExactlyOnTheOldestRetainedIsAdmitted() {
        // The oldest retained id is a surviving entry, so a start equal to it is in-range (inclusive).
        assertThat(RedisReplay.includesAtStart("signals", AT, "501-0", xinfo("501-0")))
                .isTrue();
    }

    @Test
    void timestampedStartBelowTheOldestRetainedFailsLoudly() {
        assertThatThrownBy(() -> RedisReplay.includesAtStart("signals", AT, "400-0", xinfo("501-0")))
                .isInstanceOf(ReplayRetentionException.class)
                .hasMessageContaining("signals")
                .hasMessageContaining("400-0") // requested start
                .hasMessageContaining("501-0"); // oldest retained id
    }

    @Test
    void timestampedStartOnAnEmptyStreamFailsLoudly() {
        assertThatThrownBy(() -> RedisReplay.includesAtStart("signals", AT, "1-0", emptyStreamXinfo()))
                .isInstanceOf(ReplayRetentionException.class)
                .hasMessageContaining("empty");
    }
}
