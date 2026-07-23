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

    /** An XINFO map with a trim floor at {@code max-deleted-entry-id} and an oldest surviving entry. */
    private static Map<String, Object> xinfo(String maxDeleted, String firstEntryId) {
        return Map.of("max-deleted-entry-id", maxDeleted, "first-entry", List.of(firstEntryId, List.of()));
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
        // "oldest retained" is what earliest() means, so a trim floor never rejects it.
        assertThat(RedisReplay.includesAtStart("signals", EARLIEST, "0-0", xinfo("500-0", "501-0")))
                .isTrue();
    }

    @Test
    void timestampedStartAboveTheTrimFloorIsAdmitted() {
        assertThat(RedisReplay.includesAtStart("signals", AT, "600-0", xinfo("500-0", "501-0")))
                .isTrue();
    }

    @Test
    void timestampedStartBelowTheTrimFloorFailsLoudly() {
        assertThatThrownBy(() -> RedisReplay.includesAtStart("signals", AT, "400-0", xinfo("500-0", "501-0")))
                .isInstanceOf(ReplayRetentionException.class)
                .hasMessageContaining("signals")
                .hasMessageContaining("400-0") // requested start
                .hasMessageContaining("500-0") // trim floor
                .hasMessageContaining("501-0"); // oldest surviving id
    }

    @Test
    void startExactlyOnTheTrimFloorFailsLoudly() {
        // The floor id is the highest id already deleted, so a start equal to it is gone too (≤).
        assertThatThrownBy(() -> RedisReplay.includesAtStart("signals", AT, "500-0", xinfo("500-0", "501-0")))
                .isInstanceOf(ReplayRetentionException.class);
    }

    @Test
    void anUntrimmedStreamAdmitsAnyStart() {
        // max-deleted-entry-id "0-0" means nothing has ever been trimmed.
        assertThat(RedisReplay.includesAtStart("signals", AT, "1-0", xinfo("0-0", "1-0")))
                .isTrue();
    }
}
