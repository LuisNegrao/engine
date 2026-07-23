package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayRange;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The pure {@link ReplayPositions} mapping. Expected stream IDs are hardcoded literals — deriving
 * them through the same code under test would make the assertions tautologies unable to catch a
 * wrong mapping.
 */
class ReplayPositionsTest {

    // 2026-07-10T14:03:22.145Z — chosen so the epoch-millis literal below is unmistakably deliberate.
    private static final Instant T = Instant.parse("2026-07-10T14:03:22.145Z");
    private static final long T_MILLIS = 1_783_692_202_145L;

    @Test
    void tMillisLiteralMatchesTheInstant() {
        // Guards the hand-computed literal the mapping assertions rely on.
        assertThat(T.toEpochMilli()).isEqualTo(T_MILLIS);
    }

    @Test
    void startIdOfEarliestIsStreamOrigin() {
        assertThat(ReplayPositions.startId(ReplayPosition.earliest())).isEqualTo("0-0");
    }

    @Test
    void startIdOfAtIsMillisWithZeroSeq() {
        // Inclusive lower bound: seq 0 is the first entry that can exist in that millisecond.
        assertThat(ReplayPositions.startId(ReplayPosition.at(T))).isEqualTo("1783692202145-0");
    }

    @Test
    void startIdOfOffsetIsTheTokenVerbatim() {
        assertThat(ReplayPositions.startId(ReplayPosition.offset("1783692202145-7")))
                .isEqualTo("1783692202145-7");
    }

    @Test
    void endIdAbsentIsTheSampledTail() {
        assertThat(ReplayPositions.endId(null, "1783692202145-4")).isEqualTo("1783692202145-4");
    }

    @Test
    void endIdOfAtIsMillisWithNoSeq() {
        // Inclusive upper bound: no seq lets Redis expand to the max seq — the whole millisecond.
        assertThat(ReplayPositions.endId(ReplayPosition.at(T), "unused")).isEqualTo("1783692202145");
    }

    @Test
    void endIdOfOffsetIsTheTokenVerbatim() {
        assertThat(ReplayPositions.endId(ReplayPosition.offset("1783692202145"), "unused"))
                .isEqualTo("1783692202145");
    }

    @Test
    void offsetTokenMustBeAStreamIdShape() {
        assertThatThrownBy(() -> ReplayPositions.startId(ReplayPosition.offset("not-an-id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not-an-id");
        assertThatThrownBy(() -> ReplayPositions.startId(ReplayPosition.offset("12-34-56")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void barePlainMillisTokenIsAccepted() {
        assertThat(ReplayPositions.startId(ReplayPosition.offset("42"))).isEqualTo("42");
    }

    @Test
    void offsetRangeOverTwoStreamsIsRejected() {
        ReplayRange range = ReplayRange.from(ReplayPosition.offset("42"));
        assertThatThrownBy(() -> ReplayPositions.requireSingleStreamForOffset(
                        range, List.of("md.tick.trade.A", "md.tick.trade.B")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("single stream");
    }

    @Test
    void offsetRangeOverOneStreamIsAccepted() {
        ReplayRange range = ReplayRange.from(ReplayPosition.offset("42"));
        ReplayPositions.requireSingleStreamForOffset(range, List.of("md.tick.trade.A")); // no throw
    }

    @Test
    void nonOffsetRangeOverManyStreamsIsFine() {
        // earliest()/at() address every stream by ID, so the single-stream rule does not apply.
        ReplayRange range = ReplayRange.from(ReplayPosition.earliest());
        ReplayPositions.requireSingleStreamForOffset(range, List.of("md.tick.trade.A", "md.tick.trade.B"));
    }
}
