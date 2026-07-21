package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Step 5 acceptance: a {@link StreamTrimmer} sweep drops entries older than the rule window, keeps
 * in-window entries, and never touches {@code dlq.*} / {@code replay.*}.
 *
 * <p>Requires the docker-compose Redis: {@code docker compose up -d}.
 *
 * <p>Aging a stream is just writing explicit IDs: stream IDs are millisecond timestamps, so an
 * entry XADDed with id {@code <now − 13h>-0} <em>is</em> 13 hours old. Explicit IDs must arrive in
 * ascending order, so the aged entries are written first.
 */
class StreamTrimmerIntegrationTest {

    private static final String TRADE_STREAM = "md.tick.trade.TEST-A.ITEST";
    private static final String DLQ_STREAM = "dlq.ITEST";
    private static final String REPLAY_STREAM = "replay.ITEST";

    /**
     * Approximate MINID trimming evicts whole radix-tree nodes only — it never splits one. An
     * entry larger than {@code stream-node-max-bytes} (default 4096) gets a node to itself, so
     * with 8 KiB entries every aged entry is individually reclaimable and "old gone, fresh
     * intact" can be asserted exactly instead of within a node-sized tolerance.
     */
    private static final Map<String, byte[]> OVERSIZED_BODY = Map.of("event", new byte[8192]);

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> commands;

    @BeforeEach
    void connect() {
        client = RedisClient.create("redis://localhost:6379");
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        commands = connection.sync();
    }

    @AfterEach
    void cleanUp() {
        commands.del(TRADE_STREAM, DLQ_STREAM, REPLAY_STREAM);
        connection.close();
        client.shutdown();
    }

    @Test
    void sweepDropsAgedEntriesKeepsFreshOnesAndSparesDlqAndReplay() {
        Instant now = Instant.now();
        long agedMillis = now.minus(Duration.ofHours(13)).toEpochMilli(); // outside the 12 h window
        long freshMillis = now.minus(Duration.ofHours(1)).toEpochMilli(); // inside it

        for (int seq = 0; seq < 20; seq++) {
            commands.xadd(TRADE_STREAM, new XAddArgs().id(agedMillis + "-" + seq), OVERSIZED_BODY);
        }
        for (int seq = 0; seq < 5; seq++) {
            commands.xadd(TRADE_STREAM, new XAddArgs().id(freshMillis + "-" + seq), OVERSIZED_BODY);
        }
        commands.xadd(DLQ_STREAM, new XAddArgs().id(agedMillis + "-0"), OVERSIZED_BODY);
        commands.xadd(REPLAY_STREAM, new XAddArgs().id(agedMillis + "-0"), OVERSIZED_BODY);

        try (StreamTrimmer trimmer =
                new StreamTrimmer(connection, RetentionPolicy.standard(), Clock.fixed(now, ZoneOffset.UTC))) {
            trimmer.runOnce();
        }

        var remaining = commands.xrange(TRADE_STREAM, Range.<String>unbounded());
        assertThat(remaining).hasSize(5);
        assertThat(remaining).allSatisfy(entry -> assertThat(entry.getId()).startsWith(freshMillis + "-"));

        // dlq.* and replay.* carry no retention rule prefix, so a full sweep must leave them alone.
        assertThat(commands.xlen(DLQ_STREAM)).isEqualTo(1);
        assertThat(commands.xlen(REPLAY_STREAM)).isEqualTo(1);
    }
}
