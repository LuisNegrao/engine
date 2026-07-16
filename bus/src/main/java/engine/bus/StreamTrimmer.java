package engine.bus;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The age-based retention sweeper of ADR 0002 §4: every 60 s, {@code XTRIM <stream> MINID ~ <now −
 * window>} for each {@link RetentionPolicy} rule. Stream entry IDs are millisecond timestamps, so
 * the epoch-milli cutoff <em>is</em> the age comparison — one cheap command per stream, no entry
 * inspection.
 *
 * <p>Exact-name rules trim unconditionally ({@code XTRIM} on a missing key returns 0); class-prefix
 * rules discover their streams with a {@code SCAN} cursor loop, which ADR 0002 permits for tooling
 * (it forbids it only for production consumers). Because the rule prefixes are the concrete stream
 * classes, {@code dlq.*} and {@code replay.*} are unreachable by construction.
 *
 * <p>The trimmer <em>borrows</em> its connection: the wiring code that constructs publisher and
 * trimmer together owns both lifecycles, and {@link #close()} stops only the scheduler.
 */
public final class StreamTrimmer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(StreamTrimmer.class.getName());
    private static final long SWEEP_INTERVAL_SECONDS = 60;
    private static final long SCAN_BATCH = 500;

    private final RedisCommands<String, byte[]> commands;
    private final RetentionPolicy retention;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean();

    public StreamTrimmer(StatefulRedisConnection<String, byte[]> connection, RetentionPolicy retention, Clock clock) {
        this.commands = Objects.requireNonNull(connection, "connection must not be null")
                .sync();
        this.retention = Objects.requireNonNull(retention, "retention must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "stream-trimmer");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts the 60 s sweep schedule; the first sweep runs immediately. */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("trimmer already started");
        }
        scheduler.scheduleWithFixedDelay(this::sweepSafely, 0, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void sweepSafely() {
        // scheduleWithFixedDelay silently cancels the schedule if a run escapes with an
        // exception — a transient Redis hiccup must cost one sweep, not all future ones.
        try {
            runOnce();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "trim sweep failed; retrying next interval", e);
        }
    }

    /** One full sweep over every retention rule; package-private so tests drive it directly. */
    void runOnce() {
        for (RetentionPolicy.Rule rule : retention.rules()) {
            String minId = String.valueOf(clock.instant().minus(rule.window()).toEpochMilli());
            XTrimArgs args = XTrimArgs.Builder.minId(minId).approximateTrimming();
            if (rule.prefix().endsWith(".")) {
                trimMatching(rule.prefix(), args);
            } else {
                commands.xtrim(rule.prefix(), args);
            }
        }
    }

    private void trimMatching(String prefix, XTrimArgs args) {
        KeyScanArgs scanArgs = KeyScanArgs.Builder.matches(prefix + "*").limit(SCAN_BATCH);
        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<String> page = commands.scan(cursor, scanArgs);
            for (String stream : page.getKeys()) {
                commands.xtrim(stream, args);
            }
            cursor = page;
        } while (!cursor.isFinished());
    }

    /** Stops the scheduler; the borrowed connection stays open for its owner. */
    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
