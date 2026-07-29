package engine.bus.bench;

import engine.bus.RetentionPolicy;
import engine.core.event.InstrumentId;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.lang.management.MemoryPoolMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The soak profile's leak detector: a 15 s series of Redis {@code used_memory} and JVM heap-after-GC,
 * plus the end-of-run {@code XLEN} census that shows retention trimming actually engaged.
 *
 * <p>"Flat after warm-up" needs a number or it is a vibe. The series is cut into three equal-count
 * thirds; the <b>first third is discarded</b> — the retention window is still filling, so growth there
 * is the design working, not a leak — and the run passes when the <b>final third's median is at most
 * {@value #DRIFT_ALLOWANCE}× the middle third's median</b>. At the story's PT30M duration the discarded
 * third is exactly the first 10 minutes. Median-of-thirds is robust to the AOF-rewrite and GC spikes
 * that a max-based or regression test would false-positive on, while 10 % headroom still catches any
 * real leak — a leak compounds far past 10 % over the remaining 20 minutes.
 *
 * <p>JVM heap is read <em>after a forced collection</em> ({@link System#gc()}, then {@code totalMemory
 * − freeMemory}): coarse, but a leak check needs a trend, not precision, and the live set after a full
 * GC is the only heap number a leak cannot hide behind. The non-invasive alternative — summing the heap
 * pools' {@linkplain MemoryPoolMXBean#getCollectionUsage() collection usage} — was tried and rejected:
 * the bench's live set is three humongous {@code long[]} arrays that no young collection ever touches,
 * so the pools report single-digit MB and the flatness verdict degenerates into quantization noise.
 * The cost is 120 stop-the-world pauses per soak; they land in {@code max} latency, which is reported,
 * and the gate is on p99, which they are far too rare to move.
 *
 * <p>The sampler owns its own Redis connection: {@code INFO memory} and the {@code XLEN} census are
 * observation, and must never queue behind the publisher's or a subscriber's traffic.
 */
public final class SoakSampler implements AutoCloseable {

    /** Sampling cadence for both series. */
    static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(15);

    /** How much the final third's median may exceed the middle third's and still count as flat. */
    static final double DRIFT_ALLOWANCE = 1.10;

    /** One sample per third is the least that can be judged at all; below it the verdict is a violation. */
    private static final int MIN_SAMPLES = 3;

    private static final long BYTES_PER_MB = 1024 * 1024;

    private final BenchConfig config;
    private final List<InstrumentId> universe;
    private final RetentionPolicy retention;
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    /** Preallocated before the clock starts, like every other buffer here — see the class javadoc. */
    private final long[] redisBytes;

    private final long[] jvmBytes;

    private final Thread sampler;
    private volatile int samples;
    private volatile boolean sampling = true;

    public SoakSampler(String redisUri, BenchConfig config, List<InstrumentId> universe, RetentionPolicy retention) {
        this.config = config;
        this.universe = universe;
        this.retention = retention;
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        // The drain tail can outlast the measured window, so size for it rather than risk dropping
        // the samples that matter most — the last ones.
        int capacity = (int) (config.duration().toSeconds() / SAMPLE_INTERVAL.toSeconds()) + 16;
        this.redisBytes = new long[capacity];
        this.jvmBytes = new long[capacity];
        this.sampler = new Thread(this::sample, "bench-soak-sampler");
        this.sampler.setDaemon(true);
    }

    /** Starts sampling; call immediately before the measured window, with the first sample at t=0. */
    public void start() {
        sampler.start();
    }

    /**
     * Stops sampling, takes the {@code XLEN} census and judges both series. Call once, after the drain
     * and before {@link #close()} — the census needs the connection this class owns.
     */
    public Result result() {
        stopSampling();
        int n = samples;
        RedisCommands<String, String> commands = connection.sync();
        List<StreamCensus> census = List.of(
                census(commands, TickGenerator.quoteStreams(universe), TickGenerator.quoteShare()),
                census(commands, TickGenerator.tradeStreams(universe), TickGenerator.tradeShare()));
        return new Result(series("redis used_memory", redisBytes, n), series("jvm heap after gc", jvmBytes, n), census);
    }

    @Override
    public void close() {
        stopSampling();
        connection.close();
        client.shutdown();
    }

    private void stopSampling() {
        if (!sampling) {
            return;
        }
        sampling = false;
        sampler.interrupt();
        try {
            sampler.join(SAMPLE_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sample() {
        int index = 0;
        while (sampling && index < redisBytes.length) {
            try {
                redisBytes[index] = redisUsedMemory();
                jvmBytes[index] = heapAfterCollection();
                samples = ++index; // published after both writes, so a reader never sees a half sample
                Thread.sleep(SAMPLE_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Sampling is observation, never the thing under test: one failed read must not kill
                // the run. Stop the series rather than pollute it with a fabricated value.
                return;
            }
        }
    }

    /** {@code used_memory} from {@code INFO memory}: what Redis reports it is actually holding. */
    private long redisUsedMemory() {
        String info = connection.sync().info("memory");
        for (String line : info.split("\r?\n")) {
            if (line.startsWith("used_memory:")) {
                return Long.parseLong(line.substring("used_memory:".length()).trim());
            }
        }
        throw new IllegalStateException("INFO memory has no used_memory line");
    }

    /** Live heap after a forced full collection — the one heap number a leak cannot hide behind. */
    private static long heapAfterCollection() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Cuts a series into equal-count thirds and judges the last against the middle. The first third is
     * kept in the report for context but excluded from the verdict — it is the fill phase.
     */
    private static Series series(String name, long[] values, int count) {
        if (count < MIN_SAMPLES) {
            return new Series(name, count, List.of(), false, false, 0);
        }
        int firstCut = count / 3;
        int secondCut = 2 * count / 3;
        Third first = third("first", values, 0, firstCut);
        Third middle = third("middle", values, firstCut, secondCut);
        Third last = third("final", values, secondCut, count);
        // A zero middle median can only mean Redis reported nothing at all; treat any growth off zero
        // as unbounded rather than dividing by it.
        double ratio;
        if (middle.medianBytes() == 0) {
            ratio = last.medianBytes() == 0 ? 1.0 : Double.POSITIVE_INFINITY;
        } else {
            ratio = (double) last.medianBytes() / middle.medianBytes();
        }
        return new Series(name, count, List.of(first, middle, last), true, ratio <= DRIFT_ALLOWANCE, ratio);
    }

    private static Third third(String label, long[] values, int fromIndex, int toIndex) {
        long[] sorted = Arrays.copyOfRange(values, fromIndex, toIndex);
        Arrays.sort(sorted);
        return new Third(
                label, sorted.length, sorted[0], GroupProbe.percentile(sorted, 0.50), sorted[sorted.length - 1]);
    }

    /**
     * End-of-run {@code XLEN} across one stream class against what its retention window implies. This
     * is the direct evidence that trimming engaged: at 5 minutes and 400 quotes/s/stream the streams
     * should sit near 120k entries, not near the 720k a 30-minute untrimmed run would leave.
     */
    private StreamCensus census(RedisCommands<String, String> commands, List<String> streams, double share) {
        RetentionPolicy.Rule rule = retention.ruleFor(streams.get(0));
        // Everything the run published is still on the stream until the window outruns it — and what the
        // run published includes the warmup. A run shorter than warmup+duration can only hold what it
        // wrote, so expecting a full window's worth would flag "trimming failed" on every short run.
        Duration published = config.warmup().plus(config.duration());
        Duration effective = rule.window().compareTo(published) <= 0 ? rule.window() : published;
        double perStreamRate = config.rate() * share / config.instruments();
        long expected = Math.round(perStreamRate * (effective.toNanos() / 1_000_000_000.0));
        long[] lengths = streams.stream().mapToLong(commands::xlen).sorted().toArray();
        return new StreamCensus(
                rule.prefix() + "*",
                rule.window(),
                expected,
                lengths.length,
                lengths[0],
                GroupProbe.percentile(lengths, 0.50),
                lengths[lengths.length - 1]);
    }

    /** Everything the soak profile adds to the report, and the extra clauses it adds to the gate. */
    public record Result(Series redis, Series jvm, List<StreamCensus> census) {

        public Result {
            census = List.copyOf(census);
        }

        /** Empty when both series are flat; each entry names the series and by how much it drifted. */
        public List<String> violations() {
            List<String> violations = new ArrayList<>();
            for (Series series : List.of(redis, jvm)) {
                if (!series.judged()) {
                    violations.add(String.format(
                            "%s: %d memory samples is too few to judge flatness (need at least %d)",
                            series.name(), series.samples(), MIN_SAMPLES));
                } else if (!series.flat()) {
                    violations.add(String.format(
                            "%s grew after warm-up: final-third median is %.2fx the middle-third median (allowance %.2fx)",
                            series.name(), series.ratio(), DRIFT_ALLOWANCE));
                }
            }
            return violations;
        }
    }

    /**
     * One sampled series. {@code judged} is false when the run was too short to cut into thirds, in
     * which case {@code thirds} is empty and the missing verdict is itself a violation — a soak that
     * cannot judge flatness has not proved anything.
     */
    public record Series(String name, int samples, List<Third> thirds, boolean judged, boolean flat, double ratio) {

        public Series {
            thirds = List.copyOf(thirds);
        }
    }

    /** Min/median/max of one third of a series, in bytes. */
    public record Third(String label, int samples, long minBytes, long medianBytes, long maxBytes) {

        public long minMb() {
            return minBytes / BYTES_PER_MB;
        }

        public long medianMb() {
            return medianBytes / BYTES_PER_MB;
        }

        public long maxMb() {
            return maxBytes / BYTES_PER_MB;
        }
    }

    /** End-of-run entry counts for one stream class against its window-implied expectation. */
    public record StreamCensus(
            String prefix,
            Duration window,
            long expected,
            int streams,
            long minLength,
            long medianLength,
            long maxLength) {}
}
