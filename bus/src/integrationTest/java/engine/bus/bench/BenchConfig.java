package engine.bus.bench;

import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;

/**
 * Parsed command line for {@link EndToEndBench}. The defaults <em>are</em> the smoke profile, so the
 * story's "one command" acceptance criterion is literally {@code ./gradlew :bus:e2eBench}.
 *
 * <p>Two profiles, not two programs: {@code --soak} is the same measurement run for 30 minutes with
 * memory sampling on top ({@link #memorySampling()}). Any drift between the smoke path and the soak
 * path would make the soak prove the wrong thing.
 *
 * <p>Unknown or malformed flags fail loudly — a typo'd flag that silently keeps the default would
 * publish a report labelled with a configuration the run never used.
 */
public record BenchConfig(
        int instruments,
        int rate,
        int groups,
        int publishers,
        Duration warmup,
        Duration duration,
        boolean soak,
        boolean writeBaseline) {

    static final int DEFAULT_INSTRUMENTS = 20;
    static final int DEFAULT_RATE = 10_000;
    static final int DEFAULT_GROUPS = 3;
    static final int DEFAULT_PUBLISHERS = 1;
    static final Duration DEFAULT_WARMUP = Duration.parse("PT30S");
    static final Duration SMOKE_DURATION = Duration.parse("PT2M");
    static final Duration SOAK_DURATION = Duration.parse("PT30M");

    public static final String USAGE =
            """
            usage: e2eBench [options]
              --instruments N     instruments to round-robin (default 20; each is 2 streams)
              --rate N            aggregate events/s across all instruments (default 10000)
              --groups N          consumer groups bench-strategy-1..N (default 3)
              --publishers N      generator threads sharding the schedule (default 1)
              --warmup ISO8601    unmeasured warmup window (default PT30S)
              --duration ISO8601  measured window (default PT2M smoke, PT30M soak)
              --soak              soak profile: PT30M and memory sampling
              --write-baseline    overwrite this profile's file under docs/baselines/
              --help              print this and exit""";

    public BenchConfig {
        requirePositive(instruments, "--instruments");
        requirePositive(rate, "--rate");
        requirePositive(groups, "--groups");
        requirePositive(publishers, "--publishers");
        if (warmup.isNegative()) {
            throw new IllegalArgumentException("--warmup must not be negative, got " + warmup);
        }
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("--duration must be positive, got " + duration);
        }
        if (publishers > rate) {
            throw new IllegalArgumentException(
                    "--publishers " + publishers + " exceeds --rate " + rate + "; each shard needs at least 1 event/s");
        }
    }

    /**
     * Parses {@code --args} into a config. {@code --duration} wins over the {@code --soak} default
     * regardless of flag order, so a short soak-shaped smoke test stays possible.
     */
    public static BenchConfig parse(String... args) {
        int instruments = DEFAULT_INSTRUMENTS;
        int rate = DEFAULT_RATE;
        int groups = DEFAULT_GROUPS;
        int publishers = DEFAULT_PUBLISHERS;
        Duration warmup = DEFAULT_WARMUP;
        Duration duration = null; // resolved after --soak is known
        boolean soak = false;
        boolean writeBaseline = false;

        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--soak" -> soak = true;
                case "--write-baseline" -> writeBaseline = true;
                case "--instruments" -> instruments = intValue(flag, args, ++i);
                case "--rate" -> rate = intValue(flag, args, ++i);
                case "--groups" -> groups = intValue(flag, args, ++i);
                case "--publishers" -> publishers = intValue(flag, args, ++i);
                case "--warmup" -> warmup = durationValue(flag, args, ++i);
                case "--duration" -> duration = durationValue(flag, args, ++i);
                default -> throw new IllegalArgumentException("unknown flag: " + flag);
            }
        }

        if (duration == null) {
            duration = soak ? SOAK_DURATION : SMOKE_DURATION;
        }
        return new BenchConfig(instruments, rate, groups, publishers, warmup, duration, soak, writeBaseline);
    }

    /** True when {@code --help} was asked for; checked before {@link #parse} so it never fails on it. */
    public static boolean isHelp(String... args) {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    /** {@code smoke} or {@code soak} — names the baseline file and labels the report. */
    public String profile() {
        return soak ? "soak" : "smoke";
    }

    /**
     * Where {@code --write-baseline} writes this profile's report. One file per profile: each run
     * overwrites its own file wholesale and git history is the versioning. Relative to the repo root,
     * which the {@code :bus:e2eBench} task sets as its working directory.
     */
    public Path baselinePath() {
        return Path.of("docs", "baselines", "bus-e2e-" + profile() + ".md");
    }

    /** Redis/JVM memory series are only sampled (and gated on) in the soak profile. */
    public boolean memorySampling() {
        return soak;
    }

    /** Two streams per instrument: {@code md.tick.quote.*} and {@code md.tick.trade.*}. */
    public int streams() {
        return instruments * 2;
    }

    /** Events the measured window schedules — also the per-group sample-array size (preallocated). */
    public long scheduledEvents() {
        return Math.round(rate * (duration.toNanos() / 1_000_000_000.0));
    }

    private static int intValue(String flag, String[] args, int index) {
        String raw = value(flag, args, index);
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects an integer, got: " + raw);
        }
    }

    private static Duration durationValue(String flag, String[] args, int index) {
        String raw = value(flag, args, index);
        try {
            return Duration.parse(raw);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(flag + " expects an ISO-8601 duration (e.g. PT2M), got: " + raw);
        }
    }

    private static String value(String flag, String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " expects a value");
        }
        return args[index];
    }

    private static void requirePositive(int value, String flag) {
        if (value <= 0) {
            throw new IllegalArgumentException(flag + " must be positive, got " + value);
        }
    }
}
