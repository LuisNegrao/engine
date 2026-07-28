package engine.bus.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The harness's single rendering path: {@link #render()} produces the exact text that goes to stdout
 * and — under {@code --write-baseline} — to {@code docs/baselines/bus-e2e-<profile>.md}. Byte-identical
 * output means the committed baseline is literally what the console showed, with nothing to reconcile.
 *
 * <p>Sections are appended in run order (header, then results as later steps produce them); the report
 * owns no measurement logic beyond the percentile/verdict arithmetic it is handed.
 */
public final class BenchReport {

    /** The gate the story fixes: per-group p99 publish→handler must stay under this. */
    static final Duration P99_BUDGET = Duration.ofMillis(50);

    /**
     * How close to the target rate counts as sustaining it. The generator schedules exactly {@code
     * rate × duration} events and the wall clock includes the in-flight drain, so a flawless run lands
     * fractionally <em>under</em> target and can never exceed it — a bare {@code >= rate} would fail
     * every green run. A publisher that genuinely cannot keep up blocks on the in-flight window and
     * misses by far more than 1 %, so this tolerance costs the gate no teeth.
     */
    private static final double THROUGHPUT_TOLERANCE = 0.99;

    private final BenchConfig config;
    private final String redisVersion;
    private final Instant startedAt;
    private final List<String> sections = new ArrayList<>();

    public BenchReport(BenchConfig config, String redisVersion, Instant startedAt) {
        this.config = config;
        this.redisVersion = redisVersion;
        this.startedAt = startedAt;
    }

    /** Appends a rendered section verbatim; sections print in the order they were added. */
    public void addSection(String section) {
        sections.add(section);
    }

    /** The publish side: what the generator actually put on the bus during the measured window. */
    public void addGenerator(TickGenerator.Result result) {
        StringBuilder section = new StringBuilder();
        section.append(String.format("-------- generator --------%n"));
        section.append(String.format("scheduled     : %,d events over %s%n", result.scheduled(), config.duration()));
        section.append(String.format("published     : %,d%n", result.published()));
        section.append(String.format("failed        : %,d%n", result.failed()));
        if (result.failed() > 0) {
            section.append(String.format("first failure : %s%n", result.firstFailure()));
        }
        section.append(String.format("wall          : %.3f s%n", result.wallSeconds()));
        section.append(String.format(
                "throughput    : %,.0f events/s achieved (%.2f%% of target)%n",
                result.eventsPerSecond(), 100.0 * result.eventsPerSecond() / config.rate()));
        section.append("=================================================");
        sections.add(section.toString());
    }

    /**
     * The delivery side, one block per consumer group. Fan-out isolation is read off this table:
     * every group should show the same delivered count, 0 redeliveries, 0 final lag, and its own p99
     * inside the budget.
     */
    public void addGroups(List<GroupProbe.Result> results) {
        StringBuilder section = new StringBuilder();
        section.append(String.format("-------- consumer groups --------%n"));
        for (GroupProbe.Result result : results) {
            section.append(String.format("group         : %s%n", result.group()));
            section.append(String.format("  delivered   : %,d%n", result.delivered()));
            section.append(String.format("  redeliveries: %,d%n", result.redeliveries()));
            section.append(String.format("  throughput  : %,.0f events/s%n", result.eventsPerSecond()));
            section.append(String.format(
                    "  latency     : p50 %d ms, p90 %d ms, p99 %d ms, max %d ms%n",
                    result.p50Millis(), result.p90Millis(), result.p99Millis(), result.maxMillis()));
            section.append(String.format("  lag         : max %,d, final %,d%n", result.maxLag(), result.finalLag()));
        }
        section.append("=================================================");
        sections.add(section.toString());
    }

    /**
     * The gate, evaluated over everything measured. Every violated clause is named — a bare {@code
     * FAIL} would send whoever runs this hunting through the tables for the reason.
     *
     * <p>Sustained throughput is judged on the <em>generator's</em> achieved rate, not on a group's
     * delivered-per-second: the latter's denominator includes the drain tail, so a perfectly healthy
     * run scores a few tenths of a percent under target. What the groups must prove is that they lost
     * nothing ({@code delivered >= published}) and stayed inside the latency budget.
     */
    public Verdict verdict(TickGenerator.Result generated, List<GroupProbe.Result> groups) {
        List<String> violations = new ArrayList<>();
        if (generated.eventsPerSecond() < config.rate() * THROUGHPUT_TOLERANCE) {
            violations.add(String.format(
                    "sustained throughput %,.0f events/s is below the %,d events/s target (tolerance %.0f%%)",
                    generated.eventsPerSecond(), config.rate(), THROUGHPUT_TOLERANCE * 100));
        }
        if (generated.failed() > 0) {
            violations.add(String.format("%,d publishes failed", generated.failed()));
        }
        for (GroupProbe.Result group : groups) {
            if (group.delivered() < generated.published()) {
                violations.add(String.format(
                        "%s lost events: delivered %,d of %,d published",
                        group.group(), group.delivered(), generated.published()));
            }
            if (group.p99Millis() >= P99_BUDGET.toMillis()) {
                violations.add(String.format(
                        "%s p99 %d ms is not under the %d ms budget",
                        group.group(), group.p99Millis(), P99_BUDGET.toMillis()));
            }
        }
        return new Verdict(violations);
    }

    /** Appends the verdict block; call last so it reads as the report's bottom line. */
    public void addVerdict(Verdict verdict) {
        StringBuilder section = new StringBuilder();
        section.append(String.format("-------- verdict --------%n"));
        section.append(String.format("result        : %s%n", verdict.passed() ? "PASS" : "FAIL"));
        for (String violation : verdict.violations()) {
            section.append(String.format("violation     : %s%n", violation));
        }
        section.append("=================================================");
        sections.add(section.toString());
    }

    public String render() {
        StringBuilder out = new StringBuilder(header());
        for (String section : sections) {
            out.append('\n').append(section);
        }
        return out.toString();
    }

    /**
     * Writes the report to this profile's baseline file, byte-identical to what stdout showed — the
     * committed baseline is then literally the console output, with nothing to reconcile.
     */
    public void writeBaseline() throws IOException {
        Path path = config.baselinePath();
        Files.createDirectories(path.toAbsolutePath().getParent());
        Files.writeString(path, render() + System.lineSeparator());
    }

    /** Pass/fail plus every clause that failed; empty violations is the only way to pass. */
    public record Verdict(List<String> violations) {

        public Verdict {
            violations = List.copyOf(violations);
        }

        public boolean passed() {
            return violations.isEmpty();
        }
    }

    private String header() {
        StringBuilder header = new StringBuilder();
        header.append(String.format("======== NEG-22 end-to-end bus bench (%s) ========%n", config.profile()));
        header.append(String.format(
                "machine       : %s %s, %d cores, java %s%n",
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(),
                System.getProperty("java.version")));
        header.append(
                String.format("heap max      : %,d MB%n", Runtime.getRuntime().maxMemory() / (1024 * 1024)));
        header.append(String.format("redis         : %s%n", redisVersion));
        header.append(String.format("started       : %s%n", startedAt));
        header.append(String.format(
                "instruments   : %d (%d streams, BENCH-01.ITEST ..)%n", config.instruments(), config.streams()));
        header.append(
                String.format("rate          : %,d events/s aggregate (80%% quote / 20%% trade)%n", config.rate()));
        header.append(String.format("groups        : %d (bench-strategy-1..%d)%n", config.groups(), config.groups()));
        header.append(String.format("publishers    : %d%n", config.publishers()));
        header.append(String.format("warmup        : %s (unmeasured)%n", config.warmup()));
        header.append(String.format(
                "duration      : %s (%,d scheduled events)%n", config.duration(), config.scheduledEvents()));
        header.append(String.format("memory sampling: %s%n", config.memorySampling() ? "on (15 s)" : "off"));
        header.append(String.format(
                "target        : >= %,d events/s sustained, per-group p99 < %d ms%n",
                config.rate(), P99_BUDGET.toMillis()));
        header.append("=================================================");
        return header.toString();
    }
}
