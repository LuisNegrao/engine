package engine.bus.bench;

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

    public String render() {
        StringBuilder out = new StringBuilder(header());
        for (String section : sections) {
            out.append('\n').append(section);
        }
        return out.toString();
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
