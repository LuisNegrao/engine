package engine.bus.bench;

import engine.bus.RedisStreamsEventSubscriber;
import engine.bus.SubscriberTuning;
import engine.core.bus.EventSelector;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.bus.Subscription;
import engine.core.serde.EventCodec;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * One consumer group under measurement: a real {@link RedisStreamsEventSubscriber} reading every bench
 * stream under group {@code bench-strategy-N}, a recording handler, and a 1 Hz lag sampler.
 *
 * <p>Latency is <b>publish→handler</b>: handler receipt minus the envelope's {@code ingestedAt}, the
 * publish-side stamp. Generator and consumers share one JVM and therefore one clock, so the delta is a
 * real elapsed time and not a clock-skew artifact. {@code occurredAt} is deliberately not used — it is
 * source time, which the generator fabricates, and its gap is NEG-21's feed latency, a different metric.
 *
 * <p>The dispatch path allocates nothing: {@link System#currentTimeMillis()} (no {@code Instant}), a
 * write into a <em>preallocated</em> {@code long[]}, and two atomic increments. A sample buffer that
 * grew for 30 minutes would be indistinguishable from the leak the soak profile exists to catch.
 */
public final class GroupProbe implements AutoCloseable {

    /** How often the sampler reads {@link Subscription#lag()}. */
    private static final Duration LAG_SAMPLE_INTERVAL = Duration.ofSeconds(1);

    private final String group;
    private final RedisStreamsEventSubscriber subscriber;
    private final Subscription subscription;

    /** Preallocated before the clock starts; sized to the measured window's scheduled events. */
    private final long[] latenciesMillis;

    private final AtomicInteger sampleIndex = new AtomicInteger();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong maxLag = new AtomicLong();
    private final AtomicBoolean recording = new AtomicBoolean();
    private final Thread lagSampler;
    private volatile boolean sampling = true;
    private volatile long measureStartNanos;
    private volatile long measureWallNanos;

    private GroupProbe(
            String group, RedisStreamsEventSubscriber subscriber, long capacity, List<EventSelector> selectors) {
        if (capacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("measured window schedules " + capacity + " events, beyond one array");
        }
        this.group = group;
        this.subscriber = subscriber;
        this.latenciesMillis = new long[(int) capacity];
        // Subscribed here, in the constructor, so every probe is live before the generator's first
        // publish: a LATEST group created late silently misses the head and the delivered==published
        // accounting then fails for a reason that looks like data loss.
        this.subscription = subscriber.subscribe(selectors, SubscribeOptions.of(StartPosition.LATEST), event -> {
            if (!recording.get()) {
                return; // warmup traffic: delivered, acked, not measured
            }
            long deltaMillis = System.currentTimeMillis() - event.ingestedAt().toEpochMilli();
            int index = sampleIndex.getAndIncrement();
            if (index < latenciesMillis.length) {
                latenciesMillis[index] = deltaMillis;
            }
            delivered.incrementAndGet();
        });
        this.lagSampler = new Thread(this::sampleLag, "bench-lag-" + group);
        this.lagSampler.setDaemon(true);
        this.lagSampler.start();
    }

    /**
     * Builds and immediately subscribes {@code config.groups()} probes over every bench stream. Each
     * group gets its own subscriber (its own connection and poll thread) — that is what fan-out
     * isolation means here.
     */
    public static List<GroupProbe> createAll(
            String redisUri, EventCodec codec, BenchConfig config, List<EventSelector> selectors) {
        return java.util.stream.IntStream.rangeClosed(1, config.groups())
                .mapToObj(n -> {
                    String group = "bench-strategy-" + n;
                    RedisStreamsEventSubscriber subscriber = new RedisStreamsEventSubscriber(
                            redisUri, codec, group, group + "-consumer", SubscriberTuning.standard());
                    return new GroupProbe(group, subscriber, config.scheduledEvents(), selectors);
                })
                .toList();
    }

    /** Starts recording: called after the warmup has fully drained, immediately before the clock starts. */
    public void arm() {
        sampleIndex.set(0);
        delivered.set(0);
        maxLag.set(0);
        measureStartNanos = System.nanoTime();
        recording.set(true);
    }

    /** Stops recording and freezes the wall clock used for this group's delivered throughput. */
    public void disarm() {
        measureWallNanos = System.nanoTime() - measureStartNanos;
        recording.set(false);
    }

    public String group() {
        return group;
    }

    public long lag() {
        return subscription.lag();
    }

    /**
     * Blocks until every probe reports zero lag, so the tail of the run is measured rather than
     * truncated.
     *
     * @throws IllegalStateException on deadline, naming the stuck group and its residual lag — a hung
     *     harness that never reports is worse than a red one
     */
    public static void awaitDrain(List<GroupProbe> probes, Duration deadline) {
        long deadlineNanos = System.nanoTime() + deadline.toNanos();
        while (true) {
            GroupProbe behind =
                    probes.stream().filter(p -> p.lag() > 0).findFirst().orElse(null);
            if (behind == null) {
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new IllegalStateException("group " + behind.group() + " did not drain within " + deadline
                        + "; residual lag " + behind.lag());
            }
            // Poll no faster than the lag sampler: lag() costs 2 round trips per stream (80 for the
            // default universe), so a tight drain loop would spend more of the transport's round-trip
            // budget observing the backlog than the consumers get to drain it.
            LockSupport.parkNanos(LAG_SAMPLE_INTERVAL.toNanos());
        }
    }

    /** 1 Hz {@code lag()} sampling; only the max matters between samples, so no series is retained. */
    private void sampleLag() {
        while (sampling) {
            try {
                long lag = subscription.lag();
                maxLag.accumulateAndGet(lag, Math::max);
                Thread.sleep(LAG_SAMPLE_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // A lag read is observation, never the thing under test: never let it kill the run.
                return;
            }
        }
    }

    /** Snapshot of this group's measured window; {@code published} comes from the generator. */
    public Result result(long published) {
        int n = Math.min(sampleIndex.get(), latenciesMillis.length);
        long[] sorted = Arrays.copyOf(latenciesMillis, n);
        Arrays.sort(sorted);
        double wallSeconds = measureWallNanos / 1_000_000_000.0;
        return new Result(
                group,
                delivered.get(),
                delivered.get() - published,
                delivered.get() / wallSeconds,
                percentile(sorted, 0.50),
                percentile(sorted, 0.90),
                percentile(sorted, 0.99),
                n == 0 ? 0 : sorted[n - 1],
                maxLag.get(),
                subscription.lag());
    }

    /**
     * Exact percentile by nearest-rank on ascending-sorted samples: index {@code ceil(q·n) − 1}, the
     * same arithmetic {@code BusMonitor} uses. No interpolation, no bucketing (NEG-17 fidelity).
     */
    static long percentile(long[] sortedAscending, double quantile) {
        int n = sortedAscending.length;
        if (n == 0) {
            return 0;
        }
        int index = (int) Math.ceil(quantile * n) - 1;
        if (index < 0) {
            index = 0;
        } else if (index >= n) {
            index = n - 1;
        }
        return sortedAscending[index];
    }

    @Override
    public void close() {
        sampling = false;
        subscription.close();
        subscriber.close();
    }

    /**
     * One group's measured window. {@code redeliveries} is {@code delivered − published}: at-least-once
     * is the contract being certified, so duplicates are reported, never silently deduplicated.
     */
    public record Result(
            String group,
            long delivered,
            long redeliveries,
            double eventsPerSecond,
            long p50Millis,
            long p90Millis,
            long p99Millis,
            long maxMillis,
            long maxLag,
            long finalLag) {}
}
