package engine.bus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publish-path instrumentation for {@link RedisStreamsEventPublisher}, always on (NEG-21).
 *
 * <p>Lives in the {@code engine.bus} root, beside {@code inFlight}, because it is instrumentation
 * <em>of the publisher</em> written on the hot path — moving it into {@code engine.bus.monitor} would
 * force the publisher to import monitoring code, inverting the one-way arrow the subpackage exists to
 * enforce. The {@link engine.bus.monitor.BusMonitor} takes this object and reads it each sweep; no
 * {@code core} interface grows.
 *
 * <p>Three things are measured:
 *
 * <ul>
 *   <li><b>{@code published}/{@code failed}</b> — interval counters ({@link LongAdder}), read and
 *       reset by {@link #drain()}. {@code failed > 0} is the memory-wall tripwire ADR 0002 §4
 *       promises NEG-21 will watch: a rejected, timed-out, or OOM {@code XADD} increments it.
 *   <li><b>{@code inFlight}</b> — a live gauge, not an interval counter: it is bounded by Lettuce's
 *       4096 request queue and a climbing value means Redis is slower than the publish rate. Sampled
 *       at sweep time, never drained.
 *   <li><b>per-source feed latency</b> — {@code ingestedAt − occurredAt} samples keyed by {@code
 *       source}, appended exact-and-uncapped (NEG-17 fidelity; ~24 KB/interval nominal) and swapped
 *       out by {@link #drain()} for the monitor to sort into exact percentiles.
 * </ul>
 *
 * <p>The latency swap is double-buffered under a short lock: a sample recorded mid-drain lands in the
 * next interval, never dropped, never counted twice. All methods are safe for concurrent callers.
 */
public final class PublisherStats {

    private final LongAdder published = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final AtomicLong inFlight = new AtomicLong();

    private final Object latencyLock = new Object();
    private Map<String, LongSamples> latencies = new HashMap<>();

    public void incrementPublished() {
        published.increment();
    }

    public void incrementFailed() {
        failed.increment();
    }

    public long incrementInFlight() {
        return inFlight.incrementAndGet();
    }

    public long decrementInFlight() {
        return inFlight.decrementAndGet();
    }

    /** The live in-flight gauge — read at sweep time, never reset. */
    public long inFlight() {
        return inFlight.get();
    }

    /** Appends one feed-latency sample for {@code source} to the current interval's buffer. */
    public void recordLatency(String source, long latencyMillis) {
        synchronized (latencyLock) {
            latencies.computeIfAbsent(source, k -> new LongSamples()).add(latencyMillis);
        }
    }

    /**
     * Atomically closes the current interval: swaps in a fresh latency buffer and read-resets the
     * counters, returning what accumulated since the previous drain. Samples recorded concurrently
     * with the swap belong to the next interval.
     */
    public Drain drain() {
        Map<String, long[]> samples;
        synchronized (latencyLock) {
            Map<String, LongSamples> swappedOut = latencies;
            latencies = new HashMap<>();
            samples = new HashMap<>(swappedOut.size());
            swappedOut.forEach((source, s) -> samples.put(source, s.toArray()));
        }
        return new Drain(published.sumThenReset(), failed.sumThenReset(), samples);
    }

    /**
     * One interval's drained counters plus the raw per-source latency samples (unsorted — the monitor
     * sorts and index-selects percentiles).
     */
    public record Drain(long published, long failed, Map<String, long[]> latencySamplesBySource) {
        public Drain {
            latencySamplesBySource = Map.copyOf(latencySamplesBySource);
        }
    }

    /** Growable primitive long buffer — no boxing on the hot path. */
    private static final class LongSamples {
        private long[] values = new long[16];
        private int size;

        void add(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        long[] toArray() {
            return Arrays.copyOf(values, size);
        }
    }
}
