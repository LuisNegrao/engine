package engine.bus.bench;

import engine.core.bus.EventPublisher;
import engine.core.bus.EventSelector;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Payload;
import engine.core.event.QuoteTick;
import engine.core.event.Side;
import engine.core.event.TradeTick;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * The load source: one paced publish loop over the bench instrument universe, through the <em>real</em>
 * {@link engine.bus.RedisStreamsEventPublisher} (real {@code StreamNames} routing, real {@code MAXLEN}
 * stamping) so the measured path is the production one.
 *
 * <p>Traffic shape is ADR 0002 §4's sizing universe: {@value #DEFAULT_VENUE}-suffixed instruments,
 * 80 % {@link QuoteTick} / 20 % {@link TradeTick}, fully-populated payloads, round-robin across
 * instruments — {@code instruments × 2} live streams. The mix is chosen by slot index, not randomly,
 * so two runs publish byte-identical traffic and their numbers are comparable.
 *
 * <p>Pacing is an <em>absolute</em> schedule ({@code start + i × paceNanos}): after a GC or scheduler
 * pause it bursts to catch up, which is the honest meaning of "sustained N/s". A relative sleep would
 * silently drift slow and report a rate it never had.
 */
public final class TickGenerator {

    /** Bench-only venue: keeps these streams disjoint from anything real (the {@code PublishBench} rule). */
    static final String DEFAULT_VENUE = "ITEST";

    static final String SOURCE = "bench-feed";

    /** Matches the publisher's Lettuce request-queue bound, so queue-full failures stay out of the numbers. */
    static final int IN_FLIGHT_WINDOW = 4096;

    /** One slot in five is a trade: the ADR's 4:1 quote/trade ratio. */
    private static final int MIX_PERIOD = 5;

    private final EventPublisher publisher;
    private final BenchConfig config;
    private final List<InstrumentId> instruments;
    private final QuoteTick[] quotes;
    private final TradeTick[] trades;

    public TickGenerator(EventPublisher publisher, BenchConfig config) {
        this.publisher = publisher;
        this.config = config;
        this.instruments = universe(config.instruments());
        this.quotes = new QuoteTick[instruments.size()];
        this.trades = new TradeTick[instruments.size()];
        for (int i = 0; i < instruments.size(); i++) {
            // Payloads are precomputed per instrument: the wire size (and so the load on Redis) is
            // what the bench is reproducing, and rebuilding identical BigDecimals per event would
            // only add generator garbage to a run that also judges memory flatness.
            quotes[i] = quoteFor(i);
            trades[i] = tradeFor(i);
        }
    }

    /** {@code BENCH-01.ITEST … BENCH-NN.ITEST} — the instrument universe, in publish order. */
    public static List<InstrumentId> universe(int count) {
        List<InstrumentId> universe = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            universe.add(new InstrumentId(String.format(Locale.ROOT, "BENCH-%02d", i + 1), DEFAULT_VENUE));
        }
        return List.copyOf(universe);
    }

    /**
     * The explicit (type, instrument) selectors covering every stream this generator writes — Redis
     * Streams has no pattern-subscribe, so consumers enumerate all {@code count × 2} of them.
     */
    public static List<EventSelector> selectors(List<InstrumentId> universe) {
        List<EventSelector> selectors = new ArrayList<>(universe.size() * 2);
        for (InstrumentId instrument : universe) {
            selectors.add(EventSelector.of(QuoteTick.class, instrument));
            selectors.add(EventSelector.of(TradeTick.class, instrument));
        }
        return List.copyOf(selectors);
    }

    /** The stream names this generator writes; used to reset bench state before a run. */
    public static List<String> streams(List<InstrumentId> universe) {
        List<String> streams = new ArrayList<>(universe.size() * 2);
        for (InstrumentId instrument : universe) {
            streams.add("md.tick.quote." + instrument);
            streams.add("md.tick.trade." + instrument);
        }
        return List.copyOf(streams);
    }

    public List<InstrumentId> instruments() {
        return instruments;
    }

    /**
     * Unmeasured pass at the target rate: JIT compilation and the Lettuce connection ramp both cost
     * far more than the per-event budget, and folding them into the measured window would understate
     * every number in the report (the {@code PublishBench} lesson).
     */
    public void warmup() throws InterruptedException {
        if (!config.warmup().isZero()) {
            execute(slotsFor(config.warmup()));
        }
    }

    /** The measured window: publishes {@link BenchConfig#scheduledEvents()} events on the schedule. */
    public Result measure() throws InterruptedException {
        return execute(config.scheduledEvents());
    }

    /**
     * Publishes {@code slots} events on the absolute schedule, sharded across {@code --publishers}
     * threads (thread {@code s} takes every {@code publishers}-th slot, so the instrument/mix pattern
     * is identical whatever the shard count), then waits for every in-flight publish to complete by
     * draining the whole in-flight window.
     */
    private Result execute(long slots) throws InterruptedException {
        Semaphore window = new Semaphore(IN_FLIGHT_WINDOW);
        AtomicLong published = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        double paceNanos = 1_000_000_000.0 / config.rate();
        long scheduleStart = System.nanoTime();

        List<Thread> shards = new ArrayList<>(config.publishers());
        for (int shard = 0; shard < config.publishers(); shard++) {
            long first = shard;
            Thread thread = new Thread(
                    () -> {
                        try {
                            for (long slot = first; slot < slots; slot += config.publishers()) {
                                awaitSlot(scheduleStart, slot, paceNanos);
                                window.acquire();
                                publisher.publish(eventFor(slot)).whenComplete((v, err) -> {
                                    if (err == null) {
                                        published.incrementAndGet();
                                    } else {
                                        failed.incrementAndGet();
                                        firstFailure.compareAndSet(null, err);
                                    }
                                    window.release();
                                });
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    },
                    "bench-generator-" + shard);
            shards.add(thread);
            thread.start();
        }
        for (Thread thread : shards) {
            thread.join();
        }

        // Draining the full window proves every completion callback has run, so the counters below
        // are final and the wall clock covers the whole publish path, not just its submission.
        window.acquire(IN_FLIGHT_WINDOW);
        long wallNanos = System.nanoTime() - scheduleStart;

        return new Result(slots, published.get(), failed.get(), firstFailure.get(), wallNanos);
    }

    /** Parks until this slot's absolute deadline; returns immediately when already behind (catch-up burst). */
    private static void awaitSlot(long scheduleStart, long slot, double paceNanos) {
        long target = scheduleStart + (long) (slot * paceNanos);
        long now;
        while ((now = System.nanoTime()) < target) {
            LockSupport.parkNanos(target - now);
        }
    }

    /**
     * The event for one slot. Instruments round-robin; the trade slots walk diagonally
     * ({@code (instrument + round) % 5 == 4}) so every instrument gets the same 20 % trade share
     * instead of four instruments carrying all the trades.
     */
    private Event eventFor(long slot) {
        int index = (int) (slot % instruments.size());
        long round = slot / instruments.size();
        boolean trade = (index + round) % MIX_PERIOD == MIX_PERIOD - 1;
        Payload payload = trade ? trades[index] : quotes[index];
        // occurredAt = now: the generator has no real source clock, and a fabricated occurredAt gap
        // would show up as NEG-21 feed latency, which is not what this harness measures.
        return Event.of(SOURCE, instruments.get(index), Instant.now(), payload);
    }

    private long slotsFor(Duration window) {
        return Math.round(config.rate() * (window.toNanos() / 1_000_000_000.0));
    }

    private static QuoteTick quoteFor(int index) {
        BigDecimal bid = new BigDecimal("67231.50").add(BigDecimal.valueOf(index));
        BigDecimal ask = bid.add(new BigDecimal("0.50"));
        return new QuoteTick(bid, new BigDecimal("1.2345"), ask, new BigDecimal("0.9876"));
    }

    private static TradeTick tradeFor(int index) {
        BigDecimal price = new BigDecimal("67231.75").add(BigDecimal.valueOf(index));
        Side aggressor = index % 2 == 0 ? Side.BUY : Side.SELL;
        return new TradeTick(price, new BigDecimal("0.0042"), aggressor);
    }

    /** One generator pass. {@code failed} counts publish futures that completed exceptionally. */
    public record Result(long scheduled, long published, long failed, Throwable firstFailure, long wallNanos) {

        public double eventsPerSecond() {
            return published / (wallNanos / 1_000_000_000.0);
        }

        public double wallSeconds() {
            return wallNanos / 1_000_000_000.0;
        }
    }
}
