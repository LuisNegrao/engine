package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PublisherStatsTest {

    @Test
    void drainReturnsTheIntervalCountersAndResetsThem() {
        PublisherStats stats = new PublisherStats();
        stats.incrementPublished();
        stats.incrementPublished();
        stats.incrementFailed();

        PublisherStats.Drain first = stats.drain();
        assertThat(first.published()).isEqualTo(2);
        assertThat(first.failed()).isEqualTo(1);

        // A second drain with nothing recorded in between sees zeroes — counters reset.
        PublisherStats.Drain second = stats.drain();
        assertThat(second.published()).isZero();
        assertThat(second.failed()).isZero();
    }

    @Test
    void drainSwapsOutTheLatencySamplesPerSource() {
        PublisherStats stats = new PublisherStats();
        stats.recordLatency("binance", 10);
        stats.recordLatency("binance", 30);
        stats.recordLatency("oms", 5);

        PublisherStats.Drain drain = stats.drain();
        assertThat(drain.latencySamplesBySource()).containsOnlyKeys("binance", "oms");
        assertThat(drain.latencySamplesBySource().get("binance")).containsExactly(10L, 30L);
        assertThat(drain.latencySamplesBySource().get("oms")).containsExactly(5L);

        // Samples belong to exactly one interval: the next drain is empty.
        assertThat(stats.drain().latencySamplesBySource()).isEmpty();
    }

    @Test
    void latencySamplesGrowBeyondTheInitialBufferCapacity() {
        PublisherStats stats = new PublisherStats();
        for (int i = 0; i < 100; i++) {
            stats.recordLatency("binance", i);
        }
        long[] samples = stats.drain().latencySamplesBySource().get("binance");
        assertThat(samples).hasSize(100);
        assertThat(samples[0]).isZero();
        assertThat(samples[99]).isEqualTo(99);
    }

    @Test
    void inFlightIsALiveGaugeNotResetByDrain() {
        PublisherStats stats = new PublisherStats();
        assertThat(stats.inFlight()).isZero();

        assertThat(stats.incrementInFlight()).isEqualTo(1);
        assertThat(stats.incrementInFlight()).isEqualTo(2);
        assertThat(stats.inFlight()).isEqualTo(2);

        stats.drain(); // a drain must not touch the gauge
        assertThat(stats.inFlight()).isEqualTo(2);

        assertThat(stats.decrementInFlight()).isEqualTo(1);
        assertThat(stats.inFlight()).isEqualTo(1);
    }

    @Test
    void drainedResultIsImmutable() {
        PublisherStats stats = new PublisherStats();
        stats.recordLatency("binance", 1);
        var samples = stats.drain().latencySamplesBySource();
        assertThatThrownBy(() -> samples.put("x", new long[0])).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void concurrentRecordingLosesNoSamples() throws Exception {
        PublisherStats stats = new PublisherStats();
        int threads = 8;
        int perThread = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);

        // A background drainer runs concurrently with recording; every drained sample is counted, so
        // a swap that dropped samples lands between copy and clear would show up as a short total.
        AtomicInteger drainedTotal = new AtomicInteger();
        Thread drainer = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                long[] s = stats.drain().latencySamplesBySource().get("binance");
                if (s != null) {
                    drainedTotal.addAndGet(s.length);
                }
            }
        });

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < perThread; i++) {
                    stats.recordLatency("binance", i);
                }
            });
        }
        drainer.start();
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        drainer.interrupt();
        drainer.join(TimeUnit.SECONDS.toMillis(5));

        // Final drain sweeps up anything recorded after the drainer's last pass.
        long[] tail = stats.drain().latencySamplesBySource().get("binance");
        int finalTotal = drainedTotal.get() + (tail == null ? 0 : tail.length);
        assertThat(finalTotal).isEqualTo(threads * perThread);
    }

    @Test
    void countersAreSafeUnderConcurrentIncrements() throws Exception {
        PublisherStats stats = new PublisherStats();
        int threads = 8;
        int perThread = 50_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < perThread; i++) {
                    stats.incrementPublished();
                    stats.incrementFailed();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        PublisherStats.Drain drain = stats.drain();
        assertThat(drain.published()).isEqualTo((long) threads * perThread);
        assertThat(drain.failed()).isEqualTo((long) threads * perThread);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
