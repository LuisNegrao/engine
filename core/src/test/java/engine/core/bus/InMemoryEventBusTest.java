package engine.core.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.event.Bar;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.QuoteTick;
import engine.core.event.Side;
import engine.core.event.TradeTick;
import engine.core.serde.SampleEvents;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The {@link InMemoryEventBus} fixture: selector matching, per-subscription full copies, in-place
 * retry to {@code deadLetters()}, reentrancy-safe publish-from-handler, and zero lag.
 */
class InMemoryEventBusTest {

    private static final InstrumentId ETH = InstrumentId.parse("ETH-USDT.BINANCE");
    private static final SubscribeOptions LATEST = SubscribeOptions.of(SubscribeOptions.StartPosition.LATEST);

    @Test
    void matchesByPayloadType() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> received = new ArrayList<>();
        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, received::add);

        bus.publish(SampleEvents.tradeTick());
        bus.publish(SampleEvents.quoteTick());

        assertThat(received).hasSize(1);
        assertThat(received.get(0).payload()).isInstanceOf(TradeTick.class);
    }

    @Test
    void matchesByInstrument() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> received = new ArrayList<>();
        bus.subscribe(List.of(EventSelector.of(TradeTick.class, SampleEvents.BTC)), LATEST, received::add);

        Event btc = SampleEvents.tradeTick();
        Event eth = SampleEvents.event(
                ETH, new TradeTick(new BigDecimal("3200.00"), new BigDecimal("1.0"), engine.core.event.Side.BUY));
        bus.publish(btc);
        bus.publish(eth);

        assertThat(received).containsExactly(btc);
    }

    @Test
    void matchesByBarInterval() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> received = new ArrayList<>();
        bus.subscribe(List.of(EventSelector.bars(Duration.ofMinutes(1), SampleEvents.BTC)), LATEST, received::add);

        Event oneMinute = SampleEvents.bar(); // 1m
        Event fiveMinute = SampleEvents.event(
                SampleEvents.BTC,
                new Bar(
                        Instant.parse("2026-07-10T14:00:00Z"),
                        Duration.ofMinutes(5),
                        new BigDecimal("67200.00"),
                        new BigDecimal("67250.50"),
                        new BigDecimal("67180.00"),
                        new BigDecimal("67231.50"),
                        new BigDecimal("1500.00")));
        bus.publish(oneMinute);
        bus.publish(fiveMinute);

        assertThat(received).containsExactly(oneMinute);
    }

    @Test
    void fullCopyToEverySubscription() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> a = new ArrayList<>();
        List<Event> b = new ArrayList<>();
        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, a::add);
        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, b::add);

        Event tick = SampleEvents.tradeTick();
        bus.publish(tick);

        assertThat(a).containsExactly(tick);
        assertThat(b).containsExactly(tick);
    }

    @Test
    void retriesThenDeadLetters() {
        InMemoryEventBus bus = new InMemoryEventBus();
        AtomicInteger attempts = new AtomicInteger();
        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, event -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("boom");
        });

        Event tick = SampleEvents.tradeTick();
        bus.publish(tick);

        assertThat(attempts).hasValue(InMemoryEventBus.MAX_ATTEMPTS);
        assertThat(bus.deadLetters()).hasSize(1);
        assertThat(bus.deadLetters().get(0).event()).isEqualTo(tick);
        assertThat(bus.deadLetters().get(0).failure())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }

    @Test
    void publishFromHandlerDoesNotRecurse() {
        InMemoryEventBus bus = new InMemoryEventBus();
        AtomicInteger depth = new AtomicInteger();
        AtomicInteger maxDepth = new AtomicInteger();
        AtomicInteger republished = new AtomicInteger();
        List<Event> handled = new ArrayList<>();

        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, event -> {
            int d = depth.incrementAndGet();
            maxDepth.accumulateAndGet(d, Math::max);
            handled.add(event);
            if (republished.getAndIncrement() == 0) {
                bus.publish(SampleEvents.tradeTick()); // re-enters publish from within the handler
            }
            depth.decrementAndGet();
        });

        bus.publish(SampleEvents.tradeTick());

        assertThat(handled).hasSize(2); // original + the one published from the handler
        assertThat(maxDepth).hasValue(1); // never nested: the second drained after the first returned
    }

    @Test
    void lagIsAlwaysZero() {
        InMemoryEventBus bus = new InMemoryEventBus();
        Subscription subscription = bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, event -> {});

        assertThat(subscription.lag()).isZero();
        bus.publish(SampleEvents.tradeTick());
        assertThat(subscription.lag()).isZero();
    }

    @Test
    void closedSubscriptionStopsReceiving() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> received = new ArrayList<>();
        Subscription subscription = bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, received::add);

        subscription.close();
        bus.publish(SampleEvents.tradeTick());

        assertThat(received).isEmpty();
    }

    @Test
    void roundTripsAsBothPublisherAndSubscriber() {
        // One object wired as a component's whole bus: it publishes and consumes its own event.
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> seen = new ArrayList<>();
        bus.subscribe(List.of(EventSelector.of(QuoteTick.class)), LATEST, seen::add);

        Event quote = SampleEvents.quoteTick();
        bus.publish(quote);

        assertThat(seen).containsExactly(quote);
    }

    @Test
    void replayDeliversRecordedInstances() {
        InMemoryEventBus bus = new InMemoryEventBus();
        Event a = SampleEvents.tradeTick();
        Event b = SampleEvents.tradeTick();
        bus.publish(a);
        bus.publish(b);

        List<Event> replayed = new ArrayList<>();
        Replay replay = bus.replay(
                List.of(EventSelector.of(TradeTick.class)), ReplayRange.from(ReplayPosition.earliest()), replayed::add);

        // Same instances, so same eventId — nothing is re-minted on replay.
        assertThat(replayed).containsExactly(a, b);
        assertThat(replayed.get(0).eventId()).isEqualTo(a.eventId());
        assertThat(replay.done().toCompletableFuture().join()).isEqualTo(2L);
    }

    @Test
    void replayNarrowsBySelector() {
        InMemoryEventBus bus = new InMemoryEventBus();
        bus.publish(SampleEvents.tradeTick());
        bus.publish(SampleEvents.quoteTick());
        bus.publish(SampleEvents.tradeTick());

        List<Event> replayed = new ArrayList<>();
        Replay replay = bus.replay(
                List.of(EventSelector.of(TradeTick.class)), ReplayRange.from(ReplayPosition.earliest()), replayed::add);

        assertThat(replayed).allSatisfy(e -> assertThat(e.payload()).isInstanceOf(TradeTick.class));
        assertThat(replayed).hasSize(2);
        assertThat(replay.done().toCompletableFuture().join()).isEqualTo(2L);
    }

    @Test
    void replayAtBoundsAreInclusiveOnIngestedAt() {
        InMemoryEventBus bus = new InMemoryEventBus();
        Event first = tickIngestedAt(Instant.parse("2026-07-10T10:00:00Z"));
        Event middle = tickIngestedAt(Instant.parse("2026-07-10T10:01:00Z"));
        Event last = tickIngestedAt(Instant.parse("2026-07-10T10:02:00Z"));
        bus.publish(first);
        bus.publish(middle);
        bus.publish(last);

        List<Event> single = new ArrayList<>();
        bus.replay(
                List.of(EventSelector.of(TradeTick.class)),
                ReplayRange.between(middle.ingestedAt(), middle.ingestedAt()),
                single::add);
        // Both edges land exactly on middle's ingestedAt: inclusive both ends.
        assertThat(single).containsExactly(middle);

        List<Event> fromMiddle = new ArrayList<>();
        bus.replay(List.of(EventSelector.of(TradeTick.class)), ReplayRange.from(middle.ingestedAt()), fromMiddle::add);
        // Inclusive start at middle, through the tail.
        assertThat(fromMiddle).containsExactly(middle, last);
    }

    @Test
    void replayHandlerThrowAbortsWithCause() {
        InMemoryEventBus bus = new InMemoryEventBus();
        bus.publish(SampleEvents.tradeTick());
        bus.publish(SampleEvents.tradeTick());
        bus.publish(SampleEvents.tradeTick());

        List<Event> delivered = new ArrayList<>();
        Replay replay = bus.replay(
                List.of(EventSelector.of(TradeTick.class)), ReplayRange.from(ReplayPosition.earliest()), event -> {
                    delivered.add(event);
                    if (delivered.size() == 2) {
                        throw new IllegalStateException("boom");
                    }
                });

        CompletableFuture<Long> done = replay.done().toCompletableFuture();
        assertThat(done).isCompletedExceptionally();
        assertThatThrownBy(done::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessage("boom");
        // Aborted on the throwing event; the third was never delivered — no retry, no dead-letter.
        assertThat(delivered).hasSize(2);
        assertThat(bus.deadLetters()).isEmpty();
    }

    @Test
    void componentReplaysItsOwnHistoryThroughTheSameHandler() {
        InMemoryEventBus bus = new InMemoryEventBus();
        List<Event> seen = new ArrayList<>();
        EventHandler handler = seen::add;
        bus.subscribe(List.of(EventSelector.of(TradeTick.class)), LATEST, handler);

        Event a = SampleEvents.tradeTick();
        Event b = SampleEvents.tradeTick();
        bus.publish(a);
        bus.publish(b);
        assertThat(seen).containsExactly(a, b); // live

        Replay replay = bus.replay(
                List.of(EventSelector.of(TradeTick.class)), ReplayRange.from(ReplayPosition.earliest()), handler);

        // Same handler re-driven over the recording: the last two are the replayed copies.
        assertThat(seen).containsExactly(a, b, a, b);
        assertThat(replay.done().toCompletableFuture().join()).isEqualTo(2L);
    }

    private static Event tickIngestedAt(Instant ingestedAt) {
        return new Event(
                UUID.randomUUID(),
                "test-feed",
                SampleEvents.BTC,
                SampleEvents.OCCURRED,
                ingestedAt,
                new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY));
    }
}
