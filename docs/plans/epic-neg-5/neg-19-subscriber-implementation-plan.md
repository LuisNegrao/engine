# NEG-19 — Subscriber Abstraction with Consumer Groups and At-Least-Once Delivery: Implementation Plan

Implementation plan for [NEG-19](https://linear.app/negraolu/issue/NEG-19/subscriber-abstraction-with-consumer-groups-and-at-least-once-delivery). The consuming half of the bus: an `EventSubscriber` contract in `core` (components subscribe by payload type and instrument, never by stream name) and a Redis Streams consumer-group implementation in `bus` with at-least-once delivery, crash recovery via the pending list, and `dlq.*` poison parking per ADR 0002 §3. This story gates every consumer in the system — the archiver, strategies, risk, OMS, and NEG-20/NEG-21 all read through this interface.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Interface shape | `interface EventSubscriber extends AutoCloseable { Subscription subscribe(List<EventSelector> selectors, SubscribeOptions options, EventHandler handler); }` in `engine.core.bus`. Group + consumer identity are constructor state of the implementation, not `subscribe` parameters | ADR 0002: one component = one group name reused across every stream it consumes — so the group belongs to the subscriber instance, exactly like the Redis URI belongs to the publisher instance. `subscribe` is wiring-time, not hot-path, so unlike `publish` it **throws synchronously** on bad input (unknown selector shape, missing instrument on a partitioned type): a mis-wired consumer should fail at startup, loudly, not deliver an error to a stage nobody reads. |
| Subscription addressing | `EventSelector` record in `core`: `(Class<? extends Payload> payloadType, InstrumentId instrumentId, Duration barInterval)`, nullable components with `maybeX()` views, factories `of(type)`, `of(type, instrument)`, `bars(interval, instrument)`. `bus` resolves selectors to streams via a new `StreamNames.streamFor(EventSelector)` | Components must express *what* they want in domain terms; only bus code names streams (modules.md litmus #3). Redis has no pattern-subscribe, so per ADR 0002 §1 the selector list is explicit — one selector per (type, instrument). Whether a type is instrument-partitioned is topology knowledge, so that validation lives in `bus` at subscribe time, not in the core record. |
| Handler contract | `@FunctionalInterface EventHandler { void handle(Event event) throws Exception; }` — normal return means handled (ack), any throw means failure (retry → DLQ). One dispatch thread per subscription; the handler may block — that *is* the backpressure | A returned-boolean or async-stage handler API just re-derives exceptions worse. Synchronous single-threaded dispatch gives the strongest cheap guarantee: per-stream order preserved, no handler ever runs concurrently with itself for a subscription. The Javadoc carries the acceptance-criteria contract prominently: **handlers must be idempotent** (at-least-once delivery; `eventId` is the dedup key), which thread calls, that blocking is allowed and what it does (stalls this subscription only). |
| Ack semantics | `XACK` after the handler returns, never before dispatch. Failed handler ⇒ no ack ⇒ entry stays in the PEL for redelivery | Ack-before-dispatch is at-most-once wearing at-least-once's clothes — a crash between ack and handler loses the event silently. Ack-after is the entire delivery guarantee; everything else in this design (PEL drain, claim sweep, DLQ) exists to serve it. |
| Crash recovery / resume | On start, drain own PEL first (`XREADGROUP` from id `0` — own pending only), then main loop on `>`. A periodic claim sweep (every 5 s) uses `XPENDING` (IDLE ≥ 30 s) + `XCLAIM` to take over entries from dead or stuck consumers | Same consumer name restarting ⇒ its unacked entries redeliver immediately from its own PEL (the kill-and-restart acceptance test). A *dead instance whose name never returns* is covered by the claim sweep. `XPENDING`+`XCLAIM` rather than `XAUTOCLAIM` because the poison decision needs the delivery count and `XAUTOCLAIM` hides it; `XCLAIM` without `JUSTID` both increments the counter and returns the entry body in one call. Consumer names must be **stable across restarts** (config, e.g. hostname) — a random name orphans crash leftovers until the sweep finds them 30 s later. |
| Poison policy | Delivery count ≥ 5 ⇒ park on `dlq.<stream>` and `XACK` the original; processing continues. Undecodable bytes park immediately — no retries. DLQ entry fields (frozen wire constants): `event` (original bytes), `stream`, `group`, `consumer`, `deliveries`, `error`, `failedAt`. DLQ `XADD` carries `MAXLEN ~ 100000` | 5 deliveries × 30 s claim backoff parks a poison message in ~2 min while healthy events flow around it (at-least-once explicitly permits out-of-order retry; the Javadoc says so). Retrying a decode failure is cargo-cult — the bytes will not improve. The failing *group* is an entry field, not part of the stream name, per ADR 0002 §3. The cap is runaway protection in the ADR §4 layer-2 sense: a poison flood must not eat the memory wall; NEG-21 alerts on `dlq.*` depth long before 100k. |
| Slow-consumer policy | `Subscription.lag()` — undelivered (`XINFO GROUPS` lag) + pending, summed across the subscription's streams. `LagPolicy` is a sealed interface: `ProcessAll` (default) or `SkipToLatest(long threshold)`. Skip = `XGROUP SETID $` + ack own pending, logged loud, counted. **Only `md.*` streams may skip** — a `SkipToLatest` subscription containing any non-market-data selector throws at `subscribe` | The issue asks for the decision: yes, market-data consumers may skip-to-latest — a strategy computing on 10-minute-old ticks is worse than one that gaps and resumes fresh. Order/fill/command consumers may **never** skip: a missed fill is a corrupted position, full stop. Enforcing at subscribe time makes the policy structural, not advisory. Emitting lag as `Metric` events on the bus is NEG-21's wiring; this story exposes the number. |
| New-group start position | `StartPosition.LATEST` (`$`) or `EARLIEST` (`0`) in `SubscribeOptions`, caller-chosen, no default | Genuinely bimodal: a strategy joining mid-day wants from-now; the archiver wants everything still in the window. Defaulting either way silently mis-serves the other half, so the caller must say. Applies only when the group is *created*; an existing group always resumes from its own state — no offset management by the caller, ever. |
| Connections & threading | One dedicated Lettuce connection + one daemon poll thread per subscription. Sync commands on that thread: `XREADGROUP GROUP g c COUNT 256 BLOCK 1000` across all the subscription's streams, then dispatch → ack → claim-sweep check → repeat. Subscriber-specific `ClientOptions` with command timeout 5 s (> BLOCK), pinned by test | A blocking `XREADGROUP` monopolizes its connection — sharing one with the publisher (or another subscription) would stall unrelated commands behind the block. The publisher's 1 s timeout pin is *wrong* for this connection (it would kill every blocking read mid-block); the subscriber gets its own pinned options. Single-threaded sync loop = no locks, and the dispatch thread in the Javadoc is simply "the subscription's thread". |
| In-memory implementation | `InMemoryEventBus` in `core` testFixtures, implementing **both** `EventPublisher` and `EventSubscriber`. Synchronous dispatch on `publish`; every subscription behaves as its own group (full copy); handler failure retries up to 5 then lands in an inspectable `deadLetters()` list. `InMemoryEventPublisher` stays for publish-only tests | The issue says "paired with the in-memory publisher" — one object wired into a component under test exercises its full publish/subscribe surface, including its poison handling, with zero Redis. It matches events against selectors directly (payload type + instrument + interval) — no stream names, because stream names don't exist in core. No load-sharing simulation: that is Redis group mechanics, tested in `bus`. |
| Wire coupling to NEG-18 | Read entry field `RedisStreamsEventPublisher.EVENT_FIELD`; decode via the injected `EventCodec` | The constant exists precisely so this story reads it instead of re-typing `"event"`. Unknown-eventType decode failures surface as poison (immediate DLQ), which is the right behavior for a version-skewed consumer: evidence parked, flow continues. |

## Package layout

```
core/src/main/java/engine/core/bus/
├── EventSubscriber.java             ← the interface; handler contract Javadoc lives here
├── EventHandler.java                ← @FunctionalInterface, throws Exception = failure
├── EventSelector.java               ← (payloadType, instrument?, barInterval?) + factories
├── SubscribeOptions.java            ← StartPosition enum + sealed LagPolicy
└── Subscription.java                ← close() + lag()

core/src/testFixtures/java/engine/core/bus/
└── InMemoryEventBus.java            ← implements EventPublisher + EventSubscriber

bus/src/main/java/engine/bus/
├── StreamNames.java                 ← + streamFor(EventSelector), dlqFor(stream)
├── SubscriberTuning.java            ← block/batch/claim/maxDeliveries knobs + standard()
├── RedisStreamsEventSubscriber.java ← group lifecycle, subscriptions
└── DeadLetter.java                  ← frozen DLQ field constants + park helper
```

## Step 1 — Core contracts

The five types above, no implementation. The `EventSubscriber` Javadoc is where the issue's "document this contract prominently" acceptance criterion is satisfied — it must state, explicitly:

1. **At-least-once, therefore idempotent handlers.** Any event may be delivered more than once (crash between handle and ack, claim races, publisher retries). Duplicates carry the same `eventId` — that is the dedup key, and deduplication is the handler's job.
2. **Threading.** Each subscription owns one dispatch thread; the handler is only ever invoked from it, never concurrently with itself. Events from one logical stream arrive in order; no ordering across streams (ADR 0002 §5) — and a *retried* event arrives out of order even within its stream.
3. **Blocking is allowed and is the backpressure.** A slow handler stalls only its own subscription; lag becomes observable via `Subscription.lag()` and the subscription's `LagPolicy` decides what happens next.
4. **Failure semantics.** A throwing handler triggers redelivery with backoff; an event that keeps failing is parked on a dead-letter stream with the error attached, acked, and never redelivered — processing continues past it.
5. **`subscribe` throws on invalid selectors** (wiring-time failure); `Subscription.close()` stops dispatch after the in-flight event completes, with a bounded wait; `EventSubscriber.close()` closes all subscriptions.

`EventSelector` validates in its compact constructor what core *can* know: `payloadType` non-null, `barInterval` only meaningful with `Bar` (non-`Bar` + interval throws). Nullable components get `maybeInstrumentId()` / `maybeBarInterval()` per the house rule. `SubscribeOptions.of(StartPosition)` defaults the lag policy to `ProcessAll` — there is deliberately no full-defaults factory, per the start-position decision.

**Verify:** `./gradlew :core:dependencies --configuration apiElements` still shows no Jackson, no Lettuce; `./gradlew build` green.

## Step 2 — testFixtures: `InMemoryEventBus`

Implements both interfaces. `publish` matches the event against every live subscription's selectors (type, then instrument if the selector has one, then interval for bars) and dispatches synchronously on the caller's thread through a per-subscription reentrancy-safe queue (a handler that publishes re-enqueues; the outer drain loop delivers — no recursive dispatch). Handler throw ⇒ immediate in-place retries up to 5, then the event goes to a public `deadLetters()` list with the exception attached. `failWith` is not duplicated here — publish-failure simulation stays `InMemoryEventPublisher`'s job.

Unit tests in core's own test suite: selector matching per payload type and instrument, full copy to two subscriptions, retry-then-dead-letter, publish-from-handler doesn't recurse, `lag()` returns 0 (delivery is synchronous — say so in its Javadoc).

**Verify:** `./gradlew build` green; a component-style test wires one `InMemoryEventBus` as both publisher and subscriber and sees its own event round-trip.

## Step 3 — `StreamNames` selector resolution + DLQ names

Two additions, same class, same style as `streamFor(Event)`:

- `static String streamFor(EventSelector selector)` — switch over `selector.payloadType()`: partitioned types require the selector's instrument (throw `IllegalArgumentException` naming the type if absent), `Bar` additionally requires the interval and maps it through the existing vocabulary; single-stream types ignore instrument and interval. This is where "which types are partitioned" lives — core never knows.
- `static String dlqFor(String stream)` — `"dlq." + stream`, per ADR 0002 §3.

Unit tests: every payload type resolves to the ADR string (hardcoded literals, not computed — same anti-tautology rule as NEG-18's routing test); `TradeTick` selector without instrument throws; `Bar` without interval throws; `dlqFor("orders.intents")` is `"dlq.orders.intents"`.

**Verify:** `./gradlew :bus:test` green.

## Step 4 — `SubscriberTuning` + `RedisStreamsEventSubscriber` happy path

`SubscriberTuning.standard()`: `block 1s, batchCount 256, claimInterval 5s, claimMinIdle 30s, maxDeliveries 5, dlqMaxlen 100_000` — a record so integration tests can shrink the claim timings to milliseconds.

Constructor: `(String redisUri, EventCodec codec, String group, String consumerName, SubscriberTuning tuning)`. Eager connect, fail fast — same startup contract as the publisher. Subscriber-specific options, pinned by a unit test exactly like the publisher's:

```java
static ClientOptions clientOptions() {
    return ClientOptions.builder()
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(5))) // MUST exceed BLOCK
            .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(1)).build())
            .autoReconnect(true)
            .build();
}
```

`subscribe(selectors, options, handler)`:

1. Resolve selectors → distinct stream list via Step 3 (throws here on bad selectors — wiring-time). Enforce the skip rule here too: `SkipToLatest` + any stream not starting with `md.` ⇒ `IllegalArgumentException`.
2. Per stream: `XGROUP CREATE <stream> <group> <$ or 0> MKSTREAM`; catch `BUSYGROUP` and continue — that *is* the idempotent creation the ADR consequences call for. `MKSTREAM` because subscribing legitimately precedes the first publish.
3. Open a dedicated connection, start daemon thread `bus-sub-<group>-<n>`, return the `Subscription` handle.

The loop body (sync commands throughout): startup PEL drain — `XREADGROUP … STREAMS <streams> 0 0 …` repeatedly until every stream returns empty, dispatching and acking each entry (these are this consumer's own crash leftovers); then the main loop — read with `>` and `BLOCK 1000`, for each entry decode via `codec`, invoke handler, `XACK` on normal return; on throw, log and *do not ack* (Step 5's machinery redelivers). Any loop-level `RedisException` is caught, logged, and retried after a short sleep — a transient Redis outage must cost iterations, not the subscription (autoReconnect brings the connection back; the same lesson as the trimmer's swallowed sweep).

**Verify:** `./gradlew :bus:test` green (options pin, selector/skip validation); integration smoke — subscribe, publish one event through the NEG-18 publisher, handler receives the decoded equal event, `XPENDING` shows zero after.

## Step 5 — Crash recovery and poison parking

The claim sweep, run on the poll thread every `claimInterval` between reads, per stream:

```java
for (PendingMessage p : commands.xpending(stream, group, idleAtLeast(tuning.claimMinIdle()), LIMIT)) {
    if (p.getRedeliveryCount() >= tuning.maxDeliveries()) {
        List<StreamMessage> claimed = commands.xclaim(stream, consumerArgs, minIdle, p.getId());
        // claimed empty or nil body ⇒ entry was trimmed while pending: XACK, count lost, log
        DeadLetter.park(commands, stream, group, consumerName, p, claimed, error);  // XADD dlq.* + XACK
    } else {
        // XCLAIM (no JUSTID): increments delivery count, returns the body — dispatch it now
    }
}
```

- Handler-failure retry and dead-instance takeover are deliberately **one mechanism**: an unacked entry — whether its consumer crashed or its handler threw — ages past `claimMinIdle` and gets claimed. `claimMinIdle` doubles as the retry backoff.
- `DeadLetter.park` writes the frozen fields from the decisions table (`error` = exception class + message + first frames, truncated to 2 KB) with `MAXLEN ~ dlqMaxlen`, then `XACK`s the original. Park and ack are two commands, not atomic: a crash between them re-parks on redelivery — a duplicate DLQ entry with the same `eventId`, which at-least-once already promised. Note it in `DeadLetter`'s Javadoc rather than pretending to fix it with a transaction nobody needs.
- Decode failures short-circuit: park immediately from the dispatch path, no retry cycle.
- The error recorded is the *last* failure — good enough for NEG-21's monitoring; keeping full failure history is a non-goal.

Unit-test the pure parts (threshold decision, error truncation, field set); behavior is integration-tested in Step 7.

**Verify:** `./gradlew :bus:test` green; integration — a handler that always throws sends its entry to `dlq.<stream>` with `deliveries == maxDeliveries` while a healthy event published after it is handled long before the poison parks.

## Step 6 — Lag and skip-to-latest

- `Subscription.lag()`: per stream, `XINFO GROUPS` lag (entries never delivered to the group) + `XPENDING` count (delivered, unacked), summed. `XINFO` lag can be **null** after trimming cuts into undelivered entries — treat null as "unknown", surface the pending part, and **never trigger a skip on unknown lag** (skipping on bad data is how order-of-magnitude mistakes happen; say so in a comment).
- `SkipToLatest(threshold)`: checked on the poll thread each claim interval. When a stream's lag exceeds the threshold: `XGROUP SETID <stream> <group> $`, then `XACK` this consumer's own pending on that stream (skipping forward while old entries sit in the PEL just re-delivers the past through the claim sweep — the skip must clear both). Log at WARN with stream, group, and skipped count; increment a counter exposed on the `Subscription`. The skip is group-wide by design — if the group is behind, the group skips; that is the documented semantic, not a bug.

**Verify:** integration — subscription with `SkipToLatest(1000)` whose handler blocks on a latch; publish 5,000 ticks; release the latch; the consumer resumes near the tail, the skip counter is > 0, and total handled is far below 5,000. Unit — `SkipToLatest` on an `orders.intents` selector throws at subscribe.

## Step 7 — Integration tests (the acceptance criteria)

In `bus/src/integrationTest` against the docker-compose Redis, reusing NEG-18's hygiene: test-only instruments (`TEST-A.ITEST`), track and `DEL` every stream (now including `dlq.*` streams and `XGROUP DESTROY` for groups) in `@AfterEach`, aggressive `SubscriberTuning` (block 100 ms, claimMinIdle 200 ms, claimInterval 100 ms) so nothing sleeps its way to a minute-long suite.

1. **Two groups, full copies** — groups `strategy-a` and `risk-manager` subscribe the same intents stream (`EARLIEST`); publish 200 `OrderIntent`s; each group receives all 200, in stream order, independently.
2. **Load sharing within a group** — two subscribers, same group, consumer names `instance-a`/`instance-b`, one tick stream; publish 500; the union of handled `eventId`s is exactly 500 with no overlap (Redis delivers each entry to one consumer), both instances handled > 0.
3. **Kill mid-stream, restart, no loss** — publish 300 intents; consumer processes ~100 then is killed via a package-private `killForTest()` (abandons the loop without acking the in-flight batch — a real `kill -9` stand-in); restart with the **same consumer name**; startup PEL drain plus resume delivers the rest. Assert: distinct handled `eventId`s == all 300 published (no gaps); handled count ≥ 300 with any surplus being repeats of the same `eventId`s (duplicates carry the same `eventId` — the acceptance wording, literally).
4. **Poison to DLQ** — handler throws for one chosen `eventId` among 100; assert the entry lands on `dlq.<stream>` with `deliveries == maxDeliveries`, decodable original bytes, non-empty `error`, correct `group`; `XPENDING` for the group is empty (original acked); the other 99 all handled.
5. **Resume, no manual offsets** — subscribe (`LATEST`), receive 50, close cleanly; publish 100 more while down; resubscribe same group: exactly the 100 missed arrive, nothing replayed, no position passed by the caller.
6. **Skip-to-latest** — Step 6's test.

**Verify:** `docker compose up -d && ./gradlew :bus:integrationTest` green.

## Step 8 — Land it

Branch `luismarcosnegrao/neg-19-subscriber-abstraction-with-consumer-groups-and-at-least` (Linear's suggested name), one commit per step.

1. `git checkout main && git pull && git checkout -b <branch>`
2. Before each commit: `./gradlew spotlessApply build`
3. Full sweep before the PR: `docker compose up -d && ./gradlew build :bus:integrationTest`
4. Update `docs/modules.md`: the `bus` class table's "Subscriber, consumer groups, DLQ parking (NEG-19)" row becomes the concrete classes; core's package table gains the subscriber contract types.
5. PR body: the handler-contract Javadoc pasted verbatim (it is an acceptance criterion — make it reviewable without opening the diff), and the kill/restart test output.

## Definition of done (mapped to the issue)

- [ ] Two independent consumer groups each receive a full copy of a published stream → Step 7.1.
- [ ] Kill a consumer mid-stream, restart it: no events lost, duplicates carry the same `eventId` (integration test) → Steps 5 + 7.3, resume semantics 7.5.
- [ ] Poison message on the dead-letter stream after N attempts; healthy events continue flowing → Steps 5 + 7.4.
- [ ] Handler contract (idempotency, threading, blocking) documented in the interface → Step 1 Javadoc, pasted into the PR (Step 8.5).
- [ ] Slow-consumer policy defined and enforced (issue scope bullet) → Step 6: lag exposed, `md.*`-only skip enforced at subscribe.
- [ ] In-memory implementation paired with the in-memory publisher → Step 2.

## Pitfalls to expect

- **Lettuce's command timeout kills blocking reads.** The publisher's pinned 1 s timeout would fail every `XREADGROUP BLOCK 1000` from the inside. The subscriber's own options (timeout 5 s > BLOCK 1 s) exist for exactly this; the pin test keeps a refactor from "unifying" the two option sets and breaking every read.
- **`>` and `0` are different reads.** `XREADGROUP … >` delivers only never-delivered entries; id `0` delivers only this consumer's own PEL. Skip the startup `0`-drain and crash leftovers sit invisible until the claim sweep — the restart test passes slowly and flakily instead of failing loudly. Drain first, always.
- **Ack placement is the whole guarantee.** `XACK` before the handler returns turns the design into at-most-once and no test fails immediately — it only shows up as a lost event after a crash. The kill/restart test (7.3) is the regression guard; never weaken it to "approximately all".
- **`XAUTOCLAIM` hides the delivery count.** It doesn't return redelivery counters, so a poison threshold built on it guesses. `XPENDING` (counts) + `XCLAIM` (claim + body, counter incremented) is two commands but the decision is exact. And never `JUSTID` on the claim — it skips the counter increment, making every poison message immortal.
- **Trimmed-while-pending entries.** The retention sweeper can trim an entry that is still in a PEL; claiming it returns nothing. Without an explicit `XACK`-and-log path those PEL entries live forever and the claim sweep re-chews them every interval. Count them as lost — NEG-21 will want that number.
- **`claimMinIdle` races long handlers.** An entry being processed for longer than min-idle looks abandoned and gets claimed by a peer ⇒ concurrent duplicate processing. 30 s default buys headroom; the real defense is the documented idempotency contract, not tuning. Say in the Javadoc: handlers slower than min-idle *will* see duplicates.
- **Skip-to-latest must clear the PEL too.** `XGROUP SETID $` alone moves only the delivery cursor; skipped-but-pending entries come back through the claim sweep — a "skip" that replays the past piecemeal. Skip = SETID + ack own pending, one operation conceptually.
- **A throwing handler must not kill the poll loop.** Same failure shape as `scheduleWithFixedDelay` in the trimmer: one escaped exception silently ends consumption while the process looks healthy. Handler calls are individually try/caught; loop-level Redis errors sleep-and-retry. The subscription dying quietly is the one behavior this module may never exhibit.
