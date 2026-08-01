# NEG-18 — Publisher Abstraction over Redis Streams: Implementation Plan

Implementation plan for [NEG-18](https://linear.app/negraolu/issue/NEG-18/publisher-abstraction-over-redis-streams). First production code in `bus`; it turns ADR 0002 §3–4 into classes: the stream table becomes `StreamNames`, the retention table becomes `RetentionPolicy` + the `MAXLEN ~` caps + the trimmer task. The `EventPublisher` interface lands in `core` — components depend on it, never on Redis; that is the NATS/Kafka door the issue exists to keep open.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Interface shape | `interface EventPublisher extends AutoCloseable { CompletionStage<Void> publish(Event event); }` in new package `engine.core.bus` | Async is non-negotiable on the tick path (see the pipelining pitfall). `CompletionStage`, not `CompletableFuture`: callers get completion/failure signals but no `complete()` handle. Tick producers may ignore the stage; anything publishing an `OrderIntent` must observe it. |
| Return type | `Void`, not the Redis entry ID | Entry IDs are transport-specific; returning one would leak Redis through the abstraction the issue exists to build. Nothing upstream has a use for it — correlation is `eventId`. |
| Redis-down semantics | **Fail fast, no buffering.** Down at startup ⇒ constructor throws. Down mid-run ⇒ `publish` returns an already-failed stage while Lettuce auto-reconnects; unresponsive ⇒ command timeout fails the stage. Never accepted-and-queued. | A tick delivered late is worthless; an order intent delivered late is dangerous — it executes without the market context that produced it. Same principle as ADR 0002's memory wall: the bus fails loud. The caller (feed handler drops ticks, strategy runner halts on failed intents) owns the response — policy lives above the transport. Documented as the *contract* in `EventPublisher` Javadoc, so any future implementation inherits it. |
| Bounded in-flight window | Lettuce `requestQueueSize` = 4096, `disconnectedBehavior = REJECT_COMMANDS`, `TimeoutOptions` enabled, command timeout 1 s, `autoReconnect` on | This *is* the issue's "backpressure-aware buffering": the only buffer is the bounded in-flight pipeline; when full, `publish` fails immediately. 4096 ≈ 2 s of ADR burst rate (~2k events/s). Reconnecting is fine — accepting commands while disconnected is not (see pitfall #1). |
| Stream entry shape | One field: `event` → `EventCodec.encode()` bytes, via `RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)` | The codec stays the single wire authority; `EnvelopeView` already gives infra envelope-only reads from those bytes, so exploding envelope fields into entry fields would duplicate the schema in a second place. Field name is a frozen wire constant — NEG-19 and the archiver read it. |
| Stream routing | `StreamNames` class in `bus`: static table keyed by payload class, mirroring ADR 0002 §3 exactly | ADR: "never derive stream names by string-mangling eventTypes at runtime." Instrument-scoped types (`TradeTick`, `QuoteTick`, `Bar`) throw if `instrumentId` is null; `Bar` maps its `Duration` through the fixed interval vocabulary (`1m`,`5m`,`15m`,`1h`,`4h`,`1d`) and throws on anything else. It lives in `bus`, not `core`: only bus code (publisher, NEG-19 subscriber, trimmer) may name streams — that's the point of the abstraction. |
| Trimmer task | `StreamTrimmer` in `bus`, scheduled every 60 s: `XTRIM <stream> MINID ~ <now − window>` per stream class; discovers streams by `SCAN` on the class prefixes; skips `dlq.*` and `replay.*` | ADR 0002 §4 assigns it to this story. `SCAN` is explicitly allowed for tooling (forbidden only for production *consumers*). `dlq.*` is NEG-21's evidence; `replay.*` is NEG-20's namespace — the trimmer touches neither. |
| In-memory publisher | `InMemoryEventPublisher` in `core` **testFixtures** (new `java-test-fixtures` plugin), alongside `SampleEvents` moved there from core's tests | Other modules' unit tests get it via `testImplementation(testFixtures(project(":core")))` without it shipping in main. It records events in order (thread-safe), completes stages immediately, and has a `failWith(RuntimeException)` switch so callers' Redis-down handling is unit-testable. It does **not** route to stream names — routing is Redis topology, tested in `bus`. |
| Batch publish | **Not built.** Measure first (Step 7); add only if the pipelined single-event path misses the target | Lettuce pipelines async commands on one connection natively — an explicit batch API would duplicate what the transport already does. The issue says measure first; the numbers decide. |

## Package layout

```
core/src/main/java/engine/core/bus/
└── EventPublisher.java              ← the interface; error contract in Javadoc

core/src/testFixtures/java/engine/core/
├── bus/InMemoryEventPublisher.java  ← recording fake for other components' tests
└── serde/SampleEvents.java          ← moved from core test sources

bus/src/main/java/engine/bus/
├── StreamNames.java                 ← ADR 0002 §3 table as code
├── RetentionPolicy.java             ← ADR 0002 §4 table: class prefix → (window, MAXLEN cap)
├── RedisStreamsEventPublisher.java  ← Lettuce; connection lifecycle, XADD MAXLEN ~
└── StreamTrimmer.java               ← 60 s MINID sweeper
```

Delete `BusPlaceholder.java` — the publisher is the real bus content it was holding space for.

## Step 1 — `EventPublisher` in core

The interface plus contract Javadoc. The Javadoc is where the Redis-down decision is *documented* (issue acceptance): publish never blocks, never buffers beyond the implementation's bounded in-flight window, and fails the returned stage on unavailability, timeout, or a full window; `close()` drains in-flight publishes with a bounded wait.

```java
package engine.core.bus;

public interface EventPublisher extends AutoCloseable {

    CompletionStage<Void> publish(Event event);

    @Override
    void close(); // narrowed: no checked exception, so try-with-resources stays clean
}
```

- **One error channel:** all failures arrive on the stage, never as a synchronous throw — including routing errors like a null `instrumentId` on a tick. Implementations wrap their body in try/catch and return `CompletableFuture.failedStage(e)`.
- The contract Javadoc must state, explicitly: (1) non-blocking; (2) the only buffer is a bounded in-flight window — full window ⇒ immediate failure, never queueing; (3) implementations are thread-safe; (4) **a failed stage does not prove the event didn't land** — a timeout can lose the response rather than the write, so retries can duplicate (at-least-once; dedup on `eventId`/`clientOrderId` is downstream's job); (5) `close()` waits for in-flight publishes with a bounded deadline, then releases resources.

**Verify:** `./gradlew :core:dependencies --configuration apiElements` still shows no Lettuce, no Jackson (the NEG-15/16 boundary checks hold).

## Step 2 — testFixtures: in-memory publisher + shared samples

Add `java-test-fixtures` to `core/build.gradle.kts`; move `SampleEvents` from `core/src/test` to fixtures (core's own tests keep compiling — the plugin wires test → testFixtures automatically); add `InMemoryEventPublisher`. Wire `implementation(testFixtures(project(":core")))` into both of `bus`'s test suites.

- The plugin line is the whole Gradle change in core: `` `java-test-fixtures` `` in the `plugins` block. New source root: `core/src/testFixtures/java`.
- `git mv core/src/test/java/engine/core/serde/SampleEvents.java core/src/testFixtures/java/engine/core/serde/SampleEvents.java` — the class and its factories must become `public`; test classes are package-private by habit, but fixtures are a published API consumed from `bus`.
- The fake, in `core/src/testFixtures/java/engine/core/bus/`:

```java
public final class InMemoryEventPublisher implements EventPublisher {

    private final List<Event> published = new CopyOnWriteArrayList<>();
    private volatile RuntimeException failure;

    @Override
    public CompletionStage<Void> publish(Event event) {
        RuntimeException f = failure;
        if (f != null) {
            return CompletableFuture.failedStage(f);
        }
        published.add(event);
        return CompletableFuture.completedStage(null);
    }

    /** Everything published so far, in publish order. */
    public List<Event> published() { return List.copyOf(published); }

    /** Makes all subsequent publishes fail with {@code failure}; pass {@code null} to heal. */
    public void failWith(RuntimeException failure) { this.failure = failure; }

    @Override
    public void close() {}
}
```

Deliberately no stream routing in it — routing is Redis topology, owned and tested in `bus` (Step 3). `failWith` is what lets a component's unit tests exercise its Redis-down handling against the fail-fast contract.

**Verify:** `./gradlew build` green; a scratch bus unit test can instantiate `InMemoryEventPublisher` and see published events in order.

## Step 3 — `StreamNames`

Static routing: `String streamFor(Event event)`. Table-driven per the decisions above — and the "table" should be a switch over the sealed `Payload`:

```java
public final class StreamNames {

    private StreamNames() {}

    public static String streamFor(Event event) {
        return switch (event.payload()) {
            case TradeTick t -> "md.tick.trade." + requireInstrument(event);
            case QuoteTick q -> "md.tick.quote." + requireInstrument(event);
            case Bar b -> "md.bar." + intervalToken(b.interval()) + "." + requireInstrument(event);
            case Signal s -> "signals";
            case OrderIntent o -> "orders.intents";
            case Fill f -> "orders.fills";
            case Metric m -> "metrics";
            case Command c -> "commands";
        };
    }
}
```

- **No `default` branch, on purpose.** `Payload` is sealed, so a 9th payload type makes this a *compile error* — stronger than any test. Keep the completeness test anyway: the switch pins coverage, the test pins the exact ADR strings.
- `requireInstrument(event)` returns `event.instrumentId().toString()` and throws `IllegalArgumentException` naming the payload type when it's null.
- `intervalToken(Duration)` is a private `Map<Duration, String>` with exactly the six ADR entries (`Duration.ofMinutes(1)` → `"1m"` … `Duration.ofDays(1)` → `"1d"`); a miss throws `IllegalArgumentException` quoting the duration and pointing at the ADR 0002 §3 vocabulary.

Unit tests:

1. **Completeness sweep** — for every entry in `PayloadRegistry.standard()`, a sample event resolves to exactly the ADR 0002 §3 example column. Adding a 9th payload type without a stream mapping fails this test — that's the guard the ADR asked for.
2. Instrument-scoped payload with null `instrumentId` throws; single-stream payloads route the same with or without one.
3. Every vocabulary interval maps (`PT1M`→`1m` … `P1D`→`1d`); `PT90S` throws naming the offending duration.

**Verify:** `./gradlew :bus:test` green.

## Step 4 — `RetentionPolicy` + `RedisStreamsEventPublisher`

- `RetentionPolicy.standard()`: ADR §4 retention table verbatim — stream-class prefix → (window `Duration`, MAXLEN cap). Longest-prefix match; used by both the publisher (cap) and the trimmer (window).
- Publisher: takes a `redis://` URI string (Lettuce types stay out of the constructor), builds `RedisClient` with the client options from the decisions table, one shared stateful connection (thread-safe, pipelines all async commands). `publish` = `StreamNames.streamFor` → `xadd(stream, XAddArgs maxlen(cap).approximateTrimming(), Map.of("event", codec.encode(event)))` → adapt the Lettuce future to `CompletionStage<Void>`. `close()` awaits in-flight, then shuts down client and event-loop resources.
- Unit test pins the client options (REJECT_COMMANDS, queue size, timeout enabled) — the *configuration is the mid-run failure behavior*, so it gets pinned like the ObjectMapper settings were in NEG-16.

`RetentionPolicy` shape:

```java
public record RetentionPolicy(List<Rule> rules) {

    /** prefix is either an exact stream name or a class prefix ending in '.' */
    public record Rule(String prefix, Duration window, long maxlen) {}

    public static RetentionPolicy standard() { /* the 8 ADR 0002 §4 rows */ }

    public Rule ruleFor(String stream) { /* longest match wins; no match throws */ }
}
```

Matching: exact equality for rules whose prefix has no trailing `.` (`orders.intents`, `signals`, …), `startsWith` for class prefixes (`md.tick.quote.`, `md.bar.`). Longest match wins. **No match throws** — an unrecognized stream means `StreamNames` and the policy diverged, which is a bug to surface, not a case to default.

Publisher skeleton (the load-bearing lines):

```java
public final class RedisStreamsEventPublisher implements EventPublisher {

    public static final String EVENT_FIELD = "event"; // frozen wire constant; NEG-19 + archiver read it

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisAsyncCommands<String, byte[]> commands;
    private final AtomicLong inFlight = new AtomicLong();

    public RedisStreamsEventPublisher(String redisUri, EventCodec codec, RetentionPolicy retention) {
        this.client = RedisClient.create(redisUri);
        client.setOptions(clientOptions()); // MUST precede connect()
        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.commands = connection.async();
        // ...
    }

    static ClientOptions clientOptions() {
        return ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(4096)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(1)))
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(1)).build())
                .autoReconnect(true) // the default, but this line *is* the documented decision
                .build();
    }

    @Override
    public CompletionStage<Void> publish(Event event) {
        try {
            String stream = StreamNames.streamFor(event);
            XAddArgs args = new XAddArgs()
                    .maxlen(retention.ruleFor(stream).maxlen())
                    .approximateTrimming();
            inFlight.incrementAndGet();
            return commands.xadd(stream, args, Map.of(EVENT_FIELD, codec.encode(event)))
                    .whenComplete((id, err) -> inFlight.decrementAndGet())
                    .thenAccept(id -> {});
        } catch (RuntimeException e) {
            return CompletableFuture.failedStage(e);
        }
    }
}
```

- `clientOptions()` is `static` and package-private precisely so the pin test can assert on it *without a Redis connection* — the constructor connects eagerly (that eager `connect()` throwing `RedisConnectionException` is the Step 6.3 down-at-startup behavior; the 1 s `connectTimeout` is what makes it prompt).
- One connection, shared, no pooling: Lettuce connections are thread-safe and pipeline all async commands on one channel — pooling would only fragment the pipeline.
- `close()`: deadline-poll `inFlight` down to zero (≤2 s total, ~10 ms sleeps — close is not a hot path), then `connection.close()` and `client.shutdown()`.
- `RetentionPolicy` unit tests alongside: longest-prefix beats shorter, exact names don't prefix-match (`orders.intentsX` throws), unknown stream throws, and `standard()` reproduces the 8 ADR rows (windows *and* caps).

**Verify:** `./gradlew build` green from root (integration behavior lands in Step 6).

## Step 5 — `StreamTrimmer`

Constructor takes connection + `RetentionPolicy` + `Clock`; a `runOnce()` method does one sweep (SCAN class prefixes, `XTRIM MINID ~ (now − window)` each), and a `start()`/`close()` pair owns the 60 s scheduler. `runOnce()` + injected `Clock` is what makes it testable without sleeping.

```java
public final class StreamTrimmer implements AutoCloseable {

    public StreamTrimmer(StatefulRedisConnection<String, byte[]> connection,
                         RetentionPolicy retention, Clock clock) { /* ... */ }

    void runOnce() { /* one sweep; package-private so tests call it directly */ }

    public void start() { /* single-daemon-thread ScheduledExecutorService, 60 s fixed delay */ }

    @Override
    public void close() { /* stops the scheduler only — the connection is caller-owned */ }
}
```

- Per rule in `runOnce()`: exact-name rules trim unconditionally (`XTRIM` on a missing key returns 0, harmless — no existence check needed); prefix rules run a `SCAN` cursor loop with `KeyScanArgs.Builder.matches(rule.prefix() + "*").limit(500)` and trim each hit. Sync commands are fine — the trimmer is not a hot path.
- The trim itself: `commands.xtrim(stream, XTrimArgs.Builder.minId(String.valueOf(clock.instant().minus(rule.window()).toEpochMilli())).approximateTrimming())` — stream IDs are ms timestamps, so that epoch-milli string *is* the age cutoff.
- **Wrap each sweep in try/catch-log.** `scheduleWithFixedDelay` silently cancels the schedule if a run escapes with an exception — a transient Redis hiccup must cost one sweep, not all future ones. (The MAXLEN caps are the backstop if the trimmer dies anyway, but don't lean on the backstop.)
- The trimmer *borrows* the connection (constructor argument, caller-owned, never closed by the trimmer) — wiring code constructs publisher + trimmer together and owns both lifecycles.
- Because rule prefixes are the concrete class prefixes (`md.tick.trade.` …), `dlq.*` and `replay.*` are unreachable by construction; the test still asserts it.

**Verify:** integration test — `XADD` entries with *explicit old IDs* (XADD accepts them via `XAddArgs.id("<epochMillis>-0")`; IDs are just ms timestamps, so aging a stream is writing `now−13h` as the ID — and explicit IDs must be written in ascending order, old entries first), `runOnce()` with a fixed `Clock`, assert old entries gone and in-window entries intact, `dlq.*`/`replay.*` streams untouched.

## Step 6 — Integration tests (the acceptance criteria)

In `bus/src/integrationTest` against the docker-compose Redis:

1. **Routing, all 8 types** — publish one fully-populated sample event per payload type through the real publisher; for each, `XRANGE` the exact ADR-prescribed stream, decode the `event` field through `JsonEventCodec`, assert round-trip equality. Proves a component publishes any type having imported only `engine.core.*`.
2. **MAXLEN cap honored** — publisher with a test `RetentionPolicy` (cap ~100), publish 1000; `XLEN` lands near the cap. Assert *bounded* (say, < 300), not exact — `~` trimming is approximate by design.
3. **Redis down at startup** — constructor against a closed port (`localhost:6390`) throws promptly; no hang.
4. **Redis unresponsive mid-run** — `DEBUG SLEEP 3` from a second connection, then publish: the stage fails with a timeout within ~1 s, well before the sleep ends. This is the deterministic stand-in for a mid-run outage; the full `docker compose stop redis` → publishes fail fast → `start` → publishes resume sequence is run manually once and its transcript recorded in the PR (acceptance box "Redis-down behavior matches the documented semantics").
5. **Trimmer** — Step 5's test.

Mechanics that make these tests honest and repeatable:

- **Test-only instruments** (`TEST-A.ITEST`, `TEST-B.ITEST`) so per-instrument stream names can never collide with real data on the shared dev Redis; track every stream a test writes and `DEL` it in `@AfterEach`. The single streams (`orders.intents`, …) are unavoidably the real names — deleting them after the test is acceptable on the dev instance; never point these tests at anything else.
- **6.1 routing:** parameterized over the 8 sample events; the expected stream name per event is a **hardcoded string literal from ADR 0002 §3**, not computed via `StreamNames` — computing it would make the test a tautology that can't catch a wrong table. Read back with sync `XRANGE`, pull `EVENT_FIELD` from the entry body, decode through `new JsonEventCodec(PayloadRegistry.standard())`, assert the decoded event equals the published one.
- **6.2 cap:** test policy with cap 100 on `md.tick.trade.`; publish 1000 ticks and await all stages; assert `XLEN` is between 100 and 300 — bounded on both sides proves trimming happened *and* tolerates `~` approximation.
- **6.3 down at startup:** `assertThatThrownBy(() -> new RedisStreamsEventPublisher("redis://localhost:6390", …))` — with Step 4's 1 s `connectTimeout` this fails with `RedisConnectionException` in about a second; also assert the elapsed time stayed under ~3 s (the *promptness* is the contract).
- **6.4 unresponsive:** issue `DEBUG SLEEP 3` from a second connection **async, without awaiting** (it blocks the whole server — issued sync, your test thread would sleep through the window it's trying to observe); then publish and assert `stage.toCompletableFuture().get(2, SECONDS)` throws with a `RedisCommandTimeoutException` cause in ~1 s; finally await the DEBUG future so the server is responsive again before the next test. If the Lettuce version lacks a `debugSleep` method, dispatch it raw (`CommandType.DEBUG` with args `SLEEP 3`).

**Verify:** `docker compose up -d && ./gradlew :bus:integrationTest` green.

## Step 7 — Measure, then decide about batching

A throughput harness in `integrationTest` (excluded from the normal run via a JUnit tag): publish N=100k `TradeTick`s async with the bounded window, record sustained events/s and per-publish p50/p99 latency; print, don't assert.

Target: **≥5,000 events/s sustained with p99 ≤ 5 ms** — 2.5× the ADR's ~2k/s burst envelope for 20 pairs, so the 100-instrument future doesn't immediately reopen this story. Pipelined Lettuce on localhost should clear this by an order of magnitude; if it does, batch publish is explicitly *not built* and the numbers go in the PR as the baseline for the NEG-22 smoke test. Only if it misses does a `publishBatch` (single `xadd` pipeline flush) get designed.

How to build it:

- **Not a JUnit test** — a `public static void main` class `PublishBench` in the `integrationTest` source set, run from the IDE (or a one-off `JavaExec` task). This sidesteps tag/filter plumbing and guarantees it never runs in CI.
- Warm up with ~10k unmeasured publishes first (JIT + connection ramp), then measure N = 100k `TradeTick`s.
- **Backpressure exactly as production would:** a `Semaphore(4096)` acquired before each `publish`, released in `whenComplete` — matches the request-queue bound and stops queue-full failures from polluting the numbers.
- Per-publish latency: `System.nanoTime()` before `publish`, capture the delta in `whenComplete` into a preallocated `long[N]` (index from an `AtomicInteger`); afterwards sort and read p50/p99/max. No histogram dependency needed at this N. Throughput = N / wall-clock over the measured phase.
- Print machine spec, Redis version (`INFO server`), N, events/s, p50/p99/max — paste that block verbatim into the PR.

**Verify:** numbers recorded in the PR description (acceptance box "publish path measured").

## Step 8 — Land it

Branch `luismarcosnegrao/neg-18-publisher-abstraction-over-redis-streams` (Linear's suggested name), one commit per step, PR with the measurement numbers and the manual Redis-stop transcript, tick the four acceptance boxes on NEG-18.

The closing checklist, in order:

1. `git checkout main && git pull && git checkout -b luismarcosnegrao/neg-18-publisher-abstraction-over-redis-streams`
2. Before each commit: `./gradlew spotlessApply build`
3. Full sweep before the PR: `docker compose up -d && ./gradlew build :bus:integrationTest`
4. The manual transcript: a scratch loop publishing one tick/second while you run `docker compose stop redis` (observe: each stage fails within ~1 s, nothing queues) then `docker compose start redis` (observe: publishes succeed again, nothing stale replays). Terminal output goes in the PR body next to the bench numbers.
5. Delete `BusPlaceholder.java` in whichever step touches `bus` first, if not already done.

## Definition of done (mapped to the issue)

- [ ] Component publishes any event type importing nothing Redis/Lettuce-specific → interface in `core` (Step 1) + routing test imports check (Step 6.1).
- [ ] Events land on the ADR 0002 streams (integration test vs docker-compose Redis) → Step 6.1.
- [ ] Redis-down behavior matches documented semantics, tested with Redis stopped → contract Javadoc (Step 1), options pin (Step 4), Steps 6.3/6.4 + manual stop transcript.
- [ ] Throughput and p99 recorded in the PR → Step 7.

## Pitfalls to expect

- **Lettuce's defaults are exactly the forbidden behavior.** Out of the box: `disconnectedBehavior = DEFAULT` (accepts commands while disconnected) and `requestQueueSize = Integer.MAX_VALUE` — a Redis outage silently queues *unbounded* order intents, then replays them all on reconnect, minutes stale. This is the issue's "silent unbounded buffering is forbidden" clause hiding in a client library default. The options pin in Step 4 exists so no refactor can quietly regress it.
- **Blocking per event destroys the performance story.** `publish(e).toCompletableFuture().join()` in a loop serializes to one round-trip per event (~thousands/s at best); the pipelined async path is the entire throughput margin. The Step 7 harness must measure the async path with the bounded window, and Javadoc should warn tick-path callers off per-event joins.
- **`XADD` auto-creates streams.** A malformed name doesn't error — it silently mints a new stream that no consumer and no trimmer knows about. `StreamNames` being the only place names are formed, plus Step 6.1 asserting exact names, is the whole defense.
- **`MAXLEN ~` is approximate.** Redis trims at radix-tree macro-node boundaries; `XLEN` after trimming exceeds the cap by up to ~100 entries. Assert bounds, never equality — an exact-match test is flaky by design.
- **`close()` must actually drain.** Dropping the client while publishes are in flight fails them spuriously; and an unclosed `RedisClient` leaves non-daemon event-loop threads alive (the NEG-15 smoke test learned this — its try-with-resources comment). Publisher owns client *and* client resources; integration tests use try-with-resources throughout.
- **The trimmer's `SCAN` is allowed; a consumer's is not.** Don't let the discovery helper migrate into NEG-19's subscriber later — ADR 0002 permits SCAN for tooling only. Say so in its Javadoc now.
- **Command timeout is not a delivery guarantee.** A timed-out `XADD` may still have landed (the response was lost, not necessarily the write) — so a caller retrying a failed intent publish can duplicate it. Fine for ticks; the OMS story handles intent idempotency via `clientOrderId`. Note it in the contract Javadoc so nobody discovers it in production.
