# The Message Bus

class diagram: https://www.figma.com/board/jU5kxNzlSHaHQp639EhcXM/Message-Bus-%E2%80%94-Class-Diagram--NEG-5-?node-id=0-1&t=NeQNZlbV5ATfwNt7-1

What the bus is, what every class in it does, and how the load-bearing parts actually work.

Scope: `engine.core.bus`, `engine.core.serde`, `engine.bus`, `engine.bus.monitor` — everything delivered by epic NEG-5 (NEG-15 … NEG-22). Adjacent docs: `ARCHITECTURE.md` for the system picture, `docs/modules.md` for *which module a new class belongs in*, ADR 0002 for the stream topology and retention table, ADR 0003 for the metric vocabulary. Where this doc and an accepted ADR disagree, the ADR wins.

---

## 1. What the bus does

Every component of the engine — feed handlers, strategies, the risk manager, the OMS, the archiver, the UI — talks to exactly one thing: the bus. Nothing calls anything else directly. A component publishes `Event`s and subscribes to the ones it cares about, and that is the entire integration surface.

Four properties define it:

**Components address the bus by *what*, never by *where*.** A strategy asks for "trade ticks for BTC-USDT" (`EventSelector`), not for the stream `md.tick.trade.BTCUSDT@BINANCE`. Translating a selector into a stream name is bus code's job and nobody else's. This is what lets the topology change without touching a single component.

**Delivery is at-least-once, ordered per stream.** An event may arrive more than once — a crash between handling and acknowledging, a claim race, a publisher retry. Redeliveries carry the same `eventId`; deduplicating on it is the handler's job. Within one stream events arrive in publish order. Across streams there is no ordering, and nothing may depend on one.

**History is re-drivable through the same code path.** `EventSubscriber.replay` re-delivers a recorded range of the same streams to the same `EventHandler`, with original envelopes. A handler cannot tell replayed traffic from live. This is the mechanism behind the project's core rule — one code path for backtest, paper and live — and it is why there is no separate backtesting framework.

**The bus reports on itself, over itself.** `BusMonitor` reads Redis health every 15 s and publishes it as ordinary `Metric` events on the `metrics` stream, plus a Prometheus endpoint. Monitoring is not a side channel; it is a normal bus participant.

### One trip through the bus

```
component
   │  publish(Event)
   ▼
EventPublisher ──▶ StreamNames.streamFor(event)      pick the stream
                   EventCodec.encode(event)          envelope + payload → bytes
                   RetentionPolicy.ruleFor(stream)   MAXLEN cap for this stream class
                   XADD stream MAXLEN ~ cap          one round trip, never blocks
                                │
                                ▼
                         Redis Stream  ◀── StreamTrimmer, every 60 s: XTRIM MINID (age window)
                                │
                                ▼
EventSubscriber ── XREADGROUP (consumer group, blocking) ──▶ decode ──▶ EventHandler.handle
                                                                            │
                                            returns ──▶ XACK (fire-and-forget)
                                            throws  ──▶ leave pending
                                                          │
                                          claim sweep every 5 s, entries idle ≥ 30 s
                                                          ├─ under 5 deliveries → XCLAIM, retry
                                                          └─ 5th delivery      → park on dlq.<stream>
```

`BusMonitor` observes all of the above on its own connection and publishes the result back through `EventPublisher`.

---

## 2. Every class, in one line

### `engine.core.bus` — the contracts (no Lettuce, no Jackson)

| Class | What it is |
|---|---|
| `EventPublisher` | The publish contract: non-blocking `publish(Event)` returning a `CompletionStage`, and a draining `close()`. |
| `EventSubscriber` | The consume contract: `subscribe(...)` for live traffic, `replay(...)` for recorded history. |
| `EventHandler` | Functional interface for handling one event. Return = acknowledge, throw = redeliver. |
| `Subscription` | Live subscription handle: `lag()`, `skipCount()`, `close()`. |
| `Replay` | Running-replay handle: `done()` completes with the delivered count at end-of-range; `close()` cancels. |
| `EventSelector` | What a consumer wants — payload type, optional instrument, optional bar interval. Never a stream name. |
| `SubscribeOptions` | Wiring-time subscription config: where a new group starts, and what it does when it falls behind. |
| `SubscribeOptions.StartPosition` | `LATEST` or `EARLIEST`. No default — the caller must choose. Applies only when the group is first created. |
| `SubscribeOptions.LagPolicy` | Sealed: either `ProcessAll` or `SkipToLatest`. |
| `SubscribeOptions.ProcessAll` | Never skip; deliver every retained event even under unbounded lag. The normal choice. |
| `SubscribeOptions.SkipToLatest` | Jump to the tail once lag crosses a threshold. Legal on `md.*` streams only. |
| `ReplayRange` | A bounded slice of history: inclusive start, optional end (absent = through the tail sampled at start). |
| `ReplayPosition` | Sealed edge of a range: `Earliest`, `At(Instant)`, or `Offset(String)`. |
| `ReplayPosition.Earliest` | The oldest event still retained. Valid only as a start. |
| `ReplayPosition.At` | An **ingest-time** boundary at millisecond precision — when the bus received the event, not its `occurredAt`. |
| `ReplayPosition.Offset` | An opaque transport-issued token. Core keeps it a bare `String`; the bus validates it as a Redis stream ID. |
| `ReplayRetentionException` | Unchecked. Raised when a replay asks for a range the bus no longer holds. |

### `engine.core.serde` — the wire format

| Class | What it is |
|---|---|
| `EventCodec` | The (de)serialization contract. `encode`, `envelope`, `decode`. The only thing the bus knows about the wire format. |
| `JsonEventCodec` | The Jackson implementation. The single Jackson-aware type in the whole engine. |
| `EnvelopeView` | The envelope fields readable without touching the payload — what generic infrastructure needs. |
| `PayloadRegistry` | Maps payload classes ↔ the frozen wire `eventType` / `schemaVersion` pairs. |
| `PayloadRegistry.PayloadType` | One registry row: `eventType`, `schemaVersion`, payload class. |

### `engine.core.event` — what the bus carries

| Class | What it is |
|---|---|
| `Event` | The uniform envelope: `eventId`, `source`, nullable `instrumentId`, `occurredAt`, `ingestedAt`, `payload`. |
| `Payload` | Sealed marker permitting exactly eight bodies. Sealing is what makes `StreamNames`' switch exhaustive. |
| `TradeTick`, `QuoteTick`, `Bar` | Market data. Instrument-partitioned; `Bar` additionally carries an interval. |
| `Signal`, `OrderIntent`, `Fill` | Strategy output, order requests, executions. |
| `Metric`, `Command` | Telemetry and control. Both are instrument-agnostic — they route to a single stream each. |
| `InstrumentId` | `symbol` + `venue`, with `parse`/`toString` round-tripping the wire form. |
| `Side`, `OrderType`, `TimeInForce`, `CommandAction` | Closed vocabularies used by the payloads above. |

### `engine.bus` — the Redis Streams implementation

| Class | What it is |
|---|---|
| `RedisStreamsEventPublisher` | `EventPublisher` over Lettuce. One `XADD` per event, capped by the retention rule. |
| `RedisStreamsEventSubscriber` | `EventSubscriber` over consumer groups. Owns the group name and this consumer's stable identity. |
| `…EventSubscriber.RedisSubscription` | Private inner class: one dedicated connection + poll thread per subscription. Where delivery actually happens. |
| `RedisReplay` | Package-private `Replay`: batched `XRANGE` over a bounded ID range, k-way merged. Never writes to Redis. |
| `StreamNames` | The ADR 0002 §3 stream table as code. The only place a stream name is ever formed. |
| `RetentionPolicy` | The ADR 0002 §4 retention table: stream class → age window + `MAXLEN` cap. |
| `RetentionPolicy.Rule` | One row. A trailing dot means "stream class prefix"; no trailing dot means an exact stream name. |
| `StreamTrimmer` | The 60 s `XTRIM MINID` sweeper that enforces the age windows. |
| `SubscriberTuning` | Block / batch / claim-interval / claim-min-idle / max-deliveries / DLQ-cap knobs. |
| `DeadLetter` | Parks a poison entry on `dlq.<stream>` and owns the frozen DLQ field names. |
| `PublisherStats` | Publish-path counters and per-source latency samples, drained once per monitor sweep. |
| `PublisherStats.Drain` | One interval's read-reset counters plus raw, unsorted latency samples. |
| `ReplayPositions` | Package-private: maps `ReplayPosition` shapes to Redis stream IDs and enforces the single-stream offset rule. |
| `XInfoReplies` | Folds Lettuce's alternating key/value `XINFO` replies into a map. Shared by subscriber, replay and monitor. |

### `engine.bus.monitor` — self-observability

| Class | What it is |
|---|---|
| `BusMonitor` | The sweep scheduler on its own connection: discover streams, read health, publish `Metric` events. |
| `BusSnapshot` | One sweep's readings as pure data. No Redis type leaks in, so it is fabricable in a unit test. |
| `BusSnapshot.StreamReading` | Per stream: `entries-added` (the rate source), length, oldest-entry timestamp. |
| `BusSnapshot.GroupReading` | Per (group, stream): lag and pending. Lag is `OptionalLong` — empty means *unknown*, never zero. |
| `BusSnapshot.DlqReading` | Per `dlq.*`: depth and the newest parked entry. |
| `BusSnapshot.DlqLastError` | The newest DLQ entry's group, error text and timestamp. |
| `BusSnapshot.MemoryReading` | Redis `used_memory` and `maxmemory` (`0` = unlimited). |
| `MetricNames` | The ADR 0003 metric grammar as typed builders, so nobody concatenates a metric name by hand. |
| `MonitorTuning` | Sweep interval and endpoint port. `standard()` is 15 s on port 9464. |
| `MetricsEndpoint` | Package-private JDK `HttpServer` serving `GET /metrics` in Prometheus text from the last sweep. |

### `core` test fixtures

| Class | What it is |
|---|---|
| `InMemoryEventBus` | Publisher + subscriber in one, no Redis. Records every published event and replays synchronously. |
| `InMemoryEventPublisher` | Recording publisher for components that only publish. |
| `SampleEvents` | Canonical event instances for tests across modules. |

---

## 3. The critical components

### 3.1 `StreamNames` — the topology, and why it is frozen

`StreamNames` is a static-only class with three methods, and it is the highest-consequence file in the bus.

```
md.tick.trade.<instrument>      md.bar.<interval>.<instrument>      signals
md.tick.quote.<instrument>      orders.intents | orders.fills       metrics | commands
dlq.<sourceStream>              replay.*  (reserved, never written)
```

Bar intervals are a closed vocabulary: `1m 5m 15m 1h 4h 1d`. Adding one means amending ADR 0002 first.

Two design decisions carry real weight.

**Stream names are not derived from wire `eventType` strings.** It would be easy to mangle `"tick.trade"` into `md.tick.trade.*`, and it would be wrong: `eventType` is a wire discriminator owned by the serde registry, and stream names are transport addresses. Coupling them means renaming an `eventType` silently redirects a stream, and every consumer of the old name goes quiet while the archive splits in two. Both sets of strings are frozen by ADR, separately.

**The `switch` over `Payload` is exhaustive with no `default`.** Because `Payload` is sealed, adding a ninth payload type breaks compilation here until someone assigns it a stream. That is deliberate: the failure mode of a `default` branch is a new event type silently landing on the wrong stream, or an exception in production. A compile error is a much better place to discover you have not thought about topology.

`streamFor(EventSelector)` is the other half, and it is where "which payload types are instrument-partitioned" lives. `core` cannot know that — an `EventSelector` carries only a type plus optional instrument and interval — so the mandatory-instrument rule is enforced here, at subscribe time, and a mis-wired consumer fails at startup rather than delivering nothing forever.

### 3.2 `RedisStreamsEventPublisher` — the publish path

The contract is strict: **`publish` never blocks and never throws.** Every failure — transport down, timeout, encode error, unroutable event — arrives on the returned `CompletionStage`. There is exactly one error channel, so callers write one error path.

One `publish` is one `XADD`. No batching, no client-side queue. The only buffering is Lettuce's bounded in-flight window, and when it is full the publish fails immediately rather than growing a backlog in the JVM — a bus that queues while the transport is down converts a Redis outage into an OOM.

Each `XADD` carries `MAXLEN ~ <cap>` taken from the matching retention rule, so the entry-count bound is enforced continuously on the write path. The age window is a separate, slower mechanism (§3.5); the two are complementary, not redundant.

The `stats` bookkeeping deserves a note because it is easy to get wrong: every `incrementInFlight` has exactly one matching decrement, on either the `whenComplete` hook or the synchronous-throw `catch`. A leak in that pairing would make the `bus.publisher.inFlight` gauge drift upward forever and eventually read as a stuck publisher.

Two failure classes are counted differently, on purpose. Operational failures (rejected while disconnected, command timeout, an OOM `XADD` — the memory-wall tripwire of ADR 0002 §4) increment `failed`. Pre-transport failures (an unroutable event, an encode bug) return a failed stage but are *not* counted: they are programming errors, and mixing them into the operational counter would make the alert on `bus.publisher.failed` fire for a bug that no amount of Redis capacity would fix.

**A failed stage does not prove the event did not land.** A command timeout can lose the response rather than the write. Retrying is legal and may duplicate — which is exactly why the delivery contract is at-least-once and handlers deduplicate on `eventId`.

`close()` waits up to 2 s for in-flight publishes to complete, then closes the connection and shuts the client down.

### 3.3 `RedisStreamsEventSubscriber` — the delivery loop

This is the most intricate component in the bus. The consumer group name and this consumer's stable identity are **constructor state, not `subscribe` arguments**: one component reads every stream it needs under one group (ADR 0002), and the consumer name must survive restarts so a crashed instance reclaims its own pending entries.

Each `subscribe` call creates a `RedisSubscription` with its own connection and its own daemon poll thread. The handler is invoked only from that thread, never concurrently with itself.

**The loop.** On startup, drain this consumer's own pending list first (`XREADGROUP` from id `0`) — crash leftovers redeliver before any new entry. Then repeatedly: blocking `XREADGROUP` for up to `block` (1 s) for up to `batchCount` (256) entries, dispatch each, and between reads run the claim sweep if `claimInterval` (5 s) has elapsed. A `RedisException` costs one iteration and a 1 s sleep, never the subscription — auto-reconnect brings the connection back.

**Dispatch.** Decode, hand to the handler, and on normal return `XACK`. A throwing handler leaves the entry pending and records the error for a possible later park. Undecodable bytes are parked *immediately* with no retry cycle — bytes that will not decode now will not decode in 30 s either. An entry with no event field at all has nothing worth preserving, so it is acked and counted lost.

**Acks are fire-and-forget, and that is a throughput decision.** Nothing in the loop depends on an ack's reply, so awaiting one costs a network round trip per event and caps a group at `1/RTT` events per second. Measured on the NEG-22 harness: with synchronous acks a group tops out near 2,000 events/s and three groups fall behind at 10k/s; asynchronous, the same three groups sustain ~10k/s each at p99 5 ms. The safety argument is that a lost ack leaves the entry pending and the claim sweep redelivers it — the same net that already catches a crash between handling and acking, so at-least-once is unchanged. The DLQ paths deliberately keep *synchronous* acks, because there the ack must not overtake the `XADD` that parks the body; that ordering is a correctness gate, not a performance choice.

**The claim sweep is one mechanism doing two jobs.** Every `claimInterval`, `XPENDING` lists entries idle past `claimMinIdle` (30 s). For each, if the delivery count has reached `maxDeliveries` (5) the entry is poison: claim it to read its body, park it on `dlq.<stream>` with the captured error, and ack the original. Otherwise `XCLAIM` takes it over, increments its delivery count, and re-dispatches. Crash-takeover from a dead consumer and retry-after-a-throwing-handler are the same code path — which is why `claimMinIdle` doubles as the retry backoff and **must exceed the slowest healthy handler**. A handler slower than 30 s will be treated as dead and its entry handed to a peer; the defence against the resulting duplicate work is the idempotency contract, not the timeout value.

**Lag and skip-to-latest.** `lag()` is undelivered (`XINFO GROUPS`) plus delivered-but-unacked (`XPENDING`), summed over every stream. It runs on the subscriber's *control* connection, not the subscription's, so a caller polling lag never queues behind the poll thread's blocking read. Redis reports `lag` as nil once trimming has cut into the undelivered range; that is surfaced as an empty `OptionalLong` meaning **unknown, never zero** — and `SkipToLatest` explicitly refuses to fire on an unknown reading, because skipping on a bad number is how order-of-magnitude mistakes happen. When it does fire, it `XGROUP SETID`s to `$` *and* acks this consumer's own pending, because moving the cursor alone would leave PEL entries to come back through the claim sweep. Every skip increments `skipCount()` — the observable proof that market data was intentionally gapped.

`close()` stops the loop, joins the poll thread for `block + 2 s`, flushes outstanding acks within 2 s, and closes the connection. The ack flush matters: without it a clean shutdown drops acks still in flight and every one of those entries is redelivered on restart — correct, but duplicate work a clean stop should not create. Awaiting only the *last* ack suffices, since one connection completes its commands in order.

### 3.4 `RedisReplay` — history through the live path

A replay is a **pure reader**: batched `XRANGE` over a bounded stream-ID range, on its own connection and its own daemon thread. No consumer group, no acknowledgements, no dead-lettering, no writes of any kind. Any number of replays can run concurrently with live consumption without affecting it.

The delivery path is byte-identical to live by construction: both call the same `decodeEntry` method, and both invoke the same `EventHandler` type from a single thread. Events carry their original envelopes — same `eventId`, same `occurredAt`, nothing re-minted. This is what makes "a strategy cannot tell backtest from live" a structural fact rather than an aspiration.

Multiple streams are merged k-way by stream ID, with ties broken by stream name, so an identical bounded replay over untrimmed data yields an identical sequence. Handlers still must not rely on cross-stream order — live never honours it, and a handler that quietly depends on replay's determinism will behave differently in production.

Positions are **ingest-time**, not event-time. `ReplayPosition.at(instant)` addresses when the bus *received* events. In normal operation that is within publish latency of `occurredAt`; after a feed outage the two diverge arbitrarily. Event-time-exact replay, and anything older than the retention window, belongs to the Historical Data Store (NEG-7) — not here.

Every exit path completes `done()` exactly once: the delivered count on exhaustion, `ReplayRetentionException` if a trim overtakes a running replay, the chained cause if a handler throws or an entry will not decode, `CancellationException` on `close()` before exhaustion. A replay never silently computes across a hole in its data — the retention guard runs up front, on the caller's thread, before a single event is delivered.

An offset-bounded range must resolve to exactly one stream: an opaque token is a position in one specific stream's ID space and means nothing in another's.

### 3.5 `RetentionPolicy` + `StreamTrimmer` — bounding the bus

Retention is two independent caps per stream class, from ADR 0002 §4:

| Stream class | Age window | `MAXLEN` cap |
|---|---|---|
| `md.tick.quote.` | 12 h | 2,000,000 |
| `md.tick.trade.` | 12 h | 1,000,000 |
| `md.bar.` | 14 d | 50,000 |
| `signals` | 14 d | 500,000 |
| `orders.intents` | 30 d | 1,000,000 |
| `orders.fills` | 30 d | 1,000,000 |
| `metrics` | 48 h | 2,000,000 |
| `commands` | 30 d | 100,000 |

The entry cap rides every `XADD`; the age window is enforced by `StreamTrimmer`'s 60 s `XTRIM MINID` sweep. Exact-name rules match by equality, class-prefix rules (trailing dot) by `startsWith`, and the longest matching prefix wins. An unmatched stream **throws** rather than defaulting — every stream this policy is asked about was minted by `StreamNames`, so a miss means the two have diverged, which is a bug to surface loudly rather than paper over with a fallback window.

The sweep contains one non-obvious loop that is worth understanding before touching it. `trimFully` calls `XTRIM` *repeatedly* until it reports zero removed, rather than once. With approximate trimming and no explicit `LIMIT`, Redis caps the work at `100 × stream-node-max-entries` — 10,000 entries by default — and returns having deleted only that much. One call per stream per 60 s sweep therefore enforces the window only up to ~166 entries/s per stream; above that the stream grows without bound while the sweep quietly reports success. NEG-22's soak caught exactly this at 400 entries/s per stream.

Looping is also deliberately preferred to `LIMIT 0` (unbounded in one call): Redis is single-threaded, and the first sweep after an outage or a window change could face millions of stale entries and block every other client for the whole deletion. Ten thousand deletions per call keeps each command short and lets the event loop serve other traffic in between, at the cost of a few more round trips.

The trimmer *borrows* its connection — the wiring code that constructs the publisher and trimmer together owns both lifecycles, and `close()` stops only the scheduler. Its sweep survives its own exceptions, because `scheduleWithFixedDelay` silently cancels the schedule if a run escapes: a Redis hiccup must cost one sweep, not all future ones.

Note what retention is *not*. It bounds the bus, which is a transport, not an archive. Anything that must outlive these windows is the Historical Data Store's job (ADR 0002 §6). Nothing here is a place to save disk by discarding ticks — see the data-fidelity principle in `CLAUDE.md`.

### 3.6 `BusMonitor` — the bus watching itself

One sweep, every 15 s, on a dedicated connection: `SCAN … TYPE stream` to discover every stream key (server-side type filtering, so non-stream keys never come back), then per-stream `XINFO STREAM` and `XINFO GROUPS`, per-`dlq.*` depth and newest entry, `INFO memory`, plus the publisher's in-flight gauge and a `drain()` of its interval counters. `replay.*` is reserved and never observed. Everything is folded into a `BusSnapshot` — pure data, no Redis type leaking in, which is what makes the derivation and emission logic unit-testable against fabricated snapshots.

Emission turns the snapshot into one `Metric` event per inventory row, named via `MetricNames`, and publishes them through the injected `EventPublisher`. The monitor is an ordinary publisher; its metrics land on the `metrics` stream like anything else, and a Grafana dashboard reads them the same way a strategy would.

Rates are derived from the `entries-added` delta against the *previous* snapshot, so the first sweep emits no rates. Several emissions are deliberately conditional: `lagUnknown` fires only when Redis reports `lag` as nil, oldest-age only for a non-empty stream, and a feed source with no samples this interval emits nothing at all — because absence of traffic is not zero latency, and averaging it as zero is how a dead feed looks healthy.

`bus.monitor.sweepMillis` watches the watcher. When a metric publish fails it is logged and left alone, because the failure already increments the publisher's own `failed` counter, which the next sweep reports — the monitor observing itself, as designed.

`MetricsEndpoint` is the interim Grafana surface until the control plane exists: a JDK `HttpServer` on port 9464 serving `GET /metrics` in Prometheus text from a volatile reference to the last completed sweep. A scrape issues no Redis command. The newest DLQ error is rendered as a `bus_dlq_last_error` info-metric rather than a metric value — a stack trace does not belong in a `BigDecimal`.

### 3.7 The `EventCodec` seam

The bus never mentions Jackson. It holds an `EventCodec` and calls `encode` / `decode`, so replacing JSON with a binary format is a change to one package in `core` and nothing else.

The return shapes encode a deliberate distinction. An unknown `eventType` is normal forward-compatibility — an older consumer meeting a newer producer — so `decode` returns `Optional.empty()` and the subscriber parks the entry rather than crashing the subscription. Malformed JSON or a missing required envelope field is corrupt data, i.e. a bug, and **throws**. Conflating the two would either hide corruption or turn a routine rolling deploy into an outage.

`envelope(byte[])` reads the envelope without touching the payload, which is what lets generic infrastructure — the archiver, replay, metrics — inspect an event whose payload type it does not know. `eventType` stays a raw `String` and `schemaVersion` a raw `int` precisely so that parsing never depends on a registry lookup succeeding.

---

## 4. Invariants

Breaking any of these is a data-loss or correctness event, not a refactor.

1. **Wire `eventType` strings and stream names that appear in an accepted ADR are frozen.** Renaming one splits the archive and silences consumers.
2. **Handlers must be idempotent on `eventId`.** At-least-once is the contract, not an edge case.
3. **Nothing may depend on cross-stream ordering.** Per-stream order is guaranteed; anything else is coincidence, and replay's determinism must not be mistaken for a promise live can keep.
4. **`core`'s public API stays free of Jackson and Lettuce.** Checked, not hoped: `./gradlew :core:dependencies --configuration apiElements`.
5. **Only bus code names a stream.** Components use `EventSelector`.
6. **`claimMinIdle` must exceed the slowest healthy handler**, and the subscriber's command timeout must exceed its `XREADGROUP` block. Both are pinned by tests.
7. **Empty lag means unknown, never zero.** Never act on an absent reading.
8. **A new `Payload` type must be given a stream in `StreamNames`.** The compiler enforces it; do not add a `default`.

## 5. What the bus deliberately does not do

- **It does not persist.** It is a transport with a retention window. Long-term history is the Historical Data Store's job (NEG-7).
- **It does not deduplicate.** That is the handler's, on `eventId` or `clientOrderId`.
- **It does not batch publishes or queue while the transport is down.** A full in-flight window fails fast instead of growing a backlog in the JVM.
- **It does not order across streams**, and offers no transaction spanning more than one.
- **It does not conflate or downsample market data** to fit a resource budget. Raw ticks, no exceptions (ADR 0002).
- **It does not retry poison forever.** Five deliveries, then `dlq.<stream>`, then processing continues past it.

## 6. Where to look next

| Question | File |
|---|---|
| Why these streams, these windows, these names? | `docs/adr/0002-stream-topology-naming-and-retention.md` |
| Why `TradeTick` and `QuoteTick` are separate events | `docs/adr/0001-separate-trade-and-quote-tick-events.md` |
| The metric vocabulary and how it is exposed | `docs/adr/0003-bus-metric-names-and-exposure.md` |
| Which module does my new class go in? | `docs/modules.md` |
| How each story was built, step by step | `docs/plans/neg-1[5-9]-*.md`, `neg-2[0-2]-*.md` |
| The system this bus is one slice of | `ARCHITECTURE.md` |

Running the Redis-backed suites needs `docker compose up -d`; they are excluded from plain `build`.
