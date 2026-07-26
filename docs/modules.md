# Module responsibilities

Two modules exist today. This doc answers one question: **when you add a class, which module does it go in?** (The system-level picture is `ARCHITECTURE.md`; the stream topology is ADR 0002.)

## Dependency map

```
bus ──▶ core          (bus/build.gradle.kts: api(project(":core")))
core ──▶ nothing      (Jackson is an internal detail of one package, never exposed)
```

The arrow only ever points this way. `core` must compile with no infrastructure on its classpath — that is checked, not hoped: `./gradlew :core:dependencies --configuration apiElements` must show neither Jackson nor Lettuce.

## core — the engine's shared language

What every component speaks: the event model, its wire codec, and the *contracts* for moving events around. If two different components must agree on a type, that type lives here.

| Package | Responsibility |
|---|---|
| `engine.core.event` | The `Event` envelope, the sealed `Payload` hierarchy (8 types), and value types (`InstrumentId`, `Side`, …). Zero framework imports — plain records. Nullable components carry `maybeX()` Optional views. |
| `engine.core.serde` | The wire format, behind the `EventCodec` interface (`PayloadRegistry`, `EnvelopeView`, `JsonEventCodec`). The **only** package allowed to import Jackson; swapping JSON for a binary codec touches only this package. |
| `engine.core.bus` | Transport *contracts*: `EventPublisher`, and the consuming side `EventSubscriber` / `EventHandler` / `Subscription` plus the addressing/config types `EventSelector` and `SubscribeOptions` (`StartPosition`, sealed `LagPolicy`). The replay contract (NEG-20) lives here too: `EventSubscriber.replay`, the sealed `ReplayPosition` (`earliest`/`at`/`offset`), `ReplayRange`, the `Replay` handle (`done()` end-of-range signal), and `ReplayRetentionException`. Interfaces and value types only — components subscribe by payload type and instrument, never by stream name; no implementation against a real transport, ever. |
| testFixtures | Shared test helpers published to other modules' test suites: `SampleEvents`, `InMemoryEventPublisher`, and `InMemoryEventBus` (publisher + subscriber in one, for exercising a component's full publish/subscribe surface with no Redis; records every published event and implements `replay` synchronously so a component can re-drive its own history). Test doubles for core contracts live here, not in any module's main sources. |

**Belongs in core:** types all components need; interfaces that keep infrastructure swappable; pure domain values; invariant checks on those values.

**Never in core:** anything that names a Redis stream, imports Lettuce, or knows the topology; component business logic (strategy, risk, OMS rules); connection or retention configuration.

## bus — the Redis Streams transport

The one place that knows events travel over Redis. It implements core's messaging contracts per ADR 0002 and owns everything that would change if the transport changed.

| Class (current / planned) | Responsibility |
|---|---|
| `StreamNames` | The ADR 0002 §3 stream table as code: event → stream address, `EventSelector` → stream, and `dlqFor(stream)`. The only place stream names are formed. |
| `RetentionPolicy` (NEG-18) | The ADR 0002 §4 retention table: stream class → window + MAXLEN cap. |
| `RedisStreamsEventPublisher` (NEG-18) | Lettuce connection lifecycle, fail-fast publish, per-XADD trim caps. |
| `StreamTrimmer` (NEG-18) | The 60 s `XTRIM MINID` sweeper. |
| `RedisStreamsEventSubscriber` (NEG-19) | Consumer-group subscribe with at-least-once delivery: dedicated connection + poll thread per subscription, startup PEL drain, `XACK`-after-handler, the claim sweep (crash-takeover and handler-retry as one mechanism), lag observation and `md.*`-only skip-to-latest. |
| `SubscriberTuning` (NEG-19) | Block/batch/claim-interval/claim-min-idle/max-deliveries/DLQ-cap knobs; `standard()` is production, tests shrink the timings. |
| `DeadLetter` (NEG-19) | The `dlq.<stream>` parking mechanics and frozen DLQ field constants (ADR 0002 §3). |
| `RedisReplay` (NEG-20) | Pure-reader replay of a bounded stream-ID range: dedicated connection + daemon thread per replay, batched `XRANGE`, k-way merge by stream ID (tie-broken by name), retention guard on the oldest retained id, `done()` completion on every exit path. Never writes to Redis — no group, no ack, no DLQ. |
| `ReplayPositions` (NEG-20) | Pure `ReplayPosition` → Redis stream-ID mapping and validation (offset-token shape, single-stream rule). |
| `PublisherStats` (NEG-21) | Always-on publish-path instrumentation owned by the publisher: `published`/`failed` interval counters, the `inFlight` gauge, and an exact per-source feed-latency buffer with a lossless double-buffer `drain()`. In the root, not `monitor`, because it is written on the hot path beside `inFlight` — moving it under `monitor` would make the publisher import monitoring code and invert the one-way arrow. |
| `XInfoReplies` (NEG-21) | Shared folding of Redis `XINFO` replies (`asFieldMap`/`asString`), used by the subscriber, replay, and monitor. Root infrastructure with three consumers, not monitoring; `public` (not package-private) precisely so `engine.bus.monitor` can see it across the subpackage boundary. |

### `engine.bus.monitor` — bus self-observability (NEG-21)

Five classes that read the bus's health and publish it as ordinary `Metric` events through the core `EventPublisher` (the bus reports on itself over itself). This subpackage's boundary is **organizational, not architectural**: the dependency arrow points `engine.bus.monitor → engine.bus` and never back, and nothing on the publish/subscribe delivery path imports it. The litmus tests below do **not** distinguish a monitor from the subscriber — `BusMonitor` is as Redis-specific as `RedisStreamsEventSubscriber`; both are `bus`. The subpackage exists only to make the one-way dependency enforceable that a flat package could merely assert in a comment.

| Class (NEG-21) | Responsibility |
|---|---|
| `MetricNames` | The ADR 0003 metric-name grammar as code: typed builders (`streamRate`, `groupLag`, `feedLatency`, …) and the frozen `LatencyPercentile` vocabulary, so no caller assembles a metric name by concatenation. |
| `BusSnapshot` | One sweep's readings as an immutable value type (streams, groups, DLQs, memory, publisher drain, in-flight, sweep duration). Pure data — no Redis type leaks in, so it is fabricable in a unit test. |
| `BusMonitor` | The scheduler (single daemon thread, sweep survives its own exceptions) on its own connection: `SCAN … TYPE stream` discovery, per-stream/group/DLQ/memory reads, and emission of one `Metric` per inventory row via the injected publisher. A pure reader — no group, no ack, no write to any observed stream. |
| `MonitorTuning` | Sweep interval and endpoint port; `standard()` is 15 s / 9464. The percentile set is not tunable — ADR 0003 froze it. |
| `MetricsEndpoint` | The interim Grafana-readable surface: a JDK `HttpServer` serving `GET /metrics` in Prometheus text (ADR 0003 §3 name derivation, spec-compliant label escaping, `bus_dlq_last_error` info-metric). A scrape reads a volatile reference and issues no Redis command. |

Its `integrationTest` suite runs against the docker-compose Redis and is deliberately excluded from plain `build`.

**Belongs in bus:** stream addressing and topology, connection management, retention/trimming, consumer-group and dead-letter mechanics, transport-specific error mapping.

**Never in bus:** payload semantics (what a `Fill` means), decisions about *which* events to publish (a component concern), anything another module would need to import — components depend on `core`'s interfaces and get the bus implementation handed to them at wiring time.

## The litmus tests

Four questions settle nearly every placement:

1. **Would this class change if Redis were swapped for NATS/Kafka?** → `bus`.
2. **Do two or more components need to agree on this type?** → `core`.
3. **Does it import Lettuce or name a stream?** → `bus`, no exceptions.
4. **Is it a contract other modules will code against?** → `core` (interface), with the implementation in `bus`.

If a class seems to need a home in both — say, a helper that routes payloads to streams — it is a bus class, and the part components actually need belongs behind a core interface.

## Future modules

The ARCHITECTURE.md components (feed handlers, strategy runtime, risk manager, OMS, archiver, …) become sibling modules. Each depends on `core` only; none may depend on `bus`. The composition root (whatever `main` wires the engine together) is the single place that instantiates bus classes and injects them as core interfaces. The archiver is the canonical example: it is "a bus consumer" operationally, but as a module it depends on core's subscriber contract — it never imports `engine.bus`.
