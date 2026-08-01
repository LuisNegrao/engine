# NEG-21 — Bus Observability: Rates, Consumer Lag, Dead-Letter Monitoring: Implementation Plan

Implementation plan for [NEG-21](https://linear.app/negraolu/issue/NEG-21/bus-observability-rates-consumer-lag-dead-letter-monitoring). Everything lands in `bus`: a `BusMonitor` sweeper that periodically reads the bus's health from Redis (rates, per-group lag, DLQ depth, memory, window age), drains publisher-side counters, and publishes the readings as ordinary `Metric` events through the core `EventPublisher` — the bus reports on itself over itself. A Prometheus-text endpoint on the JDK's built-in HTTP server is the interim Grafana-readable surface. A short ADR 0003 records the metric name grammar and the exposure decision. `core` is untouched except for nothing — the existing three-field `Metric` payload carries every reading. This story is measurement only (thresholds and alerting belong to the UI/ops epics) and is the last gate before NEG-22 proves the backbone under load.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Collector shape | One `BusMonitor` class in `bus`, following the `StreamTrimmer` pattern: single-thread scheduled executor, `start()`/`close()`, a sweep that survives its own exceptions. Owns its `RedisClient` (it also runs an HTTP server — it is a service, not a helper); emits through an injected core `EventPublisher` | The monitor is the third bus-owned background task (trimmer, now monitor); reusing the proven shape means the failure mode is already understood: a Redis hiccup costs one sweep, never the schedule. Emitting through `EventPublisher` — not raw `XADD` — is the dogfooding the issue asks for: metrics ride the same publish path, the same `StreamNames` routing, the same MAXLEN caps as everything else. |
| Package placement | New `engine.bus.monitor` subpackage for the five self-contained classes (`BusMonitor`, `BusSnapshot`, `MonitorTuning`, `MetricsEndpoint`, `MetricNames`); `PublisherStats` and `XInfoReplies` stay in the `engine.bus` root | The subpackage makes an invariant enforceable that a flat package can only assert in a comment: nothing on the publish/subscribe path depends on monitoring — the arrow points `engine.bus.monitor → engine.bus`, never back. The two root exceptions are dictated by that arrow plus a Java mechanic (packages are not hierarchical for visibility — the subpackage cannot see the root's package-private members): `PublisherStats` is instrumentation *of the publisher*, written on the hot path beside `inFlight`, and moving it into the subpackage would force the publisher to import monitoring code, inverting the arrow; `XInfoReplies` is subscriber-extracted folding with three consumers (subscriber, replay, monitor) — root infrastructure, not monitoring. It crosses the boundary via a small public surface, fine inside a module no component may import. This split is organizational, not architectural — `BusMonitor` is as Redis-specific as the subscriber, so the modules.md litmus tests don't separate them; modules.md will say so. |
| Monitor is a pure reader of what it observes | Discovery via `SCAN … TYPE stream`; per-stream `XINFO STREAM`/`XINFO GROUPS`/`XLEN`/`XPENDING`; `INFO memory`. No group creation, no acks, no writes to any observed stream. `replay.*` skipped (reserved); `dlq.*` classified as DLQs, not streams | Same argument that made NEG-20's replay a pure reader: an observer that mutates group state can perturb the thing it measures. ADR 0002 §1 permits `SCAN` for tooling and forbids it only for production consumers — a monitor is tooling. The only thing the monitor ever writes is its own `Metric` events, and those go through the publisher like anyone else's. |
| Metric event encoding | Keep `Metric(name, value, owner)` exactly as is. Name grammar `bus.<area>.<measure>[.<stream>]` — fixed-vocabulary lowercase segments, the stream name (when present) appended verbatim as the tail. `owner` carries the party the number belongs to: the **consumer group** for lag/pending, the **event source** for feed latency, `bus` for infrastructure (rates, depth, DLQ, memory, publisher, monitor) | The tempting alternative — adding a tags map to `Metric` — is a schema-version bump on a frozen wire type to solve a problem the existing fields already solve: `owner` *is* the second dimension. Group names may contain dots (`oms.recovery`, ADR 0002 §3), so encoding the group into the dotted name would be unparseable; putting it in `owner` sidesteps that entirely. Stream names as the name's tail are unambiguous because the measure vocabulary is fixed-position. QuestDB queries stay trivial: `WHERE name LIKE 'bus.group.lag.%'`, series per `(name, owner)`. |
| Publish-rate source | Redis-side: `entries-added` delta between sweeps ÷ interval, from `XINFO STREAM`. Not publisher-side counters | `entries-added` is lifetime-monotonic (trimming does not move it) and authoritative across every publisher process — when feeds and strategies become separate JVMs, per-stream rate keeps meaning "what the bus ingested", not "what this process sent". Requires Redis ≥ 7.0; the compose image is 7.4. Publisher counters still exist, but they answer a different question (below). |
| Lag semantics | Per (group, stream), discovered from `XINFO GROUPS`: `bus.group.lag.<stream>` = undelivered (`lag` field) + pending (`XPENDING` count) — the same total `Subscription.lag()` reports; `bus.group.pending.<stream>` separately. When Redis reports `lag` as nil (trimming cut the undelivered range), emit `bus.group.lagUnknown.<stream>` = 1 and **no** lag sample — never 0 | NEG-19 already learned this the careful way: nil means *unknown*, and unknown reported as zero is how a drowning consumer looks healthy. A nil lag additionally means trimming has eaten entries the group never saw — that is not a gap in a chart, it is an event worth its own series. Emitting pending separately matters because pending ≠ behind: high pending with low lag is a stuck handler, high lag with low pending is a slow one — the two failure modes need different fixes. |
| Feed latency | Recorded **at publish time** in the publisher: `ingestedAt − occurredAt` per `source`, exact samples appended to a per-source buffer, drained by the monitor each sweep into exact p50/p90/p99/max + count (sort the drained samples; no histogram library, no bucketing) | The publisher is the one place every event passes with its envelope open — the monitor can't get per-event data from `XINFO`. Exact-and-uncapped is deliberate (NEG-17 principle: fidelity first, RAM is the lever), and the math says it's free: 8 bytes × 200 events/s nominal × 15 s ≈ 24 KB per interval, ~240 KB at a 10× burst. HdrHistogram would buy approximation and a dependency to avoid kilobytes. Internal components flow through the same recorder (their gap ≈ 0, keyed by their own `source`) — harmless, and cheaper than deciding which sources are "feeds". |
| Publisher error/buffer metrics | `PublisherStats` object owned by `RedisStreamsEventPublisher`, always on: `published`/`failed` interval counters (a failed future — rejected while disconnected, timed out, OOM `XADD` — increments `failed`), plus the existing `inFlight` gauge sampled at sweep time. Monitor takes the stats object at construction (both classes live in `bus`; no core interface grows) | The publisher's documented semantics are fail-fast and loud (REJECT_COMMANDS, 1 s timeout, noeviction OOM surfacing) — but today "loud" means a failed `CompletionStage` the caller sees and nobody counts. `failed > 0` is the memory-wall tripwire ADR 0002 §4 promises NEG-21 will watch. `inFlight` *is* the buffer metric: it is bounded by Lettuce's 4096 request queue, and a climbing gauge means Redis is slower than the publish rate. |
| Memory and window age (ADR 0002 duties) | Emit raw measures: `bus.redis.memoryUsedBytes` and `memoryMaxBytes` from `INFO memory`, and per-stream `bus.stream.oldestAgeSeconds` = now − `first-entry` ID millis. No threshold logic anywhere in this story | ADR 0002 says "NEG-21 alerts at 80% memory and when a tick stream's oldest entry is younger than its window"; the issue (later, narrower) says alerting is out of scope. Resolved in the issue's favor: this story emits the measures, and "80%" / "younger than window" become dashboard threshold expressions over them — pure config, exactly where a revisable number belongs. `first-entry` is the field that actually tracks `XTRIM` (the NEG-20 correction; `max-deleted-entry-id` never moves here). |
| DLQ monitoring | Depth: `bus.dlq.depth.<sourceStream>` = `XLEN dlq.<sourceStream>`, owner `bus`, for every discovered DLQ. Last-error surfacing: `XREVRANGE … COUNT 1` reads the frozen `DeadLetter` fields (`error`, `group`, `failedAt`) and surfaces them on the HTTP endpoint as a Prometheus info-metric (`bus_dlq_last_error{stream,group,error} 1`) — not as a `Metric` event | `Metric.value` is a `BigDecimal`; an exception string does not belong in it, and inventing a text-payload event type for this is a core change with one consumer. The info-metric pattern is the standard Prometheus answer for string facts, and the DLQ entry itself (bytes preserved, replayable) remains the durable record — the endpoint just makes the newest one visible without `redis-cli`. |
| Interim Grafana-readable form (the issue's "decide and note") | Prometheus text exposition over `com.sun.net.httpserver.HttpServer` (JDK built-in, zero dependencies), default port 9464, serving the **last completed snapshot** — a scrape never touches Redis. Names mechanically derived from the grammar: `bus.group.lag.<stream>` → `bus_group_lag{stream="…",group="…"}` | The real pipeline is already decided elsewhere: archiver → QuestDB (NEG-7) → Grafana's first-class Postgres-wire datasource — metrics arrive there for free because they are ordinary bus events. The interim surface therefore only needs to be standard and disposable. A log was rejected (needs Loki to be Grafana-readable — more infra than it saves); JSON was rejected (nonstandard, needs a plugin). Prometheus text is curl-able today, scrapeable the day a Prometheus exists, and ~60 lines to render. |
| Sweep interval and the `metrics` stream budget | Default **15 s**, configurable (`MonitorTuning`); integration tests run at ~100 ms. Consequence, computed: ~45 live streams at the initial 20-pair universe ⇒ ~300 Metric events/sweep ⇒ ~20 events/s ⇒ ADR 0002's 2M `MAXLEN` cap on `metrics` fills in ~28 h — *under* the stated 48 h guarantee. Amend the ADR 0002 §4 metrics row: window 48 h → 24 h, guarantee "≥24 h of metrics" (cap unchanged) | The guarantee as written silently fails the moment this story ships, so amend it honestly rather than discover it in production. This is exactly the "revisable configuration, not architecture" carve-out ADR 0002 defines — windows move, decisions don't. 24 h on the bus is generous for a transport window: the archiver puts every metric in QuestDB within seconds, and that is the permanent home (fidelity is preserved forever; only the shock-absorber window shrinks). 15 s keeps lag incidents visible in near-real-time; 60 s would fit 48 h but is too coarse to watch a consumer drown. |
| ADR 0003 | Small ADR: the name grammar, the owner conventions, the exposure decision, and what is frozen vs revisable — the **grammar and owner rules freeze** (metric names become permanent QuestDB data the moment the archiver lands — renaming a series orphans its history, same argument that froze `eventType`); the **metric inventory stays revisable** until NEG-13 builds dashboards on it | Without this, NEG-13 hardcodes whatever strings it finds and *that* becomes the accidental freeze. Writing down which half is contract and which half is inventory is one page and prevents the next rename from being a data-loss event nobody noticed. |

## Metric inventory (first edition — revisable per ADR 0003)

| Name | Value | Owner |
|---|---|---|
| `bus.stream.rate.<stream>` | events/s, `entries-added` delta ÷ interval | `bus` |
| `bus.stream.depth.<stream>` | `XLEN` | `bus` |
| `bus.stream.oldestAgeSeconds.<stream>` | now − `first-entry` ID | `bus` |
| `bus.group.lag.<stream>` | undelivered + pending | consumer group |
| `bus.group.pending.<stream>` | `XPENDING` count | consumer group |
| `bus.group.lagUnknown.<stream>` | 1 (emitted only when `lag` is nil) | consumer group |
| `bus.dlq.depth.<sourceStream>` | `XLEN dlq.<sourceStream>` | `bus` |
| `bus.publisher.published` / `bus.publisher.failed` | count this interval | `bus` |
| `bus.publisher.inFlight` | gauge at sweep time | `bus` |
| `bus.feed.latencyMillis.p50` / `.p90` / `.p99` / `.max` | exact percentile this interval | event source |
| `bus.feed.eventCount` | events recorded this interval | event source |
| `bus.redis.memoryUsedBytes` / `bus.redis.memoryMaxBytes` | `INFO memory` (`maxBytes` 0 = unlimited) | `bus` |
| `bus.monitor.sweepMillis` | sweep duration | `bus` |

## Package layout

```
bus/src/main/java/engine/bus/
├── PublisherStats.java              ← published/failed counters, per-source latency buffers; public drain()
├── RedisStreamsEventPublisher.java  ← + stats recording on the publish path
├── XInfoReplies.java                ← asFieldMap/asString extracted from the subscriber (shared folding)
└── monitor/                         ← depends on the root; the root never depends on it
    ├── MetricNames.java             ← the grammar as code: builders + fixed vocabulary (pure)
    ├── BusSnapshot.java             ← one sweep's readings as a value type (pure, unit-testable)
    ├── BusMonitor.java              ← scheduler: scan → read → snapshot → emit Metrics → hand to endpoint
    ├── MonitorTuning.java           ← interval, endpoint port, percentile set; standard() = 15 s / 9464
    └── MetricsEndpoint.java         ← JDK HttpServer, Prometheus text from the last snapshot

docs/adr/0003-bus-metric-names-and-exposure.md
```

## Step 1 — ADR 0003 and `MetricNames`

Write ADR 0003 (grammar, owner conventions, Prometheus-name derivation, frozen-vs-revisable split, the interim-exposure decision with the rejected alternatives) and amend the ADR 0002 §4 metrics row (48 h → 24 h guarantee, rationale one line, cap unchanged). Then `MetricNames`: static builders (`streamRate(stream)`, `groupLag(stream)`, `feedLatency(percentileToken)`, …) so no caller ever assembles a metric name by string concatenation — the `StreamNames` lesson applied to metrics. Unit tests pin every builder against hardcoded expected strings (anti-tautology rule).

**Verify:** `./gradlew :bus:test` green; ADR 0003 committed alongside.

## Step 2 — Publisher instrumentation: `PublisherStats`

`PublisherStats` with `LongAdder` counters (`published`, `failed`) and a per-source latency recorder: `record(source, latencyMillis)` appends to a growable long buffer per source; `drain()` atomically swaps buffers and returns the interval's samples plus counter deltas (double-buffer swap under a short lock — samples recorded mid-drain land in the next interval, none are dropped). Wire into `RedisStreamsEventPublisher.publish`: record latency and increment `published` on the success path of the existing `whenComplete`, `failed` on the error path *and* on the synchronous-throw path (both already funnel through the `inFlight` bookkeeping — the counters ride the same hooks). Expose `stats()`.

Unit tests on `PublisherStats` pure (record/drain cycles, drain-resets-interval, concurrent recording); publisher wiring lands in Step 6's integration tests.

**Verify:** `./gradlew build` green; existing NEG-18 integration tests untouched and green.

## Step 3 — Snapshot collection

Extract `asFieldMap`/`asString` from `RedisStreamsEventSubscriber` into `XInfoReplies` (pure refactor, subscriber delegates — one folding path for XINFO replies, third consumer incoming; public, not package-private, because `engine.bus.monitor` cannot see the root's package-private members). Then the read side of `BusMonitor.sweep()`:

- `SCAN … MATCH * TYPE stream` cursor loop; classify each key: `dlq.` prefix → DLQ list, `replay.` prefix → skip, else live stream.
- Per live stream: `XINFO STREAM` (`entries-added`, `first-entry` id, `length`), `XINFO GROUPS` (name, `lag` — nil-safe per the NEG-19 pattern), `XPENDING` summary per discovered group.
- Per DLQ: `XLEN` + `XREVRANGE … COUNT 1` folding the frozen `DeadLetter` fields.
- `INFO memory` → `used_memory`, `maxmemory`.

All of it folds into a `BusSnapshot` record (streams, groups, DLQs, memory, publisher drain, timestamp). Rate computation = pure function of (previous snapshot, current snapshot): `entries-added` delta ÷ elapsed; **negative delta ⇒ emit nothing for that stream this sweep** (key was deleted and recreated — test hygiene does this constantly). First sweep emits no rates (no baseline). Injected `Clock` throughout.

Unit tests against fabricated reply structures: classification, nil-lag handling, negative-delta suppression, `first-entry` age math, DLQ field folding.

**Verify:** `./gradlew :bus:test` green.

## Step 4 — Emission

`BusSnapshot` → `List<Event>`: one `Metric` per inventory row, built via `MetricNames`, `owner` per the conventions, envelope via `Event.of(monitorSource, null, sweepInstant, metric, clock)`, published through the injected `EventPublisher`. Percentiles: sort the drained per-source samples, exact index selection (`ceil(q × n) − 1`), emit p50/p90/p99/max + count; a source with zero samples this interval emits nothing (absence of traffic ≠ zero latency). `lagUnknown` emitted only when fired.

Unit tests: snapshot-in, expected `Metric` list out — hardcoded names, values, owners; percentile math pinned against hand-computed values for known sample sets (including n=1 and the p99-of-10-samples edge); zero-sample suppression.

**Verify:** `./gradlew :bus:test` green.

## Step 5 — `MetricsEndpoint`

JDK `HttpServer` serving `GET /metrics`: render the last completed `BusSnapshot` in Prometheus text exposition — grammar names mechanically converted (`bus.group.lag.<stream>` → `bus_group_lag{stream="…",group="…"}`, stream/owner demoted from name-tail/owner-field to labels), plus `bus_dlq_last_error{stream,group,error} 1` info-metrics with label values escaped per the exposition spec (backslash, quote, newline — DLQ error strings contain stack-trace newlines, this *will* fire). Port from `MonitorTuning`; port 0 supported with a bound-port getter (integration tests must not fight over 9464). No snapshot yet → 200 with only a comment line. A scrape never issues a Redis command — it reads a volatile reference.

Unit tests: rendering pinned against exact expected exposition text for a fabricated snapshot; escaping cases; the empty-snapshot response.

**Verify:** `./gradlew :bus:test` green; `curl localhost:9464/metrics` against a locally-run monitor shows the series.

## Step 6 — Integration tests (the acceptance criteria)

In `bus/src/integrationTest`, NEG-18/19/20 hygiene: test-only instruments, every stream (including `metrics` and `dlq.*`) tracked and `DEL`ed, monitor at ~100 ms interval, endpoint on port 0. Assertions read the monitor's output the honest way: subscribe to `Metric` events on the bus (a dedicated test group) — proving the dogfooding path end to end — with the endpoint asserted where it is the deliverable.

1. **Lag rises and recovers** (criterion 1) — synthetic producer publishing ticks continuously; a consumer whose handler sleeps; collect `bus.group.lag.<stream>` samples for its group and assert a strictly-increasing run; release the sleep, assert samples fall back to ~0. One test, both halves.
2. **DLQ depth observable** (criterion 2) — reuse the NEG-19 poison setup (always-throwing handler, `maxDeliveries` 2): assert `bus.dlq.depth.<stream>` goes absent→≥1 in the metric feed, and the endpoint's `bus_dlq_last_error` carries the thrown exception's class and the failing group.
3. **Feed latency percentiles** (criterion 3) — publish events whose `occurredAt` sits at fabricated offsets behind `ingestedAt` (10…100 ms, known distribution); assert emitted p50/p99/max per source equal the hand-computed exact values.
4. **Publisher failure counter** — publish against a stopped Redis (or an absurd payload timeout): `bus.publisher.failed` > 0 next sweep; recovery shows `published` resuming.
5. **Rates and window age** — publish n events in a measured window; `bus.stream.rate` within tolerance; `bus.stream.oldestAgeSeconds` grows sweep-over-sweep on a quiet stream.
6. **Pure-reader promise** — after a full monitoring cycle over streams with live groups: `XINFO GROUPS` on observed streams shows no monitor group, no new keys besides `metrics` (and the test's own), monitor `close()` leaves no connection (`CLIENT LIST`).

**Verify:** `docker compose up -d && ./gradlew :bus:integrationTest` green.

## Step 7 — Land it

Branch `luismarcosnegrao/neg-21-bus-observability-rates-consumer-lag-dead-letter-monitoring` (Linear's name), one commit per step.

1. Before each commit: `./gradlew spotlessApply build`.
2. Full sweep before the PR: `docker compose up -d && ./gradlew build :bus:integrationTest`.
3. Update `docs/modules.md`: the bus table gains `PublisherStats` and `XInfoReplies` rows, plus a short `engine.bus.monitor` sub-section for the five monitoring classes — stating explicitly that the subpackage boundary is organizational (one-way dependency on the root; nothing on the delivery path imports it), not architectural: the litmus tests don't distinguish monitor from subscriber, both are Redis-specific. The "never in bus" list is unaffected (nothing here is component-facing — the monitor is wired at composition-root time like the trimmer).
4. PR body: the metric inventory table verbatim, the ADR 0002 metrics-row amendment called out, and the lag-rise-and-recover test output.

## Definition of done (mapped to the issue)

- [ ] Slowed consumer's lag visibly increases and recovers in the metrics → Steps 3+4 (collection/emission) + Step 6.1.
- [ ] Dead-letter depth increments observable when the poison test runs → Step 3 (DLQ scan) + Step 6.2; last-error surfacing → Step 5 info-metric + Step 6.2.
- [ ] Feed latency percentiles per source derivable from emitted metrics → emitted *directly* (p50/p90/p99/max per source), Steps 2+4 + Step 6.3.
- [ ] Per-stream publish rate and per-group lag collected periodically (scope) → Step 3, `entries-added` delta and nil-safe lag.
- [ ] Publisher error/buffer metrics (scope) → Step 2 counters + `inFlight` gauge + Step 6.4.
- [ ] Metrics emitted as `Metric` events on the bus **and** Grafana-readable form decided and noted (scope) → Step 4 (dogfooding, asserted end-to-end in every Step 6 test) + Step 5 endpoint, decision recorded in ADR 0003.
- [ ] Threshold alerting out of scope (scope) → nothing in this story compares a reading to a limit; ADR 0002's "80%" duty resolved to raw-measure emission (decisions table).

## Pitfalls to expect

- **The monitor observes itself — bound the feedback.** The `metrics` stream is a live stream, so the monitor reports its rate, depth, and its consumers' lag; its own publishes also feed `PublisherStats` under the monitor's source. That is all correct and desired — but ~300 events/sweep is itself the dominant `metrics`-stream traffic, so the Step 6 rate test must use a dedicated data stream, not `metrics`, or the assertion chases its own tail.
- **`entries-added` resets on key deletion.** Integration tests `DEL` streams constantly; a recreated stream's counter restarts and the delta goes negative. Suppress the sample (Step 3), never clamp to zero — a fabricated zero is a lie in a rate series.
- **Nil `lag` is unknown, never zero.** Same trap NEG-19 dodged; it will try again here because the fabricated-reply unit tests make it easy to default the field. `lagUnknown` exists precisely so the honest answer has somewhere to go.
- **Prometheus label escaping will fire on day one.** DLQ error strings are multi-line stack traces with quotes in exception messages. Escape backslash → `\\`, quote → `\"`, newline → `\n` per the exposition spec, and pin it with a test whose input contains all three — an unescaped newline silently truncates the scrape for *every* series after it.
- **A scrape must never touch Redis.** The endpoint reads a volatile snapshot reference, nothing else. Wiring the handler to "just quickly XLEN" turns a Grafana refresh storm into bus load — the observer becoming the load is the classic monitoring failure.
- **Percentile buffer swap loses samples if done lazily.** Swap-under-lock with the recorder writing to whichever buffer is current; a drain that copies-and-clears the same list the hot path appends to drops whatever lands between copy and clear. Step 2's concurrency test exists to catch exactly this.
- **`maxmemory` can be 0.** The compose Redis may run unlimited; `memoryMaxBytes` 0 means "no limit", and any derived percentage must be dashboard-side and nil-safe. Emit both raw values, derive nothing.
- **Port 9464 collides in CI and parallel test runs.** Every integration test binds port 0 and asks the endpoint for its actual port. The fixed default is for production wiring only.
- **Sweep cost scales with stream count.** ~45 streams × 3–4 commands ≈ 150–200 round-trips per sweep — trivial at 15 s on localhost, but keep the commands on the monitor's own connection so a slow sweep queues behind nobody, and let `bus.monitor.sweepMillis` watch the watcher: if it ever approaches the interval, that metric is the early warning.
