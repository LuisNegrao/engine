# ADR 0003 — Bus metric names and exposure

- Status: Accepted
- Date: 2026-07-24
- Context: [NEG-21 — Bus observability: rates, consumer lag, dead-letter monitoring](https://linear.app/negraolu/issue/NEG-21/bus-observability-rates-consumer-lag-dead-letter-monitoring)

The bus reports on itself over itself: a `BusMonitor` sweeper reads Redis health
(rates, lag, DLQ depth, memory, window age) and publishes the readings as ordinary
`Metric` events through the same `EventPublisher` every component uses. This ADR
freezes how those metrics are *named* and how they are *exposed* for interim
Grafana consumption, and draws the line between what is frozen contract and what is
revisable inventory.

## Decision

1. **Metric names follow one grammar:** `bus.<area>.<measure>[.<stream>]` —
   fixed-vocabulary lowercase segments, with the observed stream name (when the
   measure is per-stream) appended verbatim as the tail.
2. **The three-field `Metric(name, value, owner)` is unchanged.** No tags map, no
   schema bump. `owner` carries the party the number belongs to; it is the second
   dimension.
3. **`owner` conventions:** the **consumer group** for lag/pending, the **event
   source** for feed latency, `bus` for infrastructure (rates, depth, DLQ, memory,
   publisher, monitor).
4. **Interim exposure is Prometheus text** over the JDK's built-in
   `com.sun.net.httpserver.HttpServer`, default port 9464, serving the last
   completed snapshot — a scrape never touches Redis.
5. **The grammar and owner rules freeze; the metric inventory stays revisable**
   until NEG-13 builds dashboards on it.

## 1. Name grammar

```
name    := "bus" "." area "." measure [ "." stream ]
area     := "stream" | "group" | "dlq" | "publisher" | "feed" | "redis" | "monitor"
measure  := [a-zA-Z]+                       (fixed per-area vocabulary)
stream   := the observed stream name, verbatim (may contain dots)
```

- Segments are fixed-vocabulary and fixed-position. The measure vocabulary is
  closed per area, so the parser knows exactly where the measure ends and the
  stream tail begins — the stream name may itself contain dots
  (`md.tick.trade.BTC-USDT.BINANCE`) and this stays unambiguous only *because* the
  measure position is fixed.
- `measure` uses lowerCamel where a measure is multi-word (`oldestAgeSeconds`,
  `lagUnknown`, `memoryUsedBytes`, `latencyMillis`, `sweepMillis`, `eventCount`,
  `inFlight`) — the unit rides in the name so the dashboard never guesses.
- **No caller assembles a name by string concatenation.** `MetricNames` in
  `engine.bus.monitor` is the single source of truth: typed builders
  (`streamRate(stream)`, `groupLag(stream)`, `feedLatency(percentile)`, …) and
  constants for the unpartitioned names. This is the `StreamNames` lesson (ADR
  0002 §3) applied to metrics: a name is data, not a string literal scattered
  across the codebase.

### Why not a tags map on `Metric`

The tempting alternative — adding `Map<String,String> tags` to `Metric` — is a
schema-version bump on a frozen wire type (ADR 0002 froze `eventType` and the wire
shape) to solve a problem the existing fields already solve. `owner` *is* the
second dimension. Consumer-group names may contain dots (`oms.recovery`, ADR 0002
§3), so encoding the group into the dotted name would be unparseable; putting it in
`owner` sidesteps that entirely.

## 2. Owner conventions

`owner` names the party a number belongs to, which is exactly the series
identity a query groups by:

| measure kind | `owner` | example |
|---|---|---|
| lag, pending, lagUnknown | the **consumer group** | `archiver`, `strategy-momentum` |
| feed latency percentiles, event count | the **event source** | `binance`, `oms` |
| rates, depth, oldest age, DLQ depth, memory, publisher, monitor | `bus` | `bus` |

QuestDB queries stay trivial: a lag chart is `WHERE name LIKE 'bus.group.lag.%'`,
one series per `(name, owner)` — the stream in the name, the group in the owner.

## 3. Prometheus-name derivation

The interim endpoint (§4) converts grammar names to Prometheus exposition
mechanically, demoting the name-tail stream and the `owner` field to labels:

```
bus.group.lag.md.tick.trade.BTC-USDT.BINANCE   owner=archiver
  → bus_group_lag{stream="md.tick.trade.BTC-USDT.BINANCE",group="archiver"} <value>
```

- Dots in the fixed prefix become underscores; the stream tail becomes a
  `stream="…"` label; `owner` becomes the label whose key names the party
  (`group`, `source`, or omitted when `owner` is `bus`).
- DLQ last-error is a Prometheus **info-metric** — `bus_dlq_last_error{stream,
  group,error} 1` — not a `Metric` event: `Metric.value` is a `BigDecimal` and an
  exception string does not belong in it. Label values are escaped per the
  exposition spec (backslash → `\\`, quote → `\"`, newline → `\n`); DLQ error
  strings are multi-line stack traces, so this escaping is load-bearing, not
  theoretical.

## 4. Interim exposure: Prometheus text

Metrics are ordinary bus events, so their permanent path is already decided
elsewhere: archiver → QuestDB (NEG-7) → Grafana's first-class Postgres-wire
datasource. The interim surface therefore only needs to be **standard and
disposable**, and Prometheus text over the JDK `HttpServer` (zero dependencies) is
exactly that: curl-able today, scrapeable the day a Prometheus exists, ~60 lines to
render, and — critically — a scrape reads a volatile snapshot reference and issues
**no Redis command**, so a Grafana refresh storm never becomes bus load.

Rejected alternatives:

- **A log line** — needs Loki to be Grafana-readable, more infra than it saves.
- **JSON endpoint** — nonstandard, needs a Grafana plugin; buys nothing over the
  Prometheus text format that every scraper already speaks.

Default port **9464**; production wiring uses it, but every integration test binds
port 0 and asks the server for its actual port (9464 collides in CI and parallel
runs).

## 5. Frozen vs revisable

- **Frozen (contract):** the grammar (§1), the owner conventions (§2), and the
  Prometheus derivation (§3). Metric names become permanent QuestDB data the moment
  the archiver lands — renaming a series orphans its history, the same argument
  that froze `eventType` in ADR 0002. Revisiting any of these requires a new ADR.
- **Revisable (inventory):** *which* metrics exist. The first-edition inventory
  (below) can gain, drop, or retune measures as NEG-13 builds dashboards, so long
  as every name obeys the frozen grammar. Writing this split down now prevents
  NEG-13 from hardcoding whatever strings it finds and making *that* the accidental
  freeze.

### Metric inventory (first edition — revisable)

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

## Consequences

- NEG-21 implements `MetricNames` as the grammar-in-code and emits the inventory
  above; NEG-13 (dashboards) reads these names and owners as a contract, and
  computes thresholds (ADR 0002's "80% memory", "oldest younger than window") as
  dashboard expressions over the raw measures — this story emits measures, never
  compares them to limits.
- The ADR 0002 §4 `metrics` retention row is amended (48h → 24h guarantee, cap
  unchanged): the monitor's own ~20 events/s fills the 2M cap in ~28h, and QuestDB
  is the permanent home anyway. Revisable configuration, not architecture.
- The grammar, owner conventions, and Prometheus derivation are frozen; the metric
  inventory is revisable until NEG-13. Renaming a frozen series is a data-loss
  event once the archiver lands, not a refactor.
