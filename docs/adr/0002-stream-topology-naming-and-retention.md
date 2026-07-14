# ADR 0002 — Stream topology, naming and retention

- Status: Accepted
- Date: 2026-07-14
- Context: [NEG-17 — Design stream topology, naming conventions and retention policy](https://linear.app/negraolu/issue/NEG-17/design-stream-topology-naming-conventions-and-retention-policy)

## Decision

Redis Streams topology for the message bus:

1. **Market data is partitioned per instrument, per event type** — one stream per
   (event type, `SYMBOL.VENUE`) pair, e.g. `md.tick.trade.BTC-USDT.BINANCE`.
2. **Control and execution use single streams** — `orders.intents`, `orders.fills`,
   `commands`, `metrics`, `signals`.
3. **One naming grammar** for every stream, consumer group and dead-letter stream,
   with the mapping table below as the single source of truth.
4. **Ticks are published raw** — no conflation. The bus retains a bounded window
   (target **≥12h** for ticks); the Historical Data Store (NEG-7) is the only
   permanent archive. Trimming is age-based (`XTRIM MINID`) with entry-count caps
   as runaway protection.
5. **Ordering is guaranteed within a stream and nowhere else.**

## 1. Stream granularity: per instrument, per event type

Redis has no server-side filtering *within* a stream: a subscriber receives and
deserializes every entry. With a market-data firehose, a strategy watching 3 of 100
instruments would pay for the other 97. Partitioning per instrument makes
subscription itself the filter.

- **Streams are cheap.** Each stream is an ordinary Redis key; at 100+ instruments
  × a handful of event types we hold a few hundred keys, which Redis is indifferent
  to. A single `XREAD`/`XREADGROUP` call can block on many streams at once.
- **Per event type too, not just per instrument.** Trade and quote ticks are
  distinct payload types with distinct consumers (ADR 0001); interleaving them in
  one stream would reintroduce client-side filtering. Bars additionally carry the
  interval in the stream name — a 5m strategy never sees 1m bars.
- **Per-venue partitioning was rejected** as a worst-of-both middle ground: it
  neither bounds what a subscriber must filter nor reduces the number of concepts.

The trade-off, accepted knowingly: Redis Streams has no pattern-subscribe (no
`PSUBSCRIBE` equivalent), so a subscriber names its streams explicitly. The
subscriber abstraction (NEG-19) takes an explicit instrument/stream list; tooling
may discover streams via `SCAN` on a prefix, but production consumers never should.

## 2. Control and execution streams: single streams

`orders.intents`, `orders.fills`, `commands`, `metrics` and `signals` are each one
stream, not partitioned:

- **Risk correctness requires a total order over all order intents.** Position and
  exposure limits are portfolio-level; the Risk Manager must see intents in one
  well-defined sequence across all instruments and strategies. Only a single stream
  gives that (see §5 — there is no ordering across streams).
- The same holds for the OMS observing `orders.fills`, and for `commands`
  (a `KILL` must not race a `START` on another partition).
- **Volume is trivial** next to ticks, so the filtering argument from §1 does not
  apply. `signals` starts as a single stream for the same reason; if signal volume
  ever grows to tick-like rates, splitting it per instrument is a config-and-grammar
  amendment, not an architectural change.

## 3. Naming grammar

```
stream := path [ "." SYMBOL "." VENUE ]
path   := segment ("." segment)*        segment := [a-z0-9]+
```

- `path` segments are lowercase alphanumeric, dot-separated.
- Instrument-partitioned streams end in exactly `InstrumentId.toString()` —
  uppercase, venue-qualified, e.g. `BTC-USDT.BINANCE`. Symbols may contain `-` but
  never `.`, so the instrument is always the last two dot-segments when present.
- Stream names are **transport addresses**; wire `eventType` strings
  (`PayloadRegistry`) are **payload discriminators**. They are related but not
  derived from each other (e.g. eventType `order.intent` → stream
  `orders.intents`). The table below is the single source of truth; NEG-18 turns it
  into a `StreamNames` class. Never derive stream names by string-mangling
  eventTypes at runtime — a rename would become silent data loss.

### Stream table (every event type in the model)

| eventType | Stream | Partitioning | Example |
|---|---|---|---|
| `tick.trade` | `md.tick.trade.{SYMBOL}.{VENUE}` | per instrument | `md.tick.trade.BTC-USDT.BINANCE` |
| `tick.quote` | `md.tick.quote.{SYMBOL}.{VENUE}` | per instrument | `md.tick.quote.ETH-USDT.BINANCE` |
| `bar` | `md.bar.{interval}.{SYMBOL}.{VENUE}` | per interval + instrument | `md.bar.1m.BTC-USDT.BINANCE` |
| `signal` | `signals` | single | `signals` |
| `order.intent` | `orders.intents` | single | `orders.intents` |
| `fill` | `orders.fills` | single | `orders.fills` |
| `metric` | `metrics` | single | `metrics` |
| `command` | `commands` | single | `commands` |

**Bar intervals** use a fixed vocabulary mapping to the `Duration` in the `Bar`
payload: `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. Adding an interval means adding it
here first.

### Consumer groups and dead-letter streams

- **Consumer group** names are the kebab-case component name, optionally suffixed
  with a purpose: `risk-manager`, `archiver`, `strategy-momentum`,
  `oms.recovery`. One component = one group name, reused across every stream it
  consumes (groups are per-stream in Redis; the *name* is what stays uniform).
- **Dead-letter streams** (poison messages parked by NEG-19, monitored by NEG-21)
  are the consumed stream prefixed with `dlq.`: `dlq.orders.intents`,
  `dlq.md.tick.trade.BTC-USDT.BINANCE`. The failing consumer group is recorded in
  the dead-lettered entry, not the stream name.
- **Reserved prefix:** `replay.` is reserved for the replay story (NEG-20), so
  replayed history can never be confused with, or pollute, live streams.

## 4. Retention and trimming

**Principle: the bus is a transport with a bounded window; the Historical Data
Store (NEG-7, QuestDB) is the only permanent archive.** Nothing ever queries the
bus as history (§6).

### Raw ticks, no conflation

Quotes are published **raw** — every top-of-book update, not a rate-capped
snapshot. Conflation would permanently limit the archive (the archiver consumes
from the bus, so whatever the bus loses, history loses) and thereby foreclose
future strategies sensitive to fine quote dynamics. We deliberately pay for that
optionality in RAM, and **RAM is the designated scaling lever**: the budget starts
at 4 GB and grows before any data-fidelity decision is revisited.

### Sizing (the math behind the numbers)

Measured with the NEG-16 codec on fully-populated sample events: `TradeTick` 297 B,
`QuoteTick` 330 B on the wire; with Redis stream overhead we budget **~390/430 B
per entry in memory**. Assumed nominal rates for the initial universe of **20
Binance pairs** (to be validated by the NEG-22 harness): quotes average 8/s per
instrument (majors 20–60/s, long tail 1–5/s), trades average 2/s.

| | rate | bytes/s | 12h |
|---|---|---|---|
| quotes, 20 pairs | 160/s | ~69 KB/s | ~3.0 GB |
| trades, 20 pairs | 40/s | ~16 KB/s | ~0.7 GB |
| everything else (bars, orders, signals, metrics, commands) | — | — | < 0.3 GB |
| **total** | | ~85 KB/s | **~4.0 GB** |

Consequences, stated plainly:

- At the initial **4 GB** budget, 12h of raw ticks fits **at nominal rates with no
  burst headroom**. Volatility spikes run 3–10× nominal for hours; during such
  periods the effective window compresses below 12h (the caps and memory wall
  below decide what gives).
- The 12h guarantee holds *comfortably* at **≥8 GB**. Growing RAM is the accepted
  remedy, per the decision above — not shrinking data.
- At 100+ instruments, tick memory scales ~5×: same windows need ~16–20 GB, or
  windows shrink proportionally. Nothing else in this design changes.

### Mechanism

Three layers, from contract to backstop:

1. **Age-based sweep (the contract).** A trimmer task (owned by the bus module,
   implemented alongside NEG-18) runs every 60 s and issues
   `XTRIM <stream> MINID ~ <now - window>` per stream class. Stream IDs are
   millisecond timestamps, so MINID *is* age. Always approximate (`~`) trimming —
   exact trimming fights the radix-tree macro-nodes for no benefit.
2. **Per-stream entry cap (runaway protection).** Publishers pass
   `MAXLEN ~ <cap>` on `XADD`, sized ≈2× the busiest stream's nominal window count
   (table below). This bounds any single stream if the sweeper dies or a feed runs
   away; it is not the retention contract.
3. **Memory wall (last resort).** Redis runs `maxmemory 4gb` (grows with the
   budget), `maxmemory-policy noeviction`. On OOM, `XADD` fails and the publisher
   surfaces the error — the bus fails loud rather than silently evicting arbitrary
   keys. NEG-21 alerts at 80% memory and when any tick stream's oldest entry is
   younger than its guaranteed window.

### Retention table

| Stream class | Window (MINID sweep) | MAXLEN cap / stream | Guarantee |
|---|---|---|---|
| `md.tick.quote.*` | 12h | 2,000,000 | ≥12h of raw quotes at nominal rates |
| `md.tick.trade.*` | 12h | 1,000,000 | ≥12h of raw trades at nominal rates |
| `md.bar.*` | 14d | 50,000 | ≥14d of bars |
| `signals` | 14d | 500,000 | ≥14d of signals |
| `orders.intents` | 30d | 1,000,000 | ≥30d of intents |
| `orders.fills` | 30d | 1,000,000 | ≥30d of fills |
| `metrics` | 48h | 2,000,000 | ≥48h of metrics |
| `commands` | 30d | 100,000 | ≥30d of commands |

Windows are per-class configuration, not architecture: revising them (e.g. after a
RAM upgrade) amends this table and a config value, nothing more. Order/command
windows are generous because volume is trivial and they are the audit trail of
record *on the bus* — but the store remains the permanent copy.

## 5. Ordering guarantees

**Guaranteed: total order within one stream.** `XADD` is serialized per key and
stream IDs are strictly monotonic. Every consumer of `orders.intents` sees every
intent in the same order; likewise for each individual market-data stream.

**Not guaranteed: any ordering across streams.** Explicitly including:

- trade vs quote ticks of the *same instrument* (separate streams per ADR 0001) —
  a consumer needing both correlates on `occurredAt` and accepts jitter;
- the same event type across *different instruments*;
- `orders.intents` vs `orders.fills` — a fill can be observed before the intent
  that caused it by a consumer reading both streams.

Two corollaries that downstream stories must not violate:

- **Risk/OMS correctness depends on §2.** Partitioning `orders.intents` would
  silently destroy the total order that pre-trade checks rely on. Any future
  sharding of execution streams requires revisiting this ADR.
- **Windows and replay run on ingestion time, not event time.** Stream IDs (and
  therefore trimming and `XRANGE`) reflect *arrival*; `occurredAt` is stamped at
  the source. After a feed outage and reconnect, entries with old `occurredAt`
  values sit inside the retention window at recent stream IDs. Backtests order by
  `occurredAt` (from the store); bus-side consumers order by stream ID.

## 6. Archive and replay boundary (consistency with NEG-5 / NEG-7)

- **The archiver is an ordinary bus consumer** — consumer group `archiver`
  subscribed to every stream class in §3's table — writing everything to QuestDB
  (NEG-7). It gets no side channel; if the bus loses data before the archiver
  reads it, history loses it (this is why the memory wall fails loud).
- **QuestDB is the only permanent archive.** Anything older than a stream's window
  exists *only* in the store: no overlap, no gap. Because delivery is
  at-least-once (NEG-19), the archiver may write duplicates; deduplication on
  `eventId` is the store's concern (NEG-7), not the bus's.
- **Replay (NEG-20) always sources from the store**, never from bus `XRANGE`. One
  code path, no window-edge cases; the same replay works for yesterday and for two
  years ago. Replayed events flow under the reserved `replay.` prefix so they can
  never contaminate live streams or the archiver. (Serving very recent ranges from
  the bus is a possible future optimization, owned by NEG-20 — not part of this
  design.)
- The bus window's actual purpose: late-joining or restarted live consumers
  catching up via their consumer group's pending/last-delivered position, and
  operational inspection. It is a shock absorber, not a database.

## Consequences

- NEG-18 (publisher) implements the stream table as a `StreamNames` mapping, the
  `MAXLEN ~` caps, and the trimmer task; NEG-19 (subscriber) takes explicit stream
  lists, creates consumer groups idempotently, and parks poison messages on
  `dlq.*`; NEG-21 monitors memory, per-stream window age, and `dlq.*` depth.
- The wire `eventType` strings and the stream names in §3 are frozen — renaming
  either is a data-loss event, not a refactor.
- Every retention number is revisable configuration; the *decisions* (raw ticks,
  per-instrument partitioning, single execution streams, store-only replay) are
  the architecture. Revisit this ADR if any of those four change.
