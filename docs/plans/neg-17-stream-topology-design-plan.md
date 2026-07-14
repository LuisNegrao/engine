# NEG-17 — Stream Topology, Naming and Retention: Design Plan

Plan for [NEG-17](https://linear.app/negraolu/issue/NEG-17/design-stream-topology-naming-conventions-and-retention-policy). This is a design story: the deliverable is **ADR 0002** (`docs/adr/0002-stream-topology-naming-and-retention.md`), no production code. It gates NEG-18 (publisher) and NEG-19 (subscriber), which will implement whatever this ADR says — so every name and number in it is effectively frozen once those stories start.

## The one constraint that shapes everything

**Redis Streams live in RAM.** Retention windows are memory budgets, not disk budgets. Back-of-envelope: 100 instruments × ~20 events/s average (quotes dominate) × ~300 B/event ≈ 600 KB/s ≈ **50 GB/day** — the issue's illustrative "bus retains ≥24h of ticks" does not survive that math on a single machine. The ADR must do this sizing explicitly and set per-class windows the hardware can actually hold, with the Historical Data Store (NEG-7) as the *only* permanent archive. Expect the honest answer to be something like: quotes hours, trades ~a day, bars/orders/fills weeks.

## Recommended answers to the five design questions

These are the positions the ADR should take (with rationale written out); the ADR is where they get argued and locked.

| Question | Recommendation | Why |
|---|---|---|
| Market-data granularity | **Per instrument, per event type**: one stream per (event type, SYMBOL.VENUE) | Redis has no server-side filtering *within* a stream — a firehose makes a strategy watching 3 of 100 instruments deserialize 97% waste. Streams are cheap keys (thousands are fine; `XREAD` blocks on many keys in one call), and per-instrument replay is the natural backtest unit. Per-venue is a worst-of-both middle ground. |
| Trade vs quote streams | **Separate streams**, `md.tick.trade.*` and `md.tick.quote.*` | Direct consequence of ADR 0001: separate payload types, and consumers rarely want them interleaved. Bars likewise get the interval in the name (`md.bar.1m.*`) so a 5m strategy never filters 1m bars client-side. |
| Control/execution streams | **Confirm single streams**: `orders.intents`, `orders.fills`, `commands`, `metrics`, and `signals` | Risk checks are portfolio-level: the Risk Manager needs a total order over *all* intents, which only a single stream gives (ordering across streams doesn't exist). Volumes are tiny next to ticks, so the filtering-cost argument doesn't apply. `signals` starts as one stream (low volume, sentiment-style); split per instrument later only if volume forces it. |
| Naming grammar | One rule: `{context}.{type...}[.{SYMBOL}.{VENUE}]` — lowercase dotted segments; instrument suffix is exactly `InstrumentId.toString()` (uppercase, symbol may contain `-`, never `.`) | Boring and mechanically parseable: instrument = last two segments when present. Stream names are transport addresses, wire `eventType` strings are payload discriminators — related but **not identical** (e.g. eventType `order.intent` → stream `orders.intents`); the ADR's mapping table is the single source of truth, and NEG-18 turns it into a `StreamNames` class. |
| Retention | **Age-based `XTRIM MINID`** sweeper per stream class (stream IDs are ms timestamps, so MINID *is* age), plus a hard `MAXLEN ~` safety cap at publish time | The contract is "≥ N hours replayable", which MAXLEN can't express; MINID can. The cap bounds RAM if the sweeper dies. Always approximate (`~`) trimming — exact trimming fights the radix-tree macro-nodes for no benefit. |
| Ordering | Guaranteed: **total order within one stream** (IDs are monotonic, XADD is serialized per key). Not guaranteed: anything across streams — trade vs quote of the same instrument, two instruments, intents vs fills | Spell out the consequences: risk/OMS correctness *depends* on the single-intents-stream decision; a strategy merging trades+quotes correlates on `occurredAt` itself; replay order = stream-ID (ingestion) order, not `occurredAt` order. |

## Worked naming table (acceptance criterion: every event type in the model)

All 8 registered eventTypes from `PayloadRegistry.standard()` must appear with a concrete stream example:

| eventType | Stream name (example) | Partitioning |
|---|---|---|
| `tick.trade` | `md.tick.trade.BTC-USDT.BINANCE` | per instrument |
| `tick.quote` | `md.tick.quote.BTC-USDT.BINANCE` | per instrument |
| `bar` | `md.bar.1m.BTC-USDT.BINANCE` | per interval + instrument |
| `signal` | `signals` | single |
| `order.intent` | `orders.intents` | single |
| `fill` | `orders.fills` | single |
| `metric` | `metrics` | single |
| `command` | `commands` | single |

The grammar section should also name two things the sibling stories will otherwise invent ad hoc:
- **Consumer groups**: `{component}` or `{component}.{purpose}` (e.g. `risk-manager`, `archiver`) — NEG-19 needs this on day one.
- **Dead-letter streams**: `dlq.{stream}` (e.g. `dlq.orders.intents`) — NEG-21 monitors these; naming them now closes the loop.

Bar-interval tokens need a spelled-out vocabulary (`1m`, `5m`, `1h`, `1d`) with the mapping to the `Duration` carried in the `Bar` payload.

## Steps

### Step 1 — Sizing appendix first

Estimate events/s and bytes/event per stream class (measure a real encoded `TradeTick`/`QuoteTick` from the NEG-16 codec — don't guess bytes). Derive RAM per hour of retention per class. This paragraph decides the retention numbers; writing it first stops the ADR promising windows the box can't hold. Consider whether quote **conflation** (publish top-of-book at most every X ms) belongs in the design now or is explicitly deferred to the feed handler.

**Verify:** the chosen windows × the sizing math fit inside a stated RAM budget with ≥2× headroom.

### Step 2 — Draft ADR 0002

Same shape as ADR 0001 (Status/Date/Context header, Decision, Rationale, Consequences), one section per design question, ending with the worked naming table and a retention table (stream class → window → mechanism → cap). State the replay-window guarantee per class in one quotable sentence each (e.g. "`md.tick.trade.*` retains ≥ N hours").

### Step 3 — Consistency review against NEG-5 replay and NEG-7 (acceptance criterion)

The ADR must draw the archive boundary with no overlap or gap:
- The **archiver** is a bus consumer like any other; QuestDB (NEG-7) is the only permanent store. The bus is a transport with a bounded window — never queried as history.
- **Replay (NEG-20)** sources from the store, full stop; the bus window exists for late-joining live consumers and crash recovery, not for backtests. (The alternative — replay from `XRANGE` when inside the window — is an optimization NEG-20 may add; the ADR should say whose job that is.)
- Check NEG-18/NEG-19 descriptions for anything that assumes a different topology; flag mismatches on the Linear issues rather than silently absorbing them.

### Step 4 — Land it

Commit the ADR (plus this plan if not already committed) on a `neg-17` branch, PR, and tick the three acceptance boxes on NEG-17. No code changes, so CI is trivially green — the review *is* the verification.

## Definition of done (mapped to the issue)

- [ ] ADR 0002 committed, covering all five questions with rationale → Steps 1–2.
- [ ] Reviewed against NEG-5 replay and NEG-7 for archive/replay overlap or gap → Step 3.
- [ ] Naming grammar has a worked example for **every** eventType in `PayloadRegistry.standard()` → the 8-row table above, copied into the ADR.

## Pitfalls to expect

- **No pattern-subscribe on streams.** Unlike pub/sub `PSUBSCRIBE`, a consumer can't subscribe to `md.tick.trade.*` — per-instrument topology means NEG-19's subscriber must take an explicit instrument list (or discover streams via `SCAN` on the prefix). The ADR should acknowledge this cost; it's the price of cheap filtering and it's fine, but it must be a stated decision.
- **Consumer groups are per stream.** Per-instrument streams multiply group bookkeeping (create-if-absent on every stream). Boring, but NEG-19 needs to know it's expected.
- **Don't derive stream names by string-mangling eventTypes at runtime.** The mapping is a static table (ADR now, `StreamNames` class in NEG-18). Runtime derivation turns a rename into silent data loss.
- **`occurredAt` vs stream-ID time.** Trimming and replay windows are in *ingestion* time (stream IDs); a feed outage then reconnect can put old `occurredAt` values inside the window. Say so in the ordering section — backtests care.
- **Number of streams is not the risk; RAM is.** Don't spend ADR space defending 400 stream keys (Redis is indifferent); spend it on the sizing appendix.
