# ADR 0004 — Instrument model and feed architecture

- Status: Accepted
- Date: 2026-08-02
- Context: [NEG-23 — Design the instrument model and feed architecture](https://linear.app/negraolu/issue/NEG-23/design-the-instrument-model-and-feed-architecture)
- Evidence: [NEG-29 Binance field-availability appendix](../plans/epic-neg-6/neg-29-binance-field-availability-appendix.md) — every field below traces to a row of its table.

## Decision

The instrument model and the data-feed architecture every story in the Data Feeds
epic implements:

1. **Instrument reference data is a composed model** — a flat `Instrument` record
   of *universal* trading constraints plus a sealed `InstrumentSpec` component
   carrying what varies by asset class. v1 ships one spec, `CryptoSpotSpec`.
2. **Reference data is fetched from venue metadata at startup**, validated against
   the configured universe and held in memory. It is **not published on the bus** —
   there is no `instrument` eventType in v1.
3. **One canonical symbol grammar, per asset class** — uppercase, `-` legal, `.`
   illegal (inherited from ADR 0002 §3). Crypto spot is `BASE-QUOTE`. The
   venue↔canonical mapping is an explicit bimap built at startup from venue data,
   never string-guessed at runtime.
4. **The feed adapter does exactly one thing**: venue websocket → normalized
   `TradeTick`/`QuoteTick` published via `EventPublisher`. Bar aggregation is an
   ordinary bus consumer, not adapter code, with windows wall-clock-aligned on
   `occurredAt` and no bar published for an empty window.
5. **Timestamp assignment is fixed**: `occurredAt` = the venue's event timestamp,
   `ingestedAt` = stamped at normalization, `source` = the feed component name.
   Negative feed latency is reported, never clamped.
6. **Connection liveness and per-instrument data staleness are separate signals.**
   A dead connection always reconnects; a silent instrument never does.
7. **The subscription universe is committed static config**, validated at startup
   against venue metadata; an unknown symbol fails the process loud. Hot-add is
   not v1.
8. **Publishing raw onto the bus *is* the archival path** — ADR 0002 §6 reaffirmed,
   unchanged.

## 1. Instrument reference model

An `Instrument` describes a tradable thing: what it is, and the venue's rules for
sizing and pricing an order in it. Identity is already solved — `InstrumentId` is
venue-qualified and asset-class-agnostic (NEG-16) — so this section decides
*description* only.

**The shape is composed: flat where the data is universal, sealed where it varies.**

```java
public record Instrument(
        InstrumentId instrumentId,
        BigDecimal priceTickSize,       // universal venue trading constraints
        BigDecimal quantityStepSize,
        BigDecimal minQuantity,
        BigDecimal maxQuantity,         // LIMIT-order bound — see below
        BigDecimal minNotional,         // nullable → maybeMinNotional()
        BigDecimal maxNotional,         // nullable → maybeMaxNotional()
        InstrumentSpec spec) {}         // what actually varies by asset class

public sealed interface InstrumentSpec {
    AssetClass assetClass();
    String settlementCurrency();
}

public record CryptoSpotSpec(String base, String quote) implements InstrumentSpec {
    public AssetClass assetClass() { return AssetClass.CRYPTO; }
    public String settlementCurrency() { return quote; }
}
```

`EquitySpec` and `FutureSpec` are added to the `permits` list when Interactive
Brokers lands. Their *fields* are deliberately not guessed today.

**Why the universal half is genuinely universal.** The NEG-29 spike measured what
Binance publishes: price tick size, quantity step size, min/max quantity, min/max
notional — all populated, for all five sample symbols, all as `BigDecimal`. These
are not crypto concepts that happen to fit elsewhere. NASDAQ publishes a tick size
(SEC Rule 612: $0.01 above $1.00), CME publishes 0.25 index points for ES, and
Interactive Brokers' own `ContractDetails` carries `minTick`/`minSize`/
`sizeIncrement` for every asset class it supports while keeping strike, expiry and
multiplier on `Contract`. The split this ADR draws is the industry's, not ours.

**Why the varying half must not sit on the record.** `baseCurrency`/`quoteCurrency`
is a *pair* concept. It is meaningful for crypto and FX and meaningless for
`AAPL.NASDAQ` and `ESZ26.CME`, which have a settlement currency and no base at all.
Leaving those two components on the universal record would harden a crypto
assumption into the type that every future asset class must then null out — the
precise failure this epic exists to prevent.

### Rejected: a flat record with nullable per-asset-class extras

The original proposal (NEG-23 plan) was one flat record with `baseCurrency`,
`quoteCurrency` and nullable extras (`contractMultiplier`, `expiry`) reserved for
futures. It is rejected because the nullability grows without bound: options alone
add strike, right and underlying, and every one of them is `null` for the 1371
Binance spot instruments we actually have. The composed model has *fewer*
speculative nulls than the flat one — `contractMultiplier` and `expiry` become
`FutureSpec` components that simply do not exist yet, rather than permanent nulls
on every crypto instrument.

### Rejected: sibling `CryptoInstrument` / `EquityInstrument` types

A full sealed hierarchy at the top level is rejected because it inflicts asset-class
dispatch on consumers that do not need it. The NEG-29 consumer inventory splits
cleanly: risk fat-finger checks and OMS rounding need **only** the universal
constraints, and under a sibling hierarchy they would have to `switch` on asset
class to reach a tick size whose meaning is identical in every branch. Position and
P&L accounting is the one consumer that genuinely varies, and composition gives it
exhaustive pattern matching over `spec` without taxing everyone else.

This is not a speculative hierarchy. v1 ships exactly one `permits` entry, designed
against measured data. `EquitySpec` is a *shape* reservation, not a field guess, and
adding it later changes no component of `Instrument`, no `eventType` and no stream
name — so ADR 0002's data-loss rule never comes near it.

### Placement, and two rules the spike forced

- **`Instrument`, `InstrumentSpec`, `AssetClass` live in `core`** (`engine.core.event`).
  Two-plus components must agree on the type — risk reads tick size, OMS reads step
  size and notional bounds, feeds populate all of it — which is modules.md litmus
  test 2. Everything that touches XChange's `ExchangeMetaData` lives in `feeds`
  (litmus test 3: imports a venue library). NEG-24 does not need to relitigate this.
- **Reference-data `BigDecimal`s are stored exactly as the venue supplied them and
  rendered with `toPlainString()`, never `toString()`.** XChange returns values
  `stripTrailingZeros()`'d, which yields negative scale for round numbers: Binance's
  `maxQty=9000.00000000` arrives as `9E+3`, and `toString()` emits `"9E+3"`. A
  quantity cap that prints in scientific notation into a log, a config file or an
  order payload is a production incident waiting for its first round-numbered
  instrument.
- **`maxQuantity` is the limit-order bound.** Binance publishes a separate,
  liquidity-derived `MARKET_LOT_SIZE` — 152.84 BTC against `LOT_SIZE`'s 9000 for
  `BTCUSDT`, a 59× difference that moves during the day. It is not reference data
  and is not modeled; a market-order path that needs it reads it live (NEG-9/NEG-10).

## 2. Population and distribution

**Reference data is fetched from the venue at startup, validated against the
configured universe, and held in memory by the feeds process.**

**It is not published on the bus in v1.** No `instrument` eventType is registered.
ADR 0002's consequences freeze wire `eventType` strings once published — renaming
one is a data-loss event — so registering a type today, for zero current consumers,
buys a permanent obligation and nothing else. When risk or OMS need reference data
in their own processes, distributing it becomes *their* story and amends this ADR.
The door is reserved; we do not walk through it.

**Rejected: hand-maintained static config of tick sizes and lot sizes.** It drifts
from the venue silently, and the venue is the authority anyway. The failure mode is
an order rejected in production for a step size that changed months ago.

Two properties of the fetched data, measured in NEG-29, that consumers must know:

- **It is a snapshot of tradable symbols at startup.** XChange surfaced 1371
  instruments against exactly the 1371 `TRADING` symbols of Binance's 3670; halted
  (`BREAK`) symbols never appear. There is no mid-session refresh: a symbol that
  halts after startup keeps its in-memory entry. Acceptable for v1, because a
  universe change already means a restart (§6).
- **The feeds module builds its own symbol bimap** from the venue instrument list
  rather than calling `BinanceAdapters`' static helpers. Those helpers are correct
  — they invert the split from venue data, not by guessing — but they read a
  process-global mutable registry populated as a side effect of exchange
  construction, and they throw `NullPointerException` before it is populated. An
  explicit bimap built at startup has the same data, no hidden ordering
  requirement, and fails with our own error message.

## 3. Canonical symbol grammar

**Inherited invariant (ADR 0002 §3, not re-decided here):** an instrument-partitioned
stream ends in `InstrumentId.toString()` — uppercase, venue-qualified. Symbols may
contain `-` and **never** `.`, because the venue is parsed off as the last dot
segment. Symbols land inside frozen stream names, so a later grammar change renames
streams, which is data loss.

| Asset class | Canonical symbol rule |
|---|---|
| Crypto spot | `BASE-QUOTE`, both uppercase (`BTC-USDT`) |
| Crypto derivatives | venue contract code, uppercase (`1000SHIBUSDT` → `1000SHIB-USDT-PERP`) |
| Equities | ticker with `.` → `-` normalization (`BRK.B` → `BRK-B`) |
| Futures | venue contract code, passed through (`ESZ26`); expiry lives in reference data, never in the symbol |

**The mapping is a translation, not a parse.** Binance's native `BTCUSDT` carries no
separator, and the split is not lexical: `USDTTRY` is `USDT`/`TRY` (not `USDTTR`/`Y`
or `USD`/`TTRY`), and `1000SATSUSDT` is `1000SATS`/`USDT`. Only the venue's own
instrument list settles it — Binance publishes `baseAsset`/`quoteAsset` per symbol,
and that is what the startup bimap is built from. No regex, ever.

### Worked mapping table

| Venue native | Base / quote from venue data | Canonical `InstrumentId` | Rule exercised |
|---|---|---|---|
| Binance `BTCUSDT` | `BTC` / `USDT` | `BTC-USDT.BINANCE` | the base case |
| Binance `ETHBTC` | `ETH` / `BTC` | `ETH-BTC.BINANCE` | quote is not a stablecoin |
| Binance `USDTTRY` | `USDT` / `TRY` | `USDT-TRY.BINANCE` | base *contains* a common quote asset — suffix-stripping yields `USDTTR` and is wrong |
| Binance `1000SATSUSDT` | `1000SATS` / `USDT` | `1000SATS-USDT.BINANCE` | digit-prefixed base; split is metadata-driven |
| Binance `1000SHIBUSDT` (**futures**, not spot) | `1000SHIB` / `USDT` | `1000SHIB-USDT-PERP.BINANCE` | a non-spot instrument on the same venue needs a distinct symbol, or it would collide with a spot listing of the same pair |
| NASDAQ `BRK.B` (future work) | — | `BRK-B.NASDAQ` | `.` → `-`; the dot is grammar-illegal because it is the venue separator |
| CME `ESZ26` (future work) | — | `ESZ26.CME` | contract code passes through unchanged |

All seven round-trip: canonical → native is a bimap lookup, native → canonical is
the same lookup inverted, and the four Binance rows were verified against the live
venue in NEG-29 (`BTCUSDT` → `BTC/USDT` → `BTCUSDT`, and likewise for the rest).

**`BRK.B` is decided, not deferred:** the canonical symbol is `BRK-B`, the
normalization is applied once when reference data is loaded, and the dotted form
never enters the engine. This costs three lines today and a stream rename later.

## 4. Component boundaries

**The feed adapter does exactly one thing:** subscribe to the venue's websocket,
normalize each message into a `TradeTick` or `QuoteTick`, wrap it in an `Event` and
hand it to `EventPublisher`. It does not aggregate, enrich, filter or store.

Normalization must not blur ADR 0001's split: a venue trade message becomes a
`TradeTick`, a book update becomes a `QuoteTick`, and nothing produces a hybrid.
Venues that multiplex both onto one websocket channel demultiplex in the adapter.

**Bar aggregation is an ordinary bus consumer** (NEG-27): it subscribes to
`md.tick.trade.*`, derives bars, and publishes `Bar` events back onto the bus.

**Rejected: aggregating bars inside the adapter to save a bus hop.** The hop is the
feature. A bus-consumer aggregator derives bars identically from live ticks and from
NEG-20 replays, which is the one-code-path rule applied to derived data; in-adapter
aggregation would make bars a live-only artifact and force the backtest path to
reimplement them — two implementations of the same OHLCV arithmetic, guaranteed to
diverge. It also puts bars through the same at-least-once delivery, dead-lettering
and observability machinery as every other event, for free.

A corollary worth stating because NEG-27 depends on it: the aggregator needs
**nothing** from reference data. OHLCV arithmetic uses no tick size, no step size
and no notional bound (NEG-29 §5). That is what lets it run unchanged over replayed
history, where no feed and no venue metadata exist.

### Bar window semantics

Ratified here because NEG-27 defers them to this ADR, and because they are what
makes the placement decision above testable:

- **Windows are wall-clock-aligned on `occurredAt`, in UTC, half-open.** A 1m bar
  covers `[12:03:00, 12:04:00)`. Aligning on `occurredAt` rather than `ingestedAt`
  is the whole reason a replayed run reproduces a live run byte-for-byte — NEG-27's
  litmus test. Aligning on arrival time would make bars a function of when the
  engine happened to read the stream.
- **A window closes on the first tick past its boundary, with a timer as backstop.**
  Without the timer a quiet instrument's bar would sit unpublished until its next
  trade, which for the long tail can be minutes.
- **No trades in a window → no bar.** Synthesizing a flat bar from the previous
  close fabricates data that never happened, and the archive would keep it forever.
  A gap in the bar series is information: it says nothing traded. This is the
  project's data-fidelity principle applied to derived data.
- **Every interval is derived independently from ticks, never rolled up from lower
  bars.** Deriving `5m` from five `1m` bars is cheaper and wrong in the ways that
  matter: it compounds any error in the lower interval and makes a late or missing
  `1m` bar silently corrupt the `5m` series. Each interval reads the same tick
  stream and is exact on its own.
- **A closed bar is immutable.** A tick arriving with an `occurredAt` inside an
  already-closed window is dropped and counted, never published as a correction.
  ADR 0002's streams have no update semantics — a second `Bar` for the same window
  would be a new event, and every consumer would have to guess which one wins.
  Out-of-order ticks *within* the open window are accepted normally.

## 5. Timestamping

The envelope already fixes the fields — `occurredAt` is source time, `ingestedAt` is
engine receive time, and the gap between them *is* the feed-latency metric (`Event`
javadoc). This ADR fixes their **values**:

- **`occurredAt` = the venue's event timestamp** carried in the websocket message
  (trade time, book-update time). Never local time when the venue supplied one.
- **`ingestedAt` = stamped by `Event.of(...)`** at the moment of normalization, from
  the system clock.
- **`source` = the feed component name**, `binance-feed`. This value becomes the
  `owner` of `bus.feed.latencyMillis.*` under ADR 0003 §2's "event source" rule, so
  it is a series identity in Grafana, not a free-text label. (ADR 0003's `binance`
  is an illustrative example; `binance-feed`, as documented on `Event.source`, is
  the value.)
- **No venue timestamp → local receive time**, and the fallback is counted. A feed
  silently substituting local time for source time would report zero latency and
  look healthy; the count is what distinguishes "fast venue" from "no data".

**Negative feed latency is reported, never clamped.** Venue and local clocks drift,
so `occurredAt > ingestedAt` will occur. That is a *finding* about clock skew —
exactly what the latency metric exists to surface. Clamping to zero, or swapping the
timestamps, would corrupt the one signal that reveals the problem. This codifies
existing behavior rather than requesting a change: `RedisStreamsEventPublisher`
already computes `Duration.between(occurredAt, ingestedAt).toMillis()` and records
it unclamped.

Consistent with ADR 0002 §5: bus windows, trimming and replay run on *ingestion*
time (stream IDs), while backtests order by `occurredAt` from the store. After a
reconnect, events with old `occurredAt` values legitimately sit at recent stream IDs.

## 6. Feed status and staleness

NEG-26 defers its thresholds here. The decision is the **two-signal split**, and the
numbers are configuration in the ADR 0002 sense — revisable, and validated by the
NEG-28 soak test, which exists precisely to replace assumed rates with measured ones.

**Connection liveness and data staleness are different signals and must not be
conflated.**

- **Connection liveness comes from the websocket transport** — the venue's own
  ping/pong frames, which arrive regardless of whether any instrument is trading. A
  dead connection is an unambiguous outage: reconnect with backoff, resubscribe the
  *entire* universe, emit the gap marker. Default: no frame of any kind for **90 s**
  is a dead connection.
- **Data staleness is per instrument and is never an outage on its own.** A quiet
  pair going silent is normal — ADR 0002's own sizing notes the long tail runs
  1–5 quotes/s against 20–60/s for majors, and quieter still overnight. Staleness is
  published as a `Metric` and surfaced in Grafana; it does **not** trigger reconnect
  and does not kill the process. Defaults: **30 s** without a quote, **300 s**
  without a trade.

The consequence worth stating plainly: **a per-instrument silence never reconnects
the feed, and a dead connection always does.** Inverting either one produces a
recognizable failure — reconnect storms driven by an idle instrument, or a feed that
sits happily on a dead socket because nothing was expected to trade anyway.

Thresholds are per-instrument overridable in the universe config (§7), because a
single global number cannot be right for both a major and the tail. Metric names
follow ADR 0003's grammar under the `feed` area, through typed builders in the feeds
module — never string concatenation, per that ADR's `MetricNames` rule.

## 7. Subscription universe

**A committed config file lists the canonical instrument ids the feed subscribes
to.** Initial universe: ~20 Binance spot pairs, selected as the top spot pairs by
volume — the same 20 ADR 0002's retention math is sized on.

- **Validated at startup** against venue metadata. An id absent from the venue's
  instrument list — a typo, a delisting, a halted symbol — fails the process loud.
  A feed that starts with 19 of 20 subscriptions is worse than one that refuses to
  start, because the missing instrument's absence looks exactly like a quiet market.
- **Changing the universe is an edit plus a restart.** Restart cost for a feed is a
  reconnect and resubscribe, which is the same path NEG-26's resilience work already
  implements and exercises.

**Rejected: hot-add of instruments at runtime.** It requires dynamic stream
creation, retention registration and subscriber discovery — real machinery, with no
consumer asking for it, to avoid a restart that costs seconds.

**Rejected: subscribing to everything the venue offers.** ADR 0002's 4 GB budget is
sized on 20 pairs at ~85 KB/s for a 12h tick window. Binance lists 1371 tradable
spot symbols; subscribing to all of them is roughly 20× nominal on the tick streams
alone and blows the memory wall in under an hour. The universe is deliberate, and
growing it is a sizing decision made against that table — not a default.

## 8. Archival path (ADR 0002 §6 reaffirmed)

No new decisions here; restated because this epic is the first producer that must
honor it:

- **Publishing raw onto the bus *is* the archival path.** The archiver is an
  ordinary bus consumer writing to QuestDB (NEG-7); it gets no side channel from the
  feed.
- **QuestDB is the only permanent archive.** Whatever the bus loses before the
  archiver reads it, history loses — which is why the memory wall fails loud rather
  than evicting.
- **This epic's obligation ends at "everything the venue sent reaches the bus,
  unconflated."** No conflation, no rate-capping, no downsampling in the adapter,
  per ADR 0002 §4 and the project's data-fidelity-over-resources principle.

## 9. Deferred, with owners

Every open point below is assigned to a named story; none is left open.

| Deferred | Owner | Why not now |
|---|---|---|
| Trading calendars — sessions, holidays, halts, futures rolls | the IB/equities story | Binance is 24/7 so the question never arises for v1. **It is load-bearing for later:** NEG-26's staleness detection and NEG-27's bar aggregation both currently assume "no data means the feed is broken", which is false the moment an instrument has a closing bell. Calendars are venue-and-session scoped, not `Instrument` fields. |
| Price bands and `MARKET_LOT_SIZE` as fat-finger inputs | NEG-9 | Binance publishes both (`PERCENT_PRICE_BY_SIDE` is its own fat-finger check, ±20% on `USDTTRY`) and both move intraday. Dynamic venue state, not reference data. |
| Venue rate limits | NEG-26 | `exchangeInfo` publishes four; XChange discards them. If backoff wants a venue-supplied budget rather than hard-coded numbers, the feed reads them itself. Venue-scoped, not per-instrument. |
| Reference data on the bus (an `instrument` eventType) | a future ADR amendment | §2 — zero consumers today; registering an eventType is permanent. |
| `EquitySpec` / `FutureSpec` fields | the IB/equities story | §1 — shape reserved, fields deliberately not guessed. |
| Mid-session reference-data refresh | NEG-26, if it proves needed | §2 — a universe change is already a restart. |

## Consequences

- **NEG-24** implements `Instrument`, `InstrumentSpec`, `CryptoSpotSpec` and
  `AssetClass` in `core`; the `feeds` module gets the venue metadata client, the
  startup symbol bimap and the universe config loader. `feeds` depends on `core`
  only — never on `bus` (modules.md "Future modules").
- **NEG-25** publishes `TradeTick`/`QuoteTick` with `source = "binance-feed"` and
  the §5 timestamp assignment, and validates its universe at startup per §6.
- **NEG-26** owns reconnect/resubscribe, the §6 two-signal status model and its
  thresholds, and the no-venue-timestamp fallback counter. It may not "fix" negative
  latency.
- **NEG-27** builds the aggregator as a bus consumer with no `Instrument`
  dependency, implementing §4's window semantics — including the empty-window and
  closed-bar-immutability rules, which its replay-equivalence test depends on.
- **NEG-28** validates the §6 thresholds and §7's universe sizing against realized
  rates; both are configuration this ADR expects it to correct.
- **Frozen by this ADR:** the composed model's *split* (universal constraints vs
  per-asset-class spec), the symbol grammar and its per-asset-class rules, the
  adapter's single responsibility, the timestamp assignment, and the no-clamping
  rule. Changing the grammar renames streams and is a data-loss event.
- **Revisable configuration:** the universe list and its size, the selection rule,
  which venue is first, and the specific `permits` entries as asset classes land.
- Revisit this ADR if reference data must go on the bus, if hot-add becomes a real
  requirement, or if a venue is added whose symbols cannot be expressed under §3.
