# NEG-23 — Instrument Model and Feed Architecture: Design Plan

Plan for [NEG-23](https://linear.app/negraolu/issue/NEG-23/design-the-instrument-model-and-feed-architecture). This is a design story: the deliverable is **ADR 0004** (`docs/adr/0004-instrument-model-and-feed-architecture.md`), no production code beyond what the ADR needs to demonstrate naming/mapping rules. It gates the entire rest of the Data Feeds epic — NEG-24 (feeds module + instrument model), NEG-25 (Binance adapter), NEG-26 (resilience), NEG-27 (bar aggregation) all implement whatever this ADR says, so every field, grammar rule and boundary in it is effectively frozen once those stories start.

## The one constraint that shapes everything

**The instrument model must be generic without being speculative.** The epic demands multi-asset from day one, but only crypto ships this year — every field we freeze for futures/equities before touching those markets is a guess that IB's TWS API gets to falsify later. The resolution: split *identity* (already solved — `InstrumentId` is venue-qualified and asset-class-agnostic) from *description* (this ADR), model the description with the fields every asset class provably has, and make asset-class-specific extras nullable with `maybeX()` views rather than inventing a sealed per-asset-class hierarchy nobody can validate yet. The generic promise is kept by the **canonical symbol grammar**: it must already have an answer for `BRK.B` (a dot — which `InstrumentId` forbids in the venue position and ADR 0002 forbids entirely) and for futures contract codes, because retrofitting a grammar after streams exist is a data-loss event per ADR 0002.

## Recommended answers to the six design questions

These are the positions the ADR should take (with rationale written out); the ADR is where they get argued and locked.

| Question | Recommendation | Why |
|---|---|---|
| Instrument reference model | One flat `core` record — working name `Instrument` — with the universal fields (`instrumentId`, `assetClass` enum, `baseCurrency`, `quoteCurrency`, `priceTickSize`, `quantityStepSize`, all `BigDecimal` like the tick payloads) plus nullable extras (`minNotional` now; contract multiplier/expiry reserved for futures). Nullable components get `maybeX()` views per project convention. | Two-plus components will agree on this type (risk fat-finger checks need tick size, OMS needs lot rounding and min notional, feeds need all of it) → `core` by the modules.md litmus test. A sealed per-asset-class hierarchy is rejected as speculative: we'd be designing `FutureInstrument` against zero requirements. Revisit the hierarchy when IB lands — a flat record with unused nullable fields converts cheaply; a wrong sealed hierarchy doesn't. |
| Reference data population | Fetched by the feed at startup from venue metadata (XChange `ExchangeMetaData` for Binance), validated against the configured universe, held in memory. **Not published on the bus** in v1 — no `instrument` eventType. | Only the feeds process consumes reference data today; putting it on the wire would freeze an eventType (ADR 0002: renames are data loss) for zero current consumers. When risk/OMS need it, distributing reference data becomes their story and gets its own ADR amendment. Rejected alternative: hand-maintained static config of tick sizes — drifts from the venue silently, and the venue is the authority anyway. |
| Canonical symbol grammar | Per asset class, one rule producing symbols that are uppercase, may contain `-`, never `.` (ADR 0002 §3 frozen constraint). Crypto spot: `BASE-QUOTE` (`BTC-USDT`). Equities (later): ticker with `.`→`-` normalization (`BRK-B`). Futures (later): venue contract code (`ESZ26`). Venue-native ↔ canonical mapping is an explicit per-venue translation owned by the feeds module, validated at startup — never string-guessed at runtime. | Binance says `BTCUSDT` with no separator — splitting it *requires* venue metadata (is `1000SHIBUSDT` base `1000SHIB` or `1000SHIBUSD`+`T`?), which is exactly why the mapping must come from the venue's own instrument list, not a regex. The grammar must be decided for all asset classes *now* even though only crypto ships: symbols land in frozen stream names (`md.tick.trade.{SYMBOL}.{VENUE}`), so a later grammar change renames streams = data loss. |
| Component boundaries | The adapter does exactly one thing: venue websocket → normalized `TradeTick`/`QuoteTick` events via `EventPublisher`. Bar aggregation is an **ordinary bus consumer** (NEG-27), not adapter code. Ratify this split in the ADR. | One-code-path rule applied to derived data: a bus-consumer aggregator derives bars identically from live ticks and NEG-20 replays; in-adapter aggregation would make bars a live-only artifact and force the backtest path to reimplement them. Rejected alternative — aggregate in-adapter to "save a bus hop": the hop is the feature; it buys replay equivalence and puts bars through the same at-least-once/observability machinery as everything else. |
| Timestamping | The envelope already decides the fields (`occurredAt` = source time, `ingestedAt` = engine receive, gap = feed latency — `Event` javadoc). The ADR decides assignment: `occurredAt` = the venue's event timestamp (trade time / book-update time from the websocket message); `ingestedAt` stamped via `Event.of(...)` at normalization; `source` = the feed's component name (e.g. `binance-feed`). Venue messages lacking a timestamp: fall back to local receive time and count the fallback in a metric. Clock skew making `occurredAt > ingestedAt`: report it (negative feed latency is a *finding* about venue/local clocks), never clamp it. | The alternative — `occurredAt` = local receive — would destroy the only cross-stream correlation signal ADR 0002 §5 offers consumers (trade/quote correlation runs on `occurredAt`) and make feed latency unmeasurable. Clamping skew would silently corrupt the latency metric NEG-21 already surfaces. |
| Subscription universe | Static config: a committed file listing canonical instrument ids (the initial ~20 Binance pairs, selection rule: top spot pairs by volume). Validated at startup against venue metadata — unknown symbol fails the process loud. Changing the universe = edit config + restart; hot-add is explicitly not v1. | Restart cost is trivial for a feed (reconnect + resubscribe is the same path as NEG-26 resilience); hot-add machinery (dynamic stream creation, retention registration, subscriber discovery) is real complexity with no consumer yet. Rejected alternative: subscribe-to-everything — ADR 0002's sizing math is built on a deliberate universe, and 400+ Binance pairs would blow the 4 GB budget ~20×. |

Plus the **archival path statement** (no design freedom, just reaffirmation): publishing raw onto the bus *is* the archival path per ADR 0002 §6 — the archiver is an ordinary bus consumer, QuestDB (NEG-7) is the only permanent archive, and this epic's obligation ends at "everything the venue sent reaches the bus, unconflated."

## Worked examples (acceptance criterion: mapping demonstrated, not asserted)

The ADR must carry a worked mapping table at least this adversarial:

| Venue native | XChange form | Canonical `InstrumentId` | Rule exercised |
|---|---|---|---|
| Binance `BTCUSDT` | `CurrencyPair` BTC/USDT | `BTC-USDT.BINANCE` | base/quote split from venue metadata |
| Binance `1000SHIBUSDT` | BTC-style pair 1000SHIB/USDT | `1000SHIB-USDT.BINANCE` | digits in base symbol; split is metadata-driven, not lexical |
| NASDAQ `BRK.B` (future) | — | `BRK-B.NASDAQ` | the `.`→`-` normalization; dot is grammar-illegal |
| CME `ESZ26` (future) | — | `ESZ26.CME` | contract code passes through; expiry lives in reference data, not the symbol |

And a candidate record sketch (illustrative, NEG-24 implements it):

```java
public record Instrument(
        InstrumentId instrumentId,
        AssetClass assetClass,          // CRYPTO, EQUITY, FUTURE, ... — enum, not string
        String baseCurrency,
        String quoteCurrency,
        BigDecimal priceTickSize,
        BigDecimal quantityStepSize,
        BigDecimal minNotional)         // nullable → maybeMinNotional()
```

## Steps

### Step 1 — Spike: venue metadata and consumer field inventory

Two short investigations that stop the ADR from designing against imagined data. (a) Run XChange against Binance once and dump `ExchangeMetaData` for a handful of pairs — record which candidate fields (tick size, step size, min notional, min/max quantity) are actually populated and at what precision; note anything important that is missing. (b) Inventory the downstream consumers-to-be: what does a fat-finger check (NEG-9), OMS quantity rounding (NEG-10), and the adapter itself each need from reference data. Output: a field-availability table (`field × {who needs it, Binance source, populated?}`) committed as an appendix under `docs/plans/epic-neg-6/`.

**Verify:** every field in the proposed `Instrument` record has both a named consumer and a confirmed Binance source — or an explicit "reserved, nullable" justification.

**Done (NEG-29):** [`neg-29-binance-field-availability-appendix.md`](neg-29-binance-field-availability-appendix.md). Headlines for Step 2: the sketch survives, plus `minQuantity` and `maxNotional`; `minNotional` is XChange's `getCounterMinimumAmount()`; the base/quote split is venue-supplied and round-trips on all five adversarial symbols, but via a process-global static that NPEs before remote init (NEG-24 should build its own bimap); XChange `stripTrailingZeros()`-es every numeric, so `maxQuantity` arrives as `9E+3` and the ADR must mandate `toPlainString()`.

**Amends the "Instrument reference model" row above:** the recommendation is now a **composed** model — a flat universal record (tick/step/min-max quantity and notional) plus a sealed `InstrumentSpec` component carrying what varies by asset class, shipping with `CryptoSpotSpec` alone. `baseCurrency`/`quoteCurrency` are a pair concept only crypto and FX have; leaving them on the universal record hardens the crypto assumption the epic exists to prevent. This is not the speculative hierarchy that row rejected — `EquitySpec`/`FutureSpec` are shape reservations with no fields guessed today, and adding one changes no `Instrument` component, no eventType and no stream name. Argued in [appendix §6](neg-29-binance-field-availability-appendix.md#6-what-neg-30-should-lift-into-adr-0004).

### Step 2 — Draft ADR 0004, part 1: instrument model and symbol grammar

Sections for questions 1–3 (reference model, population, symbol grammar), same shape as ADR 0001/0002: each a `##` section arguing the choice and naming the rejected alternatives, ending with the worked mapping table and record sketch. The grammar section states the ADR 0002 §3 constraint (`-` legal, `.` illegal) as an inherited invariant and gives the per-asset-class rules.

**Verify:** every row of the worked mapping table round-trips on paper under the stated rules; the `BRK.B` row is decided, not deferred.

### Step 3 — Draft ADR 0004, part 2: feed architecture

Sections for questions 4–6 plus the archival statement: component boundaries (bar aggregation as bus consumer — argued, since NEG-27 builds on it), timestamp assignment rules including the no-venue-timestamp fallback and the no-clamping rule, universe config and the restart-not-hot-add position, and the §6 reaffirmation of ADR 0002's archive boundary.

**Verify:** NEG-25/26/27's descriptions each name at least one decision this ADR must supply (timestamps, feed-status thresholds, aggregator placement, window semantics) — check each is answered by an explicit section, not left to the implementing story.

### Step 4 — Consistency review and acceptance

Read the draft against ADR 0001 (tick split), ADR 0002 (grammar, frozen streams, archive boundary), ADR 0003 (metric naming — the timestamp section feeds the latency metric), and the five open epic stories. Flag any mismatch on the Linear issues rather than silently absorbing it (the NEG-17 precedent). Then: `Status: Accepted`, commit plan + appendix + ADR on the `neg-23` branch, PR, tick the acceptance boxes on NEG-23.

**Verify:** the two NEG-23 definition-of-done boxes check off against concrete sections; no TODO or "TBD" survives in the accepted ADR.

## Definition of done (mapped to the issue)

- [x] Accepted ADR in `docs/adr/` answering all six questions, each a `##` section naming the rejected alternatives → [ADR 0004](../../adr/0004-instrument-model-and-feed-architecture.md), §§1–3 (model, population, grammar) and §§4–8 (boundaries, timestamps, feed status, universe, archival). Two sections beyond the six were forced by the consistency review: §6 feed status (NEG-26 defers its thresholds here) and §4's bar window semantics (NEG-27 defers those here).
- [x] No code beyond what the ADR needs to demonstrate naming/mapping rules → the record sketch and mapping table are markdown; the only executable artifact was the throwaway NEG-29 spike, which landed as a table. `git status` shows `docs/` only.

## Pitfalls to expect

- **The Binance symbol split is not lexical.** `BTCUSDT` cannot be split by pattern (`1000SHIBUSDT`, `USDTBRL` both break naive suffix rules). The mapping must be driven by the venue's own instrument list; if the spike shows XChange's `CurrencyPair` already encodes the split, say so in the ADR and lean on it.
- **Don't let the spike leak XChange into `core`.** The `Instrument` record is a `core` type; anything that touches `ExchangeMetaData` is `feeds` (modules.md litmus test 3 by analogy: imports a venue library → feeds). The ADR should state the placement explicitly so NEG-24 doesn't relitigate it.
- **`BigDecimal` everywhere, scale from the venue.** Tick payloads already use `BigDecimal`; a `double` tick size of 0.00000001 is a rounding bug waiting for the OMS. Preserve the venue's stated precision — don't normalize scale.
- **Negative feed latency is data.** Venue-vs-local clock skew will produce `occurredAt > ingestedAt` occasionally. Clamping or swapping timestamps corrupts the one metric that would reveal the skew. Decide it in the ADR so the adapter doesn't "helpfully" fix it.
- **Don't invent an `instrument` eventType as a side effect.** Wire eventTypes are frozen once published (ADR 0002 consequences). Reference-data-on-the-bus is a real future need with zero current consumers — reserve the door, don't walk through it.
- **The grammar must survive asset classes we haven't touched.** The cheap trap is a crypto-only rule ("symbol = BASE-QUOTE") stated as *the* rule. State the per-asset-class table now — `BRK.B` and `ESZ26` cost three lines each today and a stream rename later.
