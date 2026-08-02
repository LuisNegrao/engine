# NEG-29 — Binance Field Availability and Consumer Inventory (appendix)

Appendix produced by [NEG-29](https://linear.app/negraolu/issue/NEG-29/spike-binance-venue-metadata-and-consumer-field-inventory), per its [plan](neg-29-binance-metadata-and-field-inventory-design-plan.md). It exists so ADR 0004 ([NEG-30](https://linear.app/negraolu/issue/NEG-30)) freezes the `Instrument` record against observed venue data, and so [NEG-24](https://linear.app/negraolu/issue/NEG-24) knows how much symbol-mapping machinery to build. The spike program is reproduced at the end; it is not in the build.

## Run conditions

| | |
|---|---|
| Run at | 2026-08-01T23:05Z |
| XChange | `org.knowm.xchange:xchange-core` + `xchange-binance` **5.2.2** (latest release) |
| Host | `https://api.binance.com` (XChange default). **No geo-block** — HTTP 200, no fallback to `data-api.binance.vision` needed. |
| Auth | None. `ExchangeSpecification.setShouldLoadRemoteMetaData(true)` and nothing else — reference data needs no API key. |
| Raw reference | `GET /api/v3/exchangeInfo` (per-symbol via `?symbols=[…]`, plus one full fetch: **17.5 MB**, 3670 symbols) |
| Sample symbols | `BTCUSDT`, `ETHBTC`, `USDTTRY`, `1INCHUSDT`, `1000SATSUSDT` — all `status=TRADING` on spot |

**Sample substitution:** the issue named `1000SHIBUSDT` as the adversarial case. That is a **futures** symbol; the spot-listed equivalents used here are `1000SATSUSDT` (digit-prefixed base, and Binance itself publishes its `LOT_SIZE` at scale 2 rather than 8) and `1INCHUSDT`. `USDTTRY` was added as a harder case: naive suffix-stripping on it yields base `USDTTR`.

## Headline findings

1. **The base/quote split is venue-supplied and correct** — XChange keys instrument metadata by `CurrencyPair`, built from Binance's own `baseAsset`/`quoteAsset`. All five adversarial symbols split correctly and round-trip exactly. No lexical guessing anywhere in the path.
2. **…but the inverse mapping is process-global mutable static state that NPEs before remote init.** `BinanceAdapters.adaptSymbol` / `toCurrencyPair` throw `NullPointerException` on a fresh JVM and only work after an exchange with `shouldLoadRemoteMetaData(true)` has been constructed. This is an init-ordering landmine for NEG-25's websocket path — see the verdict in §4.
3. **Every field in the NEG-23 record sketch is populated for all five symbols**, and `minNotional` is present under a misleading name: `InstrumentMetaData.getCounterMinimumAmount()`. The *numbers* survive; the *shape* does not — `baseCurrency`/`quoteCurrency` are a pair concept only crypto and FX have, so §6 recommends a composed model (universal record + sealed `InstrumentSpec`) rather than a flat one.
4. **XChange does not preserve the venue's stated scale.** Binance sends `tickSize=0.01000000` (scale 8); XChange returns `0.01` (scale 2). It has run `stripTrailingZeros()`, which for whole numbers yields **negative scale**: `LOT_SIZE.maxQty=9000.00000000` arrives as `9E+3` (scale −3). `toString()` on that value emits `"9E+3"`. This is the one place the plan's "preserve venue precision" rule is already violated by the library.
5. **Max quantity is order-type-dependent and dynamic, and XChange only exposes half of it.** For `BTCUSDT`, `LOT_SIZE.maxQty` is 9000 BTC (what XChange surfaces) while `MARKET_LOT_SIZE.maxQty` is 152.84 BTC — a 59× difference, recomputed by the venue from liquidity. XChange drops `MARKET_LOT_SIZE` entirely.
6. **Binance publishes its own price bands, and they move.** `USDTTRY` carries `PRICE_FILTER` bounds of 34.50–51.80 and `PERCENT_PRICE_BY_SIDE` multipliers against a 5-minute average price. These are exactly the fat-finger inputs NEG-9 will want, they are dropped by XChange, and they are **not static reference data** — modelling them on `Instrument` would bake a stale snapshot into the engine.
7. **XChange surfaces only `TRADING` symbols.** Its instrument map held 1371 entries; the full `exchangeInfo` had 3670 symbols of which exactly 1371 were `TRADING` (the rest `BREAK`). Startup universe validation gets status filtering for free — and, symmetrically, a symbol that halts vanishes from a later fetch.
8. **Rate limits are dropped.** `exchangeInfo` publishes four (`REQUEST_WEIGHT` 6000/min, `ORDERS` 100/10s, `ORDERS` 200000/day, `RAW_REQUESTS` 300000/5min); `ExchangeMetaData.getPublicRateLimits()` and `getPrivateRateLimits()` are both `null`. NEG-26's backoff has no venue-supplied budget to work from unless the feed reads them itself.

## 1. Field availability table

Verdicts: **required** = goes in the model; **on the spec** = lives on the sealed `InstrumentSpec` component rather than the universal record (§6); **reserved-nullable** = component exists but is null for Binance spot; **not modeled** = deliberately excluded, reason given.

| Field | Who needs it | Binance `exchangeInfo` source | XChange accessor | Populated? (observed) | Verdict |
|---|---|---|---|---|---|
| `instrumentId` | everyone (bus addressing, ADR 0002 stream names) | `symbol` + `baseAsset`/`quoteAsset` | map key `CurrencyPair` in `ExchangeMetaData.getInstruments()` | yes — 5/5, splits correct | **required** |
| `assetClass` | NEG-9 exposure-per-asset-class; NEG-10 routing | — (endpoint identity only: this is the spot API) | — | absent — the venue has no such concept | **on the spec** — `InstrumentSpec.assetClass()`, not a standalone component; engine-assigned, never venue-read |
| `baseCurrency` | NEG-10 position accounting; canonical symbol | `baseAsset` | `CurrencyPair.getBase()` | yes — `BTC`, `ETH`, `USDT`, `1INCH`, `1000SATS` | **on the spec** — a *pair* concept, meaningless for equities/futures; `CryptoSpotSpec.base()` |
| `quoteCurrency` | NEG-10 P&L currency; units of `minNotional` | `quoteAsset` | `CurrencyPair.getCounter()` | yes — `USDT`, `BTC`, `TRY`, `USDT`, `USDT` | **on the spec** — `CryptoSpotSpec.quote()`; generalizes as `InstrumentSpec.settlementCurrency()` |
| `priceTickSize` | NEG-10 price rounding; NEG-9 price sanity; NEG-25 display | `PRICE_FILTER.tickSize` | `InstrumentMetaData.getPriceStepSize()` | yes — 5/5. `0.01` [s=2], `0.00001` [s=5], `0.01` [s=2], `0.0001` [s=4], `0.00000001` [s=8]. Venue scale 8 stripped. | **required** |
| `quantityStepSize` | NEG-10 lot rounding (live **and** simulated fill engine) | `LOT_SIZE.stepSize` | `InstrumentMetaData.getAmountStepSize()` | yes — 5/5. `0.00001`, `0.0001`, `1` [s=0], `0.1`, `1` [s=0]. `1000SATS` venue-sent `1.00` → `1`. | **required** |
| `minQuantity` | NEG-10 pre-submit validation; NEG-9 reject | `LOT_SIZE.minQty` | `InstrumentMetaData.getMinimumAmount()` | yes — 5/5, equals `stepSize` on all five | **required** — *add to the NEG-23 sketch* |
| `maxQuantity` | NEG-9 fat-finger upper bound | `LOT_SIZE.maxQty` | `InstrumentMetaData.getMaximumAmount()` | yes — 5/5, but **limit-order bound only**, and arrives with negative scale (`9E+3`, `1E+5`, `92141578`, `9E+5`, `3.63486E+10`) | **required**, documented as the LIMIT bound; see §2 for `MARKET_LOT_SIZE` |
| `minNotional` | NEG-9 pre-trade reject; NEG-10 pre-submit | `NOTIONAL.minNotional` | `InstrumentMetaData.getCounterMinimumAmount()` | yes — 5/5, distinct values: `5`, `0.0001`, `10`, `5`, `1` (confirms the mapping empirically) | **required** — nullable in the record, because a non-Binance venue may not publish it |
| `maxNotional` | NEG-9 fat-finger upper bound (currency-denominated, survives price moves better than `maxQuantity`) | `NOTIONAL.maxNotional` | `InstrumentMetaData.getCounterMaximumAmount()` | yes — 5/5: `9E+6`, `9E+6`, `9E+7`, `9E+6`, `9E+6` | **required** — *add to the NEG-23 sketch* |
| `priceScale` / `volumeScale` | nobody | derived from tick/step | `getPriceScale()` / `getVolumeScale()` | yes — 5/5, consistent with tick/step on every symbol | **not modeled** — redundant with the step sizes; two sources of truth for precision is a bug factory |
| `tradingFee` | nobody yet (NEG-10 needs *account* fees, not venue defaults) | **not from `exchangeInfo`** — an XChange-side default | `getTradingFee()` | `0.1` on all 5, units ambiguous (percent, not fraction), identical for every symbol | **not modeled** — a hard-coded 0.1 that P&L would silently trust is worse than no field |
| `marketOrderEnabled` | NEG-10 order-type gating | `orderTypes[]` (collapsed to a boolean) | `isMarketOrderEnabled()` | `true` on all 5 | **not modeled** in v1 — lossy collapse of a 6-value list; NEG-10 reads `orderTypes` itself when it needs this |
| contract multiplier | futures, post-IB | absent (spot endpoint) | `getContractValue()` | `null` on all 5 | **not modeled** in v1 — under the composed model this is a `FutureSpec` component, not a nullable on the universal record. XChange having the accessor confirms the concept is real, not that we should carry it now. |
| expiry | futures, post-IB | absent | — | absent | **not modeled** in v1 — future `FutureSpec` component |
| `tradingFeeCurrency`, `feeTiers` | nobody | — | `getTradingFeeCurrency()`, `getFeeTiers()` | `null` on all 5 | **not modeled** |
| market-order max quantity | NEG-9/NEG-10, market orders only | `MARKET_LOT_SIZE.maxQty` | **none — dropped** | venue: `152.84`, `1624.31`, `566983.18`, `212536.37`, `2495597623.05` — liquidity-derived, moves | **not modeled** — dynamic, not reference data; see §2 |
| price bands | NEG-9 fat-finger | `PRICE_FILTER.minPrice`/`maxPrice`, `PERCENT_PRICE_BY_SIDE` | **none — dropped** | venue: `USDTTRY` 34.50–51.80 with ±20% side multipliers; others ±100%/−50% | **not modeled** — moves with the market; NEG-9's story decides how it re-reads them |
| venue rate limits | NEG-26 backoff budget | top-level `rateLimits[]` | `getPublicRateLimits()` / `getPrivateRateLimits()` | **`null`** despite the venue publishing 4 | **not modeled** on `Instrument` (it is venue-scoped, not instrument-scoped) — flagged to NEG-26 |
| `status` | NEG-25 universe validation | `symbol.status` | — (used internally as a filter) | XChange exposed 1371 instruments = exactly the 1371 `TRADING` symbols of 3670 | **not modeled** — pre-filtered upstream; validation just checks membership |

**Every field in the NEG-23 record sketch has a named consumer and a confirmed Binance source.** Two additions are recommended (`minQuantity`, `maxNotional`), one field is deliberately excluded as redundant (`priceScale`/`volumeScale`), and three move off the universal record onto a sealed per-asset-class component (`assetClass`, `baseCurrency`, `quoteCurrency`) — see §6.

## 2. What Binance publishes that XChange drops

Per symbol, XChange populates **10 accessors** and leaves 3 `null`. Against the raw response that is a large reduction — the dropped groups, and whether we care:

| Dropped | Matters to | Verdict |
|---|---|---|
| `PRICE_FILTER.minPrice` / `maxPrice` | NEG-9 | Dynamic band, not reference data |
| `MARKET_LOT_SIZE` (whole filter) | NEG-9/NEG-10 | **Material** — the only market-order quantity bound, 59× tighter than `LOT_SIZE` on BTCUSDT |
| `PERCENT_PRICE_BY_SIDE` (4 multipliers + `avgPriceMins`) | NEG-9 | **Material** — this *is* Binance's own fat-finger check; NEG-9 should mirror it rather than invent thresholds |
| `NOTIONAL.applyMinToMarket` / `applyMaxToMarket` / `avgPriceMins` | NEG-10 | Minor — decides whether the notional bound binds market orders (`applyMinToMarket=true` on all 5) |
| `TRAILING_DELTA` | later | Not v1 |
| `MAX_NUM_ORDERS`, `MAX_NUM_ORDER_LISTS`, `MAX_NUM_ALGO_ORDERS`, `MAX_NUM_ORDER_AMENDS`, `ICEBERG_PARTS` | NEG-10 | Not v1 (open-order accounting) |
| `baseAssetPrecision`, `quotePrecision`, `quoteAssetPrecision`, `baseCommissionPrecision`, `quoteCommissionPrecision` | — | Redundant with tick/step |
| `orderTypes[]` | NEG-10 | Collapsed to one boolean — lossy |
| `permissions` / `permissionSets` / `isSpotTradingAllowed` / `isMarginTradingAllowed` | later | Not v1 |
| `defaultSelfTradePreventionMode`, `allowedSelfTradePreventionModes` | later | Not v1 |
| `symbol.status` | NEG-25 | Used as a filter, not exposed — acceptable |
| top-level `rateLimits[]` | NEG-26 | **Material** — see finding 8 |
| exact venue scale on every numeric | NEG-24 | **Material** — see finding 4 |

**Three material drops** (`MARKET_LOT_SIZE`, `PERCENT_PRICE_BY_SIDE`, `rateLimits`) plus the scale normalization. None of them block v1 — every field the record needs is present — but each is a reason the feeds module should keep the raw `exchangeInfo` fetch within reach rather than treating XChange as the only door to venue metadata.

## 3. Cross-check: XChange values vs raw filters (BTCUSDT)

| Raw filter | Raw value | XChange | XChange value |
|---|---|---|---|
| `PRICE_FILTER.tickSize` | `0.01000000` [scale 8] | `getPriceStepSize()` | `0.01` [scale 2] |
| `LOT_SIZE.stepSize` | `0.00001000` [scale 8] | `getAmountStepSize()` | `0.00001` [scale 5] |
| `LOT_SIZE.minQty` | `0.00001000` | `getMinimumAmount()` | `0.00001` |
| `LOT_SIZE.maxQty` | `9000.00000000` | `getMaximumAmount()` | `9E+3` [scale −3] |
| `NOTIONAL.minNotional` | `5.00000000` | `getCounterMinimumAmount()` | `5` [scale 0] |
| `NOTIONAL.maxNotional` | `9000000.00000000` | `getCounterMaximumAmount()` | `9E+6` [scale −6] |

Numerically identical in every row; **scale differs in every row**. The transformation is `stripTrailingZeros()`, verified independently: `new BigDecimal("9000.00000000").stripTrailingZeros()` → `toString()` = `"9E+3"`, `scale()` = `−3`, `toPlainString()` = `"9000"`.

## 4. Verdict for NEG-24 — the symbol mapping

**XChange supplies the split from venue data; it does not guess.** Observed round-trips after remote init:

| Native | `adaptSymbol` → | `toSymbol` back → | Round-trips |
|---|---|---|---|
| `BTCUSDT` | `BTC/USDT` | `BTCUSDT` | ✅ |
| `ETHBTC` | `ETH/BTC` | `ETHBTC` | ✅ |
| `USDTTRY` | `USDT/TRY` | `USDTTRY` | ✅ |
| `1INCHUSDT` | `1INCH/USDT` | `1INCHUSDT` | ✅ |
| `1000SATSUSDT` | `1000SATS/USDT` | `1000SATSUSDT` | ✅ |

Before remote init, in a fresh JVM, **all five throw `NullPointerException`** through both `BinanceAdapters.adaptSymbol(String, boolean)` and `BinanceAdapters.toCurrencyPair(String)`. The inverse is backed by a process-global static registry (`BinanceAdapters.putSymbolMapping(String, CurrencyPair)` is a public static mutator) populated as a side effect of `BinanceExchange` remote init.

**Recommendation for NEG-24: build our own bimap at startup from `getInstruments()`, do not call `BinanceAdapters` on the hot path.** Same venue data, same correctness, but: (a) no dependency on hidden global state whose population is a side effect of unrelated construction; (b) an unknown symbol fails with our own loud error instead of an `NPE` from a library static; (c) the universe-validation step of NEG-25 needs the map materialized anyway. The bimap is `canonical InstrumentId ⇄ native symbol`, built once, immutable thereafter — roughly 20 entries for the configured universe out of the 1371 available.

## 5. Consumer inventory

**NEG-9 Risk Manager (fat-finger / pre-trade).** Needs `maxQuantity` and `maxNotional` as hard venue bounds, `minNotional` to reject dust, and `priceTickSize` for price sanity. Notably it should *not* get its price bands from `Instrument`: Binance's own `PERCENT_PRICE_BY_SIDE` (±20% on `USDTTRY`, ±100%/−50% elsewhere, measured against a 5-minute average) is the venue's fat-finger check and it moves — NEG-9 either mirrors that logic against live bus prices or re-reads the filter, but it must not freeze a snapshot into reference data. Position/exposure limits are risk config, not venue data.

**NEG-10 OMS / Execution Gateway.** Needs `quantityStepSize` and `priceTickSize` for rounding, `minQuantity`/`minNotional` for pre-submit validation. Both back-ends need identical values: the **simulated fill engine must round exactly as the live gateway does**, or backtest and live diverge on every order — which is the one-code-path rule applied to reference data. `maxQuantity` from `LOT_SIZE` is the limit-order bound only; a market-order path needs `MARKET_LOT_SIZE`, which XChange does not provide.

**NEG-25 Binance adapter (itself).** Needs the `CurrencyPair` ⇄ native mapping and startup universe validation (unknown or non-`TRADING` symbol → fail loud). Needs nothing else from reference data: the trade/quote websocket payloads carry price, quantity and timestamps directly.

**NEG-27 bar aggregation — a named non-consumer.** Bars derive from trade ticks alone: OHLCV needs no tick size, no step size, no notional. Stated explicitly because it is load-bearing — the aggregator stays free of any `Instrument` dependency, which is what lets it run unchanged over NEG-20 replays where no feed and no venue metadata exist.

**NEG-26 resilience — venue-scoped, not instrument-scoped.** Binance publishes four rate limits that XChange discards. If NEG-26 wants a venue-supplied budget for backoff rather than hard-coded numbers, it reads `exchangeInfo`'s top-level `rateLimits[]` itself. Not an `Instrument` field.

## 6. What NEG-30 should lift into ADR 0004

- The record sketch survives contact with the venue on the numbers, with two additions, both populated 5/5 with named consumers: **`minQuantity`** and **`maxNotional`**. It does **not** survive on shape: `baseCurrency`/`quoteCurrency` are a *pair* concept that only crypto and FX have, so a flat record would harden crypto assumptions into the model — the exact failure the epic exists to prevent. Recommended shape is **composed: flat where the data is universal, sealed where it varies by asset class**.

```java
public record Instrument(
        InstrumentId instrumentId,
        BigDecimal priceTickSize,       // universal venue trading constraints
        BigDecimal quantityStepSize,
        BigDecimal minQuantity,
        BigDecimal maxQuantity,         // LIMIT-order bound; market orders differ, see §2
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

// Added additively when IB lands — no change to Instrument, no stream rename:
// record EquitySpec(String primaryExchange, String currency) implements InstrumentSpec {}
// record FutureSpec(String underlying, BigDecimal multiplier, LocalDate expiry, String currency) …
```

- **Why composition and not sibling `CryptoInstrument` / `EquityInstrument` types:** §5's consumer inventory splits exactly along that line. Risk fat-finger checks and OMS rounding need *only* the universal constraints — under a full hierarchy they would have to `switch` on asset class to reach a tick size whose meaning is identical in every branch, which is strictly worse code than a flat field. Position and P&L accounting is the one consumer that genuinely needs the varying part, and it gets exhaustive pattern matching over `InstrumentSpec`. Eight of the ten fields observed here are universal in the strong sense — NASDAQ publishes a tick size (SEC Rule 612) and CME a 0.25-point tick exactly as Binance publishes `PRICE_FILTER.tickSize`, and IB's own `ContractDetails` carries `minTick`/`minSize`/`sizeIncrement` for every asset class while keeping strike/expiry/multiplier on `Contract`. The split is the industry's, not ours.

- **This is not the speculative hierarchy the NEG-23 plan rejected.** v1 ships one `permits` entry, `CryptoSpotSpec`, designed against the data in §1 and nothing else. `EquitySpec`/`FutureSpec` are *shape* reservations, not field guesses — adding one later changes no component of `Instrument`, no `eventType` and no stream name, so ADR 0002's data-loss rule never comes near it. What the plan was right to reject is inventing `FutureSpec`'s fields today; that still stands, and the sketch above deliberately leaves those two lines commented out.

- **Name it `InstrumentSpec`, never anything `Payload`-shaped.** `Payload` is the frozen sealed hierarchy of wire events in `engine.core.event`, and reference data explicitly does not go on the bus in v1 (NEG-23's population decision). A sealed interface named like a payload would invite exactly the `instrument` eventType this design is avoiding.

- **State the scale rule explicitly**, because the library already broke it: values arrive `stripTrailingZeros()`'d and may carry negative scale. ADR 0004 should say reference-data `BigDecimal`s are stored as received and rendered with `toPlainString()` — never `toString()`, which turns 9000 into `"9E+3"`.
- **Say that `maxQuantity` is the limit-order bound**, so nobody later assumes it constrains market orders.
- **Keep dynamic venue state off the record**: price bands, market-order quantity caps and rate limits are not reference data. Reserve the door, per the "don't invent an `instrument` eventType" position.
- **Reference data is a snapshot of `TRADING` symbols at startup.** A symbol halting to `BREAK` mid-session does not update in memory; the ADR should say whether that is acceptable for v1 (it is — the feed restarts on a universe change anyway) rather than leaving it silent.

- **Reserve a paragraph for the trading calendar — the real multi-asset gap.** Neither the flat nor the composed model has anywhere to put sessions, holidays, halts or futures roll schedules, because Binance is 24/7 and the question never arises. It bites harder than the record shape when equities land: NEG-26's staleness detection and NEG-27's bar aggregation both currently assume *no data means the feed is broken*, which becomes false the moment an instrument has a closing bell. The calendar is not an `Instrument` field (it is venue-and-session scoped, shared across thousands of instruments) — ADR 0004 should name the gap and defer it to the IB story rather than let two components harden a 24/7 assumption in silence.

---

## Appendix A — raw evidence

Sample `exchangeInfo` filters (abridged to the fields discussed):

```
=== BTCUSDT
  PRICE_FILTER:    minPrice=0.01000000 maxPrice=1000000.00000000 tickSize=0.01000000
  LOT_SIZE:        minQty=0.00001000 maxQty=9000.00000000 stepSize=0.00001000
  MARKET_LOT_SIZE: minQty=0.00000000 maxQty=152.84116525 stepSize=0.00000000
  NOTIONAL:        minNotional=5.00000000 applyMinToMarket=true maxNotional=9000000.00000000 applyMaxToMarket=false avgPriceMins=5
  PERCENT_PRICE_BY_SIDE: bidMultiplierUp=2 bidMultiplierDown=0.5 askMultiplierUp=2 askMultiplierDown=0.5 avgPriceMins=5
=== USDTTRY
  PRICE_FILTER:    minPrice=34.50000000 maxPrice=51.80000000 tickSize=0.01000000
  LOT_SIZE:        minQty=1.00000000 maxQty=92141578.00000000 stepSize=1.00000000
  MARKET_LOT_SIZE: minQty=0.00000000 maxQty=566983.18333333 stepSize=0.00000000
  NOTIONAL:        minNotional=10.00000000 applyMinToMarket=true maxNotional=90000000.00000000 applyMaxToMarket=false avgPriceMins=5
  PERCENT_PRICE_BY_SIDE: bidMultiplierUp=1.2 bidMultiplierDown=0.8 askMultiplierUp=1.2 askMultiplierDown=0.8 avgPriceMins=5
=== 1000SATSUSDT
  PRICE_FILTER:    minPrice=0.00000001 maxPrice=1.00000000 tickSize=0.00000001
  LOT_SIZE:        minQty=1.00 maxQty=36348600000.00 stepSize=1.00
  MARKET_LOT_SIZE: minQty=0.00 maxQty=2495597623.05 stepSize=0.00
  NOTIONAL:        minNotional=1.00000000 applyMinToMarket=true maxNotional=9000000.00000000 applyMaxToMarket=false avgPriceMins=5
```

XChange `InstrumentMetaData` dump (BTCUSDT and 1000SATSUSDT shown; the other three are structurally identical):

```
### ExchangeMetaData accessors
  getCurrencies() -> map(size=492)
  getInstruments() -> map(size=1371)      # key type: CurrencyPair
  getPrivateRateLimits() -> absent (null)
  getPublicRateLimits()  -> absent (null)
  isShareRateLimits() -> true

=== BTC/USDT  base=BTC counter=USDT  nativeSymbol=BTCUSDT  metaPresent=true
  getAmountStepSize()        = 0.00001   [scale=5]
  getContractValue()         = absent (null)
  getCounterMaximumAmount()  = 9000000   [scale=-6]
  getCounterMinimumAmount()  = 5         [scale=0]
  getFeeTiers()              = absent (null)
  getMaximumAmount()         = 9000      [scale=-3]
  getMinimumAmount()         = 0.00001   [scale=5]
  getPriceScale()            = 2
  getPriceStepSize()         = 0.01      [scale=2]
  getTradingFee()            = 0.1       [scale=1]
  getTradingFeeCurrency()    = absent (null)
  getVolumeScale()           = 5
  isMarketOrderEnabled()     = true

=== 1000SATS/USDT  base=1000SATS counter=USDT  nativeSymbol=1000SATSUSDT  metaPresent=true
  getAmountStepSize()        = 1           [scale=0]     # venue sent "1.00"
  getCounterMaximumAmount()  = 9000000     [scale=-6]
  getCounterMinimumAmount()  = 1           [scale=0]
  getMaximumAmount()         = 36348600000 [scale=-5]
  getMinimumAmount()         = 1           [scale=0]
  getPriceScale()            = 8
  getPriceStepSize()         = 0.00000001  [scale=8]
  getVolumeScale()           = 0
```

## Appendix B — the spike (throwaway; not in the build)

`build.gradle.kts`:

```kotlin
plugins { application }

repositories { mavenCentral() }

dependencies {
    implementation("org.knowm.xchange:xchange-core:5.2.2")
    implementation("org.knowm.xchange:xchange-binance:5.2.2")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application { mainClass.set("Main") }

tasks.register<JavaExec>("probe") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("Probe")
}
```

`Main.java` — metadata dump. Reflective on purpose: the metadata API moved across XChange 5.x, so the spike inventories whatever accessors exist rather than asserting a guessed set.

```java
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import org.knowm.xchange.*;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.meta.ExchangeMetaData;

public class Main {

    private static final List<CurrencyPair> SAMPLE = List.of(
            new CurrencyPair("BTC", "USDT"),
            new CurrencyPair("ETH", "BTC"),
            new CurrencyPair("USDT", "TRY"),
            new CurrencyPair("1INCH", "USDT"),
            new CurrencyPair("1000SATS", "USDT"));

    public static void main(String[] args) throws Exception {
        ExchangeSpecification spec = new ExchangeSpecification(BinanceExchange.class);
        spec.setShouldLoadRemoteMetaData(true);
        Exchange exchange = ExchangeFactory.INSTANCE.createExchange(spec);
        ExchangeMetaData md = exchange.getExchangeMetaData();

        System.out.println("### host: " + exchange.getExchangeSpecification().getSslUri());
        for (Method m : sortedGetters(ExchangeMetaData.class)) {
            System.out.println("  " + m.getName() + "() = " + describe(invoke(m, md)));
        }

        Map<?, ?> instruments =
                (Map<?, ?>) invoke(ExchangeMetaData.class.getMethod("getInstruments"), md);
        System.out.println("### instrument map size = " + instruments.size());

        for (CurrencyPair pair : SAMPLE) {
            Object meta = instruments.get(pair);
            System.out.println("\n=== " + pair
                    + "  base=" + pair.getBase().getCurrencyCode()
                    + " counter=" + pair.getCounter().getCurrencyCode()
                    + "  metaPresent=" + (meta != null));
            if (meta == null) continue;
            for (Method m : sortedGetters(meta.getClass())) {
                System.out.println("  " + m.getName() + "() = " + describe(invoke(m, meta)));
            }
        }
    }

    private static List<Method> sortedGetters(Class<?> type) {
        List<Method> getters = new ArrayList<>();
        for (Method m : type.getMethods()) {
            if (m.getParameterCount() == 0
                    && (m.getName().startsWith("get") || m.getName().startsWith("is"))
                    && !m.getName().equals("getClass")
                    && m.getReturnType() != void.class) {
                getters.add(m);
            }
        }
        getters.sort(Comparator.comparing(Method::getName));
        return getters;
    }

    private static Object invoke(Method m, Object target) {
        try {
            return m.invoke(target);
        } catch (Exception e) {
            return "<threw " + e.getCause() + ">";
        }
    }

    /** BigDecimals print with their scale: scale is the data this spike is checking. */
    private static String describe(Object value) {
        if (value == null) return "absent (null)";
        if (value instanceof BigDecimal bd)
            return bd.toPlainString() + "  [scale=" + bd.scale() + ", unscaled=" + bd.unscaledValue() + "]";
        if (value instanceof Map<?, ?> map) return "map(size=" + map.size() + ")";
        return String.valueOf(value);
    }
}
```

`Probe.java` — the symbol round-trip, before and after remote init.

```java
import java.util.List;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.binance.BinanceAdapters;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.instrument.Instrument;

public class Probe {

    private static final List<String> NATIVE =
            List.of("BTCUSDT", "ETHBTC", "USDTTRY", "1INCHUSDT", "1000SATSUSDT");

    public static void main(String[] args) {
        System.out.println("### BEFORE remote init (fresh JVM)");
        probeAll();

        ExchangeSpecification spec = new ExchangeSpecification(BinanceExchange.class);
        spec.setShouldLoadRemoteMetaData(true);
        ExchangeFactory.INSTANCE.createExchange(spec);

        System.out.println("### AFTER remote init");
        probeAll();
    }

    private static void probeAll() {
        for (String symbol : NATIVE) {
            System.out.printf("  %-14s adaptSymbol -> %-28s toCurrencyPair -> %s%n",
                    symbol, adaptSymbol(symbol), toCurrencyPair(symbol));
        }
    }

    private static String adaptSymbol(String symbol) {
        try {
            Instrument instrument = BinanceAdapters.adaptSymbol(symbol, false); // false = spot
            String back = BinanceAdapters.toSymbol(instrument);
            return instrument + " (back=" + back + ", rt=" + symbol.equals(back) + ")";
        } catch (Throwable t) {
            return "<" + t.getClass().getSimpleName() + ">";
        }
    }

    private static String toCurrencyPair(String symbol) {
        try {
            CurrencyPair pair = BinanceAdapters.toCurrencyPair(symbol);
            return pair.getBase().getCurrencyCode() + "/" + pair.getCounter().getCurrencyCode();
        } catch (Throwable t) {
            return "<" + t.getClass().getSimpleName() + ">";
        }
    }
}
```

Reproduce with `gradle run` (metadata dump) and `gradle probe` (symbol round-trip). Both need outbound HTTPS to `api.binance.com` and no credentials.
