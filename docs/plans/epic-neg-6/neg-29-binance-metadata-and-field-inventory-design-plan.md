# NEG-29 — Spike: Binance Venue Metadata and Consumer Field Inventory: Design Plan

Plan for [NEG-29](https://linear.app/negraolu/issue/NEG-29/spike-binance-venue-metadata-and-consumer-field-inventory), Step 1 of the [NEG-23 design plan](neg-23-instrument-model-and-feed-architecture-design-plan.md). The deliverable is **one committed markdown appendix** — `docs/plans/epic-neg-6/neg-29-binance-field-availability-appendix.md` — carrying a field-availability table. No production code lands: the spike is a throwaway program that runs once, prints, and is deleted, with its source pasted into the appendix so the run is reproducible without being built.

> Path note: the issue says the appendix goes under `docs/plans/neg-23/`. That directory no longer exists — commit `863aa7c` moved plans under `docs/plans/epic-neg-<epic>/`, so the appendix lands beside this plan in `docs/plans/epic-neg-6/`. Same for the stale path inside the NEG-23 plan's Step 1 (fixed in Step 6 here).

What this gates: [NEG-30](https://linear.app/negraolu/issue/NEG-30) (ADR 0004 part 1) freezes the `Instrument` record against this table, and [NEG-24](https://linear.app/negraolu/issue/NEG-24) decides how much symbol-mapping machinery the `feeds` module needs based on the base/quote question below. Both are downstream of facts nobody has checked yet — that is the whole reason this spike exists ahead of the ADR.

## The three questions the spike must return with

1. **Which candidate `Instrument` fields does Binance actually populate, at what precision?** Tick size, quantity step size, min notional, min/max quantity — and what is missing that the record sketch assumes.
2. **Does XChange's `CurrencyPair` already carry the base/quote split?** `BTCUSDT` is not lexically splittable (`USDTTRY` and `1INCHUSDT` break every naive rule). If XChange hands over the split for free, NEG-24 ships a lookup map; if not, NEG-24 owns a REST call to `exchangeInfo` and the mapping table built from it.
3. **Who downstream needs each field?** A field with no named consumer and no venue source does not go in the record — it goes in the ADR as an explicitly reserved nullable, or nowhere.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Where the spike code lives | A throwaway Gradle project **outside the repo tree**, in the session scratch dir. Its `build.gradle.kts` and single `Main.java` get pasted verbatim into the appendix. | The issue is explicit that the code doesn't land. Creating the `feeds` module now steals NEG-24's first step and puts XChange in the build before ADR 0004 has said the module exists; a spike source set inside `core` or `bus` would put an exchange library on `core`'s classpath, which `docs/modules.md` litmus test 3 forbids outright. ~80 lines in a fenced block is rerunnable and costs the build nothing. |
| Do **not** touch `gradle/libs.versions.toml` | XChange coordinates stay in the throwaway project's build file. | Adding `xchange = …` to the catalog is NEG-24's commit. A catalog entry with no consumer is exactly how throwaway code stops being throwaway. |
| Data source | Both XChange `ExchangeMetaData` **and** the raw `GET /api/v3/exchangeInfo` JSON for the same symbols. | Two different failure modes hide behind one empty field: *Binance doesn't publish it* vs *XChange drops it on the floor*. Only reading both distinguishes them, and the answer decides whether NEG-25 can lean on XChange for reference data or must own the REST call. A table that can't tell those apart is not worth committing. |
| XChange version | Pin the current 5.2.x release in the throwaway project; stamp the exact version and the run timestamp in the appendix header. | The metadata API moved across 5.x (`ExchangeMetaData.getCurrencyPairs()` → `getInstruments()` returning `InstrumentMetaData`). An unversioned availability table is unfalsifiable a year from now — and this table is what ADR 0004 gets frozen against. |
| Auth | No API key. `ExchangeSpecification.setShouldLoadRemoteMetaData(true)`, nothing else. | `exchangeInfo` is a public endpoint; reference data must not require credentials for the feed to start. If it turns out it does, that is itself a finding NEG-25 needs (deployment gains a secret). |
| Symbol sample | `BTCUSDT`, `ETHBTC`, `USDTTRY`, `1INCHUSDT`, `1000SATSUSDT`, plus one high-precision small-cap. Confirm each exists on **spot** before drawing conclusions. | Adversarial for the split rule, not decorative: a quote that isn't USDT (`ETHBTC`), a base that *starts with* a quote asset's name (`USDTTRY` — naive suffix-stripping yields base `USDTTR`), and bases beginning with digits (`1INCH`, `1000SATS`). Note the issue's `1000SHIBUSDT` is a **futures** symbol, not spot — `1000SATSUSDT` is the spot-listed equivalent trap. Confirm at runtime and record which sample symbols actually resolved. |
| Precision recording | Record each numeric as its exact `BigDecimal` string **plus** `scale()`. Never `stripTrailingZeros()`, never round. | `0.00000001` and `1E-8` are the same number with different scale, and the ADR's position is "preserve the venue's stated precision — don't normalize scale". The appendix has to show what precision actually arrives or NEG-24 can't honor that rule. |
| Table shape | Six columns: `field | who needs it | Binance exchangeInfo source | XChange accessor | populated? (observed value + scale) | verdict`. Verdict ∈ `required` / `reserved-nullable` / `venue-only, not modeled`. | The issue's definition of done needs a named consumer *and* a confirmed source per field; the verdict column is what NEG-30 lifts straight into ADR 0004's record definition, so the spike ends with the decision already made, not with raw data someone has to re-interpret. |
| Absence is recorded explicitly | Every unpopulated field gets the literal word `absent` (and *why*: not in the Binance response vs present in JSON but dropped by XChange). No blank cells. | A blank cell six months from now reads as "nobody checked", which restarts this spike. |

## Layout

```
<scratch>/neg29-binance-spike/          # throwaway, outside the repo
  build.gradle.kts                      # xchange-core + xchange-binance, application plugin
  src/main/java/Main.java               # one file: metadata dump + raw exchangeInfo dump

docs/plans/epic-neg-6/
  neg-29-binance-metadata-and-field-inventory-design-plan.md   # this plan
  neg-29-binance-field-availability-appendix.md                # the deliverable
```

## Step 1 — Stand up the throwaway project and confirm the symbol sample exists on spot

Gradle `application` project in the scratch dir, `xchange-core` + `xchange-binance` (REST only — no `xchange-stream-binance`, websockets are NEG-25's problem). First run does nothing clever: fetch `GET https://api.binance.com/api/v3/exchangeInfo?symbols=["BTCUSDT",…]` with the sample list and print the raw JSON to a file in the scratch dir.

If the request returns HTTP 451, the runner's IP is geo-blocked — retry against `https://data-api.binance.vision` (same public market-data API, no auth) and record in the appendix which host produced the numbers.

**Verify:** the saved JSON contains a `symbols[]` entry for every sample symbol, each with `status: "TRADING"`. Any symbol that 400s or is missing gets swapped for a spot-listed equivalent and the substitution noted in the appendix.

## Step 2 — Dump XChange's `ExchangeMetaData` for the same symbols

`ExchangeFactory` → `BinanceExchange` with `setShouldLoadRemoteMetaData(true)`, then walk the instrument metadata map and print, per symbol: price tick size, quantity step size / amount step size, minimum and maximum amount, counter-amount minimums, trading fee, and the price/base scales — each as `value` + `scale()`, or `absent`.

Print the concrete accessor path used for each value (e.g. `ExchangeMetaData.getInstruments().get(pair).getPriceStepSize()`); that string becomes the "XChange accessor" column, and NEG-24 codes against it directly.

**Verify:** for `BTCUSDT` the printed price tick size and quantity step size match the `PRICE_FILTER.tickSize` and `LOT_SIZE.stepSize` values in the Step 1 JSON, digit for digit. A mismatch means XChange is transforming the value — that is a finding for the appendix, not something to silently accept.

## Step 3 — Diff XChange against the raw filters

Line up every field the record sketch wants against the Binance filter that carries it (`PRICE_FILTER` → tickSize/minPrice/maxPrice, `LOT_SIZE` → minQty/maxQty/stepSize, `NOTIONAL`/`MIN_NOTIONAL` → minNotional and its `applyToMarket` flag, `MARKET_LOT_SIZE` where it differs from `LOT_SIZE`). For each, classify: *present in both*, *present in Binance JSON but dropped by XChange*, *absent at the venue*.

The `MARKET_LOT_SIZE` ≠ `LOT_SIZE` case deserves its own line: if Binance publishes a different step size for market orders, "quantity step size" is not one number and the OMS story needs to know before the record freezes at one field.

**Verify:** every row of the classification names its Binance filter by name; the count of "dropped by XChange" rows is stated explicitly (including zero), because that number is what decides whether NEG-24/NEG-25 own a REST call.

## Step 4 — Settle the base/quote split question

Confirm what `CurrencyPair` carries for each sample symbol: does `getBase()`/`getCounter()` come back as `1INCH`/`USDT` and `USDT`/`TRY` correctly, and does it originate from Binance's own `baseAsset`/`quoteAsset` fields in `exchangeInfo` (check the JSON) rather than from client-side parsing? Also record the exact venue-native string XChange uses when *sending* a symbol, since the adapter has to round-trip canonical → native.

Write the finding as a one-line verdict for NEG-24: either "XChange supplies the split from venue data; the mapper is a startup-built bimap over `CurrencyPair`" or "XChange guesses; NEG-24 owns the `exchangeInfo` fetch and builds the bimap from `baseAsset`/`quoteAsset`".

**Verify:** the round-trip `BTCUSDT → BTC-USDT.BINANCE → BTCUSDT` and `USDTTRY → USDT-TRY.BINANCE → USDTTRY` both hold on paper under the stated mechanism, with the source of the split named for each direction.

## Step 5 — Inventory the consumers

For each candidate field, name the concrete consumer and what it does with it — no field survives on "seems useful":

- **Risk fat-finger check ([NEG-9](https://linear.app/negraolu/issue/NEG-9/epic-risk-manager))** — max order size and notional sanity: which of `maxQty`, `minNotional`, tick size does a pre-trade check actually read, versus what comes from risk config?
- **OMS quantity/price rounding ([NEG-10](https://linear.app/negraolu/issue/NEG-10/epic-oms-execution-gateway))** — a live order rejected for step-size violation is a production incident; the simulated fill engine must round identically or backtest and live diverge. Name the fields each back-end needs.
- **The feed adapter itself (NEG-25)** — universe validation at startup, base/quote for the canonical symbol, and whatever the websocket payloads don't carry.
- **Bar aggregation (NEG-27)** — expected to need nothing from reference data. State that explicitly; a named non-consumer is a real finding, since it keeps the aggregator free of an `Instrument` dependency.

Any field with a Binance source but no named consumer gets verdict `reserved-nullable` **with the justification written out**, or gets cut.

**Verify:** every field in the NEG-23 record sketch (`assetClass`, `baseCurrency`, `quoteCurrency`, `priceTickSize`, `quantityStepSize`, `minNotional`) appears in the consumer column with at least one named story, or carries a written reserved-nullable justification. Same for any field the venue turned out to provide that the sketch didn't anticipate.

## Step 6 — Write the appendix, delete the code, hand over the commit

Assemble `neg-29-binance-field-availability-appendix.md`: header (XChange version, API host, run timestamp, sample symbols as resolved), the six-column table, the Step 3 diff classification, the Step 4 verdict for NEG-24, and the spike source in a fenced block at the end. Fix the stale `docs/plans/neg-23/` path in the NEG-23 plan's Step 1 while here. Delete the scratch project.

Per project workflow: stage the two files and hand the commit message over — Luis commits.

**Verify:** `git status` shows exactly two modified/added files (the new appendix + the NEG-23 plan path fix) and nothing under `core/`, `bus/`, `gradle/` or `settings.gradle.kts`; `./gradlew build` is untouched by this story because nothing in the build changed.

## Definition of done (mapped to the issue)

- [ ] Every field in the proposed `Instrument` record has a named consumer → Step 5, consumer column.
- [ ] Every field has a confirmed Binance source, or an explicit "reserved, nullable" justification → Steps 2–3 (source + populated columns) and Step 5 (verdict column).
- [ ] `ExchangeMetaData` dumped for a handful of pairs including an adversarial one, with observed precision → Step 2, `value` + `scale()` per field.
- [ ] The `CurrencyPair` base/quote question answered with a verdict NEG-24 can act on → Step 4.
- [ ] Field-availability table committed under `docs/plans/epic-neg-6/` (the relocated `docs/plans/neg-23/`) → Step 6.
- [ ] Spike code does not land → Step 6 verify: no build files touched, source lives in the appendix only.

## Pitfalls to expect

- **Remote metadata is off by default.** Without `setShouldLoadRemoteMetaData(true)` XChange serves a bundled static JSON and the dump looks plausible while being stale, or empty — which reads as "Binance provides nothing". Confirm the values against the Step 1 raw JSON before believing any of them; that cross-check is the whole point of pulling both.
- **HTTP 451 from `api.binance.com`.** Binance geo-blocks some IPs outright, and XChange's default host is the blocked one. `data-api.binance.vision` serves the same public market data; record which host produced the numbers so a rerun on a different network can reproduce them.
- **`1000SHIBUSDT` is a futures symbol.** Pulling futures metadata for a spot design would quietly import contract semantics (multipliers, funding) into a spot table. Confirm every sample symbol against the *spot* `exchangeInfo` and swap in `1000SATSUSDT`/`1INCHUSDT` for the digit-prefixed-base case.
- **Full `exchangeInfo` is megabytes and carries real request weight.** Ask for `?symbols=[...]` with the sample list, not the whole venue. This is a one-shot spike; there is no reason to pull 2000 symbols to look at six.
- **The deprecated accessor still compiles.** `getCurrencyPairs()` survives alongside `getInstruments()` in 5.x and may return a differently-populated view. Record which one produced each value — NEG-24 will code against exactly what the appendix names.
- **Scale is data, not formatting.** `stripTrailingZeros()` or `toString()` on a `BigDecimal` in the dump destroys the one property the ADR promised to preserve. Print `toPlainString()` alongside `scale()`.
- **Don't start writing the mapper.** The temptation at Step 4 is to build the bimap "since the data's right there". That is NEG-24's first commit, and any code written here has to be deleted at Step 6 regardless — write the verdict sentence and stop.
- **A missing field is a finding, not a blocker.** If `minNotional` turns out absent, the spike's job is to record `absent — not in Binance response` and let NEG-30 decide whether the record keeps a reserved nullable. It is not the spike's job to invent a fallback.
