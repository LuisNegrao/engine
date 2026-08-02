# NEG-30/31/32 — Drafting and Accepting ADR 0004: Design Plan

One plan for three issues — [NEG-30](https://linear.app/negraolu/issue/NEG-30/draft-adr-0004-part-1-instrument-reference-model-and-symbol-grammar) (part 1), [NEG-31](https://linear.app/negraolu/issue/NEG-31/draft-adr-0004-part-2-feed-boundaries-timestamping-universe-archival) (part 2), [NEG-32](https://linear.app/negraolu/issue/NEG-32/adr-0004-consistency-review-and-acceptance) (review and acceptance) — because they are Steps 2–4 of the [NEG-23 design plan](neg-23-instrument-model-and-feed-architecture-design-plan.md) and produce **one file**: `docs/adr/0004-instrument-model-and-feed-architecture.md`. Three plan docs would triplicate a single outline; the sections below map one-to-one onto the three issues, and each issue's definition of done is tracked separately at the bottom.

Input of record is the [NEG-29 appendix](neg-29-binance-field-availability-appendix.md) — every `Instrument` field in the ADR must trace to a row of its table.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Instrument model shape | **Composed**: flat `Instrument` record of universal trading constraints + sealed `InstrumentSpec` for what varies by asset class, shipping `CryptoSpotSpec` only. | Supersedes NEG-30's "flat record" scope line, which predates the spike. Eight of ten observed fields are universal (NASDAQ and CME publish tick and lot sizes exactly as Binance does); `baseCurrency`/`quoteCurrency` are a *pair* concept only crypto and FX have, so keeping them on the universal record hardens the crypto assumption this epic exists to prevent. NEG-30's instruction to reject a **sibling** `CryptoInstrument`/`EquityInstrument` hierarchy still holds and gets argued. Flagged on NEG-30 per NEG-32's mismatch rule. |
| `1000SHIBUSDT` in the worked table | Keep the row, **labelled as a futures symbol**, and add spot `1000SATSUSDT`. | NEG-29 found it is futures-only. Deleting it would lose the digit-prefixed-base case; keeping it labelled turns an error into the table's only non-spot row, which is what makes the grammar's multi-asset claim concrete. |
| `source` value for feed events | `binance-feed`. | The `Event` javadoc already documents `"binance-feed"`; ADR 0003 §2's `binance` is an illustrative example, not a contract. Pick the one that matches code and say so, since `source` becomes the `owner` of `bus.feed.latencyMillis.*`. |
| Deferred items | Every one gets a **named owner story**, never a bare "TBD". Trading calendar → the IB/equities story; price bands and `MARKET_LOT_SIZE` → NEG-9; venue rate limits → NEG-26; reference-data-on-the-bus → a future ADR amendment. | NEG-32's definition of done forbids surviving TODOs but explicitly allows "assigned to a named future story". A deferral with an owner is a decision; one without is a hole. |
| Reference data placement | `core` for `Instrument`/`InstrumentSpec`/`AssetClass`; everything touching `ExchangeMetaData` in `feeds`. | modules.md litmus test 2 (two-plus components agree on it → `core`) and test 3 (imports a venue library → not `core`). Stated in the ADR so NEG-24 does not relitigate. |
| Scope discipline | The ADR decides; it does not implement. No Java lands in `core`, `bus` or a new module. | NEG-23's second definition-of-done box: "no code beyond what the ADR needs to demonstrate naming/mapping rules". Sketches in fenced blocks are the deliverable; compiled types are NEG-24's. |
| Git | Create the `neg-23` branch, stage, **hand the commit and PR to Luis**. | Project workflow — Luis commits. Linear status transitions are his too. |

## Step 1 — ADR skeleton and §1, the instrument reference model (NEG-30)

Header block in the 0001/0002 shape (`Status`/`Date`/`Context` → NEG-23), then the numbered `## Decision` list — seven entries, each quotable on its own. Then §1: the composed model, the record sketch, `core` placement, and the two rejected alternatives argued by name (flat-record-with-nullables; sibling per-asset-class hierarchy). Carries the spike's two hard rules: `BigDecimal` rendered with `toPlainString()` (XChange emits `9E+3`), and `maxQuantity` is the limit-order bound.

**Verify:** every component of the sketch traces to a row of the NEG-29 table; the two rejected alternatives are each named and argued, not merely mentioned.

## Step 2 — §2 population and §3 symbol grammar (NEG-30)

§2: startup fetch from venue metadata, held in memory, **not on the bus** — no `instrument` eventType in v1; rejects hand-maintained tick-size config and rejects bus publication, both by name. Records the spike's snapshot semantics (`TRADING`-only, no mid-session refresh) and the "build our own bimap, don't call `BinanceAdapters` statics" finding.

§3: the grammar as inherited invariant (ADR 0002 §3: uppercase, `-` legal, `.` illegal) plus per-asset-class rules and the worked mapping table — `BTCUSDT`, `1000SATSUSDT`, `USDTTRY`, `1000SHIBUSDT` (futures), `BRK.B`, `ESZ26`.

**Verify:** every row of the worked table round-trips on paper under the stated rules, and the `BRK.B` row states a decision rather than deferring it.

## Step 3 — §4 component boundaries and §5 timestamping (NEG-31)

§4: the adapter normalizes and publishes, nothing else; bar aggregation is an ordinary bus consumer, argued from the one-code-path rule, with in-adapter aggregation rejected explicitly ("the saved bus hop is the feature"). Must not blur ADR 0001's trade/quote split.

§5: `occurredAt` = venue event timestamp, `ingestedAt` = stamped by `Event.of(...)` at normalization, `source` = `binance-feed`. Missing venue timestamp → local receive plus a counted metric. Negative feed latency is reported, never clamped — and note that `RedisStreamsEventPublisher` already computes it unclamped, so this codifies behavior rather than requesting a change.

**Verify:** the section names the exact metric it feeds (`bus.feed.latencyMillis.*`, owner = `source`) and the ADR 0003 owner convention it relies on.

## Step 4 — §6 subscription universe and §7 archival statement (NEG-31)

§6: committed static config of canonical instrument ids, ~20 Binance spot pairs by volume, validated at startup against venue metadata, unknown symbol fails loud; hot-add rejected for v1; subscribe-to-everything rejected against ADR 0002's sizing math (the 4 GB budget at 20 pairs; 400+ pairs is ~20×).

§7: reaffirm ADR 0002 §6 — the archiver is an ordinary bus consumer, QuestDB is the only permanent archive, this epic's obligation ends at "everything the venue sent reaches the bus, unconflated". No new decisions here by design.

**Verify:** §6 states the universe's numbers against ADR 0002's retention table rather than inventing new ones; §7 introduces zero new architecture.

## Step 5 — Consistency review (NEG-32)

Read the full draft against ADR 0001 (tick split), 0002 (grammar, frozen names, archive boundary), 0003 (metric naming and owner conventions), and NEG-24 through NEG-28. For each of the five stories, confirm the decision it defers to "the NEG-23 ADR" has an explicit section. Record every mismatch as a Linear comment before acceptance — the NEG-17 precedent.

**Verify:** a written findings list exists with one line per checked ADR and per checked story; each finding is either resolved in the draft or filed as a comment.

## Step 6 — Accept and hand over (NEG-32)

Flip `Status:` to Accepted, tick the NEG-23 definition-of-done boxes in its plan, create the `luismarcosnegrao/neg-23-...` branch, stage the ADR and plans, hand over the commit message. No commit, no PR, no Linear status transitions — those are Luis's.

**Verify:** `grep -iE "TODO|TBD" docs/adr/0004-*.md` returns nothing; `git status` shows only `docs/` files.

## Definition of done

**NEG-30**
- [ ] Every row of the worked mapping table round-trips; `BRK.B` decided, not deferred → Step 2.
- [ ] Every `Instrument` field traces to a row of the NEG-29 spike table → Step 1.

**NEG-31**
- [ ] NEG-25, NEG-26 and NEG-27 each name decisions this ADR supplies (timestamps, feed-status thresholds, aggregator placement, window semantics); each answered by an explicit section → Steps 3–4, verified in Step 5.

**NEG-32**
- [ ] Both NEG-23 definition-of-done boxes check off against concrete ADR sections → Step 6.
- [ ] No TODO or TBD survives; every open point decided or assigned to a named story → Step 6 verify.
- [ ] Discovered mismatches with NEG-24..28 recorded on those issues before acceptance → Step 5.

## Pitfalls to expect

- **NEG-30's scope line is stale.** It says "flat record … sealed per-asset-class hierarchy argued and rejected". The composed model rejects the *sibling* hierarchy it means while still using a sealed component. Write the argument so both readings are answered, and comment on the issue rather than silently diverging.
- **Don't let §5 re-decide the envelope.** `occurredAt`/`ingestedAt` semantics are already fixed by the `Event` javadoc and ADR 0002 §5's "windows run on ingestion time" corollary. This ADR assigns *values*, it does not redefine fields.
- **The archival section is a reaffirmation, not a design.** Any new decision appearing in §7 means something was smuggled past ADR 0002 — delete it or promote it into its own section.
- **`1000SHIBUSDT` is futures.** The label is the point; an unlabelled row would re-import the spike's original error into a frozen document.
- **Deferrals need owners.** "Trading calendars are out of scope" is a hole; "trading calendars are the IB/equities story's problem, and NEG-26/NEG-27 currently assume 24/7" is a decision.
