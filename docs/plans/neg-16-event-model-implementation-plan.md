# NEG-16 — Core Event Model: Implementation Plan

Implementation plan for [NEG-16](https://linear.app/negraolu/issue/NEG-16/define-the-core-event-model-envelope-payloads). Everything lands in `core` — no bus/infra involvement. Each step ends with a verification check.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Trade vs quote ticks | **Two event types**: `TradeTick` and `QuoteTick` | They share almost no fields (price/qty/aggressor vs bid/ask levels) and consumers rarely want them interleaved in one stream. A discriminated payload would force every field to be nullable and every consumer to branch. Recorded as ADR 0001. |
| JSON library | **Jackson** (databind + jsr310), latest 2.x | The JVM default; records supported natively since 2.15. Confined to `engine.core.serde` — event records carry **zero Jackson annotations**, so replacing JSON with a binary codec later touches only the codec package. |
| Payload discriminator | **Explicit registry** (`PayloadRegistry`: eventType string ↔ record class + version), not `@JsonTypeInfo` | Polymorphic annotations tie the type hierarchy to Jackson and are a deserialization-gadget footgun. A registry also gives unknown-`eventType` handling for free (lookup miss ⇒ skip). |
| `eventType` naming | Dotted lowercase: `tick.trade`, `tick.quote`, `bar`, `signal`, `order.intent`, `fill`, `metric`, `command` | These strings will become stream/routing names in NEG-18+; pick them once, never rename. |
| In-memory shape | One `Event` record (envelope fields + `Payload payload`), `Payload` a **sealed interface** | Envelope fields live in exactly one place; sealed ⇒ exhaustive `switch` in consumers, compiler flags a forgotten case when a payload type is added. `eventType`/`schemaVersion` are *wire* concerns — derived from the registry at encode time, not stored on `Event`. |
| Money/quantities | `BigDecimal` everywhere, serialized as JSON **strings** | Per the issue. Strings also preserve scale (`"67231.50"` stays 2 d.p.), which `double`-backed JSON numbers destroy. |
| `instrumentId` format | `SYMBOL.VENUE`, venue-qualified, e.g. `BTC-USDT.BINANCE` | Own value type `InstrumentId` with `parse`/`toString`. Separator `.` (last dot wins, symbols may contain `-`); venue uppercase. Nullable on the envelope — absent on e.g. `Command`. |

## Target wire format

```json
{
  "eventId": "5f0c1c1e-9d1a-4b7e-8c2a-1f2e3d4c5b6a",
  "eventType": "tick.trade",
  "schemaVersion": 1,
  "source": "binance-feed",
  "instrumentId": "BTC-USDT.BINANCE",
  "occurredAt": "2026-07-10T14:03:22.113Z",
  "ingestedAt": "2026-07-10T14:03:22.145Z",
  "payload": { "price": "67231.50", "quantity": "0.0042", "aggressor": "BUY" }
}
```

`instrumentId` is omitted (not `null`) when absent. `occurredAt`/`ingestedAt` are both required — the gap between them is the feed-latency metric.

## Package layout

```
core/src/main/java/engine/core/
├── event/
│   ├── Event.java              ← envelope record: eventId, source, instrumentId, occurredAt, ingestedAt, payload
│   ├── Payload.java            ← sealed interface permits the 8 below
│   ├── TradeTick.java  QuoteTick.java  Bar.java  Signal.java
│   ├── OrderIntent.java  Fill.java  Metric.java  Command.java
│   ├── InstrumentId.java  Side.java  OrderType.java  TimeInForce.java  CommandAction.java
└── serde/
    ├── EventCodec.java         ← the interface a binary format would re-implement
    ├── EnvelopeView.java       ← envelope-only read, payload untouched
    ├── PayloadRegistry.java    ← eventType ↔ (class, schemaVersion)
    └── JsonEventCodec.java     ← the only file that imports Jackson
```

Delete `CorePlaceholder.java` — the event model is the real core content it was holding space for.

## Step 1 — Jackson in the version catalog

`gradle/libs.versions.toml`:

```toml
jackson = "2.18.3"   # check latest 2.x

jackson-databind = { module = "com.fasterxml.jackson.core:jackson-databind", version.ref = "jackson" }
jackson-jsr310 = { module = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310", version.ref = "jackson" }
```

`core/build.gradle.kts`: both as `implementation` (not `api` — nothing outside `core` should see Jackson types; that's the swap-the-format door).

**Verify:** `./gradlew :core:dependencies --configuration apiElements` shows no Jackson (only `runtimeClasspath` does).

## Step 2 — Value types and enums

- `InstrumentId(String symbol, String venue)` — compact constructor validates non-blank and uppercase venue; `parse("BTC-USDT.BINANCE")` splits on the **last** `.`; `toString()` re-joins.
- `Side { BUY, SELL }`, `OrderType { MARKET, LIMIT }`, `TimeInForce { GTC, IOC, FOK, DAY }`, `CommandAction { START, STOP, KILL }`.

**Verify:** unit tests for `InstrumentId` parse ↔ toString round-trip and rejection of malformed input (`"BTCUSDT"`, `""`, `"a.b.c"` parses venue `c`).

## Step 3 — Payload records + envelope

```java
public sealed interface Payload
        permits TradeTick, QuoteTick, Bar, Signal, OrderIntent, Fill, Metric, Command {}
```

| Record | Components |
|---|---|
| `TradeTick` | `price`, `quantity`, `aggressor` (Side) |
| `QuoteTick` | `bidPrice`, `bidQuantity`, `askPrice`, `askQuantity` |
| `Bar` | `intervalStart` (Instant), `interval` (Duration), `open`, `high`, `low`, `close`, `volume` |
| `Signal` | `signalType` (String, e.g. `"sentiment"`), `value`, `confidence` (nullable) |
| `OrderIntent` | `strategyId`, `side`, `quantity`, `orderType`, `limitPrice` (nullable for MARKET), `timeInForce`, `clientOrderId` |
| `Fill` | `clientOrderId`, `executedQuantity`, `price`, `fee`, `feeCurrency`, `terminal` (boolean: final vs partial) |
| `Metric` | `name` (e.g. `"pnl.unrealized"`), `value`, `owner` (strategyId or component) |
| `Command` | `target` (strategyId or `"*"`), `action`, `args` (Map\<String,String\>, may be empty — never null) |

Compact constructors do cheap invariant checks only: required fields non-null, quantities positive, `limitPrice` present iff `orderType == LIMIT`, `Bar` interval positive. No business logic.

`Event` is a plain record of the five envelope fields + `payload`; a static factory `Event.of(source, instrumentId, occurredAt, payload)` fills `eventId` (random UUID) and `ingestedAt` (clock now) for the common producing path. Take a `Clock` variant for testability.

**ADR:** `docs/adr/0001-separate-trade-and-quote-tick-events.md` — the trade-vs-quote decision and rationale from the table above, plus a Javadoc pointer on both records.

**Verify:** `./gradlew :core:build` green; a `switch` over `Payload` in a scratch test compiles without `default` (proves sealing works).

## Step 4 — Codec

```java
public interface EventCodec {
    byte[] encode(Event event);
    EnvelopeView envelope(byte[] bytes);      // parses envelope only; never fails on unknown eventType
    Optional<Event> decode(byte[] bytes);      // empty ⇔ well-formed but unregistered eventType
}
```

- `EnvelopeView` = record of the envelope fields with `eventType` as raw `String` and `schemaVersion` as `int` — this is all that generic infra (bus, archive, replay, metrics) ever reads.
- `PayloadRegistry`: immutable map both ways (`String → PayloadType(name, version, Class)` and `Class → PayloadType`). `PayloadRegistry.standard()` registers the 8 types at v1. The codec takes a registry in its constructor — this injection point is what makes the version-compat test possible without polluting the real registry.
- `JsonEventCodec`: one shared `ObjectMapper` configured with `JavaTimeModule`, `WRITE_DATES_AS_TIMESTAMPS` off, `WRITE_DURATIONS_AS_TIMESTAMPS` off (Durations as ISO-8601 `PT1M`), `FAIL_ON_UNKNOWN_PROPERTIES` **off** (this is the backward-compat mechanism — pin it with a test), `USE_BIG_DECIMAL_FOR_FLOATS` on, `NON_NULL` inclusion, and BigDecimal serialized via `ToStringSerializer` registered on a module (keeps records annotation-free).
- Decode is two-phase: read tree → build `EnvelopeView` → registry lookup on `eventType` → hit: convert `payload` subtree to the record class; miss: `Optional.empty()`. Malformed JSON or a missing required envelope field **throws** — corrupt data is a bug, unknown type is a normal forward-compat situation; the API shape encodes that distinction.

**Verify:** the Step 5 round-trip test passes.

## Step 5 — Tests (the acceptance criteria, literally)

All in `core/src/test/java/engine/core/serde/`:

1. **Round-trip, all 8 types** — a parameterized test over a list of fully-populated sample events, one per payload type: `decode(encode(e))` equals `e` envelope-and-payload. Because records use `equals`, and `BigDecimal.equals` is scale-sensitive, this *also* proves string serialization preserves scale — use samples with trailing zeros (`"67231.50"`) on purpose.
2. **BigDecimal wire shape** — assert the raw JSON contains `"price":"67231.50"` (a string, exact scale), not a number.
3. **Forward/backward compat** — two test-only payload records `CompatProbeV1(a)` / `CompatProbeV2(a, b)` registered under the *same* eventType in two codec instances (one registry each). v2-encoded bytes decode via the v1 codec (extra field ignored); v1-encoded bytes decode via the v2 codec (`b` is null). Asserts the two ObjectMapper settings that make this work never regress.
4. **Unknown eventType** — hand-built JSON with `"eventType": "tick.futuristic"`: `envelope()` returns a full `EnvelopeView`, `decode()` returns `Optional.empty()`, nothing throws. Plus the negative control: syntactically broken JSON makes both throw.
5. **Envelope-only read** — `envelope()` on bytes whose payload subtree is garbage (`"payload": {"price": "not-a-number"}`) still succeeds, proving generic infra never pays payload-parsing costs or risks.

**Verify:** `./gradlew :core:test` green; `./gradlew build` green from the repo root (bus untouched but recompiled against core).

## Step 6 — Definition of done

Mapped to the issue's acceptance criteria:

- [ ] Every event type round-trips with envelope intact → test 1.
- [ ] v2 payload read by v1-aware and v2-aware consumers → test 3.
- [ ] Unknown `eventType` handled gracefully (skip + log, not crash) → test 4; "log" is the consumer's job, the codec's job is returning `empty` instead of throwing.
- [ ] Trade-vs-quote decision documented with rationale → ADR 0001 + Javadoc on both tick records.
- [ ] Bonus boundary check unchanged from NEG-15: `:core:dependencies` still Lettuce-free, and Jackson does not leak into `core`'s API (`apiElements` check from Step 1).

Commit history suggestion: one commit per step.

## Pitfalls to expect

- **`BigDecimal.equals` is scale-sensitive** — `new BigDecimal("1.5")` ≠ `new BigDecimal("1.50")`. This makes record equality strict, which the round-trip test exploits; but it also means *producers must be consistent about scale* or dedup/compare logic downstream will mislead. Don't "fix" a failing round-trip with `compareTo` — a scale change in transit is a real serialization bug.
- **`Instant`/`Duration` need `JavaTimeModule`** — without it Jackson serializes them as objects/epoch-decimals and the compat story silently degrades. The wire-shape test (ISO-8601 strings) pins this.
- **Records + Jackson**: works annotation-free on Jackson 2.15+; if a field mysteriously arrives null, check the record component name matches the JSON key exactly — there is no `@JsonProperty` fallback by design here.
- **Don't reach for `@JsonTypeInfo`** when wiring the payload polymorphism — it moves the discriminator inside the payload, couples records to Jackson, and reopens the CVE-riddled default-typing door. The registry already does this job.
- **`Optional` as a record component** — tempting for `confidence`/`limitPrice`/`instrumentId`; skip it. Optional isn't `Serializable`, Jackson needs an extra module for it, and nullable-with-Javadoc is the established Java record convention.
- **Spotless will reformat the sample JSON in tests** if kept as text blocks with odd indentation — run `./gradlew spotlessApply` before eyeballing diffs, not after.
