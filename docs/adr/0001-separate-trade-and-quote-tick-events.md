# ADR 0001 — Separate trade and quote tick events

- Status: Accepted
- Date: 2026-07-13
- Context: [NEG-16 — Define the core event model](https://linear.app/negraolu/issue/NEG-16/define-the-core-event-model-envelope-payloads)

## Decision

Model market microstructure as **two distinct payload types**, `TradeTick` and `QuoteTick`,
rather than one unified `Tick` payload with a discriminator.

- `TradeTick` — an executed print: `price`, `quantity`, `aggressor` (`Side`).
- `QuoteTick` — a top-of-book update: `bidPrice`, `bidQuantity`, `askPrice`, `askQuantity`.

## Rationale

The two carry **almost no overlapping fields**. A trade is a single price/size with an aggressor
side; a quote is a two-sided bid/ask book snapshot. Folding them into one payload would force:

- **Every field nullable** — a unified record can populate only the trade fields *or* the quote
  fields on any given instance, so the type system stops expressing what is actually present.
- **Every consumer to branch** — a strategy reacting to trades and one maintaining a book would
  both have to inspect a discriminator and null-check before using any field, pushing validation
  that the model should guarantee out into every call site.

Consumers also **rarely want them interleaved** in a single stream: trade-driven and quote-driven
logic are usually separate subscriptions. Keeping them as two types lets each stream, and each
`switch` over `Payload`, stay precise. Because `Payload` is a sealed interface, adding or splitting
tick types remains a compile-time-checked change across all consumers.

## Consequences

- Two `eventType` wire names, `tick.trade` and `tick.quote`, registered independently in
  `PayloadRegistry`. These become stream/routing names in NEG-18+ and must not be renamed.
- A consumer that genuinely needs both correlates them via `instrumentId` + `occurredAt`, which is
  the honest cost of the split and cheaper than the alternative's pervasive null-handling.
