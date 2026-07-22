# CLAUDE.md

## Project

Trading engine (Linear: "Trading Engine" project, Negraolu team) — event-driven trading over a Redis Streams message bus. Java 21, Gradle multi-module, Spotless with palantirJavaFormat (run `./gradlew spotlessApply` before eyeballing diffs).

- `core` — event model (`Event` envelope + sealed `Payload` hierarchy in `engine.core.event`) and serde (`engine.core.serde`). Its public API must stay free of Jackson and Lettuce; these boundaries are load-bearing and checked in plans' definitions of done.
- `bus` — Redis Streams implementation (Lettuce) of the messaging abstractions defined in `core`. Its `integrationTest` suite needs the docker-compose Redis (`docker compose up -d`) and is deliberately not part of plain `build`.
- Which module a new class belongs in: `docs/modules.md` has the responsibilities and litmus tests.
- Nullable record components get an Optional view method (`maybeX()`); consumers use the view, never the raw accessor's null. Components themselves are never `Optional`.

Accepted ADRs are the source of truth: wire `eventType` strings and stream names that appear in one are frozen — renaming them is a data-loss event, not a refactor.

## Goal — what this project is building

The full picture lives in `ARCHITECTURE.md` at the repo root; read it before making design decisions. In short: a multi-asset trading engine (crypto first, then stocks/futures/commodities) made of ~10 decoupled components — data feeds, news/sentiment analysis, message bus, historical data store, strategy runtime, risk manager, OMS/execution gateway, position & P&L accounting, UI, and state persistence/recovery — communicating exclusively as publishers/subscribers on the message bus.

The core architectural rule: **one code path for backtest, paper, and live trading.** A strategy cannot tell whether events come from historical replay, a simulated fill engine, or a live exchange — backtesting swaps the data source (Historical Data Store replay) and the execution back-end (simulated fills), and nothing else in the system changes. There is deliberately no off-the-shelf backtesting framework: the event-driven engine *is* the backtester.

Technology direction (each pick's rationale is recorded in `ARCHITECTURE.md` so decisions can be revisited): Java for the whole engine; Redis Streams as the bus (Kafka judged operationally overweight at this scale); QuestDB as the historical store, with Parquet for large static datasets; XChange for crypto connectivity first, Interactive Brokers TWS for other asset classes later; LLM-based news/sentiment scoring publishing ordinary bus events; Grafana dashboards first, a Spring Boot + React control plane later. The current `core`/`bus` modules are the first slice of this system: the event model and the bus everything else plugs into.

## Workflow

- Work is tracked in Linear as NEG-* issues; branches use Linear's suggested name (`luismarcosnegrao/neg-NN-...`). A story gets a plan doc first (pattern below), then one commit per plan step.
- **Always set `priority` when creating Linear issues** — never leave "No priority" (Luis orders his views by it). Map dependency/build order onto the 1–4 scale: foundational blockers 1–2, mid-pipeline work 3, capstone/verification work 4. Set it in the same `save_issue` call that creates the issue, not as a follow-up pass.

## Design principle: data fidelity over resources

When a design trades data fidelity or completeness against RAM/disk/throughput, recommend the full-fidelity option with hardware/config as the scaling lever — never default to downsampling, conflation, or lossy compression to fit a resource budget. Present the resource cost honestly, but frame hardware growth as the remedy. Rationale (Luis, NEG-17): resource limits are temporary and purchasable; data discarded before archiving is gone forever and permanently narrows the strategy space. ADR 0002's raw-ticks/no-conflation decision codifies this.

## ADR pattern — `docs/adr/`

Files are `docs/adr/NNNN-<kebab-slug>.md`, 4-digit sequential numbering. Shape (follow 0001/0002):

- `# ADR NNNN — <Title>`
- Bullet header: `Status:` (Accepted/…), `Date:`, `Context:` linking the NEG issue.
- `## Decision` — the decisions as a short numbered list, each one quotable.
- One `##` section per design question, arguing the choice and naming the rejected alternatives.
- `## Consequences` — what downstream stories must implement or must not violate, and what is frozen architecture vs revisable configuration.

## Plan pattern — `docs/plans/`

Files are `docs/plans/neg-NN-<slug>-implementation-plan.md` (or `-design-plan.md` for stories whose deliverable is an ADR, not code). Written before implementing a story, committed with the work. Shape (follow neg-16 and neg-18):

- `# NEG-NN — <Title>: Implementation Plan` (or `: Design Plan`).
- Intro paragraph: link to the Linear issue, what lands in which module, and what the story gates.
- `## Decisions to lock in before typing` — a `| Decision | Choice | Why |` table; opinionated, with the choice argued in the Why column, referencing ADRs where they decide the matter.
- Package layout as a tree in a code block (implementation plans).
- Numbered `## Step N — …` sections, each ending with a `**Verify:**` line giving a concrete check (a Gradle command, a test that must pass, an observable).
- `## Definition of done (mapped to the issue)` — checkboxes mapping every acceptance criterion on the Linear issue to the step/test that satisfies it.
- `## Pitfalls to expect` — bullets with a bolded lead phrase; concrete traps specific to this story, not generic advice.

Tone throughout: take positions and argue them inline, use concrete numbers, no hedging.
