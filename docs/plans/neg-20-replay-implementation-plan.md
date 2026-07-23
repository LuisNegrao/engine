# NEG-20 — Replay: Deliver Historical Stream Ranges Through the Live Subscriber Interface: Implementation Plan

Implementation plan for [NEG-20](https://linear.app/negraolu/issue/NEG-20/replay-deliver-historical-stream-ranges-through-the-live-subscriber). The message-bus half of the backtesting foundation: a replay vocabulary in `core` (`EventSubscriber.replay`, `ReplayRange`/`ReplayPosition`, a `Replay` handle with an explicit end-of-range signal) and a pure-reader Redis implementation in `bus` that delivers a recorded stream range through the identical `EventHandler` path as live consumption. Scope is exactly the ADR 0002 §6 carve-out — serving very recent ranges *from the bus*, bounded by the retention window. The replay **engine** (virtual clock, speed control, store-sourced history, the reserved `replay.*` streams) is NEG-7's; this story gives that engine its bus-window mode and gives any component a way to re-drive recorded traffic today.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Replay is a pure reader | `XRANGE`-based. No consumer group, no `XACK`, no DLQ writes, no `replay.*` streams — a replay never writes a single byte to Redis | Consumer-group replay would litter Redis with per-backtest group state (or corrupt a component's real group offset), and ack/PEL machinery answers a question replay doesn't ask — "did the consumer crash?" is meaningless when the caller holds a handle and reruns at will. A pure reader means ten concurrent backtests cannot perturb live consumption in any way. The `replay.` stream prefix stays reserved for NEG-7's store-sourced engine, untouched here — nothing this story does can be confused with live traffic because nothing this story does is *on* a stream. |
| API shape | `Replay replay(List<EventSelector> selectors, ReplayRange range, EventHandler handler)` on `EventSubscriber`, beside `subscribe`. `Replay extends AutoCloseable` with `CompletionStage<Long> done()` and `close()` | Same subscriber object, same selectors, same `EventHandler` — the "cannot tell replayed from live" criterion is satisfied structurally, at the type level. No `SubscribeOptions`: start position is what `ReplayRange` *is*, and `LagPolicy` is group machinery with no referent here. `done()` is the end-of-range signal: completes with the delivered count on exhaustion, exceptionally on abort — a backtest blocks on it and knows both "data exhausted" and "how much data". Throws synchronously on bad input at wiring time, exactly like `subscribe`. |
| Position vocabulary (core) | Sealed `ReplayPosition`: `earliest()`, `at(Instant)`, `offset(String token)` — the token transport-opaque. `ReplayRange(start, end)`, `end` nullable with `maybeEnd()`; absent end = tail sampled at replay start | The issue asks for offset *or* timestamp starts; an opaque `String` token keeps core Lettuce-free (bus interprets it as a Redis stream ID and validates the `ms[-seq]` shape). Core validates only what it can: start non-null, `earliest` illegal as end, and end-before-start when both are `at()`. Absent end means "everything currently retained, then stop" — a bounded snapshot. Following past the tail into live data is what `subscribe` is for; replay always terminates. |
| `occurredAt` → position mapping | `at(Instant t)` maps to stream-ID millis: start `<t.toEpochMilli()>-0`, end `<t.toEpochMilli()>` (Redis fills the end seq to max — the whole millisecond, inclusive both ends). Delivered events are **never filtered by `occurredAt`** | ADR 0002 §5 is explicit: windows and replay run on ingestion time; stream IDs *are* ingest millis. So the documented precision is: millisecond granularity, addressing *arrival* time — which tracks `occurredAt` to within publish latency in normal operation and diverges unboundedly after a feed outage (old `occurredAt` at recent IDs). Filtering delivered events by `occurredAt` to fake event-time replay would silently drop entries — partial silence, the exact failure the issue forbids. Event-time-exact replay is the store's job (NEG-7); the Javadoc says so. |
| Retention guard | At start, per stream: `XINFO STREAM` → `max-deleted-entry-id`; requested start ≤ it ⇒ throw `ReplayRetentionException` (new, core, unchecked) before delivering anything. Re-checked after every batch read; a trim past the cursor mid-replay completes `done()` exceptionally. Missing stream: fatal for `at()`/`offset()` starts, contributes zero events for `earliest()` | `max-deleted-entry-id` is the only honest signal — the first-entry ID cannot distinguish "trimmed away" from "never existed", and window arithmetic lies because `XTRIM ~` is approximate. `earliest()` *means* "oldest retained", so it is exempt by definition; a timestamped start is a claim about specific data and must fail loudly when that data is gone. The mid-replay check closes the gap the trimmer can open under a 12h window: better to abort a backtest than let it silently compute across a hole. |
| Cross-stream order | K-way merge by stream ID ascending across the resolved streams; ties broken by stream name | Live guarantees no cross-stream order, so any interleaving is contract-compatible — but a backtest wants determinism above all, and global ingest-time order is strictly the most useful deterministic choice. Rerunning an identical bounded replay over untrimmed data delivers the identical sequence, byte for byte. The Javadoc warns handlers not to *rely* on cross-stream order, since live will never honor it. |
| Failure semantics | First handler throw or undecodable entry **aborts the replay**: `done()` completes exceptionally with the cause, naming stream, entry ID and `eventId`. No retries, no DLQ | Live retries because live failures are often transient (network, downstream); a replay is a pure function of recorded bytes and deterministic handler code — an immediate retry fails identically, so retrying is the same cargo cult NEG-19 rejected for decode failures. Parking backtest failures on `dlq.*` would pollute the streams NEG-21 monitors for *production* poison. The issue's "same ack semantics where applicable" hedge resolves to: acks are group machinery, not applicable to a pure reader — documented, not fudged. |
| Threading & connections | One dedicated connection + one daemon thread per replay (`bus-replay-<n>`), sync commands, batched `XRANGE COUNT tuning.batchCount()`, full speed. Thread closes its own connection on completion | Identical model to a subscription: the handler is invoked from exactly one thread, never concurrently with itself, blocking stalls only this replay. No pacing — the virtual clock lives in NEG-7. Reuses the subscriber's `RedisClient` (its 5 s timeout options are safe: replay issues no blocking reads). Self-closing matters: a backtest harness runs hundreds of replays and must not leak a connection per run. |
| Byte-identical dispatch | Extract the entry→`Event` decode (read `RedisStreamsEventPublisher.EVENT_FIELD`, decode via the injected codec) into one package-private helper used by both the live `dispatch` and the replay loop | The acceptance criterion asks for a *test* that the handler path is byte-identical; sharing the decode makes it true by construction, and the test then proves it observationally: same events, same envelopes, same handler interface, live vs replay. |
| In-memory implementation | `InMemoryEventBus` records every published event and implements `replay` synchronously on the caller's thread: matching recorded events are delivered before `replay` returns, `done()` already completed. `at()` maps on `ingestedAt`; `offset` token = decimal index into the recorded log; retention never expires (documented) | The fixture retains everything, so `ReplayRetentionException` is unreachable in-memory — fine: component tests use it to re-drive traffic, not to test retention edges (those are `bus` integration tests). Synchronous delivery matches the fixture's existing dispatch model and keeps component tests deterministic with zero waiting. |

## Package layout

```
core/src/main/java/engine/core/bus/
├── EventSubscriber.java             ← + replay(...); replay contract Javadoc lives here
├── ReplayPosition.java              ← sealed: Earliest | At(Instant) | Offset(String)
├── ReplayRange.java                 ← (start, end?) + factories, maybeEnd()
├── Replay.java                      ← handle: done() + close()
└── ReplayRetentionException.java    ← "the bus no longer holds (part of) this range"

core/src/testFixtures/java/engine/core/bus/
└── InMemoryEventBus.java            ← + recorded log, synchronous replay

bus/src/main/java/engine/bus/
├── ReplayPositions.java             ← position → stream-ID mapping + validation (pure)
├── RedisReplay.java                 ← merge loop, retention guard, completion
└── RedisStreamsEventSubscriber.java ← replay() wiring; decode extracted and shared
```

## Step 1 — Core contracts

The four new types plus the `EventSubscriber.replay` method. The replay contract Javadoc (on `replay`, cross-referenced from the class doc) carries the issue's documentation duties explicitly:

1. **Pure reader.** Replay never writes to the bus: no consumer group, no acknowledgements, no dead-lettering. Live consumption elsewhere is unaffected by any number of concurrent replays.
2. **Identical delivery.** The same `EventHandler` type, invoked from a single dedicated thread, never concurrently with itself; events carry their original envelopes — same `eventId`, same `occurredAt`, nothing re-minted.
3. **Positions are ingest-time.** `at(Instant)` addresses when the bus *received* events, at millisecond precision — within publish latency of `occurredAt` normally, arbitrarily far after a feed outage. Event-time-exact or beyond-retention replay belongs to the Historical Data Store (NEG-7).
4. **Deterministic and bounded.** Per-stream publish order, streams merged by ingest position with a fixed tie-break; an identical bounded replay over untrimmed data yields the identical sequence. `done()` completes with the delivered count at end-of-range.
5. **Fail-fast.** A range the bus no longer holds throws `ReplayRetentionException` up front or completes `done()` exceptionally if trimming overtakes a running replay — never silent partial data. A throwing handler or undecodable entry aborts the replay; `close()` before exhaustion completes `done()` with a `CancellationException`.

`ReplayRange` validates what core can: non-null start, `Earliest` rejected as end, `end.at() < start.at()` rejected when both are timestamps. Nullable `end` gets `maybeEnd()` per the house rule. Factories: `from(ReplayPosition start)`, `between(ReplayPosition start, ReplayPosition end)`, plus `Instant` conveniences.

**Verify:** `./gradlew :core:dependencies --configuration apiElements` still shows no Jackson, no Lettuce; `./gradlew build` green.

## Step 2 — testFixtures: `InMemoryEventBus` replay

Record every published event (append to an internal log at publish time, alongside the existing dispatch). `replay` filters the log through the selectors (same matching as live), applies the range (`ingestedAt` for `at()`, log index for `offset` tokens), delivers synchronously on the calling thread, and returns a `Replay` whose `done()` is already completed with the count. Handler throw ⇒ `done()` completes exceptionally, remaining events undelivered — same abort semantics as the real thing, minus the threads.

Unit tests in core: replayed events are the recorded instances (same `eventId`), selector narrowing works, `at()` range bounds on `ingestedAt` are inclusive, handler throw aborts with the cause, count on `done()` matches, a component wired to one `InMemoryEventBus` can publish live, then replay its own history through the same handler.

**Verify:** `./gradlew build` green.

## Step 3 — `ReplayPositions`: mapping and validation (pure)

Package-private static mapping, unit-testable without Redis:

- `startId(ReplayPosition)` — `earliest()` → `"0-0"`; `at(t)` → `"<millis>-0"`; `offset(token)` → the token, after validating `\d+(-\d+)?` (throw `IllegalArgumentException` naming the token otherwise).
- `endId(position, tailAtStart)` — absent end → the tail ID sampled per stream when the replay starts; `at(t)` → `"<millis>"` (Redis expands to max seq — the whole millisecond inclusive).
- `offset` positions (start or end) require the selector list to resolve to **exactly one stream** — an opaque per-stream ID cannot address a multi-stream merge; throw at wiring time.

Unit tests: each mapping literal (hardcoded expected strings, anti-tautology rule as ever), boundary inclusivity documented in the assertions, offset-token rejection, offset-with-two-streams rejection.

**Verify:** `./gradlew :bus:test` green.

## Step 4 — `RedisReplay` happy path

Extract `static Optional<Event> decodeEntry(EventCodec, StreamMessage<String, byte[]>)` from the subscriber's `dispatch` and re-use it verbatim from replay — the shared-path decision made real. Then `replay()` on the subscriber: resolve selectors via `resolveStreams`-style logic (no lag-policy check), validate the range via Step 3, sample per-stream tails for the absent-end case, run the retention guard (Step 5), open a dedicated connection, start the thread, return the handle.

The loop: per-stream buffer fed by `XRANGE <stream> <cursor> <end> COUNT batchCount`, k-way merge popping the smallest ID (tie-break stream name), dispatch each event to the handler, advance that stream's cursor **exclusively** (Lettuce `Range.Boundary.excluding` — the `(id` form) so batch boundaries never redeliver. A stream is exhausted when a refill returns empty; the replay completes when all are. On completion: complete `done()` with the count, close the connection, exit the thread. `close()`: signal stop, join briefly (no blocking read to wait out — 2 s), complete `done()` exceptionally with `CancellationException` if not already done; idempotent after natural completion.

**Verify:** `./gradlew :bus:test` green; integration smoke — publish 10 events via the NEG-18 publisher, bounded replay delivers all 10 equal events in order, `done()` completes with 10, `redis-cli client list` shows the replay connection gone after completion.

## Step 5 — Retention guard and abort semantics

- **Start check**, per stream, on the caller's thread before anything is delivered: `XINFO STREAM` → `max-deleted-entry-id` (fold with the existing `asFieldMap` helper). Start ID ≤ max-deleted ⇒ `ReplayRetentionException` naming the stream, the requested start and the oldest surviving ID. Missing key (`ERR no such key`): fatal for `at()`/`offset()` starts, an empty contribution for `earliest()`.
- **Per-batch check**, after each `XRANGE` returns and before its entries dispatch: re-read `max-deleted-entry-id`; if it has advanced past this stream's cursor, entries between cursor and batch have been trimmed — complete `done()` exceptionally with `ReplayRetentionException`. The check runs after the read so a trim can never invalidate entries already fetched.
- **Aborts:** handler throw and undecodable entry both stop the loop and complete `done()` exceptionally (cause chained; message carries stream, entry ID, `eventId` where decodable). Delivered-so-far stays delivered — the exception, not a rollback, is the signal.

Unit-test the pure parts (guard decision against fabricated `XINFO` maps, exception messages); behavior lands in Step 6's integration tests.

**Verify:** `./gradlew :bus:test` green.

## Step 6 — Integration tests (the acceptance criteria)

In `bus/src/integrationTest`, reusing the NEG-18/19 hygiene: test-only instruments, every stream tracked and `DEL`ed, aggressive tuning. Several tests `XADD` with **explicit IDs** to pin time boundaries — the publisher can't control ingest millis, raw `XADD` can.

1. **Live vs replay, identical delivery** — publish 300 ticks; a group subscription (`EARLIEST`) captures `List<Event>` live; a bounded replay of the same range runs the same handler class into a second list. Assert the lists are element-wise equal — record equality covers `eventId`, `occurredAt`, full envelope and payload (criteria 1 and 4 in one test: same code path observationally, nothing re-minted).
2. **End-of-range signal** — bounded replay of 100; `done()` completes with exactly 100 within the timeout; the replay thread has exited; `close()` afterward is a no-op (criterion 2).
3. **Out-of-retention fails loudly** — publish 1,000; `XTRIM MAXLEN` (exact) to 100; replay `at(before the surviving window)` throws `ReplayRetentionException` from `replay()` itself and the handler saw **zero** events (criterion 3 — never partial silence). Variant: `at()` start on a stream that has never existed also throws.
4. **Trim overtakes a running replay** — handler blocks on a latch after the first event; `XTRIM` deletes entries beyond the cursor; release the latch; `done()` completes exceptionally with `ReplayRetentionException` — partial data arrived *with* a loud signal, not silently.
5. **Deterministic multi-stream merge** — explicit-ID `XADD`s interleaved across two instrument tick streams; replay both selectors twice; both runs deliver the identical, ID-ascending sequence.
6. **Time-boundary precision** — explicit IDs at `t-1ms`/`t`/`t+1ms` prove `at(t)` start includes `t-0` and excludes `t-1ms`, and `at(t)` end includes the whole millisecond `t`.
7. **Abort semantics** — handler throws on event 50 of 100; `done()` completes exceptionally with the cause; delivered count is 49; **no `dlq.*` key exists** and `XINFO GROUPS` shows no group was created — the pure-reader promise, asserted.

**Verify:** `docker compose up -d && ./gradlew :bus:integrationTest` green.

## Step 7 — Land it

Branch `luismarcosnegrao/neg-20-replay-deliver-historical-stream-ranges-through-the-live` (Linear's suggested name), one commit per step.

1. Before each commit: `./gradlew spotlessApply build`.
2. Full sweep before the PR: `docker compose up -d && ./gradlew build :bus:integrationTest`.
3. Update `docs/modules.md`: core's `engine.core.bus` row gains the replay contract types; the bus table gains `RedisReplay` / `ReplayPositions` rows; the testFixtures line notes the in-memory replay.
4. PR body: the `replay` contract Javadoc pasted verbatim (the mapping-precision documentation is an acceptance criterion — make it reviewable without opening the diff), plus the live-vs-replay equality test output.

## Definition of done (mapped to the issue)

- [ ] Recorded range delivered via the normal subscriber interface; test asserts the handler code path is byte-identical to live consumption → Step 4 (shared `decodeEntry`, structural) + Step 6.1 (observational).
- [ ] Bounded replay terminates with an explicit end-of-range signal → `Replay.done()` (Step 1) + Step 6.2.
- [ ] Range older than retention fails with a clear error, never partial silence → Step 5 guard + Steps 6.3/6.4.
- [ ] Replayed events preserve original envelopes (same `eventId`, `occurredAt`) — no re-minting → pure reader by design + Step 6.1's record-equality assertion.
- [ ] `occurredAt`→position mapping decided and documented with its precision (scope bullet) → ingest-time millisecond mapping, Step 1 Javadoc point 3, pinned by Step 6.6.
- [ ] Identical handler contract — types, threading, ack semantics where applicable (scope bullet) → Steps 1 + 4; "where applicable" resolved and documented as not-applicable-to-a-pure-reader.

## Pitfalls to expect

- **Inclusive `XRANGE` cursors redeliver batch boundaries.** Advancing with the last-seen ID re-fetches that entry at the head of every next batch — off-by-one duplicates, or an infinite loop on a single-entry tail. Always advance with an exclusive boundary (`Range.Boundary.excluding`, the Redis `(id` form).
- **The first-entry ID cannot prove retention.** An empty or late-starting stream looks identical whether data was trimmed or never published. `max-deleted-entry-id` is the authoritative trim record — and the reason the guard needs Redis ≥ 7.0 semantics (the docker-compose image already qualifies; the smoke test will scream if it's downgraded).
- **`XTRIM ~` is approximate.** Entries older than MINID can survive a sweep, so "compute the window edge and compare" mis-reports both ways. Never do retention math from `RetentionPolicy` windows at replay time; trust only `max-deleted-entry-id`.
- **Don't "improve" time replay with `occurredAt` filtering.** After a feed outage, entries with old `occurredAt` sit at recent stream IDs (ADR 0002 §5). Filtering them out to simulate event time silently drops data — the exact partial-silence failure this story exists to prevent. Deliver the ID range; let NEG-7 own event time.
- **`done()` must complete on every exit path.** Natural exhaustion, retention abort, handler abort, `close()`, and an unexpected loop-level `RedisException` all must complete the future — a backtest harness blocks on it, and a path that forgets leaves the harness hanging forever. Structure the loop so completion happens in one `finally`.
- **Replay must not ride the control connection.** A multi-gigabyte range read sharing the subscriber's control connection would starve every concurrent `lag()` call. Dedicated connection per replay, closed by the replay thread itself — hundreds of backtest runs must not leak hundreds of connections.
- **Equal stream IDs across streams are real.** Two streams ingesting in the same millisecond with the same seq produce equal IDs; without the stream-name tie-break the merge order flips between runs and the determinism test flakes rarely and unreproducibly. Fix it in the comparator, not in the test.
- **The in-memory fixture must abort like the real one.** If `InMemoryEventBus.replay` swallows handler exceptions where `RedisReplay` aborts, a component test passes and the same component fails against Redis. The fixture's abort semantics are contract, not convenience — Step 2's tests pin them.
