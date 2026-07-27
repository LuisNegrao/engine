# NEG-22 — End-to-End Smoke and Throughput Test Harness: Implementation Plan

Implementation plan for [NEG-22](https://linear.app/negraolu/issue/NEG-22/end-to-end-smoke-and-throughput-test-harness). Everything lands in `bus/src/integrationTest` (a new `engine.bus.bench` package) plus a Gradle `JavaExec` task, a committed baseline under `docs/baselines/`, and a README section — no production code changes. The harness drives the *real* path end to end: `RedisStreamsEventPublisher` → compose Redis → `RedisStreamsEventSubscriber` consumer groups, and judges it against an explicit target. This story closes the NEG-5 Message Bus epic: it is the load proof ADR 0002's sizing assumptions have been waiting for ("to be validated by the NEG-22 harness") and the permanent regression baseline for later tuning.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Harness shape | A plain `main` (`EndToEndBench`) in `bus/src/integrationTest`, run via `./gradlew :bus:e2eBench`, never wired into `build`/`check` — the `PublishBench` precedent exactly. Pass/fail still enforced: a missed target exits non-zero, so the Gradle task fails | JUnit is the wrong container for a 30-minute soak with a human-readable report as its primary output; a `main` prints, exits, and composes with `--args`. The exit code keeps it a *gate* rather than a suggestion — CI or a pre-release checklist can run it and trust red/green. `PublishBench` stays untouched: it answers a different question (publish-side service latency, the NEG-18 batch-API decision input); this harness measures the full path, and merging them would couple two baselines that should move independently. |
| Config surface | Parsed from `--args`: `--instruments 20 --rate 10000 --groups 3 --warmup PT30S --duration PT2M`, plus `--soak` (switches duration to PT30M and enables memory sampling) and `--write-baseline`. Defaults *are* the smoke profile, so the acceptance criterion's "one command" is literally `./gradlew :bus:e2eBench` | Every number the issue calls configurable (instruments × rate × duration × groups) is a flag with an opinionated default; ISO-8601 durations because `Duration.parse` is free and unambiguous. Two profiles, not two programs: soak is the same measurement run longer with memory sampling on top, and any drift between "the smoke path" and "the soak path" would make the soak prove the wrong thing. |
| Traffic shape | 20 instruments `BENCH-01.ITEST` … `BENCH-20.ITEST`, 80 % `QuoteTick` / 20 % `TradeTick`, fully-populated payloads, round-robin across instruments, published through the real publisher (real `StreamNames` routing, real `MAXLEN` stamping) → 40 live streams | 20 instruments and the 4:1 quote/trade ratio are ADR 0002 §4's sizing universe — the harness validates those assumptions at 50× nominal rate, so it should mirror their shape. Fully-populated payloads reproduce the measured 330/297 B wire sizes ("realistic payload sizes" per the issue). The `.ITEST` venue keeps bench streams disjoint from anything real, per `PublishBench`. |
| Rate control | One generator thread, absolute-schedule pacing (`start + i × paceNanos` with `LockSupport.parkNanos`, the `PublishBench` pattern), aggregate rate across all instruments; in-flight bounded by a 4096-permit semaphore matching the publisher's Lettuce queue | An absolute schedule self-corrects after a GC or scheduler pause by bursting to catch up — which is the honest meaning of "sustained 10k/s", unlike a relative sleep that silently drifts slow. At 10 kHz the per-event budget is 100 µs against ~5–10 µs of encode+dispatch, so one thread suffices; a `--publishers` shard knob exists but defaults to 1 (pitfalls). |
| Latency definition and clock | publish→handler = handler-receipt `Instant.now()` − envelope `ingestedAt` (stamped by `Event.of` immediately before `publish`). Generator and consumers share one JVM, hence one clock | This is the issue's "using envelope timestamps" made precise. `ingestedAt` is the publish-side stamp; `occurredAt` is rejected because it is source-time — the generator fabricates it, and any fabricated gap would pollute the measurement (that gap is NEG-21's *feed latency*, a different metric). A side-channel `eventId → nanoTime` map was rejected: it measures a path no real consumer has, and the single-JVM wall clock is sub-millisecond on JDK 21 — two orders of magnitude below the 50 ms gate. |
| Multi-group scenario | N=3 `RedisStreamsEventSubscriber` instances by default, groups `bench-strategy-1..N`, each subscribing 40 explicit selectors (partitioned types require the instrument — the subscriber enforces it), `LATEST` + `ProcessAll`, `SubscriberTuning.standard()`. No-interference criterion: *every* group's delivered count equals published (+ its reported redeliveries), *every* group independently meets the latency target, and every group's lag drains to 0 at the end | Three groups model archiver + two strategies — enough to prove fan-out isolation without turning the dev box into the bottleneck (fan-out math below). `standard()` tuning because the harness certifies production behavior, not a tuned-for-benchmarks configuration. Deduplication by `eventId` was rejected: a `HashSet` of 18 M UUIDs costs >1.5 GB of heap to learn what `delivered − published` already says; the report prints that delta as `redeliveries` and a healthy run shows 0. |
| Lag measurement | `Subscription.lag()` sampled at 1 Hz per group by the harness; report per-group max-during-run and final. `BusMonitor` is *not* in the loop | The acceptance criterion says "per-group lag", and `lag()` is the same undelivered+pending number the monitor emits — read directly, without adding a fourth moving part to the system under measurement. NEG-21's integration suite already proves the monitor; re-proving it here would only blur whose overhead the numbers include. |
| Sample recording | Per-group latency arrays **preallocated** at `rate × duration` longs before the clock starts; exact percentiles by sort, `ceil(q × n) − 1` (the NEG-21 math). Soak sizing: 3 groups × 18 M × 8 B ≈ 432 MB — the bench task runs with `-Xmx2g` | Exact samples, no histogram library — fidelity first, RAM is the lever (NEG-17 principle), and 432 MB is well inside a dev heap. Preallocation is what makes the soak's JVM-flatness assertion *honest*: a sample buffer growing for 30 minutes is indistinguishable from the leak the soak exists to catch. |
| Soak retention | Bench `RetentionPolicy`: `md.tick.*` windows shortened to **5 min** (caps and every other row unchanged from `standard()`), with a real `StreamTrimmer` on its standard 60 s cadence | The production 12 h window cannot trim anything inside a 30-minute run, and untrimmed growth is 10k/s × ~400 B × 1800 s ≈ **7 GB** — past ADR 0002's 4 GB wall (which the compose Redis doesn't even configure; pitfalls). "Verify retention trimming actually bounds Redis memory" therefore *requires* a window shorter than the run: with 5 min, steady state is 10k/s × 400 B × 300 s ≈ **1.2 GB**, and the plateau is the proof. Windows are explicitly revisable configuration (ADR 0002 §4) — the mechanism under test is identical, only the number changes. |
| Flat-memory criterion | Sample every 15 s: Redis `used_memory` (`INFO memory`) and JVM used-after-GC (`System.gc()` then `totalMemory − freeMemory` — coarse, but a leak check needs a trend, not precision). Discard the first 10 min (window still filling). Pass: median of the final third ≤ 1.10 × median of the middle third, for both series | "Flat after warm-up" needs a number or it's a vibe. Median-of-thirds is robust to AOF-rewrite and GC spikes that a max-based or linear-regression test would false-positive on; 10 % headroom absorbs steady-state jitter while still catching any real leak, which compounds far past 10 % over 20 minutes. |
| Target and enforcement | Keep the issue's proposal: **≥10,000 events/s sustained** aggregate with **per-group p99 publish→handler < 50 ms**, groups=3. Enforced in code; miss → exit 1. Feasibility, so the number is a position and not a hope: 10k `XADD`/s + 3 × 10k delivered via `XREADGROUP` (batch 256) + acks ≈ 70–80k effective ops/s — inside a single Redis instance's ~100k+ budget; decode at ~2–5 µs × 30k/s spread over three dispatch threads is <5 % of three cores | 10k/s is 50× the ADR's 20-pair nominal (200/s) — generous headroom for medium/long-term strategies, yet the math says a dev box reaches it, so the gate is falsifiable rather than aspirational. If measurement disagrees, the revision is recorded in the baseline file and on the issue with the measured evidence — that is the issue's own escape hatch, and it must be *conscious*, never a silent constant edit. |
| Baseline files | `docs/baselines/bus-e2e-smoke.md` and `docs/baselines/bus-e2e-soak.md`, written only under `--write-baseline`, byte-identical to the stdout report (one rendering path): date, machine, JVM, Redis version, full config, results table, verdict | One file per profile beats one file with section-surgery: each run overwrites its own file wholesale, and git history *is* the versioning ("versioned baseline file" per the issue). Byte-identical stdout/file output means the committed baseline is exactly what the console showed — nothing to reconcile. |

## Fan-out math (what the dev box must sustain)

- Publish: 10k events/s ≈ 4 MB/s wire (400 B/entry with stream overhead).
- Deliver: 3 groups × 10k/s = 30k decodes/s in one JVM, ~40k Redis ops/s with acks batched.
- Redis memory during smoke (2 min, no trimming needed): ~10k × 150 s × 400 B ≈ 600 MB peak — fine untrimmed.
- Soak steady state with the 5-min bench window: ≈ 1.2 GB Redis, flat.

## Package layout

```
bus/src/integrationTest/java/engine/bus/bench/
├── BenchConfig.java     ← args parsing, smoke/soak defaults, validation (pure, unit-testable shape)
├── TickGenerator.java   ← paced publish loop: mix, round-robin, in-flight window, failure counting
├── GroupProbe.java      ← one consumer group: subscriber + recording handler + 1 Hz lag sampler
├── SoakSampler.java     ← 15 s Redis/JVM memory series + median-of-thirds verdict
├── BenchReport.java     ← percentiles, verdicts, markdown rendering, baseline writing
└── EndToEndBench.java   ← main: wire, warm up, measure, drain, report, exit code

docs/baselines/bus-e2e-smoke.md
docs/baselines/bus-e2e-soak.md
```

## Step 1 — Scaffolding: config, Gradle task, report skeleton

`BenchConfig` (defaults above, `Duration.parse` for time flags, fail loudly on unknown flags); the `:bus:e2eBench` `JavaExec` task next to `publishBench` (integrationTest classpath, `-Xmx2g`, `group = "verification"`, args pass-through); `BenchReport` skeleton printing the header block (`os/arch/cores/java/redis_version`, config echo — reuse the `PublishBench` header pattern). `EndToEndBench.main` wires config → header → (nothing yet) → exit 0.

**Verify:** `./gradlew :bus:e2eBench --args='--rate 100 --duration PT2S'` prints the header with the compose Redis version and exits 0.

## Step 2 — Generator

`TickGenerator`: for each scheduled slot, round-robin instrument, 80/20 quote/trade by slot index (deterministic, not random — reruns are comparable), fully-populated payload, `Event.of(source, instrument, now, tick)`, publish through the semaphore window; count `published`/`failed` and first failure. Absolute-schedule pacing; separate unmeasured warmup pass before the measured window (JIT + connection ramp, the `PublishBench` lesson). Stop = schedule exhausted, then await in-flight drain.

**Verify:** `./gradlew :bus:e2eBench --args='--duration PT30S'` reports achieved publish rate within 1 % of 10,000/s with 0 failures.

## Step 3 — Consumer groups, latency, lag

`GroupProbe`: builds its `RedisStreamsEventSubscriber` (group `bench-strategy-N`), subscribes the 40 explicit selectors **before the generator starts** (`LATEST` — a late subscribe silently loses the head and breaks the delivered==published accounting), handler records `Instant.now().toEpochMilli()`-precision delta against `ingestedAt` into the preallocated array — no boxing, no allocation on the dispatch path. A 1 Hz sampler thread reads `Subscription.lag()` per group. After the generator stops: drain phase — wait until every group's lag reaches 0 (deadline 60 s, else fail loudly with the stuck group named), so the tail of the run is measured, not truncated. Report per group: delivered, redeliveries (`delivered − published`), throughput, p50/p90/p99/max latency, max/final lag.

**Verify:** default smoke run prints all three groups with delivered == published, redeliveries 0, final lag 0, and single-digit-millisecond p50.

## Step 4 — Pass/fail gate, baseline writer, README

Verdict logic in `BenchReport`: throughput ≥ target rate and every group's p99 < 50 ms → `PASS`, else `FAIL` with the violated clause named; `EndToEndBench` exits 1 on `FAIL`. `--write-baseline` writes the profile's file under `docs/baselines/` with content identical to stdout. README gains a "Load harness" section: the two commands, what they measure, where baselines live, and the rule that baselines are only rewritten deliberately (`--write-baseline`) with the diff reviewed like code.

**Verify:** a run with `--rate 25000` (or any forced miss) exits non-zero and Gradle reports the task failed; `--write-baseline` produces a file that `diff` confirms matches the console output.

## Step 5 — Soak mode

`--soak`: duration PT30M, bench `RetentionPolicy` (5-min `md.tick.*` windows) wired into both publisher and a live `StreamTrimmer`, `SoakSampler` running from minute 0 but judging only post-warm-up thirds. Report adds: Redis memory series summary (min/median/max per third), JVM used-after-GC series, per-stream `XLEN` at end vs. window-implied expectation (~300 s × per-stream rate — the direct evidence trimming engaged), and the flatness verdicts, which join the exit-code gate in soak runs.

**Verify:** `./gradlew :bus:e2eBench --args='--soak'` against a freshly restarted compose Redis passes: Redis plateaus ≈ 1.2 GB, `XLEN` of a quote stream sits near 500/s × 300 s ≈ 150k, JVM verdict flat.

## Step 6 — Measure, decide, land

Branch `luismarcosnegrao/neg-22-end-to-end-smoke-and-throughput-test-harness` (Linear's name), one commit per step, `./gradlew spotlessApply build` before each.

1. Run the smoke profile 3× on the dev box; if all pass, the 10k/50 ms target stands — record the middle run with `--write-baseline` and commit `bus-e2e-smoke.md`. If any fail, revise the target *in the gate code and the baseline together*, with the measured numbers as the recorded rationale, and note the revision on NEG-22.
2. Run the soak once, commit `bus-e2e-soak.md`.
3. Full sweep: `docker compose up -d && ./gradlew build :bus:integrationTest` (the harness itself stays out of both — confirm by running them without Redis-breaking side effects).
4. PR body: both baseline reports verbatim, the target decision (kept or revised, with numbers), and a line closing out NEG-5's "validated by the NEG-22 harness" debt in ADR 0002 §4 — if measured per-instrument rates differ wildly from the ADR's nominal assumptions, say so on the issue; amending the ADR sizing table is a follow-up, not this story.

## Definition of done (mapped to the issue)

- [ ] One command runs the harness against docker-compose Redis and prints throughput, p50/p99 latency, per-group lag → `./gradlew :bus:e2eBench` (defaults are the smoke profile), Steps 1–3.
- [ ] Target met, or consciously revised with reasoning recorded → Step 4 gate + Step 6.1 decision, rationale in the committed baseline and on the issue.
- [ ] Soak run shows flat memory (Redis and JVM) after warm-up → Step 5 median-of-thirds verdicts, both series, in the exit-code gate.
- [ ] Baseline file committed; harness documented in the README → Steps 4 and 6 (`docs/baselines/bus-e2e-{smoke,soak}.md`, README "Load harness" section).

## Pitfalls to expect

- **Production windows cannot prove trimming in 30 minutes.** 12 h windows never fire inside the run, and per-stream `MAXLEN` caps (1–2 M) are barely grazed at 500/s/stream × 1800 s — a soak against `standard()` retention just balloons Redis ~7 GB on a compose instance with **no `maxmemory` configured** (ADR 0002's 4 GB wall is not in docker-compose.yml). The bench policy's 5-min window is the fix; do not "simplify" it away, and do not add `maxmemory` to compose as part of this story — flag it separately if wanted.
- **Subscribe before the first publish, or the accounting lies.** `LATEST` groups miss everything published before group creation; the delivered==published criterion then fails mysteriously. Wiring order is: probes subscribed → warmup → measured window.
- **The JVM-flatness assertion is only as honest as the harness's own allocation.** Preallocate every sample array before the measured window; keep the handler allocation-free (primitive array writes, no autoboxing, no per-event `String`). A harness that allocates per event *is* the memory growth it's hunting.
- **AOF rewrites will spike the tail.** The compose Redis runs `appendonly yes`; during the soak the AOF grows ~4 MB/s and auto-rewrite forks periodically — expect p99.9/max latency spikes and disk churn. Report max but gate on p99, and keep a few GB of disk free for the run.
- **Catch-up bursts are correct, not a bug.** After any pause the absolute schedule bursts to restore the average — transient in-flight climbs toward the 4096 window. That is what "sustained" means; don't smooth it away with relative sleeps, which hide missed throughput.
- **One generator thread has a ceiling.** 100 µs/event budget against ~5–10 µs of work leaves 10× headroom, but if achieved rate lands short with zero failures, the thread is the bottleneck, not the bus — shard with `--publishers 2` before concluding anything about Redis.
- **Redeliveries under a stalled dispatch thread.** A >30 s pause (claim `minIdle`) lets a peer-less claim sweep redeliver; `redeliveries > 0` in the report is the visible symptom. Report it, never dedup it silently — at-least-once is the contract being certified.
- **Don't run the harness concurrently with `:bus:integrationTest`.** Bench streams are isolated (`.ITEST`), but both loads share one Redis — concurrent runs poison each other's latency numbers.
- **Drain with a deadline.** "Wait for lag 0" without a timeout hangs forever if a group stalls; 60 s then fail naming the group and its residual lag — a hung harness that never reports is worse than a red one.
