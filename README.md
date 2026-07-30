# Trading Engine

Event-driven, multi-asset trading engine. Design and component breakdown: [ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

- **Docker Desktop** (local Redis)
- **A JVM** to launch Gradle. The build itself compiles with a pinned JDK 21 toolchain, auto-provisioned if not installed.

## Getting started

```sh
docker compose up -d      # local Redis (streams backbone)
./gradlew build           # compile + unit tests (no Docker needed)
./gradlew integrationTest # requires the compose Redis
```

## Load harness

Two manual profiles run the whole bus end to end — real publisher → compose Redis → three consumer
groups — and **fail the Gradle task** when the target is missed:

```sh
docker compose up -d                          # the harness needs the compose Redis
./gradlew :bus:e2eBench                       # smoke: 10,000 events/s for 2 min across 3 groups
./gradlew :bus:e2eBench --args='--soak'        # soak: the same load for 30 min, plus memory sampling
./gradlew :bus:e2eBench --args='--help'        # every knob: instruments, rate, groups, durations
./gradlew :bus:e2eBench --args='--write-baseline'
```

It measures sustained publish throughput, publish→handler latency (p50/p90/p99/max, from the envelope's
`ingestedAt`) and per-group lag, and gates on **≥ the configured rate sustained with every group's p99
under 50 ms and nothing lost**. Neither profile is part of `build`, `check` or `integrationTest`, and the
harness must not run at the same time as `:bus:integrationTest` — they share one Redis and would poison
each other's numbers.

The soak adds a leak gate on top: Redis `used_memory` and JVM heap-after-GC are sampled every 15 s, and
the run passes only if each series' **final-third median is within 1.10× its middle-third median** (the
first third is the fill phase and is excluded). To make that provable inside 30 minutes it runs a live
`StreamTrimmer` against a bench retention policy whose `md.tick.*` windows are **5 minutes** instead of
production's 12 hours — the caps and every other row are `RetentionPolicy.standard()` verbatim, and the
end-of-run `XLEN` census against the window-implied entry count is the direct evidence trimming engaged.
Untrimmed, 30 minutes at 10k/s is ~7 GB on a compose Redis that configures no `maxmemory` at all.

Baselines live in [`docs/baselines/`](docs/baselines), one file per profile, byte-identical to the run's
console output. They are only ever rewritten deliberately, by passing `--write-baseline`, and the diff
gets reviewed like code: a baseline that moves is either a real regression or a change worth explaining.

## Modules

- `core` — event model and shared abstractions. Never depends on infrastructure (no Redis/Lettuce).
- `bus` — Redis Streams implementation of the messaging abstractions.

## Project tracking

Work is tracked in Linear, project *Trading Engine* (epics NEG-5..NEG-14).
