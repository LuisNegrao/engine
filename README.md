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

Baselines live in [`docs/baselines/`](docs/baselines), one file per profile, byte-identical to the run's
console output. They are only ever rewritten deliberately, by passing `--write-baseline`, and the diff
gets reviewed like code: a baseline that moves is either a real regression or a change worth explaining.

## Modules

- `core` — event model and shared abstractions. Never depends on infrastructure (no Redis/Lettuce).
- `bus` — Redis Streams implementation of the messaging abstractions.

## Project tracking

Work is tracked in Linear, project *Trading Engine* (epics NEG-5..NEG-14).
