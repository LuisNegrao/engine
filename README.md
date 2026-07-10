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

## Modules

- `core` — event model and shared abstractions. Never depends on infrastructure (no Redis/Lettuce).
- `bus` — Redis Streams implementation of the messaging abstractions.

## Project tracking

Work is tracked in Linear, project *Trading Engine* (epics NEG-5..NEG-14).
