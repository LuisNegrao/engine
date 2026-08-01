# NEG-15 — Bootstrap Project Skeleton: Implementation Plan

Implementation plan for [NEG-15](https://linear.app/negraolu/issue/NEG-15/bootstrap-project-skeleton-and-local-infrastructure). Written to be executed by hand; each step ends with a verification check.

## Decisions to lock in before typing

| Decision | Choice | Why |
|---|---|---|
| Build tool | Gradle, **Kotlin DSL** (`.kts`) | Typed build scripts, IDE completion; the modern default |
| JDK | **21 (LTS)** | Current LTS with virtual threads — useful later for many websocket feeds |
| Dependency versions | **Version catalog** (`gradle/libs.versions.toml`) | One place to bump versions across all future modules |
| Formatter | **Spotless + palantir-java-format** | Applied via Gradle so it works in any editor; ends style debates now |
| Repo location | `git init` in `traditional/`, keep `ARCHITECTURE.md` at the root | Docs and code in one history |

## Step 0 — Prerequisites

Install and verify:

- JDK 21: `sdk install java 21-tem` (SDKMAN) or `brew install --cask temurin@21` → `java -version` shows 21.
- Docker Desktop running → `docker info` succeeds.
- No system Gradle needed — the wrapper is committed and is the only thing anyone ever runs.

## Step 1 — Repo and Gradle skeleton

```
traditional/
├── ARCHITECTURE.md
├── settings.gradle.kts
├── build.gradle.kts          ← root: conventions only, no code
├── gradle/libs.versions.toml
├── gradle/wrapper/…          ← committed
├── core/build.gradle.kts
│   └── src/main/java/… + src/test/java/…
├── bus/build.gradle.kts
│   └── src/main/java/… + src/test/java/… + src/integrationTest/java/…
├── docker-compose.yml
├── .gitignore                ← .gradle/, build/, .idea/
└── README.md
```

`settings.gradle.kts`:

```kotlin
rootProject.name = "trading-engine"
include("core", "bus")
```

`gradle/libs.versions.toml` — just what this issue needs, nothing speculative:

```toml
[versions]
lettuce = "6.5.5.RELEASE"   # check latest 6.x
junit = "5.11.4"
assertj = "3.27.3"

[libraries]
lettuce = { module = "io.lettuce:lettuce-core", version.ref = "lettuce" }
junit-jupiter = { module = "org.junit.jupiter:junit-jupiter", version.ref = "junit" }
assertj = { module = "org.assertj:assertj-core", version.ref = "assertj" }
```

Root `build.gradle.kts`: apply a shared convention to all subprojects — `java` plugin, toolchain pinned to 21 (`java.toolchain.languageVersion = JavaLanguageVersion.of(21)`), JUnit Platform for tests (`tasks.test { useJUnitPlatform() }`), Spotless. Pinning the *toolchain* (not just source compatibility) means Gradle downloads the right JDK if the machine has a different one — this is what makes "fresh clone just builds" true.

Generate the wrapper (`gradle wrapper --gradle-version 8.12` once with any local Gradle) and commit it.

**Verify:** `./gradlew build` succeeds with both modules empty.

## Step 2 — Module boundary

- `core/build.gradle.kts`: dependencies are **test-only** (junit, assertj). No Lettuce, ever — that is the acceptance criterion protecting the "swap Redis for NATS later" door.
- `bus/build.gradle.kts`: `implementation(project(":core"))` + `implementation(libs.lettuce)`.
- Placeholder class in each (package convention `engine.<module>`) so the modules compile and IDE import works.

**Verify:** `./gradlew :core:dependencies --configuration runtimeClasspath` shows no `io.lettuce` anywhere.

## Step 3 — docker-compose for Redis

```yaml
services:
  redis:
    image: redis:7.4-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  redis-data:
```

`--appendonly yes` matters even now: bus streams surviving a Redis restart is an assumption the later persistence work builds on, so bake it in from the first commit.

**Verify:** `docker compose up -d` → `docker compose ps` shows healthy → `redis-cli ping` returns PONG.

## Step 4 — Integration test (the wiring proof)

Integration tests are separated from unit tests via a dedicated `integrationTest` source set in `bus` with its own Gradle task, so `./gradlew build` stays green without Docker and `./gradlew integrationTest` requires it. (JUnit `@Tag` filtering works too, but source sets keep the classpaths honest.)

The test, in `bus/src/integrationTest/java/`:

```java
@Test
void roundTripsThroughRedisStreams() {
    try (RedisClient client = RedisClient.create("redis://localhost:6379")) {
        var commands = client.connect().sync();
        String id = commands.xadd("smoke.test", Map.of("k", "v"));
        var entries = commands.xrange("smoke.test", Range.create("-", "+"));
        assertThat(entries).extracting(StreamMessage::getId).contains(id);
        commands.del("smoke.test");
    }
}
```

Use `XADD`/`XRANGE` rather than plain SET/GET deliberately — it proves the exact primitive (streams) the whole epic is built on, and the same test shape gets reused in NEG-18/NEG-19.

**Verify:** `./gradlew integrationTest` passes with compose up, and **fails with a clear connection error** with compose down (run both — the failure mode is part of what is being built).

## Step 5 — README

Sections: prerequisites (JDK 21, Docker Desktop), `docker compose up -d`, `./gradlew build`, `./gradlew integrationTest`, module map (one line each: `core` = event model & abstractions, no infra deps; `bus` = Redis Streams implementation). Nothing else — the README grows with the epics.

## Step 6 — Definition of done

Run the acceptance test literally: clone the repo into a scratch directory (`git clone . /tmp/fresh-clone`) and in that copy follow *only* the README from scratch.

- [ ] Fresh clone → compose up → `./gradlew build integrationTest` all green, README-only.
- [ ] `:core:dependencies` clean of Lettuce/Redis.

Commit history suggestion: one commit per step above.

## Pitfalls to expect

- **IDE using the wrong JDK:** IntelliJ's Gradle JVM setting can differ from the toolchain — if the build works in terminal but not the IDE, check *Settings → Build Tools → Gradle → Gradle JVM*.
- **Lettuce hanging the test JVM:** forgetting to close the `RedisClient` leaves non-daemon threads alive and `integrationTest` appears to "hang" after passing. The try-with-resources above is load-bearing.
- **Port 6379 already taken** (a stray local Redis, e.g. from Homebrew): `lsof -i :6379` before blaming compose.
- **Wrapper not committed:** `gradle/wrapper/gradle-wrapper.jar` is sometimes blanket-ignored by `*.jar` rules in `.gitignore` — the fresh-clone test in Step 6 catches this, which is half the reason it exists.
