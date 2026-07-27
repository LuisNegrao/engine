// Redis Streams implementation of the messaging abstractions defined in :core.
plugins {
    `java-library`
    `jvm-test-suite`
}

dependencies {
    api(project(":core"))
    implementation(libs.lettuce)
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(libs.versions.junit.get())
            dependencies {
                implementation(libs.assertj)
                implementation(testFixtures(project(":core")))
            }
        }

        // Integration tests need the docker-compose Redis; they are not part of
        // `build` so the default build stays green without Docker.
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter(libs.versions.junit.get())
            dependencies {
                implementation(project())
                implementation(libs.lettuce)
                implementation(libs.assertj)
                implementation(testFixtures(project(":core")))
            }
            targets.all {
                testTask.configure {
                    shouldRunAfter(test)
                    // The external Redis is state Gradle cannot track as an input;
                    // never let this task be skipped as up-to-date.
                    outputs.upToDateWhen { false }
                }
            }
        }
    }
}

// NEG-18 Step 7 throughput harness: a manual `main`, never wired into build/check/integrationTest.
// Needs the docker-compose Redis. Run with: ./gradlew :bus:publishBench
tasks.register<JavaExec>("publishBench") {
    group = "verification"
    description = "Runs the publish throughput bench against the docker-compose Redis (manual only)."
    val integrationTest = sourceSets["integrationTest"]
    classpath = integrationTest.runtimeClasspath
    mainClass.set("engine.bus.PublishBench")
}

// NEG-22 end-to-end smoke/soak harness: a manual `main`, never wired into build/check/integrationTest.
// Needs the docker-compose Redis, and must not run concurrently with :bus:integrationTest.
// Run with: ./gradlew :bus:e2eBench [--args='--soak --write-baseline']
tasks.register<JavaExec>("e2eBench") {
    group = "verification"
    description = "Runs the end-to-end throughput/latency harness against the docker-compose Redis (manual only)."
    val integrationTest = sourceSets["integrationTest"]
    classpath = integrationTest.runtimeClasspath
    mainClass.set("engine.bus.bench.EndToEndBench")
    // 3 groups x 18M soak samples x 8 B is ~432 MB of preallocated latency arrays.
    jvmArgs("-Xmx2g")
    // Measure on the JDK the modules compile against, not on whatever JVM runs Gradle — the
    // baseline records this version, so it has to be the one the engine actually runs on.
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(21) }
    // So --write-baseline resolves docs/baselines/ against the repo root, not bus/.
    workingDir = rootDir
}
