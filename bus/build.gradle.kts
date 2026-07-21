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
