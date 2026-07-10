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
            }
            targets.all {
                testTask.configure {
                    shouldRunAfter(test)
                }
            }
        }
    }
}
