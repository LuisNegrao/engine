plugins {
    // Auto-provisions the JDK 21 toolchain on machines that don't have it installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "trading-engine"

include("core", "bus")
