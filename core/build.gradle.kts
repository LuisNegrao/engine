// Event model and shared abstractions. Must never depend on Redis/Lettuce
// (or any other infrastructure) — that boundary is what keeps the bus
// implementation swappable.
plugins {
    `java-library`
}

dependencies {
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
