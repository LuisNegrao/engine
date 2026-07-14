// Event model and shared abstractions. Must never depend on Redis/Lettuce
// (or any other infrastructure) — that boundary is what keeps the bus
// implementation swappable.
plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    // Jackson is confined to engine.core.serde — kept as `implementation`, never `api`,
    // so JSON types never leak past core's boundary and the wire format stays swappable.
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jsr310)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
