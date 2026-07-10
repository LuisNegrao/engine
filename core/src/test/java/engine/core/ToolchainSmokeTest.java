package engine.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Proves JUnit + AssertJ wiring and that tests run on the pinned JDK 21 toolchain. */
class ToolchainSmokeTest {

    @Test
    void testsRunOnJdk21Toolchain() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }
}
