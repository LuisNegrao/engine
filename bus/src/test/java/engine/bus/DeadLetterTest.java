package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DeadLetterTest {

    /**
     * The DLQ field names are frozen wire constants (ADR 0002 §3): monitoring and replay read them by
     * name. These literals are hardcoded on purpose — asserting against {@code DeadLetter.FIELD_*}
     * would be a tautology, so a rename must break this test.
     */
    @Test
    void dlqFieldNamesAreFrozen() {
        assertThat(DeadLetter.FIELD_EVENT).isEqualTo("event");
        assertThat(DeadLetter.FIELD_STREAM).isEqualTo("stream");
        assertThat(DeadLetter.FIELD_GROUP).isEqualTo("group");
        assertThat(DeadLetter.FIELD_CONSUMER).isEqualTo("consumer");
        assertThat(DeadLetter.FIELD_DELIVERIES).isEqualTo("deliveries");
        assertThat(DeadLetter.FIELD_ERROR).isEqualTo("error");
        assertThat(DeadLetter.FIELD_FAILED_AT).isEqualTo("failedAt");
    }

    @Test
    void describeErrorCarriesClassMessageAndFrames() {
        Exception boom = new IllegalStateException("position went negative");

        String described = DeadLetter.describeError(boom);

        assertThat(described)
                .startsWith("java.lang.IllegalStateException: position went negative")
                .contains("\n\tat " + DeadLetterTest.class.getName());
    }

    @Test
    void describeErrorHandlesNullMessage() {
        String described = DeadLetter.describeError(new NullPointerException());

        assertThat(described).startsWith("java.lang.NullPointerException").doesNotContain(": null");
    }

    @Test
    void truncateLeavesShortStringsUntouched() {
        assertThat(DeadLetter.truncate("short", DeadLetter.MAX_ERROR_BYTES)).isEqualTo("short");
    }

    @Test
    void truncateCapsLongStringsUnderTheByteLimitWithAMarker() {
        String huge = "a".repeat(DeadLetter.MAX_ERROR_BYTES * 2);

        String truncated = DeadLetter.truncate(huge, DeadLetter.MAX_ERROR_BYTES);

        assertThat(truncated.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(DeadLetter.MAX_ERROR_BYTES);
        assertThat(truncated).endsWith("(truncated)").startsWith("aaa");
    }
}
