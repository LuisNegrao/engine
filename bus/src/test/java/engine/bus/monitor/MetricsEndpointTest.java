package engine.bus.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.DlqReading;
import engine.core.event.Event;
import engine.core.event.Metric;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MetricsEndpointTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_717_171_717_171L), ZoneOffset.UTC);

    @Test
    void rendersGrammarNamesAsPrometheusWithLabels() {
        List<Event> metrics = List.of(
                metric("bus.stream.depth.metrics", 42, "bus"),
                metric("bus.group.lag.metrics", 7, "archiver"),
                metric("bus.publisher.published", 100, "bus"),
                metric("bus.feed.latencyMillis.p50", 50, "binance"));
        List<DlqReading> dlqs = List.of(new DlqReading(
                "orders.intents", 3, Optional.of(new DlqLastError("risk-manager", "boom", "2026-07-26T10:00:00Z"))));

        String rendered = MetricsEndpoint.render(metrics, dlqs);

        // Sorted, one series per line, stream/owner demoted to labels; owner "bus" carries no label.
        assertThat(rendered)
                .isEqualTo(
                        """
                        bus_dlq_last_error{stream="orders.intents",group="risk-manager",error="boom"} 1
                        bus_feed_latencyMillis_p50{source="binance"} 50
                        bus_group_lag{stream="metrics",group="archiver"} 7
                        bus_publisher_published 100
                        bus_stream_depth{stream="metrics"} 42
                        """);
    }

    @Test
    void escapesBackslashQuoteAndNewlineInLabelValues() {
        // A DLQ error is a multi-line stack trace with quotes — the escaping that keeps the scrape valid.
        DlqLastError error = new DlqLastError("g", "a\"b\\c\nd", "2026-07-26T10:00:00Z");
        List<DlqReading> dlqs = List.of(new DlqReading("orders.intents", 1, Optional.of(error)));

        String rendered = MetricsEndpoint.render(List.of(), dlqs);

        assertThat(rendered).contains("error=\"a\\\"b\\\\c\\nd\"");
        // The embedded newline is escaped, not a real line break: the whole exposition stays one line.
        assertThat(rendered.strip().lines()).hasSize(1);
    }

    @Test
    void escapeHandlesEachSpecialCharacter() {
        assertThat(MetricsEndpoint.escape("back\\slash")).isEqualTo("back\\\\slash");
        assertThat(MetricsEndpoint.escape("a\"b")).isEqualTo("a\\\"b");
        assertThat(MetricsEndpoint.escape("line1\nline2")).isEqualTo("line1\\nline2");
        assertThat(MetricsEndpoint.escape("plain")).isEqualTo("plain");
    }

    @Test
    void emptyExpositionIsASingleCommentLine() {
        assertThat(MetricsEndpoint.render(List.of(), List.of())).isEqualTo("# bus-monitor: no metrics yet\n");
    }

    @Test
    void dlqWithoutLastErrorEmitsNoInfoMetric() {
        List<DlqReading> dlqs = List.of(new DlqReading("orders.intents", 0, Optional.empty()));
        assertThat(MetricsEndpoint.render(List.of(), dlqs)).isEqualTo("# bus-monitor: no metrics yet\n");
    }

    @Test
    void servesOverHttpOnAnEphemeralPortWithoutTouchingRedis() throws Exception {
        MetricsEndpoint endpoint = new MetricsEndpoint(0);
        try {
            endpoint.start();
            int port = endpoint.boundPort();
            assertThat(port).isGreaterThan(0);

            endpoint.update(List.of(metric("bus.publisher.published", 5, "bus")), List.of());

            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(
                            HttpRequest.newBuilder()
                                    .uri(URI.create("http://localhost:" + port + "/metrics"))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("bus_publisher_published 5\n");
            assertThat(response.headers().firstValue("Content-Type"))
                    .hasValue("text/plain; version=0.0.4; charset=utf-8");
        } finally {
            endpoint.close();
        }
    }

    private static Event metric(String name, long value, String owner) {
        return Event.of(
                "bus-monitor", null, CLOCK.instant(), new Metric(name, BigDecimal.valueOf(value), owner), CLOCK);
    }
}
