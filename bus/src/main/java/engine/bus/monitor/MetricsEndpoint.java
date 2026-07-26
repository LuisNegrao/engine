package engine.bus.monitor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.DlqReading;
import engine.core.event.Event;
import engine.core.event.Metric;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The interim Grafana-readable surface (ADR 0003 §4): a JDK {@link HttpServer} serving {@code GET
 * /metrics} in Prometheus text exposition, rendered from the last sweep's metrics. Zero dependencies,
 * curl-able today, scrapeable the day a Prometheus exists — the durable path is archiver → QuestDB,
 * so this only needs to be standard and disposable.
 *
 * <p><strong>A scrape never touches Redis.</strong> The handler reads two volatile references (the
 * last emitted metrics and DLQ readings) and formats them — no command, no Redis load from a Grafana
 * refresh storm. The monitor pushes fresh data after each sweep via {@link #update}.
 *
 * <p>Names are the ADR 0003 §3 mechanical derivation of the grammar: the fixed prefix's dots become
 * underscores, the stream name-tail and the {@code owner} field demote to labels. Label values are
 * escaped per the exposition spec (backslash, quote, newline) — DLQ error strings are multi-line
 * stack traces, so an unescaped newline would truncate the scrape for every series after it.
 */
final class MetricsEndpoint implements AutoCloseable {

    /** Metric names carrying a stream name-tail: everything after the prefix is the {@code stream} label. */
    private static final List<String> STREAM_TAILED = List.of(
            "bus.stream.rate",
            "bus.stream.depth",
            "bus.stream.oldestAgeSeconds",
            "bus.group.lag",
            "bus.group.pending",
            "bus.group.lagUnknown",
            "bus.dlq.depth");

    private final HttpServer server;

    private volatile List<Event> metrics = List.of();
    private volatile List<DlqReading> dlqs = List.of();

    MetricsEndpoint(int port) {
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind metrics endpoint on port " + port, e);
        }
        server.createContext("/metrics", this::handle);
        server.setExecutor(null); // a single-threaded default executor is plenty for scrapes
    }

    void start() {
        server.start();
    }

    /** The actually-bound port — the real port when constructed with {@code 0} (tests avoid 9464 collisions). */
    int boundPort() {
        return server.getAddress().getPort();
    }

    /** Swaps in the newest sweep's output; the next scrape renders it. Called on the monitor thread. */
    void update(List<Event> metrics, List<DlqReading> dlqs) {
        this.metrics = metrics;
        this.dlqs = dlqs;
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] body = render(metrics, dlqs).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Renders the Prometheus exposition text. Lines are sorted for a stable, diffable output (order is
     * irrelevant to a scraper). Before the first sweep — no metrics and no DLQ errors — the response is
     * a single comment line, still a valid 200.
     */
    static String render(List<Event> metrics, List<DlqReading> dlqs) {
        List<String> lines = new ArrayList<>();
        for (Event event : metrics) {
            lines.add(metricLine((Metric) event.payload()));
        }
        for (DlqReading dlq : dlqs) {
            dlq.lastError().ifPresent(lastError -> lines.add(dlqLastErrorLine(dlq.sourceStream(), lastError)));
        }
        if (lines.isEmpty()) {
            return "# bus-monitor: no metrics yet\n";
        }
        Collections.sort(lines);
        return String.join("\n", lines) + "\n";
    }

    private static String metricLine(Metric metric) {
        String name = metric.name();
        String value = metric.value().toPlainString();
        for (String prefix : STREAM_TAILED) {
            if (name.startsWith(prefix + ".")) {
                LinkedHashMap<String, String> labels = new LinkedHashMap<>();
                labels.put("stream", name.substring(prefix.length() + 1));
                if (prefix.startsWith("bus.group.")) {
                    labels.put("group", metric.owner());
                }
                return line(promName(prefix), labels, value);
            }
        }
        LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        if (name.startsWith("bus.feed.")) {
            labels.put("source", metric.owner());
        }
        return line(promName(name), labels, value);
    }

    private static String dlqLastErrorLine(String sourceStream, DlqLastError lastError) {
        LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        labels.put("stream", sourceStream);
        labels.put("group", lastError.group());
        labels.put("error", lastError.error());
        return line("bus_dlq_last_error", labels, "1");
    }

    private static String line(String name, Map<String, String> labels, String value) {
        if (labels.isEmpty()) {
            return name + " " + value;
        }
        String rendered = labels.entrySet().stream()
                .map(e -> e.getKey() + "=\"" + escape(e.getValue()) + "\"")
                .collect(Collectors.joining(","));
        return name + "{" + rendered + "} " + value;
    }

    private static String promName(String dotted) {
        return dotted.replace('.', '_');
    }

    /** Escapes a label value per the exposition spec: backslash, double-quote, and newline only. */
    static String escape(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
