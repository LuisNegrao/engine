package engine.bus.monitor;

import java.time.Duration;
import java.util.Objects;

/**
 * {@link BusMonitor} tuning: how often it sweeps and which port the Prometheus endpoint binds.
 *
 * <p>{@link #standard()} is production wiring — a 15 s sweep (chosen in the plan: fine enough to
 * watch a consumer drown in near-real-time, coarse enough that the {@code metrics} stream fills its
 * 24 h window, not the old 48 h — ADR 0002 §4 as amended) on the conventional Prometheus port 9464.
 * Integration tests build their own with a ~100 ms interval and port {@code 0} (ephemeral — 9464
 * collides across parallel runs).
 *
 * <p>The feed-latency percentile set is deliberately <em>not</em> tunable: ADR 0003 froze it to
 * p50/p90/p99/max, so it lives in {@link MetricNames.LatencyPercentile}, not here.
 *
 * @param interval time between the end of one sweep and the start of the next; must be positive
 * @param endpointPort the metrics endpoint port; {@code 0} asks the OS for an ephemeral port
 */
public record MonitorTuning(Duration interval, int endpointPort) {

    public MonitorTuning {
        Objects.requireNonNull(interval, "interval must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive but was " + interval);
        }
        if (endpointPort < 0 || endpointPort > 65535) {
            throw new IllegalArgumentException("endpointPort must be 0..65535 but was " + endpointPort);
        }
    }

    /** Production defaults: 15 s sweep, Prometheus port 9464. */
    public static MonitorTuning standard() {
        return new MonitorTuning(Duration.ofSeconds(15), 9464);
    }
}
