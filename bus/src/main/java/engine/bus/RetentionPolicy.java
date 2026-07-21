package engine.bus;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * The ADR 0002 §4 retention table as code: per stream class, the age window and the entry-count
 * cap that bound how much history the bus keeps.
 *
 * <p>Two consumers read this table. The publisher stamps each {@code XADD} with {@code MAXLEN ~
 * <maxlen>} as runaway protection; the trimmer issues {@code XTRIM <stream> MINID ~ <now −
 * window>} as the age-based retention contract. Both look the rule up through {@link
 * #ruleFor(String)}, so the two mechanisms can never disagree about which numbers apply to a
 * stream.
 *
 * <p>The bus is a transport with a bounded window, not a database — the Historical Data Store is
 * the only permanent archive (ADR 0002 §4, §6).
 */
public record RetentionPolicy(List<Rule> rules) {

    /**
     * One retention row: a stream selector and the caps that apply to matching streams.
     *
     * <p>{@code prefix} is either an <em>exact</em> stream name with no trailing dot (e.g. {@code
     * "orders.intents"}), matched by string equality, or a <em>stream-class</em> prefix ending in
     * {@code '.'} (e.g. {@code "md.tick.quote."}), matched by {@link String#startsWith}. The
     * trailing dot is what distinguishes the two matching modes, so it is significant.
     *
     * @param prefix exact stream name or dot-terminated class prefix; non-blank
     * @param window age retained by the trimmer's {@code XTRIM MINID} sweep; positive
     * @param maxlen publisher {@code MAXLEN ~} entry cap; at least 1
     */
    public record Rule(String prefix, Duration window, long maxlen) {
        public Rule {
            if (prefix == null || prefix.isBlank()) {
                throw new IllegalArgumentException("prefix must be non-blank");
            }
            Objects.requireNonNull(window, "window must not be null");
            if (window.isZero() || window.isNegative()) {
                throw new IllegalArgumentException("window must be positive, was: " + window);
            }
            if (maxlen < 1) {
                throw new IllegalArgumentException("maxlen must be at least 1, was: " + maxlen);
            }
        }
    }

    public RetentionPolicy {
        Objects.requireNonNull(rules, "rules must not be null");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("rules must not be empty");
        }
        rules = List.copyOf(rules);
    }

    /**
     * The eight ADR 0002 §4 retention rows verbatim.
     *
     * <p>Every window and cap here is revisable configuration, not architecture (ADR 0002 §4): a
     * RAM upgrade or a rate revision changes these numbers, but the ADR table is the source of
     * truth and is amended first — this method mirrors it, it does not decide it.
     */
    public static RetentionPolicy standard() {
        return new RetentionPolicy(List.of(
                new Rule("md.tick.quote.", Duration.ofHours(12), 2_000_000),
                new Rule("md.tick.trade.", Duration.ofHours(12), 1_000_000),
                new Rule("md.bar.", Duration.ofDays(14), 50_000),
                new Rule("signals", Duration.ofDays(14), 500_000),
                new Rule("orders.intents", Duration.ofDays(30), 1_000_000),
                new Rule("orders.fills", Duration.ofDays(30), 1_000_000),
                new Rule("metrics", Duration.ofHours(48), 2_000_000),
                new Rule("commands", Duration.ofDays(30), 100_000)));
    }

    /**
     * Resolves the retention rule for a stream. Exact rules (no trailing dot) match by string
     * equality; class-prefix rules (trailing dot) match by {@link String#startsWith}; when more
     * than one rule matches, the one with the longest prefix wins.
     *
     * <p>An unmatched stream throws rather than defaulting: every stream this policy is asked about
     * was minted by {@link StreamNames}, so a miss means the two have diverged — a bug to surface
     * loudly, not a case to paper over with a fallback window.
     *
     * @throws IllegalArgumentException if no rule matches {@code stream}
     */
    public Rule ruleFor(String stream) {
        Objects.requireNonNull(stream, "stream must not be null");
        Rule best = null;
        for (Rule rule : rules) {
            boolean matches =
                    rule.prefix().endsWith(".") ? stream.startsWith(rule.prefix()) : stream.equals(rule.prefix());
            if (matches
                    && (best == null || rule.prefix().length() > best.prefix().length())) {
                best = rule;
            }
        }
        if (best == null) {
            throw new IllegalArgumentException(
                    "no retention rule matches stream '" + stream + "' — StreamNames and RetentionPolicy diverged");
        }
        return best;
    }
}
