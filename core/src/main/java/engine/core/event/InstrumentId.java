package engine.core.event;

import java.util.Locale;

/**
 * Venue-qualified instrument identifier in {@code SYMBOL.VENUE} form, e.g.
 * {@code BTC-USDT.BINANCE}.
 *
 * <p>The separator is the <strong>last</strong> {@code .} so that symbols may themselves contain
 * dots or dashes (only the trailing {@code .VENUE} suffix is stripped). The venue is normalized to
 * upper-case so the same market always compares equal regardless of how a producer spelled it.
 *
 * @param symbol the instrument symbol (may contain {@code -} and {@code .}); non-blank
 * @param venue the trading venue; non-blank, stored upper-case
 */
public record InstrumentId(String symbol, String venue) {

    public InstrumentId {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must be non-blank");
        }
        if (venue == null || venue.isBlank()) {
            throw new IllegalArgumentException("venue must be non-blank");
        }
        venue = venue.toUpperCase(Locale.ROOT);
    }

    /**
     * Parses a {@code SYMBOL.VENUE} string, splitting on the last {@code .}.
     *
     * @throws IllegalArgumentException if there is no {@code .}, or either side is blank
     */
    public static InstrumentId parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("instrumentId text must not be null");
        }
        int split = text.lastIndexOf('.');
        if (split < 0) {
            throw new IllegalArgumentException("instrumentId must be SYMBOL.VENUE, got: " + text);
        }
        String symbol = text.substring(0, split);
        String venue = text.substring(split + 1);
        return new InstrumentId(symbol, venue);
    }

    /** Re-joins to canonical {@code SYMBOL.VENUE} form; inverse of {@link #parse(String)}. */
    @Override
    public String toString() {
        return symbol + "." + venue;
    }
}
