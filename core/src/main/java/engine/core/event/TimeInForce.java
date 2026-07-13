package engine.core.event;

/** How long an order remains active. */
public enum TimeInForce {
    /**
     * Good-Til-Cancelled: rests on the book until it is filled or explicitly cancelled. Survives
     * across trading sessions; the venue may still impose a maximum lifetime (e.g. 90 days).
     */
    GTC,

    /**
     * Immediate-Or-Cancel: executes immediately against whatever liquidity is available — partial
     * fills are accepted — and the unfilled remainder is cancelled. Never rests on the book.
     */
    IOC,

    /**
     * Fill-Or-Kill: executes immediately and in full, or not at all — no partial fills. The
     * all-or-nothing counterpart of {@link #IOC}. Never rests on the book.
     */
    FOK,

    /**
     * Valid for the current trading day/session only; the venue expires it at session close if
     * unfilled. For 24/7 venues (crypto) the venue's day boundary applies, if it supports DAY at
     * all.
     */
    DAY
}
