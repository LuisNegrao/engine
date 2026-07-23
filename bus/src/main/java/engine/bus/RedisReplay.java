package engine.bus;

import engine.core.bus.EventHandler;
import engine.core.bus.Replay;
import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayRange;
import engine.core.bus.ReplayRetentionException;
import engine.core.event.Event;
import engine.core.serde.EventCodec;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * A pure-reader replay of a bounded stream-ID range, delivered through the same {@link EventHandler}
 * path as live consumption. One dedicated connection and one daemon thread per replay; the thread
 * reads each resolved stream with batched {@code XRANGE}, k-way merges the batches by stream ID
 * (tie-broken by stream name) into a single ingest-ordered sequence, and dispatches each decoded
 * {@link Event} to the handler at full speed. It never writes to Redis — no group, no ack, no DLQ.
 *
 * <p>{@link #done()} completes exactly once, on every exit path: with the delivered count when the
 * range drains, exceptionally with the handler's exception or an undecodable entry on abort, or with
 * a {@link CancellationException} when {@link #close()} stops it early. The thread closes its own
 * connection on exit, so a harness running hundreds of replays leaks neither connections nor threads.
 */
final class RedisReplay implements Replay {

    private final RedisClient client;
    private final EventCodec codec;
    private final List<String> streams;
    private final ReplayRange range;
    private final EventHandler handler;
    private final int batchCount;
    private final String name;
    private final Consumer<RedisReplay> onComplete;

    private final CompletableFuture<Long> done = new CompletableFuture<>();
    private volatile boolean running = true;
    private StatefulRedisConnection<String, byte[]> connection;
    private Thread thread;

    RedisReplay(
            RedisClient client,
            EventCodec codec,
            List<String> streams,
            ReplayRange range,
            EventHandler handler,
            int batchCount,
            int index,
            Consumer<RedisReplay> onComplete) {
        this.client = client;
        this.codec = codec;
        this.streams = streams;
        this.range = range;
        this.handler = handler;
        this.batchCount = batchCount;
        this.name = "bus-replay-" + index;
        this.onComplete = onComplete;
    }

    /** Opens the dedicated connection and starts the replay thread. */
    void start() {
        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.thread = new Thread(this::run, name);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public CompletionStage<Long> done() {
        return done;
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            // No blocking read to wait out — XRANGE returns promptly — so a short join suffices for the
            // in-flight handler to finish before we declare the replay cancelled.
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // No-op once the replay has already completed naturally or aborted; otherwise this is the
        // cancellation signal the caller's done() sees.
        done.completeExceptionally(new CancellationException("replay " + name + " closed before completion"));
    }

    private void run() {
        try (StatefulRedisConnection<String, byte[]> conn = connection) {
            long delivered = deliver(conn.sync());
            done.complete(delivered);
        } catch (Throwable t) {
            done.completeExceptionally(t);
        } finally {
            onComplete.accept(this);
        }
    }

    /** Runs the k-way merge until the range drains, throwing on cancellation, abort, or handler error. */
    private long deliver(RedisCommands<String, byte[]> cmds) throws Exception {
        String startId = ReplayPositions.startId(range.start());
        boolean absentEnd = range.maybeEnd().isEmpty();
        List<StreamState> states = new ArrayList<>(streams.size());
        for (String stream : streams) {
            String tail = absentEnd ? sampleTail(cmds, stream) : "0-0";
            String endId = ReplayPositions.endId(range.maybeEnd().orElse(null), tail);
            states.add(new StreamState(stream, startId, endId));
        }

        long delivered = 0;
        while (true) {
            if (!running) {
                throw new CancellationException("replay " + name + " closed before completion");
            }
            StreamState next = smallestHead(cmds, states);
            if (next == null) {
                return delivered; // every stream exhausted — the range has drained
            }
            StreamMessage<String, byte[]> message = next.buffer.poll();
            Event event = decode(next.stream, message);
            try {
                handler.handle(event);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "replay " + name + " aborted: handler failed on entry " + message.getId() + " on " + next.stream
                                + " (eventId=" + event.eventId() + ")",
                        e);
            }
            delivered++;
        }
    }

    /** The stream whose buffered head has the smallest ID; refills empty buffers first. Null when all are exhausted. */
    private StreamState smallestHead(RedisCommands<String, byte[]> cmds, List<StreamState> states) {
        StreamState best = null;
        for (StreamState state : states) {
            if (state.buffer.isEmpty() && !state.exhausted) {
                state.refill(cmds, batchCount);
            }
            StreamMessage<String, byte[]> head = state.buffer.peek();
            if (head == null) {
                continue;
            }
            if (best == null) {
                best = state;
                continue;
            }
            int cmp = compareIds(head.getId(), best.buffer.peek().getId());
            if (cmp < 0 || (cmp == 0 && state.stream.compareTo(best.stream) < 0)) {
                best = state;
            }
        }
        return best;
    }

    private Event decode(String stream, StreamMessage<String, byte[]> message) {
        Optional<Event> decoded;
        try {
            decoded = RedisStreamsEventSubscriber.decodeEntry(codec, message);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "undecodable entry " + message.getId() + " on " + stream + " aborted the replay", e);
        }
        return decoded.orElseThrow(() -> new IllegalStateException("undecodable entry " + message.getId() + " on "
                + stream + " aborted the replay: unknown eventType or missing event field"));
    }

    /** The stream's last ID when the replay begins — the far edge of an absent-end range. {@code 0-0} if empty. */
    private static String sampleTail(RedisCommands<String, byte[]> cmds, String stream) {
        List<StreamMessage<String, byte[]>> last = cmds.xrevrange(stream, Range.unbounded(), Limit.from(1));
        return last.isEmpty() ? "0-0" : last.get(0).getId();
    }

    /**
     * The start-of-replay retention guard: keeps only the streams that can honor the requested start,
     * throwing {@link ReplayRetentionException} up front — on the caller's thread, before any event is
     * delivered — for a timestamped or offset start that the bus no longer holds. A missing stream is
     * fatal for such a start but merely contributes nothing to an {@code earliest()} replay.
     */
    static List<String> liveStreamsAtStart(
            RedisCommands<String, byte[]> cmds, List<String> streams, ReplayPosition start, String startId) {
        List<String> live = new ArrayList<>(streams.size());
        for (String stream : streams) {
            if (includesAtStart(stream, start, startId, streamInfoOrNull(cmds, stream))) {
                live.add(stream);
            }
        }
        return live;
    }

    /**
     * Pure start-retention decision from a stream's {@code XINFO STREAM} field map ({@code null} =
     * missing stream). {@code max-deleted-entry-id} is the only honest trim signal — the first-entry
     * ID cannot tell "trimmed away" from "never existed" — so an {@code earliest()} start is exempt by
     * definition (it <em>means</em> oldest retained) while a start at or below the trim floor fails
     * loudly.
     *
     * @return whether the stream contributes events to the replay
     * @throws ReplayRetentionException if the requested start lies at or below the trim floor, or the
     *     stream is missing for a timestamped/offset start
     */
    static boolean includesAtStart(String stream, ReplayPosition start, String startId, Map<String, Object> xinfo) {
        boolean earliest = start instanceof ReplayPosition.Earliest;
        if (xinfo == null) {
            if (earliest) {
                return false; // "oldest retained" of a stream with nothing retained is simply nothing
            }
            throw new ReplayRetentionException(
                    "cannot replay " + stream + " from " + startId + ": stream does not exist");
        }
        if (earliest) {
            return true;
        }
        String maxDeleted = asString(xinfo.get("max-deleted-entry-id"));
        if (maxDeleted != null && compareIds(startId, maxDeleted) <= 0) {
            throw new ReplayRetentionException("replay of " + stream + " starts at " + startId
                    + " but entries up to " + maxDeleted + " have been trimmed (oldest surviving id "
                    + firstEntryId(xinfo) + ")");
        }
        return true;
    }

    /** Reads a stream's {@code XINFO STREAM} into a field map, or {@code null} if the key does not exist. */
    private static Map<String, Object> streamInfoOrNull(RedisCommands<String, byte[]> cmds, String stream) {
        try {
            return foldFields(cmds.xinfoStream(stream));
        } catch (RedisCommandExecutionException e) {
            if (e.getMessage() != null && e.getMessage().toLowerCase().contains("no such key")) {
                return null;
            }
            throw e;
        }
    }

    private static String maxDeletedEntryId(Map<String, Object> xinfo) {
        String value = asString(xinfo.get("max-deleted-entry-id"));
        return value == null ? "0-0" : value;
    }

    private static String firstEntryId(Map<String, Object> xinfo) {
        if (xinfo.get("first-entry") instanceof List<?> pair && !pair.isEmpty()) {
            return asString(pair.get(0));
        }
        return "none";
    }

    /** Folds a flat {@code XINFO} field/value list into a map keyed by field name. */
    private static Map<String, Object> foldFields(List<?> fields) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < fields.size(); i += 2) {
            map.put(asString(fields.get(i)), fields.get(i + 1));
        }
        return map;
    }

    /** {@code XINFO} field names and values arrive as {@code byte[]} under the byte-array value codec. */
    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    /** Compares two {@code ms-seq} stream IDs numerically — lexical order would rank {@code 10-0} below {@code 9-0}. */
    private static int compareIds(String a, String b) {
        long[] pa = parseId(a);
        long[] pb = parseId(b);
        int cmp = Long.compare(pa[0], pb[0]);
        return cmp != 0 ? cmp : Long.compare(pa[1], pb[1]);
    }

    private static long[] parseId(String id) {
        int dash = id.indexOf('-');
        if (dash < 0) {
            return new long[] {Long.parseLong(id), 0};
        }
        return new long[] {Long.parseLong(id.substring(0, dash)), Long.parseLong(id.substring(dash + 1))};
    }

    /** Per-stream read cursor and buffer for the merge. */
    private static final class StreamState {

        private final String stream;
        private final String startId;
        private final String endId;
        private final Deque<StreamMessage<String, byte[]>> buffer = new ArrayDeque<>();
        private boolean firstRead = true;
        private String cursor;
        private boolean exhausted;

        StreamState(String stream, String startId, String endId) {
            this.stream = stream;
            this.startId = startId;
            this.endId = endId;
        }

        /**
         * Reads the next batch. The first read is inclusive of {@code startId}; every later read
         * advances the cursor <em>exclusively</em> ({@code (id}) so a batch boundary is never
         * redelivered. An empty batch means the stream is exhausted within the range.
         *
         * <p>After the read — never before, so a trim can't invalidate entries already fetched — the
         * per-batch retention check re-reads {@code max-deleted-entry-id}: if trimming has advanced
         * past the cursor while the replay was running, entries between the cursor and this batch were
         * deleted, so the replay aborts rather than delivering across the hole.
         */
        void refill(RedisCommands<String, byte[]> cmds, int batchCount) {
            boolean isFirstRead = firstRead;
            Range<String> window = isFirstRead
                    ? Range.create(startId, endId)
                    : Range.from(Range.Boundary.excluding(cursor), Range.Boundary.including(endId));
            firstRead = false;
            List<StreamMessage<String, byte[]>> batch = cmds.xrange(stream, window, Limit.from(batchCount));

            Map<String, Object> info = streamInfoOrNull(cmds, stream);
            if (info == null) {
                throw new ReplayRetentionException(
                        "replay of " + stream + " aborted: the stream was deleted while the replay was running");
            }
            if (!isFirstRead && compareIds(maxDeletedEntryId(info), cursor) > 0) {
                throw new ReplayRetentionException("replay of " + stream + " aborted: entries were trimmed past cursor "
                        + cursor + " (max-deleted-entry-id " + maxDeletedEntryId(info)
                        + ") while the replay was running");
            }

            if (batch.isEmpty()) {
                exhausted = true;
                return;
            }
            buffer.addAll(batch);
            cursor = batch.get(batch.size() - 1).getId();
        }
    }
}
