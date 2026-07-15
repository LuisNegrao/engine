package engine.bus;

import engine.core.bus.EventPublisher;
import engine.core.event.Event;
import engine.core.serde.EventCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class RedisStreamsEventPublisher implements EventPublisher {

    /**
     * Frozen wire constant: the single entry field holding the codec-encoded event bytes. Read by
     * the NEG-19 subscriber and the archiver — renaming it silently breaks both.
     */
    public static final String EVENT_FIELD = "event";

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisAsyncCommands<String, byte[]> commands;
    private final AtomicLong inFlight = new AtomicLong();
    private final RetentionPolicy retentionPolicy;
    private final EventCodec codec;

    public RedisStreamsEventPublisher(String redisUri, EventCodec codec, RetentionPolicy retentionPolicy) {
        this.client = RedisClient.create(redisUri);
        client.setOptions(clientOptions());

        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.commands = connection.async();
        this.retentionPolicy = retentionPolicy;
        this.codec = codec;
    }

    static ClientOptions clientOptions() {
        return ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(4096)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(1)))
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(1))
                        .build())
                .autoReconnect(true)
                .build();
    }

    @Override
    public CompletionStage<Void> publish(Event event) {
        try {
            String stream = StreamNames.streamFor(event);
            XAddArgs args = new XAddArgs()
                    .maxlen(this.retentionPolicy.ruleFor(stream).maxlen())
                    .approximateTrimming();
            Map<String, byte[]> body = Map.of(EVENT_FIELD, codec.encode(event));

            inFlight.incrementAndGet();
            try {
                // Every increment has exactly one decrement: whenComplete on the normal path,
                // or this catch if xadd throws synchronously before returning a future.
                return commands.xadd(stream, args, body)
                        .whenComplete((id, err) -> inFlight.decrementAndGet())
                        .thenAccept(id -> {});
            } catch (RuntimeException e) {
                inFlight.decrementAndGet();
                throw e;
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedStage(e);
        }
    }

    @Override
    public void close() {
        // Contract: bounded drain of in-flight publishes, then transport teardown.
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < deadlineNanos) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        this.connection.close();
        this.client.shutdown();
    }
}
