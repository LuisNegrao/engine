package engine.core.event;

import java.util.Map;
import java.util.Objects;

/**
 * A control-plane instruction, e.g. start/stop/kill a strategy.
 *
 * @param target the strategy id to act on, or {@code "*"} for all; non-blank
 * @param action the action to perform
 * @param args additional arguments; may be empty but never {@code null} (defensively copied)
 */
public record Command(String target, CommandAction action, Map<String, String> args) implements Payload {

    public Command {
        Payload.requireText(target, "target");
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(args, "args must not be null (use an empty map)");
        args = Map.copyOf(args);
    }
}
