package engine.bus;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared folding for Redis {@code XINFO} replies — the one place a flat field/value list becomes a
 * map keyed by field name, and the one place a byte-array field value becomes a {@code String}.
 *
 * <p>Three call sites fold {@code XINFO} the same way: the subscriber (reading {@code lag} from
 * {@code XINFO GROUPS}), the replay retention guard (reading {@code first-entry} from {@code XINFO
 * STREAM}), and the NEG-21 monitor (reading everything). This is {@code engine.bus} root
 * infrastructure with three consumers, not monitoring — so it stays in the root, and its surface is
 * {@code public} rather than package-private precisely because {@code engine.bus.monitor} lives in a
 * different package and Java package-private members do not cross the subpackage boundary.
 */
public final class XInfoReplies {

    private XInfoReplies() {}

    /** Folds a flat {@code XINFO} field/value list (name, value, name, value, …) into a map. */
    public static Map<String, Object> asFieldMap(List<?> fields) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < fields.size(); i += 2) {
            map.put(asString(fields.get(i)), fields.get(i + 1));
        }
        return map;
    }

    /** {@code XINFO} field names and values arrive as {@code byte[]} under the byte-array value codec. */
    public static String asString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
