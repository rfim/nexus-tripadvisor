package nexus;

import java.util.LinkedHashMap;
import java.util.Map;

/** Just enough JSON for Kafka records: objects, strings with escapes, numbers, booleans, null. No dependencies. */
final class Json {
    private final String s;
    private int i;

    private Json(String s) {
        this.s = s;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String text) {
        Json p = new Json(text);
        Object v = p.value();
        p.ws();
        if (p.i != text.length() || !(v instanceof Map)) throw new IllegalArgumentException("not a JSON object at char " + p.i);
        return (Map<String, Object>) v;
    }

    private void ws() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    private Object value() {
        ws();
        if (i >= s.length()) throw new IllegalArgumentException("unexpected end at char " + i);
        char c = s.charAt(i);
        if (c == '{') return obj();
        if (c == '"') return str();
        if (s.startsWith("true", i)) { i += 4; return Boolean.TRUE; }
        if (s.startsWith("false", i)) { i += 5; return Boolean.FALSE; }
        if (s.startsWith("null", i)) { i += 4; return null; }
        int start = i;
        while (i < s.length() && "+-.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
        if (start == i) throw new IllegalArgumentException("unexpected '" + c + "' at char " + i);
        String num = s.substring(start, i);
        return num.contains(".") || num.contains("e") || num.contains("E") ? (Number) Double.parseDouble(num) : (Number) Long.parseLong(num);
    }

    private Map<String, Object> obj() {
        Map<String, Object> m = new LinkedHashMap<>();
        i++;
        ws();
        if (s.charAt(i) == '}') { i++; return m; }
        while (true) {
            ws();
            String k = str();
            ws();
            if (s.charAt(i++) != ':') throw new IllegalArgumentException("expected ':' at char " + (i - 1));
            m.put(k, value());
            ws();
            char c = s.charAt(i++);
            if (c == '}') return m;
            if (c != ',') throw new IllegalArgumentException("expected ',' or '}' at char " + (i - 1));
        }
    }

    private String str() {
        if (s.charAt(i) != '"') throw new IllegalArgumentException("expected a string at char " + i);
        StringBuilder b = new StringBuilder();
        i++;
        while (true) {
            if (i >= s.length()) throw new IllegalArgumentException("unterminated string");
            char c = s.charAt(i++);
            if (c == '"') return b.toString();
            if (c != '\\') { b.append(c); continue; }
            char e = s.charAt(i++);
            switch (e) {
                case 'n' -> b.append('\n');
                case 't' -> b.append('\t');
                case 'r' -> b.append('\r');
                case 'b' -> b.append('\b');
                case 'f' -> b.append('\f');
                case 'u' -> { b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                default -> b.append(e);
            }
        }
    }
}
