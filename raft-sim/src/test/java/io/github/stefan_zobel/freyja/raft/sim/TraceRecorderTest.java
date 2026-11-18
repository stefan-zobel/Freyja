package io.github.stefan_zobel.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

/** The trace of a run (docs/spec/verification.md, section 2.7). */
class TraceRecorderTest {

    /** A reader for the subset of JSON the recorder writes; it rejects everything else. */
    private static final class Json {
        private final String text;
        private int at;

        Json(String text) {
            this.text = text;
        }

        static Object parse(String text) {
            Json json = new Json(text);
            Object value = json.value();
            if (json.at != text.length()) {
                throw new IllegalArgumentException("trailing text at " + json.at + ": " + text);
            }
            return value;
        }

        private void expect(char c) {
            if (at >= text.length() || text.charAt(at) != c) {
                throw new IllegalArgumentException("expected '" + c + "' at " + at + ": " + text);
            }
            at++;
        }

        private Object value() {
            char c = text.charAt(at);
            if (c == '{') {
                at++;
                Map<String, Object> object = new LinkedHashMap<>();
                while (text.charAt(at) != '}') {
                    if (!object.isEmpty()) {
                        expect(',');
                    }
                    String key = string();
                    expect(':');
                    if (object.put(key, value()) != null) {
                        throw new IllegalArgumentException("duplicate key " + key);
                    }
                }
                at++;
                return object;
            }
            if (c == '[') {
                at++;
                List<Object> list = new ArrayList<>();
                while (text.charAt(at) != ']') {
                    if (!list.isEmpty()) {
                        expect(',');
                    }
                    list.add(value());
                }
                at++;
                return list;
            }
            if (c == '"') {
                return string();
            }
            if (text.startsWith("true", at)) {
                at += 4;
                return Boolean.TRUE;
            }
            if (text.startsWith("false", at)) {
                at += 5;
                return Boolean.FALSE;
            }
            int start = at;
            if (c == '-') {
                at++;
            }
            while (at < text.length() && Character.isDigit(text.charAt(at))) {
                at++;
            }
            return Long.parseLong(text.substring(start, at));
        }

        private String string() {
            expect('"');
            int end = text.indexOf('"', at);
            String value = text.substring(at, end);
            if (value.contains("\\")) {
                throw new IllegalArgumentException("escapes are not expected: " + value);
            }
            at = end + 1;
            return value;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static String record(SimOptions options, Simulation[] out) {
        StringBuilder text = new StringBuilder();
        Simulation sim = new Simulation(options);
        sim.setStepListener(new TraceRecorder(text));
        sim.run(3_000);
        out[0] = sim;
        return text.toString();
    }

    @Test
    void everyStepIsOneWellFormedLineWithTheStateAndTheMessages() {
        // The first seed whose run has a restart, so that every kind of line occurs.
        Simulation[] holder = new Simulation[1];
        String trace = null;
        for (long seed = 1; seed <= 50 && (trace == null || holder[0].stats().restarts == 0); seed++) {
            if (SimOptions.faulty(seed).crashes()) {
                trace = record(SimOptions.faulty(seed), holder);
            }
        }
        assertNotNull(trace);
        Simulation sim = holder[0];
        String[] lines = trace.split("\n");
        assertEquals(sim.stats().events + sim.stats().restarts, lines.length);
        assertTrue(sim.stats().restarts > 0, "the seed has restarts: " + sim.stats());

        long released = 0;
        Set<String> eventTypes = new TreeSet<>();
        Set<String> messageTypes = new TreeSet<>();
        Map<Long, Long> lastTerm = new HashMap<>();
        Map<Long, Long> leaderOfTerm = new HashMap<>();
        for (String line : lines) {
            Map<String, Object> step = object(Json.parse(line));
            assertEquals(List.of("time", "node", "event", "state", "persisted", "released"),
                    List.copyOf(step.keySet()));
            long node = (Long) step.get("node");
            Map<String, Object> event = object(step.get("event"));
            String type = (String) event.get("type");
            eventTypes.add(type);
            Map<String, Object> state = object(step.get("state"));
            assertEquals(12, state.size());
            long term = (Long) state.get("term");
            if (!type.equals("Restart")) {
                assertTrue(term >= lastTerm.getOrDefault(node, 0L), "the term of a running node never goes back");
            }
            lastTerm.put(node, term);
            assertTrue((Long) state.get("commitIndex") <= (Long) state.get("lastLogIndex"));
            assertTrue((Long) state.get("durableIndex") <= (Long) state.get("lastLogIndex"));
            if (state.get("role").equals("LEADER")) {
                Long earlier = leaderOfTerm.put(term, node);
                assertTrue(earlier == null || earlier == node, "one leader per term in the trace");
            }
            for (Object message : list(step.get("released"))) {
                released++;
                Map<String, Object> outbound = object(message);
                assertNotNull(outbound.get("to"));
                messageTypes.add((String) object(outbound.get("message")).get("type"));
            }
            for (Object work : list(step.get("persisted"))) {
                assertNotNull(object(work).get("entries"));
            }
        }
        assertEquals(sim.stats().messages, released);
        assertEquals(Set.of("ClientPropose", "ClientReadIndex", "MessageReceived", "Persisted", "Restart",
                "Tick"), eventTypes);
        assertEquals(Set.of("AppendEntries", "AppendResponse", "Heartbeat", "HeartbeatResponse",
                "RequestVote", "VoteResponse"), messageTypes);
        assertFalse(leaderOfTerm.isEmpty());
    }

    @Test
    void theTraceIsAFunctionOfTheSeed() {
        Simulation[] holder = new Simulation[1];
        String first = record(SimOptions.faulty(5), holder);
        assertEquals(first, record(SimOptions.faulty(5), holder));
        assertFalse(first.equals(record(SimOptions.faulty(6), holder)));
    }

    @Test
    void recordingDoesNotChangeTheRun() {
        Simulation plain = new Simulation(SimOptions.faulty(11));
        plain.run(3_000);
        Simulation[] holder = new Simulation[1];
        record(SimOptions.faulty(11), holder);
        assertEquals(plain.traceHash(), holder[0].traceHash());
    }
}
