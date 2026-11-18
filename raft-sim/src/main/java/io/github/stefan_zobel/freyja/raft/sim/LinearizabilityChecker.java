package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks that a history of operations on registers is linearizable
 * (docs/spec/verification.md, section 2.5).
 *
 * <p>The algorithm is the one of Wing and Gong with memoization of visited states, applied
 * to every key on its own: an operation is linearized next only if no other pending operation
 * returned before it was invoked, and a state (the set of linearized operations and the value
 * of the register) is never searched twice.
 *
 * <p>A register starts with the value 0. An operation without a response may take effect at
 * any time after its invocation, or never.
 */
public final class LinearizabilityChecker {

    /** The kinds of operations. */
    public enum Kind {
        /** Returns the value. */
        READ,
        /** Sets the value to {@code a}. */
        WRITE,
        /** Sets the value to {@code b} if it is {@code a}; returns 1 if it did, otherwise 0. */
        COMPARE_AND_SET
    }

    /** The response time of an operation that never returned. */
    public static final long NO_RESPONSE = Long.MAX_VALUE;

    /**
     * One operation of a history.
     *
     * @param invoke   when the operation was invoked; all times of a history are distinct
     * @param response when it returned, or {@link #NO_RESPONSE}
     * @param result   what it returned: the value read, or the outcome of a compare-and-set;
     *                 ignored for writes and for operations without a response
     */
    public record Operation(int key, Kind kind, long a, long b, long invoke, long response,
                            long result) {
        public Operation {
            if (response <= invoke) {
                throw new IllegalArgumentException("response " + response + " before invocation " + invoke);
            }
        }

        boolean completed() {
            return response != NO_RESPONSE;
        }
    }

    private LinearizabilityChecker() {
    }

    /**
     * Checks a history.
     *
     * @return {@code null} if the history is linearizable, otherwise the key whose operations
     *         are not, with those operations
     */
    public static String check(List<Operation> history) {
        Map<Integer, List<Operation>> byKey = new TreeMap<>();
        for (Operation operation : history) {
            if (operation.kind() == Kind.READ && !operation.completed()) {
                continue; // a read that never returned constrains nothing
            }
            byKey.computeIfAbsent(operation.key(), _ -> new ArrayList<>()).add(operation);
        }
        for (Map.Entry<Integer, List<Operation>> entry : byKey.entrySet()) {
            List<Operation> operations = entry.getValue();
            operations.sort((x, y) -> Long.compare(x.invoke(), y.invoke()));
            if (!linearizable(operations)) {
                return "the history of key " + entry.getKey() + " is not linearizable: " + operations;
            }
        }
        return null;
    }

    private record State(BitSet done, long value) {
    }

    private static boolean linearizable(List<Operation> operations) {
        int n = operations.size();
        int completed = 0;
        for (Operation operation : operations) {
            if (operation.completed()) {
                completed++;
            }
        }
        Set<State> visited = new HashSet<>();
        return search(operations, new BitSet(n), 0, completed, visited);
    }

    /**
     * @param remaining number of completed operations not yet linearized
     */
    private static boolean search(List<Operation> operations, BitSet done, long value,
                                  int remaining, Set<State> visited) {
        if (remaining == 0) {
            return true; // operations without a response need not take effect
        }
        if (!visited.add(new State((BitSet) done.clone(), value))) {
            return false;
        }
        // An operation can be next only if it was invoked before every pending operation
        // returned.
        long firstResponse = Long.MAX_VALUE;
        for (int i = done.nextClearBit(0); i < operations.size(); i = done.nextClearBit(i + 1)) {
            firstResponse = Math.min(firstResponse, operations.get(i).response());
        }
        for (int i = done.nextClearBit(0); i < operations.size(); i = done.nextClearBit(i + 1)) {
            Operation operation = operations.get(i);
            if (operation.invoke() > firstResponse) {
                break; // sorted by invocation: none of the following can be next either
            }
            long next = value;
            boolean consistent = true;
            switch (operation.kind()) {
                case READ -> consistent = operation.result() == value;
                case WRITE -> next = operation.a();
                case COMPARE_AND_SET -> {
                    boolean succeeds = value == operation.a();
                    if (succeeds) {
                        next = operation.b();
                    }
                    if (operation.completed()) {
                        consistent = (operation.result() == 1) == succeeds;
                    }
                }
            }
            if (!consistent) {
                continue;
            }
            done.set(i);
            boolean found = search(operations, done, next,
                    operation.completed() ? remaining - 1 : remaining, visited);
            done.clear(i);
            if (found) {
                return true;
            }
        }
        return false;
    }
}
