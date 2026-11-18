package io.github.stefan_zobel.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker.Kind;
import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker.Operation;

/** The linearizability checker (docs/spec/verification.md, section 2.5). */
class LinearizabilityCheckerTest {

    private static final long OPEN = LinearizabilityChecker.NO_RESPONSE;

    private static Operation read(long invoke, long response, long result) {
        return new Operation(0, Kind.READ, 0, 0, invoke, response, result);
    }

    private static Operation write(long invoke, long response, long value) {
        return new Operation(0, Kind.WRITE, value, 0, invoke, response, 0);
    }

    private static Operation cas(long invoke, long response, long expected, long value, boolean done) {
        return new Operation(0, Kind.COMPARE_AND_SET, expected, value, invoke, response, done ? 1 : 0);
    }

    private static void accept(Operation... history) {
        assertNull(LinearizabilityChecker.check(new ArrayList<>(List.of(history))));
    }

    private static void reject(Operation... history) {
        assertNotNull(LinearizabilityChecker.check(new ArrayList<>(List.of(history))));
    }

    @Test
    void sequentialHistories() {
        accept();
        accept(read(1, 2, 0));
        accept(write(1, 2, 5), read(3, 4, 5), write(5, 6, 6), read(7, 8, 6));
        reject(read(1, 2, 5));
        reject(write(1, 2, 5), read(3, 4, 0), read(5, 6, 5)); // a stale read
        reject(write(1, 2, 5), write(3, 4, 6), read(5, 6, 5)); // a lost update
    }

    @Test
    void concurrentOperationsMayTakeEffectInEitherOrder() {
        accept(write(1, 4, 5), read(2, 3, 0));
        accept(write(1, 4, 5), read(2, 3, 5));
        accept(write(1, 5, 5), write(2, 6, 6), read(7, 8, 5));
        accept(write(1, 5, 5), write(2, 6, 6), read(7, 8, 6));
        reject(write(1, 5, 5), write(2, 6, 6), read(7, 8, 0));
        // Two reads that follow each other cannot see the writes in opposite orders.
        reject(write(1, 10, 5), write(2, 11, 6), read(12, 13, 5), read(14, 15, 6));
        // A value cannot come back after a later read saw it replaced, without a new write.
        reject(write(1, 2, 5), write(3, 8, 6), read(4, 5, 6), read(6, 7, 5));
    }

    @Test
    void conditionalWrites() {
        accept(cas(1, 2, 0, 5, true), read(3, 4, 5));
        accept(cas(1, 2, 3, 5, false), read(3, 4, 0));
        reject(cas(1, 2, 3, 5, true));
        reject(cas(1, 2, 0, 5, false));
        // Of two concurrent conditional writes from the same value exactly one succeeds.
        accept(cas(1, 5, 0, 5, true), cas(2, 6, 0, 6, false), read(7, 8, 5));
        reject(cas(1, 5, 0, 5, true), cas(2, 6, 0, 6, true));
    }

    @Test
    void anOperationWithoutAResponseMayTakeEffectAtAnyLaterTimeOrNever() {
        accept(write(1, OPEN, 5), read(2, 3, 0), read(4, 5, 0));
        accept(write(1, OPEN, 5), read(2, 3, 0), read(4, 5, 5));
        accept(write(1, OPEN, 5), read(2, 3, 5), write(4, 5, 6), read(6, 7, 6));
        reject(write(3, OPEN, 5), read(1, 2, 5)); // not before it was invoked
        reject(write(1, OPEN, 5), read(2, 3, 5), read(4, 5, 0)); // it cannot be undone
        accept(cas(1, OPEN, 0, 5, false), read(2, 3, 5)); // the unknown outcome may be success
        accept(read(1, OPEN, 99)); // a read that never returned constrains nothing
    }

    @Test
    void keysAreIndependent() {
        Operation other = new Operation(1, Kind.WRITE, 9, 0, 1, 2, 0);
        Operation stale = new Operation(1, Kind.READ, 0, 0, 3, 4, 0);
        accept(write(5, 6, 5), other, read(7, 8, 5));
        reject(write(5, 6, 5), other, stale, read(7, 8, 5));
        assertTrue(LinearizabilityChecker.check(new ArrayList<>(List.of(other, stale))).contains("key 1"));
    }

    /** Histories produced by a real register are accepted, also when they overlap heavily. */
    @Test
    void historiesOfARealRegisterAreAccepted() {
        for (long seed = 1; seed <= 200; seed++) {
            Random random = new Random(seed);
            List<Operation> history = new ArrayList<>();
            long value = 0;
            long clock = 0;
            // Each operation takes effect at one point between its invocation and its response.
            int count = 40 + random.nextInt(40);
            long[][] pending = new long[count][];
            Kind[] kinds = Kind.values();
            int started = 0;
            int finished = 0;
            List<Integer> running = new ArrayList<>();
            List<Integer> effective = new ArrayList<>();
            while (finished < count) {
                int choice = random.nextInt(3);
                if (choice == 0 && started < count && running.size() + effective.size() < 5) {
                    pending[started] = new long[] {kinds[random.nextInt(3)].ordinal(), random.nextInt(4),
                            1 + random.nextInt(4), ++clock, 0};
                    running.add(started++);
                } else if (choice == 1 && !running.isEmpty()) {
                    int op = running.remove(random.nextInt(running.size()));
                    long[] p = pending[op];
                    Kind kind = kinds[(int) p[0]];
                    if (kind == Kind.READ) {
                        p[4] = value;
                    } else if (kind == Kind.WRITE) {
                        value = p[1];
                    } else if (value == p[1]) {
                        value = p[2];
                        p[4] = 1;
                    }
                    effective.add(op);
                } else if (choice == 2 && !effective.isEmpty()) {
                    int op = effective.remove(random.nextInt(effective.size()));
                    long[] p = pending[op];
                    long response = random.nextInt(10) == 0 ? OPEN : ++clock;
                    history.add(new Operation(0, kinds[(int) p[0]], p[1], p[2], p[3], response, p[4]));
                    finished++;
                }
            }
            assertNull(LinearizabilityChecker.check(history), "seed " + seed);
        }
    }
}
