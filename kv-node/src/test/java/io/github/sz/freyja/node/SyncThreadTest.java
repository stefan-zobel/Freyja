package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/** The thread that runs the syncs of the log of a node (docs/spec/node.md, section 1). */
class SyncThreadTest {

    @Test
    void oneSyncRunsAtATimeOnItsOwnThreadAndWakesTheNodeWhenItEnds() throws Exception {
        Semaphore woken = new Semaphore(0);
        try (SyncThread syncs = new SyncThread("test-sync", woken::release)) {
            assertTrue(syncs.done(), "none was started");
            CountDownLatch gate = new CountDownLatch(1);
            AtomicReference<String> ranOn = new AtomicReference<>();
            syncs.start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                ranOn.set(Thread.currentThread().getName());
            });
            assertFalse(syncs.done());
            assertThrows(IllegalStateException.class, () -> syncs.start(() -> {
            }));
            gate.countDown();
            syncs.await();
            assertTrue(syncs.done());
            assertEquals("test-sync", ranOn.get());
            assertTrue(woken.tryAcquire(10, TimeUnit.SECONDS));

            syncs.start(() -> ranOn.set("second"));
            assertTrue(woken.tryAcquire(10, TimeUnit.SECONDS));
            assertTrue(syncs.done());
            assertEquals("second", ranOn.get());
        }
    }

    @Test
    void closingWaitsForTheSyncThatRuns() {
        AtomicReference<String> ran = new AtomicReference<>();
        SyncThread syncs = new SyncThread("test-sync", () -> {
        });
        syncs.start(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            ran.set("done");
        });
        syncs.close();
        assertEquals("done", ran.get());
        assertThrows(IllegalStateException.class, () -> syncs.start(() -> {
        }));
    }
}
