package io.github.sz.freyja.common.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ProbesTest {

    @Test
    void aHitIsCountedForItsProbeAndForNoOther() {
        Probes.enable();
        long tails = Probes.count(Probes.Probe.TORN_TAIL_DROPPED);
        long held = Probes.count(Probes.Probe.WRITE_HELD_BACK);
        Probes.hit(Probes.Probe.TORN_TAIL_DROPPED);
        Probes.hit(Probes.Probe.TORN_TAIL_DROPPED);
        assertEquals(tails + 2, Probes.count(Probes.Probe.TORN_TAIL_DROPPED));
        assertEquals(held, Probes.count(Probes.Probe.WRITE_HELD_BACK));
    }

    @Test
    void switchingTheProbesOnAgainKeepsTheCounts() {
        Probes.enable();
        Probes.hit(Probes.Probe.DAMAGE_PROVEN_BY_SYNC_POINT);
        long before = Probes.count(Probes.Probe.DAMAGE_PROVEN_BY_SYNC_POINT);
        Probes.enable();
        assertEquals(before, Probes.count(Probes.Probe.DAMAGE_PROVEN_BY_SYNC_POINT));
    }
}
