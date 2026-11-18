package io.github.sz.freyja.lsm;

/**
 * Receives reports of damaged files (docs/spec/storage-engine.md, section 3). The engine
 * reports; it never repairs. A node uses the reports to start a replica repair.
 *
 * <p>The listener is called from whichever thread found the damage and must return quickly.
 */
@FunctionalInterface
public interface DamageListener {

    /**
     * @param damage where the damage is and what was found
     */
    void damaged(CorruptionException damage);
}
