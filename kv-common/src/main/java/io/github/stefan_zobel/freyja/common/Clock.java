package io.github.stefan_zobel.freyja.common;

/**
 * The time source of a component. Code that measures time takes it from here and never from
 * the system directly, so that a simulation can supply its own clock.
 */
@FunctionalInterface
public interface Clock {

    /** The clock of the machine. */
    Clock SYSTEM = System::nanoTime;

    /**
     * Returns a monotonic time in nanoseconds. Only differences between two readings are
     * meaningful.
     */
    long nanoTime();
}
