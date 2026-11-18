package io.github.stefan_zobel.freyja.common;

/**
 * The sink for the figures a component reports about itself. It depends on no metrics
 * library; a node connects it to its metrics registry.
 *
 * <p>Implementations must be safe for concurrent use and must return quickly.
 */
public interface Metrics {

    /** A sink that ignores everything. */
    Metrics NONE = new Metrics() {
        @Override
        public void count(String name, long delta) {
        }

        @Override
        public void gauge(String name, long value) {
        }

        @Override
        public void duration(String name, long nanos) {
        }
    };

    /** Adds {@code delta} to a counter. */
    void count(String name, long delta);

    /** Sets a gauge to its current value. */
    void gauge(String name, long value);

    /** Records one observation of a duration, for a latency histogram. */
    void duration(String name, long nanos);
}
