package io.github.sz.freyja.bench;

/**
 * A histogram of latencies with a relative error of about one percent and a fixed size: the
 * buckets grow with the values. It records microseconds from 1 to about an hour.
 *
 * <p>Values below 128 have a bucket each. Above that, every power of two is split into 64
 * buckets of equal width.
 *
 * <p>Not safe for use by several threads; each worker has its own and they are merged.
 */
public final class Histogram {

    private static final int LINEAR = 128;
    private static final int PER_OCTAVE = 64;
    private static final int OCTAVES = 26; // up to 128 * 2^26 microseconds, more than two hours

    private final long[] counts = new long[LINEAR + OCTAVES * PER_OCTAVE];
    private long total;
    private long max;
    private long sum;

    private static int bucket(long value) {
        if (value < LINEAR) {
            return (int) Math.max(0, value);
        }
        int octave = 63 - Long.numberOfLeadingZeros(value) - 7; // 128..255 is octave 0
        if (octave >= OCTAVES) {
            return LINEAR + OCTAVES * PER_OCTAVE - 1;
        }
        int within = (int) ((value >>> (octave + 1)) - PER_OCTAVE);
        return LINEAR + octave * PER_OCTAVE + within;
    }

    /** The largest value that falls into a bucket. */
    private static long upperBound(int bucket) {
        if (bucket < LINEAR) {
            return bucket;
        }
        int octave = (bucket - LINEAR) / PER_OCTAVE;
        int within = (bucket - LINEAR) % PER_OCTAVE;
        return ((long) (PER_OCTAVE + within + 1) << (octave + 1)) - 1;
    }

    /** Records one latency in microseconds. */
    public void record(long micros) {
        counts[bucket(micros)]++;
        total++;
        sum += micros;
        max = Math.max(max, micros);
    }

    /** Adds everything {@code other} has recorded. */
    public void merge(Histogram other) {
        for (int i = 0; i < counts.length; i++) {
            counts[i] += other.counts[i];
        }
        total += other.total;
        sum += other.sum;
        max = Math.max(max, other.max);
    }

    public long count() {
        return total;
    }

    public long max() {
        return max;
    }

    public double mean() {
        return total == 0 ? 0 : (double) sum / total;
    }

    /**
     * The value below which the given share of the recorded values lies, to the precision of
     * the buckets and never above the largest recorded value.
     *
     * @param quantile between 0 and 1, for example 0.999
     */
    public long percentile(double quantile) {
        if (total == 0) {
            return 0;
        }
        long rank = Math.max(1, (long) Math.ceil(quantile * total));
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= rank) {
                return Math.min(upperBound(i), max);
            }
        }
        return max;
    }
}
