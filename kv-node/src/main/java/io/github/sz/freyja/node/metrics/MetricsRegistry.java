package io.github.sz.freyja.node.metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

import io.github.sz.freyja.common.Metrics;

/**
 * The metrics registry of a node (docs/spec/operations.md, section 2): counters, gauges, and
 * histograms of durations, with no dependency on any metrics library. Everything a component
 * reports through {@link Metrics} ends here, and {@link #render} turns it into plain text for
 * the scrape endpoint.
 *
 * <p>Safe for use by several threads; reporting never blocks.
 */
public final class MetricsRegistry implements Metrics {

    /** A histogram of durations in microseconds with a relative error of about one percent. */
    private static final class Durations {
        private static final int LINEAR = 128;
        private static final int PER_OCTAVE = 64;
        private static final int OCTAVES = 26;
        final AtomicLongArray counts = new AtomicLongArray(LINEAR + OCTAVES * PER_OCTAVE);
        final LongAdder total = new LongAdder();
        final LongAdder sumMicros = new LongAdder();
        final AtomicLong max = new AtomicLong();

        void record(long micros) {
            int bucket;
            if (micros < LINEAR) {
                bucket = (int) Math.max(0, micros);
            } else {
                int octave = 63 - Long.numberOfLeadingZeros(micros) - 7;
                bucket = octave >= OCTAVES ? counts.length() - 1
                        : LINEAR + octave * PER_OCTAVE + (int) ((micros >>> (octave + 1)) - PER_OCTAVE);
            }
            counts.incrementAndGet(bucket);
            total.increment();
            sumMicros.add(micros);
            max.accumulateAndGet(micros, Math::max);
        }

        long percentile(double quantile) {
            long all = total.sum();
            if (all == 0) {
                return 0;
            }
            long rank = Math.max(1, (long) Math.ceil(quantile * all));
            long seen = 0;
            for (int i = 0; i < counts.length(); i++) {
                seen += counts.get(i);
                if (seen >= rank) {
                    long upper = i < LINEAR ? i
                            : ((long) (PER_OCTAVE + (i - LINEAR) % PER_OCTAVE + 1) << ((i - LINEAR) / PER_OCTAVE + 1)) - 1;
                    return Math.min(upper, max.get());
                }
            }
            return max.get();
        }
    }

    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final Map<String, Durations> durations = new ConcurrentHashMap<>();

    @Override
    public void count(String name, long delta) {
        counters.computeIfAbsent(name, _ -> new LongAdder()).add(delta);
    }

    @Override
    public void gauge(String name, long value) {
        gauges.computeIfAbsent(name, _ -> new AtomicLong()).set(value);
    }

    @Override
    public void duration(String name, long nanos) {
        durations.computeIfAbsent(name, _ -> new Durations()).record(Math.max(0, nanos) / 1_000);
    }

    /** The value of a counter; 0 if nothing was counted under the name. */
    public long counter(String name) {
        LongAdder counter = counters.get(name);
        return counter == null ? 0 : counter.sum();
    }

    /** The value of a gauge, or {@code null} if it was never set. */
    public Long gaugeValue(String name) {
        AtomicLong gauge = gauges.get(name);
        return gauge == null ? null : gauge.get();
    }

    /** How many durations were recorded under the name. */
    public long durationCount(String name) {
        Durations recorded = durations.get(name);
        return recorded == null ? 0 : recorded.total.sum();
    }

    /**
     * Everything as plain text, one {@code name value} per line, sorted by name. A name is
     * {@code freyja_} and the reported name with every character that is not a letter, a
     * digit, or an underscore replaced by an underscore. A duration gives five lines:
     * {@code _count}, and in microseconds {@code _p50_micros}, {@code _p99_micros},
     * {@code _p999_micros}, {@code _max_micros}.
     */
    public String render() {
        TreeMap<String, Long> lines = new TreeMap<>();
        counters.forEach((name, counter) -> lines.put(clean(name) + "_total", counter.sum()));
        gauges.forEach((name, gauge) -> lines.put(clean(name), gauge.get()));
        durations.forEach((name, recorded) -> {
            String base = clean(name);
            lines.put(base + "_count", recorded.total.sum());
            lines.put(base + "_p50_micros", recorded.percentile(0.50));
            lines.put(base + "_p99_micros", recorded.percentile(0.99));
            lines.put(base + "_p999_micros", recorded.percentile(0.999));
            lines.put(base + "_max_micros", recorded.max.get());
        });
        StringBuilder text = new StringBuilder();
        lines.forEach((name, value) -> text.append(name).append(' ').append(value).append('\n'));
        return text.toString();
    }

    private static String clean(String name) {
        StringBuilder cleaned = new StringBuilder("freyja_");
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            cleaned.append((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
                    ? c : '_');
        }
        return cleaned.toString();
    }
}
