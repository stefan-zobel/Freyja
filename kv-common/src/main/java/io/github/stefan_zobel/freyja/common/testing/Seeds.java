package io.github.stefan_zobel.freyja.common.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a randomized test body over a set of seeds (docs/spec/verification.md, section 1).
 *
 * <ul>
 *   <li>With the system property {@code freyja.seed} set, only that seed runs.</li>
 *   <li>Otherwise every seed of the corpus runs first, then the seeds {@code 1..count}. The
 *       system property {@code freyja.seeds} overrides {@code count}.</li>
 *   <li>The corpus of a test is the class-path resource {@code /seeds/<name>.txt} of the
 *       owning test class: one decimal seed per line, {@code #} starts a comment. Every seed
 *       that ever failed is added there.</li>
 *   <li>A failure names the seed and how to rerun it.</li>
 * </ul>
 */
public final class Seeds {

    /** System property that selects a single seed. */
    public static final String SEED_PROPERTY = "freyja.seed";

    /** System property that overrides the number of fresh seeds. */
    public static final String COUNT_PROPERTY = "freyja.seeds";

    /** A randomized test body. It must derive all randomness from the seed. */
    @FunctionalInterface
    public interface Body {
        void run(long seed) throws Exception;
    }

    private Seeds() {
    }

    /**
     * Runs {@code body} for the corpus of {@code name} and for {@code defaultCount} fresh
     * seeds, or for the single seed selected by {@link #SEED_PROPERTY}.
     *
     * @param owner the test class whose class path holds the corpus
     * @param name  the name of the test; also the name of its corpus file
     */
    public static void forEach(Class<?> owner, String name, int defaultCount, Body body) {
        String single = System.getProperty(SEED_PROPERTY);
        if (single != null) {
            runOne(name, Long.parseLong(single.trim()), body);
            return;
        }
        for (long seed : corpus(owner, name)) {
            runOne(name, seed, body);
        }
        int count = Integer.getInteger(COUNT_PROPERTY, defaultCount);
        for (long seed = 1; seed <= count; seed++) {
            runOne(name, seed, body);
        }
    }

    /** Returns the corpus seeds of {@code name}, or an empty list if there is no corpus. */
    public static List<Long> corpus(Class<?> owner, String name) {
        List<Long> seeds = new ArrayList<>();
        try (InputStream in = owner.getResourceAsStream("/seeds/" + name + ".txt")) {
            if (in == null) {
                return seeds;
            }
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                int comment = line.indexOf('#');
                String text = (comment >= 0 ? line.substring(0, comment) : line).trim();
                if (!text.isEmpty()) {
                    seeds.add(Long.parseLong(text));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return seeds;
    }

    private static void runOne(String name, long seed, Body body) {
        try {
            body.run(seed);
        } catch (Throwable t) {
            throw new AssertionError(name + " failed for seed " + seed
                    + "; rerun with -D" + SEED_PROPERTY + "=" + seed
                    + " and add the seed to seeds/" + name + ".txt", t);
        }
    }
}
