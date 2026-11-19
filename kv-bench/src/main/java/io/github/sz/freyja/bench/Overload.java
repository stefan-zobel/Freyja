package io.github.sz.freyja.bench;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The overload test (docs/spec/verification.md, section 7): at twice the sustainable load a
 * cluster must reject requests quickly and keep the latency of the requests it admits
 * bounded.
 *
 * <p>The clients do not send a rejected request again, so that every rejection is seen. A
 * rate is sustainable if every request of a short run at it is answered, none is rejected,
 * and the 99th percentile of the latency from the moment each request was due stays within a
 * target. A first run at the guessed rate warms the cluster and the clients up and is not
 * judged. The search doubles or halves the rate from a first guess until it finds a rate
 * that is sustainable and one that is not, and then narrows the gap down four times.
 *
 * <p>The run at twice that rate fails the test if:
 * <ul>
 *   <li>a request was neither answered nor rejected within its time;</li>
 *   <li>the 99th percentile of the time until a rejection exceeds its bound;</li>
 *   <li>the 99.9th percentile of the latency of an admitted request, from the moment it was
 *       sent, exceeds its bound;</li>
 *   <li>the load generator had to shed requests. Then the clients were too few: the excess
 *       waited in the generator and never reached the cluster, which was not overloaded.
 *       Clients carry one request each, so there must be more of them than a node admits
 *       at once.</li>
 * </ul>
 */
final class Overload {

    /**
     * @param targetP99Micros    the latency within which a rate is sustainable
     * @param probeSeconds       how long a run of the search takes
     * @param rejectP99Micros    the bound for the time until a rejection
     * @param admittedP999Micros the bound for the latency of an admitted request
     */
    record Bounds(long targetP99Micros, int probeSeconds, long rejectP99Micros, long admittedP999Micros) {
    }

    private Overload() {
    }

    /** Whether a short run at the rate of {@code config} is sustainable. */
    private static boolean sustainable(LoadGenerator.Config config, Bounds bounds, PrintStream log)
            throws InterruptedException {
        LoadGenerator.Result result = LoadGenerator.run(config.withSeconds(bounds.probeSeconds())
                .withRetryWhenOverloaded(false));
        long p99 = result.latencies().percentile(0.99);
        boolean ok = result.completed() == result.due() && p99 <= bounds.targetP99Micros();
        log.println("probe: " + config.ratePerSecond() + " per second: " + result.completed() + " of "
                + result.due() + " answered, " + result.rejected() + " rejected, " + result.failed() + " failed, "
                + result.shed() + " shed, p99 " + p99 + " us: " + (ok ? "sustainable" : "not sustainable"));
        return ok;
    }

    /**
     * Searches the highest sustainable rate, starting at the rate of {@code config}.
     *
     * @throws IllegalStateException if not even one request per second is sustainable
     */
    static int sustainableRate(LoadGenerator.Config config, Bounds bounds, PrintStream log)
            throws InterruptedException {
        int rate = config.ratePerSecond();
        int good = 0;
        int bad = 0;
        // The first run opens the connections and sessions of the clients and warms the
        // nodes up; it is not judged.
        LoadGenerator.run(config.withSeconds(bounds.probeSeconds()).withRetryWhenOverloaded(false));
        if (sustainable(config, bounds, log)) {
            good = rate;
            for (int i = 0; i < 20 && bad == 0; i++) {
                rate = (int) Math.min(Integer.MAX_VALUE / 4, 2L * rate);
                if (sustainable(config.withRate(rate), bounds, log)) {
                    good = rate;
                } else {
                    bad = rate;
                }
            }
        } else {
            bad = rate;
            while (good == 0 && rate > 1) {
                rate /= 2;
                if (sustainable(config.withRate(rate), bounds, log)) {
                    good = rate;
                } else {
                    bad = rate;
                }
            }
            if (good == 0) {
                throw new IllegalStateException("not even one request per second is sustainable");
            }
        }
        for (int i = 0; i < 4 && bad > good + 1; i++) {
            int middle = good + (bad - good) / 2;
            if (sustainable(config.withRate(middle), bounds, log)) {
                good = middle;
            } else {
                bad = middle;
            }
        }
        return good;
    }

    /** Runs at twice the sustainable rate. */
    static LoadGenerator.Result run(LoadGenerator.Config config, int sustainable) throws InterruptedException {
        return LoadGenerator.run(config.withRate(2 * sustainable).withRetryWhenOverloaded(false));
    }

    /** What the run at twice the sustainable rate did wrong; empty if nothing. */
    static List<String> problems(LoadGenerator.Result result, Bounds bounds) {
        List<String> problems = new ArrayList<>();
        if (result.failed() > 0) {
            problems.add(result.failed() + " requests were neither answered nor rejected in time");
        }
        long rejection = result.rejections().percentile(0.99);
        if (rejection > bounds.rejectP99Micros()) {
            problems.add("rejections are not quick: p99 " + rejection + " us, bound " + bounds.rejectP99Micros()
                    + " us");
        }
        long admitted = result.service().percentile(0.999);
        if (admitted > bounds.admittedP999Micros()) {
            problems.add("admitted requests took too long: p99.9 " + admitted + " us, bound "
                    + bounds.admittedP999Micros() + " us");
        }
        if (result.shed() > 0) {
            problems.add(result.shed() + " requests were shed by the load generator: too few clients to overload"
                    + " the cluster; the excess waited in the generator");
        }
        return problems;
    }
}
