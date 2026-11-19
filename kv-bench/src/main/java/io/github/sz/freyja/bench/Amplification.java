package io.github.sz.freyja.bench;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.github.sz.freyja.client.FreyjaClient;

/**
 * Write and space amplification of a run (docs/spec/verification.md, section 7), from the
 * figures of the nodes and what the clients wrote.
 *
 * <ul>
 *   <li>Write amplification: the bytes a node appended to its files during the run
 *       ({@code disk.written.bytes}), per byte of keys and values that the clients put; the
 *       mean over the nodes. Every node writes the Raft log, the state machine, and what its
 *       storage engine flushes and compacts.</li>
 *   <li>Space amplification: the bytes a node holds after the run ({@code node.stored.bytes}:
 *       its Raft log, and the tables and memtables of its state machine), per byte of keys and
 *       values that are live in the cluster; the mean over the nodes. The live bytes are
 *       counted by a scan of every key after the run.</li>
 * </ul>
 */
final class Amplification {

    static final String WRITTEN = "freyja_disk_written_bytes";
    static final String STORED = "freyja_node_stored_bytes";

    private final List<String> statuses;
    private final long[] writtenBefore;

    private Amplification(List<String> statuses, long[] writtenBefore) {
        this.statuses = statuses;
        this.writtenBefore = writtenBefore;
    }

    /** Takes the figures of the nodes before the run. */
    static Amplification before(List<String> statuses) throws IOException {
        long[] written = new long[statuses.size()];
        for (int i = 0; i < written.length; i++) {
            written[i] = figure(statuses.get(i), WRITTEN);
        }
        return new Amplification(List.copyOf(statuses), written);
    }

    private static long figure(String status, String name) throws IOException {
        Long value = NodeAccess.figures(status).get(name);
        if (value == null) {
            throw new IOException(status + " does not report " + name);
        }
        return value;
    }

    /**
     * Takes the figures of the nodes after the run and computes the amplifications.
     *
     * @param userBytes       the bytes of keys and values the clients put during the run
     * @param clientEndpoints where the nodes listen for clients, for the scan of the live data
     * @return the lines of the report
     */
    Map<String, String> after(long userBytes, List<String> clientEndpoints) throws IOException {
        long written = 0;
        long stored = 0;
        for (int i = 0; i < statuses.size(); i++) {
            written += figure(statuses.get(i), WRITTEN) - writtenBefore[i];
            stored += figure(statuses.get(i), STORED);
        }
        long live = liveBytes(clientEndpoints);
        int nodes = statuses.size();
        Map<String, String> report = new LinkedHashMap<>();
        report.put("user.written.bytes", Long.toString(userBytes));
        report.put("node.written.bytes", Long.toString(written / nodes));
        report.put("live.bytes", Long.toString(live));
        report.put("node.stored.bytes", Long.toString(stored / nodes));
        report.put("write.amplification", userBytes == 0 ? "0" : format((double) written / nodes / userBytes));
        report.put("space.amplification", live == 0 ? "0" : format((double) stored / nodes / live));
        return report;
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    /** The bytes of every key and value in the cluster, read page by page. */
    static long liveBytes(List<String> clientEndpoints) {
        long bytes = 0;
        try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(
                clientEndpoints.toArray(String[]::new)).withRequestTimeout(60_000).withKeepAlive(0))) {
            byte[] start = new byte[0];
            while (true) {
                List<FreyjaClient.Entry> page = client.scan(start, new byte[0], 1_000);
                for (FreyjaClient.Entry entry : page) {
                    bytes += entry.key().length + entry.value().length;
                }
                if (page.isEmpty()) {
                    return bytes;
                }
                byte[] last = page.getLast().key();
                start = Arrays.copyOf(last, last.length + 1); // the next key after the last one
            }
        }
    }
}
