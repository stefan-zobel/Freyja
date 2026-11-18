package io.github.sz.freyja.raft.sim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lets TLC check a recorded run against the trace specification in {@code docs/tla}
 * (docs/spec/verification.md, section 2.7). TLC is an external tool that runs as a process
 * of its own; nothing of it is on the class path.
 */
public final class TraceValidator {

    /**
     * The outcome of one validation.
     *
     * @param accepted whether the run is a behavior of the model
     * @param reason   what TLC said, with the step it could not take
     * @param steps    the number of steps of the run
     */
    public record Verdict(boolean accepted, String reason, int steps) {
    }

    private TraceValidator() {
    }

    /** The directory that holds {@code docs}: the working directory or the one above it. */
    public static Path repositoryRoot() {
        Path here = Path.of("").toAbsolutePath();
        return Files.exists(here.resolve("docs")) ? here : here.getParent();
    }

    /**
     * The jar of TLC: what the system property {@code freyja.tlc} names, or
     * {@code tools/tla2tools.jar} in the repository directory.
     */
    public static Path tlcJar() {
        return Path.of(System.getProperty("freyja.tlc",
                repositoryRoot().resolve("tools").resolve("tla2tools.jar").toString()));
    }

    /**
     * Validates {@code trace}.
     *
     * @param dir an empty directory for the files of this validation; it is created
     * @throws IllegalStateException if TLC is not where {@link #tlcJar} says, or does not end
     *                               within 20 minutes
     */
    public static Verdict validate(Path dir, TlaTraceWriter trace) throws IOException, InterruptedException {
        Path tla = repositoryRoot().resolve("docs").resolve("tla");
        Path jar = tlcJar();
        if (!Files.exists(jar)) {
            throw new IllegalStateException("TLC not found at " + jar + "; set -Dfreyja.tlc=<tla2tools.jar>");
        }
        Files.createDirectories(dir);
        for (String file : List.of("Raft.tla", "RaftTrace.tla", "RaftTrace.cfg")) {
            Files.copy(tla.resolve(file), dir.resolve(file));
        }
        Files.writeString(dir.resolve("TraceData.tla"), trace.module(), StandardCharsets.UTF_8);
        Path output = dir.resolve("tlc.out");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-XX:+UseParallelGC", "-Xss64m", "-cp", jar.toString(), "tlc2.TLC",
                "-workers", "1", "-metadir", dir.resolve("states").toString(),
                "-config", "RaftTrace.cfg", "RaftTrace.tla")
                .directory(dir.toFile())
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!process.waitFor(20, TimeUnit.MINUTES)) {
            process.destroyForcibly().waitFor();
            throw new IllegalStateException("TLC did not finish within 20 minutes");
        }
        String text = Files.readString(output, StandardCharsets.UTF_8);
        int steps = trace.steps().size();
        if (text.contains("Invariant NotDone is violated")) {
            return new Verdict(true, "accepted", steps);
        }
        String reason = "TLC did not get past the last step";
        for (String line : text.split("\n")) {
            if (line.contains("is violated") || line.contains("rror:") || line.contains("depth of the complete")) {
                reason = line.trim();
                if (!line.contains("depth of the complete")) {
                    break;
                }
            }
        }
        return new Verdict(false, reason + rejectedStep(trace, text), steps);
    }

    /** The step TLC could not take, with the steps of the same node before it. */
    private static String rejectedStep(TlaTraceWriter trace, String tlcOutput) {
        Matcher depth = Pattern.compile("depth of the complete state graph search is (\\d+)").matcher(tlcOutput);
        if (!depth.find()) {
            return "";
        }
        int at = Integer.parseInt(depth.group(1)) - 1;
        List<TlaTraceWriter.Step> steps = trace.steps();
        if (at < 0 || at >= steps.size()) {
            return "";
        }
        StringBuilder text = new StringBuilder("\nrejected step " + (at + 1) + ": "
                + TlaTraceWriter.record(steps.get(at)));
        int shown = 0;
        for (int i = at - 1; i >= 0 && shown < 6; i--) {
            if (steps.get(i).node == steps.get(at).node) {
                text.append("\n  step ").append(i + 1).append(": ").append(TlaTraceWriter.record(steps.get(i)));
                shown++;
            }
        }
        return text.toString();
    }
}
