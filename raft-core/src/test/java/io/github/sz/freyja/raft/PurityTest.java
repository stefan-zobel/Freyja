package io.github.sz.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.MemberRefEntry;
import java.lang.classfile.constantpool.PoolEntry;
import java.lang.classfile.constantpool.Utf8Entry;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * The core is pure (docs/spec/raft.md, section 1): no compiled class of it refers to clocks,
 * randomness, threads, or I/O. The test reads the constant pool of every class.
 */
class PurityTest {

    /** Packages and classes no class of the core may refer to, as internal names. */
    private static final List<String> FORBIDDEN_TYPES = List.of(
            "java/io/",
            "java/nio/",
            "java/net/",
            "java/time/",
            "java/security/",
            "java/util/concurrent/",
            "java/util/Random",
            "java/util/SplittableRandom",
            "java/util/random/",
            "java/util/Timer",
            "java/lang/Thread",
            "java/lang/Process",
            "java/lang/Runtime;",
            "java/lang/foreign/");

    /** Methods of otherwise harmless classes. */
    private static final Set<String> FORBIDDEN_METHODS = Set.of(
            "java/lang/System.currentTimeMillis",
            "java/lang/System.nanoTime",
            "java/lang/System.getenv",
            "java/lang/System.getProperty",
            "java/lang/System.identityHashCode",
            "java/lang/Math.random",
            "java/lang/StrictMath.random");

    private static Set<String> violations(byte[] classBytes) {
        Set<String> found = new TreeSet<>();
        ClassModel model = ClassFile.of().parse(classBytes);
        for (PoolEntry entry : model.constantPool()) {
            switch (entry) {
                case ClassEntry c -> check(c.name().stringValue(), found);
                case Utf8Entry u -> check(u.stringValue(), found);
                case MemberRefEntry m -> {
                    String method = m.owner().name().stringValue() + "." + m.name().stringValue();
                    if (FORBIDDEN_METHODS.contains(method)) {
                        found.add(method);
                    }
                }
                default -> {
                }
            }
        }
        return found;
    }

    private static void check(String text, Set<String> found) {
        for (String forbidden : FORBIDDEN_TYPES) {
            if (text.contains(forbidden)) {
                found.add(text);
            }
        }
    }

    private static Path classesOfTheCore() throws URISyntaxException {
        return Path.of(RaftNode.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    @Test
    void noClassOfTheCoreRefersToClocksRandomnessThreadsOrIo() throws Exception {
        Path root = classesOfTheCore();
        assertTrue(Files.isDirectory(root), "the core is tested from its class directory: " + root);
        List<Path> classFiles;
        try (Stream<Path> stream = Files.walk(root)) {
            classFiles = stream.filter(p -> p.getFileName().toString().endsWith(".class")).toList();
        }
        assertTrue(classFiles.size() >= 15, "found only " + classFiles.size() + " classes in " + root);
        assertTrue(classFiles.stream().anyMatch(p -> p.endsWith("RaftNode.class")));
        List<String> problems = new ArrayList<>();
        for (Path classFile : classFiles) {
            Set<String> found = violations(Files.readAllBytes(classFile));
            if (!found.isEmpty()) {
                problems.add(root.relativize(classFile) + " refers to " + found);
            }
        }
        assertEquals(List.of(), problems);
    }

    /** Uses what the core must not use, so that the scan can be shown to notice. */
    static final class Impure {
        long clock() {
            return System.nanoTime();
        }

        int random() {
            return new Random().nextInt();
        }

        Thread thread() {
            return Thread.currentThread();
        }

        Path path;
    }

    private static byte[] bytesOf(Class<?> type) throws IOException {
        String resource = "/" + type.getName().replace('.', '/') + ".class";
        try (InputStream in = type.getResourceAsStream(resource)) {
            return in.readAllBytes();
        }
    }

    @Test
    void theScanNoticesForbiddenReferences() throws Exception {
        Set<String> found = violations(bytesOf(Impure.class));
        assertTrue(found.contains("java/lang/System.nanoTime"), found.toString());
        assertTrue(found.contains("java/util/Random"), found.toString());
        assertTrue(found.contains("java/lang/Thread"), found.toString());
        assertTrue(found.stream().anyMatch(s -> s.contains("java/nio/file/Path")), found.toString());
        assertFalse(violations(bytesOf(LogEntry.class)).iterator().hasNext());
    }
}
