/*
 * TestLogFile.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.async.hnsw;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-invocation log file written to {@code fdb-extensions/.out/reports/}. Lets a parameterised test
 * dump a structured trace of itself (and any helpers it calls) to a dedicated file separate from the
 * shared logger output, so CI failures can be inspected without untangling interleaved parallel-test
 * log streams.
 *
 * <p>Typical usage from a parameterised test:</p>
 * <pre>
 * try (TestHelpers.TestLogFile logFile = TestHelpers.TestLogFile.create("OperationsTest.testBasicInsert", seed, config)) {
 *     try {
 *         // ... test body, with logFile.log(...) calls ...
 *         basicInsertBatch(db, hnsw, batchSize, firstId, insertFn, logFile);
 *     } catch (Throwable t) {
 *         logFile.logFailure("testBasicInsert", t);
 *         throw t;
 *     }
 * }
 * </pre>
 */
public final class TestLogFile implements AutoCloseable {
    @Nonnull
    private static final Path REPORTS_DIR = Paths.get(".out", "reports");
    @Nonnull
    private static final AtomicLong COUNTER = new AtomicLong();

    @Nonnull
    private final Path path;
    @Nonnull
    private final PrintWriter writer;

    private TestLogFile(@Nonnull final Path path, @Nonnull final PrintWriter writer) {
        this.path = path;
        this.writer = writer;
    }

    /**
     * Create a new log file for a single test invocation. The file is created under
     * {@code fdb-extensions/.out/reports/} with a name derived from {@code testName} and a
     * monotonically increasing counter. The file is opened immediately, and a header recording
     * the test name and arguments is written before the method returns.
     */
    @Nonnull
    public static TestLogFile create(@Nonnull final String testName, @Nonnull final List<Object> args) throws IOException {
        Files.createDirectories(REPORTS_DIR);
        final long index = COUNTER.incrementAndGet();
        final String sanitized = testName.replaceAll("[^A-Za-z0-9._-]", "_");
        final Path file = REPORTS_DIR.resolve(String.format(Locale.ROOT, "%s-%d.log", sanitized, index));
        // Synchronized PrintWriter; writes from FDB callback threads and the test thread are safe.
        final BufferedWriter buffered = Files.newBufferedWriter(file,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        final PrintWriter writer = new PrintWriter(buffered, true);
        final TestLogFile logFile = new TestLogFile(file, writer);
        // Header: test name, arguments, start timestamp.
        logFile.writer.println("# test=" + testName);
        for (int i = 0; i < args.size(); i++) {
            logFile.writer.println("# arg[" + i + "]=" + Objects.toString(args.get(i)));
        }
        logFile.writer.println("# startedAt=" + Instant.now());
        logFile.writer.println("# file=" + file.toAbsolutePath());
        logFile.writer.println();
        logFile.writer.flush();
        return logFile;
    }

    public static void run(final String testName, List<Object> args, TestCode testCode) throws Exception {
        try (TestLogFile logFile = create(testName, args)) {
            try {
                testCode.runTest(logFile);
            } catch (Exception e) {
                logFile.logFailure("Test Failed", e);
                throw e;
            }
        }
    }

    /**
     * Append a single timestamped log line. Format string follows {@link String#format(Locale, String, Object...)}
     * semantics. Safe to call from any thread.
     */
    public void log(@Nonnull final String fmt, @Nonnull final Object... args) {
        final String formatted = String.format(Locale.ROOT, fmt, args);
        synchronized (writer) {
            writer.print(Instant.now());
            writer.print(' ');
            writer.print('[');
            writer.print(Thread.currentThread().getName());
            writer.print("] ");
            writer.println(formatted);
        }
    }

    /**
     * Append a failure record including the stack trace. Use this before rethrowing in a test's
     * catch block.
     */
    public void logFailure(@Nonnull final String context, @Nonnull final Throwable t) {
        synchronized (writer) {
            writer.print(Instant.now());
            writer.print(" [");
            writer.print(Thread.currentThread().getName());
            writer.print("] FAILURE ");
            writer.println(context);
            t.printStackTrace(writer);
        }
    }

    @Nonnull
    public Path getPath() {
        return path;
    }

    @Override
    public void close() {
        synchronized (writer) {
            writer.println();
            writer.println("# closedAt=" + Instant.now());
            writer.close();
        }
    }

    @FunctionalInterface
    public interface TestCode {
        void runTest(TestLogFile logFile) throws Exception;
    }
}
