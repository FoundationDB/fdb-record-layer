/*
 * YamlExecutionContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.Block;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlExecutionContext {

    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    @Nonnull final String resourcePath;
    @Nullable
    private final List<String> editedFileStream;
    private boolean isDirty;
    @Nonnull
    private final YamlRunner.YamlConnectionFactory connectionFactory;
    @Nonnull
    private final List<Block> finalizeBlocks = new ArrayList<>();
    @SuppressWarnings("AbbreviationAsWordInName")
    private final List<String> connectionURIs = new ArrayList<>();

    public static class YamlExecutionError extends RuntimeException {

        private static final long serialVersionUID = 10L;

        YamlExecutionError(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    YamlExecutionContext(@Nonnull String resourcePath, @Nonnull YamlRunner.YamlConnectionFactory factory, boolean correctExplain) throws RelationalException {
        this.connectionFactory = factory;
        this.resourcePath = resourcePath;
        this.editedFileStream = correctExplain ? loadFileToMemory(resourcePath) : null;
        if (isNightly()) {
            logger.info("ℹ️ Running in the NIGHTLY context.");
            logger.info("ℹ️ Number of threads to be used for parallel execution " + getNumThreads());
            getNightlyRepetition().ifPresent(rep -> logger.info("ℹ️ Running with high repetition value set to " + rep));
        }
    }

    @Nonnull
    public YamlRunner.YamlConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public boolean shouldCorrectExplains() {
        return editedFileStream != null;
    }

    public boolean correctExplain(int lineNumber, @Nonnull String actual) {
        if (!shouldCorrectExplains()) {
            return false;
        }
        try {
            editedFileStream.set(lineNumber, "      - explain: \"" + actual + "\"");
            isDirty = true;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean isNightly() {
        return Boolean.parseBoolean(System.getProperty(YamlRunner.TEST_NIGHTLY, "false"));
    }

    public Optional<Long> getSeed() {
        final var maybeValue = System.getProperty(YamlRunner.TEST_SEED, null);
        if (maybeValue != null) {
            return Optional.of(Long.parseLong(maybeValue));
        }
        return Optional.empty();
    }

    public Optional<Integer> getNightlyRepetition() {
        final var maybeValue = System.getProperty(YamlRunner.TEST_NIGHTLY_REPETITION, null);
        if (maybeValue != null) {
            return Optional.of(Integer.parseInt(maybeValue));
        }
        return Optional.empty();
    }

    public int getNumThreads() {
        var numThreads = 1;
        if (System.getProperties().stringPropertyNames().contains(YamlRunner.TEST_MAX_THREADS)) {
            numThreads = Integer.parseInt(System.getProperty(YamlRunner.TEST_MAX_THREADS));
            Assert.thatUnchecked(numThreads > 0, "Invalid number of threads provided in the YamlExecutionContext");
        }
        return numThreads;
    }

    boolean isDirty() {
        return isDirty;
    }

    @Nullable
    List<String> getEditedFileStream() {
        return editedFileStream;
    }

    public void registerFinalizeBlock(@Nonnull Block block) {
        this.finalizeBlocks.add(block);
    }

    public void registerConnectionURI(@Nonnull String stringURI) {
        this.connectionURIs.add(stringURI);
    }

    /**
     * Infers the URI of the database to which a block should connect to.
     *
     * A block can define the connection in multiple ways:
     * 1. no explicit definition: connect to the only registered connection URI.
     *    A URI can be registered by defining a "schema_template" block before that, which sets up the database and schema for a provided schema template.
     * 2. Parameter 0: connects to the system tables (catalog).
     * 3. Parameter One-based Number: connects to the registered connection URI, number denotes the sequence of definition.
     * 4. Parameter String: connects to the defined String
     *
     * @param connectObject can be {@code null}, an {@link Integer} value or a {@link String}.
     *
     * @return a valid connection URI
     */
    public URI inferConnectionURI(@Nullable Object connectObject) {
        if (connectObject == null) {
            Assert.thatUnchecked(!connectionURIs.isEmpty(), ErrorCode.INTERNAL_ERROR, () -> "Requested a default connection URI, but none present");
            Assert.thatUnchecked(connectionURIs.size() == 1, ErrorCode.INTERNAL_ERROR,
                    () -> "Requested a default connection URI, but multiple available to choose from: " + String.join(", ", connectionURIs));
            return URI.create(connectionURIs.get(0));
        } else if (connectObject instanceof Integer) {
            final int idx = (Integer) (connectObject);
            if (idx == 0) {
                return URI.create("jdbc:embed:/__SYS?schema=CATALOG");
            }
            Assert.thatUnchecked(idx <= connectionURIs.size(), ErrorCode.INTERNAL_ERROR,
                    () -> String.format("Requested connection URI at index %d, but only have %d available connection URIs.", idx, connectionURIs.size()));
            return URI.create(connectionURIs.get(idx - 1));
        } else {
            return URI.create(Matchers.string(connectObject));
        }
    }

    @Nonnull
    public List<Block> getFinalizeBlocks() {
        return finalizeBlocks;
    }

    /**
     * Wraps exceptions/errors with more context. This is used to hierarchically add more context to an exception. In case
     * the {@link Throwable} is a {@link YamlExecutionError}, this method adds additional context to its StackTrace in
     * the form of a new {@link StackTraceElement}, else it just wraps the throwable.
     *
     * The general flow of execution of a test in any file is: file to test_block to test_run to query_config. If an
     * exception/failure occurs in testing for a particular query_config, the following is the context that can be added
     * incrementally at appropriate places in code:
     *
     * query_config: lineNumber of the expected result
     * test_run: lineNumber of query, query run as a simple statement or as prepared statement, parameters (if any)
     * test_block: lineNumber of test_block, seed used for randomization, execution properties
     *
     * @param e the throwable that needs to be wrapped
     * @param msg additional context
     * @param identifier The name of the element type to which the context is associated to.
     * @param lineNumber the line number in the YAMSQL file to which the context is associated to.
     *
     * @return wrapped {@link YamlExecutionError}
     */
    @Nonnull
    public YamlExecutionError wrapContext(@Nullable Throwable e, @Nonnull Supplier<String> msg, @Nonnull String identifier, int lineNumber) {
        String fileName;
        if (resourcePath.contains("/")) {
            final String[] split = resourcePath.split("/");
            fileName = split[split.length - 1];
        } else {
            fileName = resourcePath;
        }
        if (e instanceof YamlExecutionError) {
            final var oldStackTrace = e.getStackTrace();
            final var newStackTrace = new StackTraceElement[oldStackTrace.length + 1];
            System.arraycopy(oldStackTrace, 0, newStackTrace, 0, oldStackTrace.length);
            newStackTrace[oldStackTrace.length] = new StackTraceElement("YAML_FILE", identifier, fileName, lineNumber);
            e.setStackTrace(newStackTrace);
            return (YamlExecutionError) e;
        } else {
            // wrap
            final var wrapper = new YamlExecutionError(msg.get(), e);
            wrapper.setStackTrace(new StackTraceElement[]{new StackTraceElement("YAML_FILE", identifier, fileName, lineNumber)});
            return wrapper;
        }
    }

    @Nonnull
    private static List<String> loadFileToMemory(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final List<String> inMemoryFile = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(resourcePath), StandardCharsets.UTF_8))) {
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                inMemoryFile.add(line);
            }
        } catch (IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
        return inMemoryFile;
    }
}
