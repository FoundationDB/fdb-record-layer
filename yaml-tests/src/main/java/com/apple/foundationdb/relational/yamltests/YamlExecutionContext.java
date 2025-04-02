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
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlExecutionContext {
    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    public static final ContextOption<Boolean> OPTION_FORCE_CONTINUATIONS = new ContextOption<>("optionForceContinuation");
    public static final ContextOption<Boolean> OPTION_CORRECT_EXPLAIN = new ContextOption<>("optionCorrectExplain");
    public static final ContextOption<Boolean> OPTION_CORRECT_METRICS = new ContextOption<>("optionCorrectMetrics");
    public static final ContextOption<Boolean> OPTION_SHOW_PLAN_ON_DIFF = new ContextOption<>("optionShowPlanOnDiff");

    @Nonnull final String resourcePath;
    @Nullable
    private final List<String> editedFileStream;
    private boolean isDirty;
    @Nonnull
    private final ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> expectedMetricsMap;
    @Nonnull
    private final Map<QueryAndLocation, PlannerMetricsProto.Info> actualMetricsMap;
    private boolean isDirtyMetrics;
    @Nonnull
    private final YamlConnectionFactory connectionFactory;
    @Nonnull
    private final List<Block> finalizeBlocks = new ArrayList<>();
    @SuppressWarnings("AbbreviationAsWordInName")
    private final List<String> connectionURIs = new ArrayList<>();
    // Additional options that can be set by the runners to impact test execution
    private final ContextOptions additionalOptions;

    public static class YamlExecutionError extends RuntimeException {

        private static final long serialVersionUID = 10L;

        YamlExecutionError(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    YamlExecutionContext(@Nonnull String resourcePath, @Nonnull YamlConnectionFactory factory,
                         @Nonnull final ContextOptions additionalOptions) throws RelationalException {
        this.connectionFactory = factory;
        this.resourcePath = resourcePath;
        this.editedFileStream = additionalOptions.getOrDefault(OPTION_CORRECT_EXPLAIN, false)
                                ? loadYamlResource(resourcePath) : null;
        this.additionalOptions = additionalOptions;
        this.expectedMetricsMap = loadMetricsResource(resourcePath);
        this.actualMetricsMap = new TreeMap<>(Comparator.comparing(QueryAndLocation::getLineNumber)
                .thenComparing(QueryAndLocation::getBlockName)
                .thenComparing(QueryAndLocation::getQuery));
        if (isNightly()) {
            logger.info("ℹ️ Running in the NIGHTLY context.");
            if (shouldCorrectExplains() || shouldCorrectMetrics()) {
                logger.error("‼️ Explain and/or planner metrics cannot be modified during nightly runs.");
                Assertions.fail("‼️ Explain or planner metrics cannot be modified during nightly runs. " +
                        "Make sure maintenance annotations have not been checked in.");
            }
            logger.info("ℹ️ Number of threads to be used for parallel execution " + getNumThreads());
            getNightlyRepetition().ifPresent(rep -> logger.info("ℹ️ Running with high repetition value set to " + rep));
        }
    }

    @Nonnull
    public YamlConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public boolean shouldCorrectExplains() {
        final var shouldCorrectExplains = additionalOptions.getOrDefault(OPTION_CORRECT_EXPLAIN, false);
        Verify.verify(!shouldCorrectExplains || editedFileStream != null);
        return shouldCorrectExplains;
    }

    public boolean shouldCorrectMetrics() {
        return additionalOptions.getOrDefault(OPTION_CORRECT_METRICS, false);
    }

    public boolean shouldShowPlanOnDiff() {
        return additionalOptions.getOrDefault(OPTION_SHOW_PLAN_ON_DIFF, false);
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

    @Nonnull
    public ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> getMetricsMap() {
        return expectedMetricsMap;
    }

    @Nullable
    @SuppressWarnings("UnusedReturnValue")
    public synchronized PlannerMetricsProto.Info putMetrics(@Nonnull final String blockName,
                                                            @Nonnull final String query,
                                                            final int lineNumber,
                                                            @Nonnull final PlannerMetricsProto.Info info,
                                                            boolean isDirtyMetrics) {
        return actualMetricsMap.put(new QueryAndLocation(blockName, query, lineNumber), info);
    }

    @Nullable
    @SuppressWarnings("UnusedReturnValue")
    public synchronized void markDirty() {
        this.isDirtyMetrics = true;
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
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    boolean isDirty() {
        return isDirty;
    }

    boolean isDirtyMetrics() {
        return isDirtyMetrics;
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
     * <br>
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
                    () -> String.format(Locale.ROOT, "Requested connection URI at index %d, but only have %d available connection URIs.", idx, connectionURIs.size()));
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
     * <br>
     * The general flow of execution of a test in any file is: file to test_block to test_run to query_config. If an
     * exception/failure occurs in testing for a particular query_config, the following is the context that can be added
     * incrementally at appropriate places in code:
     * <br>
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

    /**
     * Return the value of an additional option, or a default value.
     * Additional options are options set by the test execution environment that can control the test execution, in additional
     * to the "core" set of options defined in this class.
     * @param option the option to get value for
     * @param defaultValue the default value (if option is undefined)
     * @return the defined value of the option, or the default value, if undefined
     */
    public <T> T getOption(ContextOption<T> option, T defaultValue) {
        return additionalOptions.getOrDefault(option, defaultValue);
    }

    public void saveMetricsAsBinaryProto() {
        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsBinaryProtoFileName(resourcePath)))
                .toAbsolutePath().toString();

        //
        // It is possible that some queries are repeated within the same block. These explain queries, if served from
        // the cache contain their original planner metrics when they were planned, thus they are identical and we
        // pick one of them. If not served from the cache (for instance by explicitly switching it off) we should
        // still see the same counters which is all we test for at this moment. If someone adds a testcase that
        // switches off the cache, executes an explain, changes something about the schema and then runs the same
        // query in the same block a second time, there will be pain. Don't do that! We log a warning for this case
        // but continue.
        //
        final var condensedMetricsMap = new LinkedHashMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info>();
        for (final var entry : actualMetricsMap.entrySet()) {
            final var queryAndLocation = entry.getKey();
            final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName(queryAndLocation.getBlockName())
                    .setQuery(queryAndLocation.getQuery())
                    .build();
            if (condensedMetricsMap.containsKey(identifier)) {
                logger.warn("⚠️ Repeated query in block {} at line {}", queryAndLocation.getBlockName(),
                        queryAndLocation.getLineNumber());
            } else {
                condensedMetricsMap.put(identifier,
                        entry.getValue());
            }
        }

        try (var fos = new FileOutputStream(fileName)) {
            for (final var entry : condensedMetricsMap.entrySet()) {
                PlannerMetricsProto.Entry.newBuilder()
                        .setIdentifier(entry.getKey())
                        .setInfo(entry.getValue())
                        .build()
                        .writeDelimitedTo(fos);
            }
            logger.info("🟢 Planner metrics file {} replaced.", fileName);
        } catch (final IOException iOE) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", fileName);
            Assertions.fail(iOE);
        }
    }

    public void saveMetricsAsYaml() {
        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsYamlFileName(resourcePath)))
                .toAbsolutePath().toString();

        final var mmap = LinkedListMultimap.<String, Map<String, Object>>create();
        for (final var entry : actualMetricsMap.entrySet()) {
            final var identifier = entry.getKey();
            final var info = entry.getValue();
            final var countersAndTimers = info.getCountersAndTimers();
            final var infoMap =
                    ImmutableMap.<String, Object>of("query", identifier.getQuery(),
                            "explain", info.getExplain(),
                            "task_count", countersAndTimers.getTaskCount(),
                            "task_total_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTaskTotalTimeNs()),
                            "transform_count", countersAndTimers.getTransformCount(),
                            "transform_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTransformTimeNs()),
                            "transform_yield_count", countersAndTimers.getTransformYieldCount(),
                            "insert_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getInsertTimeNs()),
                            "insert_new_count", countersAndTimers.getInsertNewCount(),
                            "insert_reused_count", countersAndTimers.getInsertReusedCount());
            mmap.put(identifier.getBlockName(), infoMap);
        }

        try (var fos = new FileOutputStream(fileName)) {
            DumperOptions options = new DumperOptions();
            options.setIndent(4);
            options.setPrettyFlow(true);
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(options);
            yaml.dump(mmap.asMap(), new PrintWriter(fos, false, StandardCharsets.UTF_8));
            logger.info("🟢 Planner metrics file {} replaced.", fileName);
        } catch (final IOException iOE) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", fileName);
            Assertions.fail(iOE);
        }
    }

    @Nonnull
    private static List<String> loadYamlResource(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final List<String> inMemoryFile = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(
                             new InputStreamReader(Objects.requireNonNull(classLoader.getResourceAsStream(resourcePath)),
                                     StandardCharsets.UTF_8))) {
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                inMemoryFile.add(line);
            }
        } catch (IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
        return inMemoryFile;
    }

    @Nonnull
    private static ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> loadMetricsResource(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final var fis = classLoader.getResourceAsStream(metricsBinaryProtoFileName(resourcePath));
        final var resultMapBuilder =
                ImmutableMap.<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info>builder();
        if (fis == null) {
            return resultMapBuilder.build();
        }
        try {
            while (true) {
                final var entry = PlannerMetricsProto.Entry.parseDelimitedFrom(fis);
                if (entry == null) {
                    return resultMapBuilder.build();
                }
                resultMapBuilder.put(entry.getIdentifier(), entry.getInfo());
            }
        } catch (final IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Nonnull
    private static String metricsBinaryProtoFileName(@Nonnull final String resourcePath) {
        return baseName(resourcePath) + ".metrics.binpb";
    }

    @Nonnull
    private static String metricsYamlFileName(@Nonnull final String resourcePath) {
        return baseName(resourcePath) + ".metrics.yaml";
    }

    @Nonnull
    private static String baseName(@Nonnull final String resourcePath) {
        final var tokens = resourcePath.split("\\.(?=[^\\.]+$)");
        Verify.verify(tokens.length == 2);
        Verify.verify("yamsql".equals(tokens[1]));
        return tokens[0];
    }

    private static class QueryAndLocation {
        @Nonnull
        private final String blockName;
        private final String query;
        private final int lineNumber;

        public QueryAndLocation(@Nonnull final String blockName, final String query, final int lineNumber) {
            this.blockName = blockName;
            this.query = query;
            this.lineNumber = lineNumber;
        }

        @Nonnull
        public String getBlockName() {
            return blockName;
        }

        public String getQuery() {
            return query;
        }

        public int getLineNumber() {
            return lineNumber;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof QueryAndLocation)) {
                return false;
            }
            final QueryAndLocation that = (QueryAndLocation)o;
            return lineNumber == that.lineNumber && Objects.equals(blockName, that.blockName) && Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(blockName, query, lineNumber);
        }
    }

    public static class ContextOptions {
        public static final ContextOptions EMPTY_OPTIONS = new ContextOptions(Map.of());

        @Nonnull
        private final Map<ContextOption<?>, Object> map;

        private ContextOptions(final @Nonnull Map<ContextOption<?>, Object> map) {
            this.map = map;
        }

        public static <T> ContextOptions of(ContextOption<T> prop, T value) {
            return new ContextOptions(Map.of(prop, value));
        }

        public static <T1, T2> ContextOptions of(ContextOption<T1> prop1, T1 value1, ContextOption<T2> prop2, T2 value2) {
            return new ContextOptions(Map.of(prop1, value1, prop2, value2));
        }

        public ContextOptions mergeFrom(ContextOptions other) {
            final Map<ContextOption<?>, Object> newMap = new HashMap<>(map);
            newMap.putAll(other.map);
            return new ContextOptions(newMap);
        }

        @SuppressWarnings("unchecked")
        public <T> T getOrDefault(ContextOption<T> prop, T defaultValue) {
            return (T)map.getOrDefault(prop, defaultValue);
        }

        @Override
        public String toString() {
            return map.toString();
        }
    }

    public static class ContextOption<T> {
        private final String name;

        public ContextOption(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ContextOption)) {
                return false;
            }
            final ContextOption<?> that = (ContextOption<?>)o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
