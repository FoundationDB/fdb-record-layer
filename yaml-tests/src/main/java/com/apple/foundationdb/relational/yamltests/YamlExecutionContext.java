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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.IncludeBlock;
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.TestAbortedException;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serial;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlExecutionContext {
    private static final Logger logger = LogManager.getLogger(YamlExecutionContext.class);

    public static final ContextOption<Boolean> OPTION_FORCE_CONTINUATIONS = new ContextOption<>("optionForceContinuation");
    public static final ContextOption<Boolean> OPTION_CORRECT_EXPLAIN = new ContextOption<>("optionCorrectExplain");
    public static final ContextOption<Boolean> OPTION_CORRECT_METRICS = new ContextOption<>("optionCorrectMetrics");
    public static final ContextOption<Boolean> OPTION_CORRECT_RESULT_METADATA = new ContextOption<>("optionCorrectResultMetadata");
    public static final ContextOption<Boolean> OPTION_ADD_RESULT_METADATA = new ContextOption<>("optionAddResultMetadata");
    public static final ContextOption<Boolean> OPTION_ADD_EXPLAIN = new ContextOption<>("optionAddExplain");
    public static final ContextOption<Boolean> OPTION_SHOW_PLAN_ON_DIFF = new ContextOption<>("optionShowPlanOnDiff");

    private static final URI SYSTEM_CATALOG_ADDRESS = URI.create("jdbc:embed:/__SYS?schema=CATALOG");

    @Nonnull final YamlReference.YamlResource topLevelResource;
    @Nonnull
    private final Set<YamlReference.YamlResource> registeredResources = new HashSet<>();

    @Nonnull
    private final YamlMetricsMaintainer metricsMaintainer;
    @Nonnull
    private final YamlFilesMaintainer filesMaintainer;
    @Nonnull
    private final YamlConnectionFactory connectionFactory;
    @SuppressWarnings("AbbreviationAsWordInName")
    private final Map<YamlReference.YamlResource, List<String>> connectionURIs = new HashMap<>();
    // Additional options that can be set by the runners to impact test execution
    @Nonnull
    private final ContextOptions additionalOptions;
    private final Map<String, String> transactionSetups = new HashMap<>();
    @Nonnull
    private Options connectionOptions = Options.NONE;

    public static class YamlExecutionError extends RuntimeException {

        @Serial
        private static final long serialVersionUID = 10L;

        YamlExecutionError(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    YamlExecutionContext(@Nonnull YamlReference.YamlResource topLevelResource, @Nonnull YamlConnectionFactory factory, @Nonnull final ContextOptions additionalOptions) throws RelationalException {
        if (isInCI() && (shouldCorrectExplains() || shouldCorrectMetrics() || shouldCorrectResultMetadata() || shouldAddResultMetadata() || shouldAddExplains())) {
            logger.error("‼️ Yamsql files cannot be modified during CI runs.");
            Assertions.fail("‼️ Yamsql files cannot be modified during CI runs. " +
                    "Make sure maintenance annotations have not been checked in.");
        }
        if (isNightly()) {
            logger.info("ℹ️ Running in the NIGHTLY context.");
            logger.info("ℹ️ Number of threads to be used for parallel execution " + getNumThreads());
            getNightlyRepetition().ifPresent(rep -> logger.info("ℹ️ Running with high repetition value set to " + rep));
        }
        this.connectionFactory = factory;
        this.topLevelResource = topLevelResource;
        this.additionalOptions = additionalOptions;
        this.metricsMaintainer = new YamlMetricsMaintainer(topLevelResource);
        this.filesMaintainer = new YamlFilesMaintainer();
        loadResourceForEditIfNeeded(topLevelResource);
        registeredResources.add(topLevelResource);
    }

    public void registerResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        if (resource == topLevelResource) {
            return;
        }
        if (registeredResources.contains(resource)) {
            throw new RuntimeException("The resource " + resource + " is already registered.");
        }
        loadResourceForEditIfNeeded(resource);
        registeredResources.add(resource);
    }

    private void loadResourceForEditIfNeeded(@Nonnull YamlReference.YamlResource resource) throws RelationalException {
        if (shouldCorrectExplains() || shouldCorrectResultMetadata() || shouldAddResultMetadata() || shouldAddExplains()) {
            filesMaintainer.loadFile(resource);
        }
    }

    public void setConnectionOptions(@Nonnull final Options connectionOptions) {
        this.connectionOptions = connectionOptions;
    }

    @Nonnull
    public YamlConnectionFactory getConnectionFactory() {
        return YamlConnectionFactoryWithOptions.newInstance(connectionFactory, connectionOptions);
    }

    public boolean shouldCorrectExplains() {
        return additionalOptions.getOrDefault(OPTION_CORRECT_EXPLAIN, false);
    }

    public boolean shouldCorrectMetrics() {
        return additionalOptions.getOrDefault(OPTION_CORRECT_METRICS, false);
    }

    public boolean shouldShowPlanOnDiff() {
        return additionalOptions.getOrDefault(OPTION_SHOW_PLAN_ON_DIFF, false);
    }

    public boolean shouldCorrectResultMetadata() {
        return additionalOptions.getOrDefault(OPTION_CORRECT_RESULT_METADATA, false);
    }

    public boolean shouldAddResultMetadata() {
        return additionalOptions.getOrDefault(OPTION_ADD_RESULT_METADATA, false);
    }

    public boolean shouldAddExplains() {
        return additionalOptions.getOrDefault(OPTION_ADD_EXPLAIN, false);
    }

    @Nonnull
    public YamlMetricsMaintainer getMetricsMaintainer() {
        return metricsMaintainer;
    }

    @Nonnull
    public YamlFilesMaintainer getFilesMaintainer() {
        return filesMaintainer;
    }

    public static boolean isInCI() {
        return Boolean.parseBoolean(System.getProperty(YamlRunner.TEST_CI, "false"));
    }

    public static boolean isNightly() {
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

    public void replaceFilesIfRequired() {
        metricsMaintainer.saveIfNeeded();
        filesMaintainer.saveIfNeeded();
    }

    public void registerConnectionURI(@Nonnull YamlReference.YamlResource resource, @Nonnull String stringURI) {
        Assert.thatUnchecked(registeredResources.contains(resource), "A YamlResource should be registered before registering available connection URIs");
        connectionURIs.computeIfAbsent(resource, ignore -> new ArrayList<>()).add(stringURI);
    }

    /**
     * Infers the connection target (URI and cluster index) for a block.
     * <br>
     * <ul>
     *   <li>no explicit declaration: Try to connect to the only registered connection URI in the local {@link YamlReference.YamlResource}.
     *    If not, try to connect to the only connection across all parent resources.
     *    A URI can be registered by defining a "schema_template" block before that, which sets up the database and schema for a provided schema template.
     *    </li>
     *    <li>Parameter 0: connects to the system tables (catalog). </li>
     *    <li>Parameter One-based Number: connects to the registered connection URI, number denotes the sequence of definitions in the local YamlResource.
     *    To access parent connection URIs, this number should be prepended by `(global)` tag.
     *    </li>
     *    <li>Parameter String: connects to the defined String</li>
     *    <li>A map form for specifying the cluster:
     * <pre>{@code
     * connect: { cluster: 1, uri: 0 }
     * connect: { cluster: 1 }
     * }</pre>
     * </li>
     * </ul>
     *
     * @param connectObject can be {@code null}, an {@link Integer}, a {@link String}, or a {@link Map} with
     *                      optional {@code cluster} and {@code uri} keys.
     *
     * @return a valid connection target
     */
    public ConnectionTarget inferConnectionTarget(@Nonnull final YamlReference.YamlResource resource, @Nullable Object connectObject) {
        Assert.thatUnchecked(registeredResources.contains(resource), "A YamlResource should be registered before registering available connection URIs");
        if (connectObject instanceof Map) {
            final Map<?, ?> connectMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(connectObject, "connect"));
            final int clusterIndex = connectMap.containsKey("cluster")
                    ? ((Number) connectMap.get("cluster")).intValue() : 0;
            final Object uriSpec = connectMap.getOrDefault("uri", null);
            return new ConnectionTarget(resolveConnectionURI(resource, uriSpec), clusterIndex);
        }
        return new ConnectionTarget(resolveConnectionURI(resource, connectObject), 0);
    }

    private URI resolveConnectionURI(@Nonnull final YamlReference.YamlResource resource, @Nullable Object connectObject) {
        if (connectObject == null) {
            return getConnectionFromConnectionURIList(resource, true, -1, true);
        } else if (connectObject instanceof Integer) {
            return getConnectionFromConnectionURIList(resource, false, (Integer) connectObject, false);
        } else {
            final var stringURI = Matchers.string(connectObject, "connection object");
            if (stringURI.startsWith("(global)")) {
                return getConnectionFromConnectionURIList(resource, false, Integer.parseInt(stringURI.substring(8).trim()), true);
            }
            return URI.create(stringURI);
        }
    }

    private URI getConnectionFromConnectionURIList(@Nonnull YamlReference.YamlResource resource, boolean defaultValue, int idx, boolean isGlobal) {
        if (defaultValue) {
            final var localList = connectionURIs.getOrDefault(resource, List.of());
            if (localList.size() == 1) {
                return URI.create(localList.get(0));
            }
            Assert.thatUnchecked(localList.isEmpty(), ErrorCode.INTERNAL_ERROR,
                    () -> "Requested a default connection URI, but multiple available to choose from in local: " + String.join(", " + localList));
            final var globalList = getGlobalConnectionURIList(resource);
            Assert.thatUnchecked(!globalList.isEmpty(), ErrorCode.INTERNAL_ERROR, () -> "Requested a default connection URI, but none present");
            Assert.thatUnchecked(globalList.size() == 1, ErrorCode.INTERNAL_ERROR,
                    () -> "Requested a default connection URI, but multiple available to choose from in global: " + String.join(", ", globalList));
            return URI.create(globalList.get(0));
        }
        final var list = !isGlobal ? connectionURIs.getOrDefault(resource, List.of()) : getGlobalConnectionURIList(resource);
        if (idx == 0) {
            return SYSTEM_CATALOG_ADDRESS;
        }
        Assert.thatUnchecked(idx <= list.size(), ErrorCode.INTERNAL_ERROR,
                () -> String.format(Locale.ROOT, "Requested connection URI at index %d, but only have %d available connection URIs.", idx, list.size()));
        return URI.create(list.get(idx - 1));
    }

    private List<String> getGlobalConnectionURIList(@Nonnull YamlReference.YamlResource resource) {
        final var resourcesBuilder = ImmutableList.<YamlReference.YamlResource>builder();
        if (resource.getParentRef() != null) {
            resourcesBuilder.addAll(resource.getParentRef().getCallStack().reverse().stream().map(YamlReference::getResource).iterator());
        }
        resourcesBuilder.add(resource);
        return resourcesBuilder.build().stream()
                .flatMap(r -> connectionURIs.getOrDefault(r, List.of()).stream())
                .collect(Collectors.toList());
    }

    public void registerTransactionSetup(final String name, final String command) {
        // Note: at the time of writing, this is only called by code that is iterating over a Map from yaml, so it will
        // not prevent two entries in the yaml file itself
        Assert.thatUnchecked(!transactionSetups.containsKey(name), ErrorCode.INTERNAL_ERROR,
                () -> "Transaction setup " + name + " is defined multiple times.");
        transactionSetups.put(name, command);
    }

    public String getTransactionSetup(final Object name) {
        return Matchers.notNull(
                transactionSetups.get(Matchers.string(name, "setup reference")),
                "transaction setup " + name + " is not defined");
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
     * @param throwable the throwable that needs to be wrapped
     * @param msg additional context
     * @param identifier The name of the element type to which the context is associated to.
     * @param reference the {@link YamlReference} of the YAMSQL file to which the context is associated to.
     *
     * @return wrapped {@link YamlExecutionError}
     */
    @Nonnull
    public static RuntimeException wrapContext(@Nonnull Throwable throwable, @Nonnull Supplier<String> msg,
                                               @Nonnull String identifier, @Nonnull final YamlReference reference) {
        if (throwable instanceof TestAbortedException) {
            return (TestAbortedException)throwable;
        } else if (throwable instanceof YamlExecutionError) {
            final var oldStackTrace = throwable.getStackTrace();
            final var newContext = composeStackTrace(reference, identifier);
            final var newStackTrace = new StackTraceElement[newContext.length + 1];
            newStackTrace[0] = oldStackTrace[0];
            System.arraycopy(newContext, 0, newStackTrace, 1, newContext.length);
            throwable.setStackTrace(newStackTrace);
            return (YamlExecutionError)throwable;
        } else {
            // wrap
            final var wrapper = new YamlExecutionError(msg.get(), throwable);
            wrapper.setStackTrace(composeStackTrace(reference, identifier));
            return wrapper;
        }
    }

    private static StackTraceElement[] composeStackTrace(@Nonnull YamlReference reference, @Nonnull String identifier) {
        final var refList = reference.getCallStack();
        return Streams.mapWithIndex(refList.stream(),
                (r, i) -> new StackTraceElement(
                        "YAML_FILE", i == 0 ? identifier : IncludeBlock.INCLUDE,
                        Objects.requireNonNull(r).getResource().getFileName(),
                        r.getLineNumber()))
                .toArray(StackTraceElement[]::new);
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

    /**
     * Loads metrics from a YAML file on disk.
     * This method provides YAML parsing capability for metrics diff analysis.
     *
     * @param filePath the path to the YAML metrics file
     * @return immutable map of identifier to info
     * @throws RelationalException if file cannot be read or parsed
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static Map<PlannerMetricsProto.Identifier, MetricsInfo> loadMetricsFromYamlFile(@Nonnull final Path filePath) throws RelationalException {
        final ImmutableMap.Builder<PlannerMetricsProto.Identifier, MetricsInfo> resultMapBuilder = ImmutableMap.builder();
        final Map<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> seen = new HashMap<>();
        if (!Files.exists(filePath)) {
            return resultMapBuilder.build();
        }

        try {
            final LoaderOptions loaderOptions = new LoaderOptions();
            loaderOptions.setAllowDuplicateKeys(true);
            final var yaml = new Yaml(new CustomYamlConstructor(loaderOptions));
            final var document = yaml.load(new BufferedInputStream(new FileInputStream(filePath.toFile())));

            if (!(document instanceof Map)) {
                return resultMapBuilder.build();
            }

            final var data = (Map<String, List<Map<?, ?>>>) document;

            // Parse each block in the YAML file
            for (final var blockEntry : data.entrySet()) {
                final var blockName = blockEntry.getKey();
                final var queries = blockEntry.getValue();

                if (queries == null) {
                    continue;
                }

                // Process each query in the block
                for (final var queryMap : queries) {
                    processQueryAtLine(queryMap, blockName, seen, resultMapBuilder, filePath);
                }
            }

            return resultMapBuilder.build();
        } catch (final IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Processes a single query with its line number information.
     */
    @SuppressWarnings("unchecked")
    private static void processQueryAtLine(@Nullable Map<?, ?> queryMap,
                                           String blockName,
                                           Map<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> seen,
                                           ImmutableMap.Builder<PlannerMetricsProto.Identifier, MetricsInfo> resultMapBuilder,
                                           Path filePath) {
        if (queryMap == null) {
            return;
        }

        String query = null;
        String explain = null;
        int lineNumber = 1; // Default line number
        for (Map.Entry<?, ?> entry : queryMap.entrySet()) {
            final Object key = entry.getKey();
            if (key instanceof CustomYamlConstructor.LinedObject) {
                CustomYamlConstructor.LinedObject linedObject = (CustomYamlConstructor.LinedObject) key;
                final String keyString = (String) linedObject.getObject();
                if ("query".equals(keyString)) {
                    query = (String) entry.getValue();
                    lineNumber = ((CustomYamlConstructor.LinedObject) key).getLineNumber();
                } else if ("explain".equals(keyString)) {
                    explain = (String) entry.getValue();
                }
            }
        }
        if (query == null) {
            // Query not found
            return;
        }

        // Extract the query string, handling LinedObject if present
        final var setup = (List<String>) queryMap.get("setup");

        // Build identifier
        final var identifierBuilder = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName(blockName)
                .setQuery(query);
        if (setup != null) {
            identifierBuilder.addAllSetups(setup);
        }
        final var identifier = identifierBuilder.build();

        // Build counters and timers
        final var countersAndTimers = PlannerMetricsProto.CountersAndTimers.newBuilder()
                .setTaskCount(getLongValue(queryMap, "task_count"))
                .setTaskTotalTimeNs(TimeUnit.MILLISECONDS.toNanos(getLongValue(queryMap, "task_total_time_ms")))
                .setTransformCount(getLongValue(queryMap, "transform_count"))
                .setTransformTimeNs(TimeUnit.MILLISECONDS.toNanos(getLongValue(queryMap, "transform_time_ms")))
                .setTransformYieldCount(getLongValue(queryMap, "transform_yield_count"))
                .setInsertTimeNs(TimeUnit.MILLISECONDS.toNanos(getLongValue(queryMap, "insert_time_ms")))
                .setInsertNewCount(getLongValue(queryMap, "insert_new_count"))
                .setInsertReusedCount(getLongValue(queryMap, "insert_reused_count"))
                .build();

        // Build info
        final var info = PlannerMetricsProto.Info.newBuilder()
                .setExplain(explain == null ? "" : explain)
                .setCountersAndTimers(countersAndTimers)
                .build();

        // Check for duplicates
        final var oldInfo = seen.get(identifier);
        if (oldInfo == null) {
            seen.put(identifier, info);
            resultMapBuilder.put(identifier, new MetricsInfo(info, filePath, lineNumber));
        } else if (!info.equals(oldInfo)) {
            logger.warn(KeyValueLogMessage.of("Metrics file contains multiple copies of the same query",
                    "file", filePath,
                    "block", identifier.getBlockName(),
                    "query", identifier.getQuery(),
                    "line", lineNumber));
        }
    }

    /**
     * Helper method to safely extract long values from YAML data.
     */
    private static long getLongValue(Map<?, ?> map, String key) {
        final var value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        throw new IllegalArgumentException("Expected numeric value for key: " + key + ", got: " + value);
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

        public static <T1, T2, T3> ContextOptions of(ContextOption<T1> prop1, T1 value1, ContextOption<T2> prop2, T2 value2, ContextOption<T3> prop3, T3 value3) {
            return new ContextOptions(Map.of(prop1, value1, prop2, value2, prop3, value3));
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
