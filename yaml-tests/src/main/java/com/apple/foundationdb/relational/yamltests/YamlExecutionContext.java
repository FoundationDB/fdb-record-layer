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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Streams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.TestAbortedException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlExecutionContext {
    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    /**
     * List of metrics field names that are tracked for planner comparison.
     * These are the core metrics that should be consistent between runs, excluding timing
     * information which can vary.
     */
    public static final List<String> TRACKED_METRIC_FIELDS = List.of(
            "task_count",
            "transform_count",
            "transform_yield_count",
            "insert_new_count",
            "insert_reused_count"
    );

    public static final ContextOption<Boolean> OPTION_FORCE_CONTINUATIONS = new ContextOption<>("optionForceContinuation");
    public static final ContextOption<Boolean> OPTION_CORRECT_EXPLAIN = new ContextOption<>("optionCorrectExplain");
    public static final ContextOption<Boolean> OPTION_CORRECT_METRICS = new ContextOption<>("optionCorrectMetrics");
    public static final ContextOption<Boolean> OPTION_SHOW_PLAN_ON_DIFF = new ContextOption<>("optionShowPlanOnDiff");

    private static final URI SYSTEM_CATALOG_ADDRESS = URI.create("jdbc:embed:/__SYS?schema=CATALOG");

    @Nonnull final YamlReference.YamlResource topLevelResource;
    @Nonnull
    private final Set<YamlReference.YamlResource> registeredResources = new HashSet<>();
    @Nonnull
    private final Map<YamlReference.YamlResource, List<String>> editedFileStream = new HashMap<>();
    @Nonnull
    private final Map<YamlReference.YamlResource, Boolean> isDirty = new HashMap<>();
    @Nullable
    private ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> expectedMetricsMap;
    @Nonnull
    private Map<QueryAndLocation, PlannerMetricsProto.Info> actualMetricsMap = new HashMap<>();
    private volatile boolean isDirtyMetrics = false;
    @Nonnull
    private final YamlConnectionFactory connectionFactory;
    @SuppressWarnings("AbbreviationAsWordInName")
    private final Map<YamlReference.YamlResource, List<String>> connectionURIs = new HashMap<>();
    // Additional options that can be set by the runners to impact test execution
    private final ContextOptions additionalOptions;
    private final Map<String, String> transactionSetups = new HashMap<>();
    @Nonnull
    private Options connectionOptions = Options.NONE;

        public static class YamlExecutionError extends RuntimeException {

        private static final long serialVersionUID = 10L;

        YamlExecutionError(String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    YamlExecutionContext(@Nonnull YamlReference.YamlResource topLevelResource, @Nonnull YamlConnectionFactory factory, @Nonnull final ContextOptions additionalOptions) {
        this.connectionFactory = factory;
        this.topLevelResource = topLevelResource;
        this.additionalOptions = additionalOptions;
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

    public void registerResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        if (registeredResources.contains(resource)) {
            throw new RuntimeException("The resource " + resource + " is already registered.");
        }
        if (shouldCorrectExplains()) {
            this.editedFileStream.put(resource, loadYamlResource(resource));
        }
        if (this.expectedMetricsMap == null) {
            this.expectedMetricsMap = loadMetricsResource(resource);
            this.actualMetricsMap = new TreeMap<>(Comparator.comparing(QueryAndLocation::getReference)
                    .thenComparing(QueryAndLocation::getBlockName)
                    .thenComparing(QueryAndLocation::getQuery));
        }
        registeredResources.add(resource);
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

    public boolean correctExplain(@Nonnull final YamlReference reference, @Nonnull String actual) {
        if (!shouldCorrectExplains()) {
            return false;
        }
        try {
            editedFileStream.get(reference.getResource()).set(reference.getLineNumber() - 1, "      - explain: \"" + actual + "\"");
            isDirty.put(reference.getResource(), true);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Nullable
    public ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> getMetricsMap() {
        return expectedMetricsMap;
    }

    @Nullable
    @SuppressWarnings("UnusedReturnValue")
    public synchronized PlannerMetricsProto.Info putMetrics(@Nonnull final String blockName,
                                                            @Nonnull final String query,
                                                            @Nonnull final YamlReference reference,
                                                            @Nonnull final PlannerMetricsProto.Info info,
                                                            @Nonnull final List<String> setups) {
        return actualMetricsMap.put(new QueryAndLocation(blockName, query, reference, setups), info);
    }

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

    public void replaceFilesIfRequired() {
        final var filePathsWithResourceCount = registeredResources.stream()
                .map(YamlReference.YamlResource::getPath)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (final var resource: registeredResources) {
            if (!isDirty.getOrDefault(resource, false)) {
                continue;
            }
            // There could arise a common scenario where a YAMSQL file is "opened" as 2 separate resource, coming from
            // different call stacks. If this file has an EXPLAIN, and warrants correction, that will be a problem if,
            // for the 2 resources pointing to same file, there is some disagreement on the values. Ideally this should
            // not happen however, I believe it's still a possibility, mainly with metrics, to be highly sensitive to
            // the environment in which the query is running. Because of this reason, just fail if we found resources
            // that are marked as dirty and pointing to the same file as any other (dirty or non-dirty) resource.
            if (filePathsWithResourceCount.getOrDefault(resource.getPath(), 0L) > 1) {
                Assertions.fail("Found duplicate entries for writing to file: " + resource.getPath());
            }
            saveYamlFile(resource);
        }
        if (!isDirtyMetrics) {
            return;
        }
        saveMetricsAsBinaryProto();
        saveMetricsAsYaml();
    }

    private void saveYamlFile(@Nonnull final YamlReference.YamlResource resource) {
        try {
            try (var writer = new PrintWriter(new FileWriter(Path.of(System.getProperty("user.dir")).resolve(Path.of("src", "test", "resources", resource.getPath())).toAbsolutePath().toString(), StandardCharsets.UTF_8))) {
                for (var line : editedFileStream.get(resource)) {
                    writer.println(line);
                }
            }
            logger.info("🟢 Source file {} replaced.", resource.getPath());
        } catch (IOException e) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", resource.getPath());
            Assertions.fail(e);
        }
    }

    public void registerConnectionURI(@Nonnull YamlReference.YamlResource resource, @Nonnull String stringURI) {
        Assert.thatUnchecked(registeredResources.contains(resource), "A YamlResource should be registered before registering available connection URIs");
        connectionURIs.computeIfAbsent(resource, ignore -> new ArrayList<>()).add(stringURI);
    }

    /**
     * Infers the URI of the database to which a block should connect to.
     * <br>
     * A block can declare a connection in multiple ways:
     * 1. no explicit declaration: Try to connect to the only registered connection URI in the local {@link YamlReference.YamlResource}.
     *    If not, try to connect to the only connection across all parent resources.
     *    A URI can be registered by defining a "schema_template" block before that, which sets up the database and schema for a provided schema template.
     * 2. Parameter 0: connects to the system tables (catalog).
     * 3. Parameter One-based Number: connects to the registered connection URI, number denotes the sequence of definitions in the local YamlResource.
     *    To access parent connection URIs, this number should be prepended by `(global)` tag.
     * 4. Parameter String: connects to the defined String
     *
     * @param connectObject can be {@code null}, an {@link Integer} value or a {@link String}.
     *
     * @return a valid connection URI
     */
    public URI inferConnectionURI(@Nonnull final YamlReference.YamlResource resource, @Nullable Object connectObject) {
        Assert.thatUnchecked(registeredResources.contains(resource), "A YamlResource should be registered before registering available connection URIs");
        if (connectObject == null) {
            return getConnectionFromConnectionURIList(resource, true, -1, true);
        } else if (connectObject instanceof Integer) {
            return getConnectionFromConnectionURIList(resource, false, (Integer) connectObject, false);
        } else {
            final var stringURI = Matchers.string(connectObject);
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
        resource.getParentRef().getCallStack().reverse();
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

    private void saveMetricsAsBinaryProto() {
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
            final var identifier = queryAndLocation.getIdentifier();
            if (condensedMetricsMap.containsKey(identifier)) {
                logger.warn("⚠️ Repeated query in block {} at {}", queryAndLocation.getBlockName(),
                        queryAndLocation.getReference());
            } else {
                condensedMetricsMap.put(identifier,
                        entry.getValue());
            }
        }

        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsBinaryProtoFileName(topLevelResource.getPath())))
                .toAbsolutePath().toString();
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

    private void saveMetricsAsYaml() {
        final var mmap = LinkedListMultimap.<String, Map<String, Object>>create();
        for (final var entry : actualMetricsMap.entrySet()) {
            final var identifier = entry.getKey().getIdentifier();
            final var info = entry.getValue();
            final var countersAndTimers = info.getCountersAndTimers();
            final var infoMap = new LinkedHashMap<String, Object>();
            infoMap.put("query", identifier.getQuery());
            // only include setup if it is non-empty, in part so that the PR that adds setup doesn't change every
            // metric in the yaml files
            if (identifier.getSetupsCount() > 0) {
                infoMap.put("setup", identifier.getSetupsList());
            }
            infoMap.put("ref", entry.getKey().getReference().toString());
            infoMap.put("explain", info.getExplain());
            infoMap.put("task_count", countersAndTimers.getTaskCount());
            infoMap.put("task_total_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTaskTotalTimeNs()));
            infoMap.put("transform_count", countersAndTimers.getTransformCount());
            infoMap.put("transform_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTransformTimeNs()));
            infoMap.put("transform_yield_count", countersAndTimers.getTransformYieldCount());
            infoMap.put("insert_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getInsertTimeNs()));
            infoMap.put("insert_new_count", countersAndTimers.getInsertNewCount());
            infoMap.put("insert_reused_count", countersAndTimers.getInsertReusedCount());
            mmap.put(identifier.getBlockName(), infoMap);
        }

        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsYamlFileName(topLevelResource.getPath())))
                .toAbsolutePath().toString();
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
    private static List<String> loadYamlResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final List<String> inMemoryFile = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(
                             new InputStreamReader(Objects.requireNonNull(classLoader.getResourceAsStream(resource.getPath())),
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
    private static ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> loadMetricsResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final var fis = classLoader.getResourceAsStream(metricsBinaryProtoFileName(resource.getPath()));
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
        private final PlannerMetricsProto.Identifier identifier;
        @Nonnull
        private final YamlReference reference;

        public QueryAndLocation(@Nonnull final String blockName, final String query, @Nonnull final YamlReference reference,
                                @Nonnull List<String> setups) {
            identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName(blockName)
                    .setQuery(query)
                    .addAllSetups(setups)
                    .build();
            this.reference = reference;
        }

        @Nonnull
        public PlannerMetricsProto.Identifier getIdentifier() {
            return identifier;
        }

        @Nonnull
        public String getBlockName() {
            return identifier.getBlockName();
        }

        @Nonnull
        public String getQuery() {
            return identifier.getQuery();
        }

        @Nonnull
        public YamlReference getReference() {
            return reference;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof QueryAndLocation)) {
                return false;
            }
            final QueryAndLocation that = (QueryAndLocation)o;
            return reference.equals(that.reference) && Objects.equals(identifier, that.identifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identifier, reference);
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
