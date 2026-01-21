/*
 * TestBlock.java
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamsqlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import com.apple.foundationdb.relational.yamltests.command.SkippedCommand;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.relational.yamltests.server.SupportedVersionCheck;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of block that serves the purpose of running tests. The block consists of:
 * <ul>
 *     <li>connectionURI: the address of the database to connect to.</li>
 *     <li>options: set of control knobs that determines how the set of tests will be executed.</li>
 *     <li>tests: the set of queries along with the configurations of how and against what they are tested. Each
 *     test translates to a {@link QueryCommand} with one or more {@link QueryConfig}.</li>
 * </ul>
 * <p>
 * There a couple of {@link TestBlock} options that controls how the tests in the block is executed. This in turn
 * determines what is tested in each test and how rigorously. These options are:
 * <ul>
 *     <li>{@code mode}: By far the most important option, mode can be either {@code ordered} or {@code randomized}
 *     to tell the block if the listed tests should be run randomly or not.</li>
 *     <li>{@code repetition}: Number of times each test should be run.</li>
 *     <li>{@code seed}: Seed with which to introduce randomness to the tests in the block.</li>
 *     <li>{@code check_cache}: Whether the test should check explicitly that the query plan has been retrieved
 *     from the cache</li>
 *     <li>{@code connection_lifecycle}: Whether to use a new connection for each test or using one throughout.</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"})
public final class TestBlock extends ConnectedBlock {

    private static final Logger logger = LogManager.getLogger(TestBlock.class);

    public static final String TEST_BLOCK = "test_block";
    static final String TEST_BLOCK_TESTS = "tests";
    static final String TEST_BLOCK_OPTIONS = "options";
    static final String TEST_BLOCK_PRESET = "preset";
    static final String TEST_BLOCK_NAME = "name";
    static final String PRESET_SINGLE_REPETITION_ORDERED = "single_repetition_ordered";
    static final String PRESET_SINGLE_REPETITION_RANDOMIZED = "single_repetition_randomized";
    static final String PRESET_SINGLE_REPETITION_PARALLELIZED = "single_repetition_parallelized";
    static final String PRESET_MULTI_REPETITION_ORDERED = "multi_repetition_ordered";
    static final String PRESET_MULTI_REPETITION_RANDOMIZED = "multi_repetition_randomized";
    static final String PRESET_MULTI_REPETITION_PARALLELIZED = "multi_repetition_parallelized";
    static final String OPTION_EXECUTION_MODE = "mode";
    static final String OPTION_EXECUTION_MODE_ORDERED = "ordered";
    static final String OPTION_EXECUTION_MODE_RANDOMIZED = "randomized";
    static final String OPTION_EXECUTION_MODE_PARALLELIZED = "parallelized";
    static final String OPTION_REPETITION = "repetition";
    static final String OPTION_SEED = "seed";
    static final String OPTION_CHECK_CACHE = "check_cache";
    static final String OPTION_CONNECTION_LIFECYCLE = "connection_lifecycle";
    static final String OPTION_CONNECTION_LIFECYCLE_TEST = "test";
    static final String OPTION_CONNECTION_LIFECYCLE_BLOCK = "block";
    static final String OPTION_CONNECTION_OPTIONS = "connection_options";
    static final String OPTION_STATEMENT_TYPE = "statement_type";
    static final String OPTION_STATEMENT_TYPE_SIMPLE = "simple";
    static final String OPTION_STATEMENT_TYPE_PREPARED = "prepared";

    /**
     * Defines the way in which the tests are run.
     */
    enum ExecutionMode {
        /**
         * Tests are ordered randomly using the seed using {@link Collections#shuffle}. If the repetition is more than
         * 1, the tests are listed sequentially as {@code 1, 1, 1, 2, 2, 2, .... n, n, n} and then shuffled.
         */
        RANDOMIZED,
        /**
         * The order of execution of test is the same as the order they are written in.
         */
        ORDERED,
        /**
         * Tests are ordered is {@link ExecutionMode#RANDOMIZED} but executed using an {@link java.util.concurrent.ExecutorService}.
         */
        PARALLELIZED
    }

    /**
     * Defines how and when the connection is created in order to run a particular instance of test.
     */
    enum ConnectionLifecycle {
        /**
         * Runs each test in a new connection. This means that a connection is created, a test is run and then the
         * connection is closed. Even if a particular test runs n times, each would run with a new connection. Note that
         * a test may have multiple {@link QueryConfig}s which are run within the same connection instance.
         */
        TEST,
        /**
         * Creates a single connection at the beginning of execution, and runs all the tests in it.
         */
        BLOCK
    }

    /**
     * Defines what API to use to issue query.
     */
    enum StatementType {
        BOTH,
        SIMPLE,
        PREPARED
    }

    @Nonnull
    final String blockName;
    @Nonnull
    private final List<Consumer<YamlConnection>> executableTestsWithCacheCheck;
    @Nonnull
    private final TestBlockOptions options;
    @Nonnull
    private final List<QueryCommand> queryCommands;
    @Nullable
    private RuntimeException maybeFailureException;

    /**
     * The set of options that defines how the tests are executed. The following options are supported currently:
     * <ul>
     *     <li>repetition: Defines the number of times a particular test should be executed. Default is 5. Note that
     *     if the `check_cache` is set to {@code true}, which is default, there is an extra repetition.</li>
     *     <li>mode: Defines how the set of tests are run. See {@link ExecutionMode}.</li>
     *     <li>seed: Seed value to be used while "randomizing" or "parallelizing" the list of tests. Default is
     *     {@link System#currentTimeMillis()}.</li>
     *     <li>checkCache: Whether the particular query should be tested against the System cache. If set to
     *     {@code true}, there is an extra run of the test that explicitly tests if the plan is retrieved from the
     *     cache, and the retrieved plan produces the correct results.</li>
     *     <li>connectionLifecycle: Defines how and when the connection is created to execute tests. See
     *     {@link ConnectionLifecycle}.</li>
     * </ul>
     *
     * The {@link TestBlockOptions} can be set in three ways:
     * <ul>
     *     <li>Using a preset config. See {@link TestBlockOptions#setWithPreset}</li>
     *     <li>Using an Options map. See {@link TestBlockOptions#setWithOptionsMap}</li>
     *     <li>Using the {@link YamlExecutionContext}. See {@link TestBlockOptions#setWithExecutionContext}</li>
     * </ul>
     */
    public static class TestBlockOptions {
        private int repetition = 5;
        private ExecutionMode mode = ExecutionMode.PARALLELIZED;
        private long seed = System.currentTimeMillis();
        private boolean checkCache = true;
        private ConnectionLifecycle connectionLifecycle = ConnectionLifecycle.TEST;
        private StatementType statementType = StatementType.BOTH;
        private SupportedVersionCheck supportedVersionCheck = SupportedVersionCheck.supported();
        private Options connectionOptions = Options.none();

        private void verifyPreset(@Nonnull String preset) {
            switch (preset) {
                case PRESET_MULTI_REPETITION_ORDERED:
                case PRESET_MULTI_REPETITION_RANDOMIZED:
                case PRESET_MULTI_REPETITION_PARALLELIZED:
                case PRESET_SINGLE_REPETITION_ORDERED:
                case PRESET_SINGLE_REPETITION_RANDOMIZED:
                case PRESET_SINGLE_REPETITION_PARALLELIZED:
                    break;
                default:
                    Assert.failUnchecked("Illegal Format: Unknown value for preset: " + preset);
                    break;
            }
        }

        private void setWithPreset(@Nonnull String preset) {
            verifyPreset(preset);
            if (PRESET_MULTI_REPETITION_ORDERED.equals(preset) || PRESET_MULTI_REPETITION_RANDOMIZED.equals(preset) || PRESET_MULTI_REPETITION_PARALLELIZED.equals(preset)) {
                repetition = 5;
            } else {
                repetition = 1;
                checkCache = false;
            }
            if (PRESET_MULTI_REPETITION_PARALLELIZED.equals(preset) || PRESET_SINGLE_REPETITION_PARALLELIZED.equals(preset)) {
                mode = ExecutionMode.PARALLELIZED;
            } else if (PRESET_MULTI_REPETITION_RANDOMIZED.equals(preset) || PRESET_SINGLE_REPETITION_RANDOMIZED.equals(preset)) {
                mode = ExecutionMode.RANDOMIZED;
            } else {
                mode = ExecutionMode.ORDERED;
            }
        }

        private void setWithOptionsMap(@Nonnull Map<?, ?> optionsMap, Set<SemanticVersion> versionsUnderTest) {
            setOptionExecutionModeAndRepetition(optionsMap);
            if (optionsMap.containsKey(OPTION_SEED)) {
                this.seed = Matchers.longValue(optionsMap.get(OPTION_SEED));
            }
            if (optionsMap.containsKey(OPTION_CHECK_CACHE)) {
                this.checkCache = Matchers.bool(optionsMap.get(OPTION_CHECK_CACHE));
            }
            if (optionsMap.containsKey(OPTION_STATEMENT_TYPE)) {
                final var value = Matchers.string(optionsMap.get(OPTION_STATEMENT_TYPE));
                switch (value) {
                    case OPTION_STATEMENT_TYPE_PREPARED:
                        this.statementType = StatementType.PREPARED;
                        break;
                    case OPTION_STATEMENT_TYPE_SIMPLE:
                        this.statementType = StatementType.SIMPLE;
                        break;
                    default:
                        Assert.failUnchecked("Illegal Format: Unknown value for option `statement_type`: " + value);
                        break;
                }
            }
            supportedVersionCheck = SupportedVersionCheck.parseOptions(optionsMap, versionsUnderTest);
            setOptionConnectionLifecycle(optionsMap);
            if (optionsMap.containsKey(OPTION_CONNECTION_OPTIONS)) {
                connectionOptions = parseConnectionOptions(Matchers.map(optionsMap.get(OPTION_CONNECTION_OPTIONS)));
            }
        }

        @Nonnull
        public static Options parseConnectionOptions(@Nonnull final Map<?, ?> map) {
            final var optionsBuilder = Options.builder();
            for (final var entry : map.entrySet()) {
                try {
                    optionsBuilder.withOption(Options.Name.valueOf(Matchers.string(entry.getKey())), entry.getValue());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return optionsBuilder.build();
        }

        private void setWithExecutionContext(@Nonnull YamlExecutionContext executionContext) {
            // Use the system-provided seed if that is available from the context.
            executionContext.getSeed().ifPresent(s -> seed = Matchers.longValue(s));
            if (executionContext.isNightly()) {
                // If the test is for nightly, nightlyRepetition is provided and the repetition set is not 1, then use
                // the nightlyRepetition value. We explicitly check for the provided repetition to not being 1 because
                // a repetition of 1 means that the tests are non-idempotent.
                if (repetition != 1) {
                    executionContext.getNightlyRepetition().ifPresent(s -> repetition = Matchers.intValue(s));
                }
            }
        }

        private void setOptionExecutionModeAndRepetition(Map<?, ?> optionsMap) {
            if (optionsMap.containsKey(OPTION_EXECUTION_MODE)) {
                final var value = Matchers.string(optionsMap.get(OPTION_EXECUTION_MODE));
                switch (value) {
                    case OPTION_EXECUTION_MODE_ORDERED:
                        this.mode = ExecutionMode.ORDERED;
                        break;
                    case OPTION_EXECUTION_MODE_RANDOMIZED:
                        this.mode = ExecutionMode.RANDOMIZED;
                        break;
                    case OPTION_EXECUTION_MODE_PARALLELIZED:
                        this.mode = ExecutionMode.PARALLELIZED;
                        break;
                    default:
                        Assert.failUnchecked("Illegal Format: Unknown value for option mode: " + value);
                        break;
                }
            }
            if (optionsMap.containsKey(OPTION_REPETITION)) {
                this.repetition = Matchers.intValue(optionsMap.get(OPTION_REPETITION));
            }
            Assert.thatUnchecked(repetition > 0, "Illegal repetition value provided. Should be greater than equal to 1");
        }

        private void setOptionConnectionLifecycle(Map<?, ?> optionsMap) {
            if (optionsMap.containsKey(OPTION_CONNECTION_LIFECYCLE)) {
                final var value = Matchers.string(optionsMap.get(OPTION_CONNECTION_LIFECYCLE));
                switch (value) {
                    case OPTION_CONNECTION_LIFECYCLE_TEST:
                        this.connectionLifecycle = ConnectionLifecycle.TEST;
                        break;
                    case OPTION_CONNECTION_LIFECYCLE_BLOCK:
                        this.connectionLifecycle = ConnectionLifecycle.BLOCK;
                        break;
                    default:
                        Assert.failUnchecked("Illegal Format: Unknown value for option mode: " + value);
                        break;
                }
            }
        }

        @Override
        public String toString() {
            return "mode: " + mode + ", repetition: " + repetition + ", seed: " + seed + ", check_cache: " + checkCache + ", connection_lifecycle: " + connectionLifecycle + ", statement_type: " + statementType;
        }
    }

    public static ImmutableList<Block> parse(int blockNumber, @Nonnull final YamsqlReference reference, @Nonnull final Object document,
                                             @Nonnull final YamlExecutionContext executionContext) {
        try {
            // Since `options` is also a top-level block, the `CustomYamlConstructor` will add the line numbers,
            // changing it from a `String` to a `LinedObject` so that we know the line numbers when logging an error,
            // but that makes it hard to look it up in the map. The call to `unlineKeys` changes it back to a String.
            // There might be a way to have it on the value, but I couldn't find it.
            // The other option would be to allow `Block` to not have a line number, and make `options` not have a line
            // number.
            final var testsMap = CustomYamlConstructor.LinedObject.unlineKeys(
                    Matchers.map(document, "test_block"));
            final var options = new TestBlockOptions();
            // check if the preset is present, if yes, set the options according to it.
            if (testsMap.get(TEST_BLOCK_PRESET) != null) {
                options.setWithPreset(Matchers.string(testsMap.get(TEST_BLOCK_PRESET)));
            }
            // higher priority than the preset is that of options map, set the options according to that, if any is present.
            if (testsMap.get(TEST_BLOCK_OPTIONS) != null) {
                options.setWithOptionsMap(CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(testsMap.get(TEST_BLOCK_OPTIONS))),
                        executionContext.getConnectionFactory().getVersionsUnderTest());
            }
            // execution context carries the highest priority, try setting options per that if it has some options to override.
            options.setWithExecutionContext(executionContext);

            final String blockName = testsMap.containsKey(TEST_BLOCK_NAME)
                                     ? Matchers.string(testsMap.get(TEST_BLOCK_NAME)) : "unnamed-" + blockNumber;
            final var testsObject = Matchers.notNull(testsMap.get(TEST_BLOCK_TESTS), "‼️ tests not found at " + reference);
            if (!options.supportedVersionCheck.isSupported()) {
                return ImmutableList.of(new SkipBlock(reference, options.supportedVersionCheck.getMessage()));
            }
            var randomGenerator = new Random(options.seed);
            final var executables = new ArrayList<Consumer<YamlConnection>>();
            final var executableTestsWithCacheCheck = new ArrayList<Consumer<YamlConnection>>();
            final var queryCommands = new ArrayList<QueryCommand>();
            final var tests = Matchers.arrayList(testsObject, "tests");
            for (var testObject : tests) {
                final var test = Matchers.arrayList(testObject, "test");
                final var resolvedCommand = Objects.requireNonNull(Command.parse(reference.getResource(), test, blockName, executionContext));
                if (resolvedCommand instanceof SkippedCommand) {
                    ((SkippedCommand)resolvedCommand).log();
                    continue;
                }
                Assert.thatUnchecked(resolvedCommand instanceof QueryCommand, "Illegal Format: Test is expected to start with a query.");
                final QueryCommand queryCommand = (QueryCommand)resolvedCommand;
                if (queryCommand.isNeedsSerialEnvironment()) {
                    options.mode = ExecutionMode.ORDERED;
                    options.repetition = 1;
                }

                queryCommands.add(queryCommand);
                var runAsPreparedMix = getRunAsPreparedMix(options.statementType, options.repetition, randomGenerator);
                for (int i = 0; i < options.repetition; i++) {
                    executables.add(createTestExecutable(queryCommand, false, randomGenerator,
                            runAsPreparedMix.getLeft().get(i), options.connectionOptions));
                }
                if (options.checkCache) {
                    executableTestsWithCacheCheck.add(createTestExecutable(queryCommand, true,
                            randomGenerator, runAsPreparedMix.getRight(), options.connectionOptions));
                }
            }
            if (options.mode != ExecutionMode.ORDERED) {
                Collections.shuffle(executables, randomGenerator);
                Collections.shuffle(executableTestsWithCacheCheck, randomGenerator);
            }

            Assert.thatUnchecked(!executables.isEmpty(), "‼️ Test block at " + reference + " have no tests to execute");
            return ImmutableList.of(new TestBlock(reference, blockName, queryCommands, executables, executableTestsWithCacheCheck,
                    executionContext.inferConnectionURI(reference.getResource(), testsMap.getOrDefault(BLOCK_CONNECT, null)), options, executionContext));
        } catch (Throwable e) {
            throw executionContext.wrapContext(e, () -> "‼️ Error parsing the test block at " + reference, TEST_BLOCK, reference);
        }
    }

    private TestBlock(@Nonnull final YamsqlReference reference, @Nonnull final String blockName, @Nonnull final List<QueryCommand> queryCommands,
                      @Nonnull final List<Consumer<YamlConnection>> executables,
                      @Nonnull final List<Consumer<YamlConnection>> executableTestsWithCacheCheck, @Nonnull final URI connectionURI,
                      @Nonnull final TestBlockOptions options, @Nonnull final YamlExecutionContext executionContext) {
        super(reference, executables, connectionURI, executionContext);
        this.blockName = blockName;
        this.queryCommands = queryCommands;
        this.options = options;
        this.executableTestsWithCacheCheck = executableTestsWithCacheCheck;
    }

    @Override
    public void execute() {
        logger.info("⚪️ Executing `test` block at {} with options {}", getReference(), options);
        try {
            if (options.mode == ExecutionMode.PARALLELIZED) {
                executeInParallelizedMode(executables);
                executeInParallelizedMode(executableTestsWithCacheCheck);
            } else {
                final var allExecutables = new ArrayList<>(executables);
                allExecutables.addAll(executableTestsWithCacheCheck);
                executeInNonParallelizedMode(allExecutables);
            }
            // Check for the caught exceptions in each of the QueryCommands.
            queryCommands.stream().map(QueryCommand::getMaybeExecutionThrowable).filter(Objects::nonNull).findFirst().ifPresent(
                    e -> {
                        final Throwable unwrappedException = unwrapExecutionExceptionIfNeeded(e);
                        maybeFailureException = executionContext.wrapContext(unwrappedException,
                                () -> "‼️ Some failed/unsuccessful test in test block at line " + getReference() + ". Options: " + options,
                                TEST_BLOCK + " [" + options + "] ", getReference());
                    });
        } catch (TestAbortedException tAE) {
            throw tAE;
        } catch (Throwable e) {
            final Throwable unwrappedException = unwrapExecutionExceptionIfNeeded(e);
            if (unwrappedException instanceof TestAbortedException) {
                throw (TestAbortedException)unwrappedException;
            }
            maybeFailureException = executionContext.wrapContext(unwrappedException,
                    () -> "‼️ Failed to execute test block at line " + getReference() + ". Options: " + options,
                    TEST_BLOCK + " [" + options + "] ", getReference());
        }
        executables.clear();
        executableTestsWithCacheCheck.clear();
    }

    @Nonnull
    private Throwable unwrapExecutionExceptionIfNeeded(@Nonnull final Throwable t) {
        if (t instanceof ExecutionException) {
            if (t.getCause() != null) {
                return t.getCause();
            }
        }
        return t;
    }

    @Nonnull
    public Optional<RuntimeException> getFailureExceptionIfPresent() {
        return maybeFailureException == null ? Optional.empty() : Optional.of(maybeFailureException);
    }

    private void executeInNonParallelizedMode(Collection<Consumer<YamlConnection>> testsToExecute) {
        if (options.connectionLifecycle == ConnectionLifecycle.BLOCK) {
            // resort to the default implementation of execute.
            executeExecutables(testsToExecute);
        } else if (options.connectionLifecycle == ConnectionLifecycle.TEST) {
            testsToExecute.forEach(this::connectToDatabaseAndExecute);
        }
    }

    /**
     * Runs the collection of executables using a fixed thread pool {@link java.util.concurrent.ExecutorService}, on a
     * thread that is not the main current, possibly multiple threads.
     *
     * @param testsToExecute collection of executables to execute.
     * @throws InterruptedException thrown if the execution does not finish within a time-bound.
     * @throws ExecutionException thrown if any the executable tasks complete exceptionally.
     */
    private void executeInParallelizedMode(Collection<Consumer<YamlConnection>> testsToExecute) throws InterruptedException, ExecutionException {
        final var executorService = Executors.newFixedThreadPool(executionContext.getNumThreads());
        final var futures = testsToExecute.stream().map(t -> executorService.submit(() -> executeInNonParallelizedMode(List.of(t)))).collect(Collectors.toList());
        executorService.shutdown();
        if (!executorService.awaitTermination(15, TimeUnit.MINUTES)) {
            throw new InterruptedException("Parallel executor did not terminate before the 15 minutes timeout.");
        }
        // Iterate through the futures to catch any uncaught errors/exceptions from the submitted tasks.
        for (var future : futures) {
            Verify.verify(!future.isCancelled());
            future.get();
        }
    }

    private static Pair<List<Boolean>, Boolean> getRunAsPreparedMix(StatementType type, int repetitions, @Nonnull Random random) {
        if (type == StatementType.SIMPLE) {
            return Pair.of(Collections.nCopies(repetitions, false), false);
        }
        if (type == StatementType.PREPARED) {
            return Pair.of(Collections.nCopies(repetitions, true), true);
        }
        // If there is only 1 repetition, _all_ the instances (1 test and cache check) should either run as a simple
        // statement or a prepared statement
        if (repetitions == 1) {
            var x = random.nextBoolean();
            return Pair.of(List.of(x), x);
        }
        // Get a mix of both
        while (true) {
            var mix = IntStream.range(0, repetitions).mapToObj(ignore -> random.nextBoolean()).collect(Collectors.toList());
            if (mix.contains(true) && mix.contains(false)) {
                return Pair.of(mix, random.nextBoolean());
            }
        }
    }

    @Nonnull
    private static Consumer<YamlConnection> createTestExecutable(QueryCommand queryCommand, boolean checkCache,
                                                                 @Nonnull Random random, boolean runAsPreparedStatement,
                                                                 @Nonnull Options connectionOptions) {
        final var executor = queryCommand.instantiateExecutor(random, runAsPreparedStatement);
        return connection -> {
            try {
                connection.setConnectionOptions(connectionOptions);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            queryCommand.execute(connection, checkCache, executor);
        };
    }
}
