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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of block that serves the purpose of running tests. The block consists of:
 * <ul>
 *     <li>connectPath: the address of the database to connect to.</li>
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
@SuppressWarnings({"PMD.GuardLogStatement"})
public class TestBlock extends Block {

    private static final Logger logger = LogManager.getLogger(TestBlock.class);

    public static final String TEST_BLOCK = "test_block";
    static final String TEST_BLOCK_TESTS = "tests";
    static final String TEST_BLOCK_OPTIONS = "options";
    static final String TEST_BLOCK_PRESET = "preset";
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
    static final String OPTION_FORCE_SIMPLE_STATEMENT = "force_simple_statement";

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

    @Nonnull
    private final List<Consumer<RelationalConnection>> executableTestsWithCacheCheck = new ArrayList<>();

    @Nonnull
    private final TestBlockOptions options = TestBlockOptions.getDefaultTestBlockOptions();

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
    private static class TestBlockOptions {
        private int repetition = 5;
        private ExecutionMode mode = ExecutionMode.PARALLELIZED;
        private long seed = System.currentTimeMillis();
        private boolean checkCache = true;
        private ConnectionLifecycle connectionLifecycle = ConnectionLifecycle.TEST;
        private boolean forceSimpleStatement;

        static TestBlockOptions getDefaultTestBlockOptions() {
            return new TestBlockOptions();
        }

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
            if (preset.equals(PRESET_MULTI_REPETITION_ORDERED) || preset.equals(PRESET_MULTI_REPETITION_RANDOMIZED) || preset.equals(PRESET_MULTI_REPETITION_PARALLELIZED)) {
                repetition = 5;
            } else {
                repetition = 1;
                checkCache = false;
            }
            if (preset.equals(PRESET_MULTI_REPETITION_PARALLELIZED) || preset.equals(PRESET_SINGLE_REPETITION_PARALLELIZED)) {
                mode = ExecutionMode.PARALLELIZED;
            } else if (preset.equals(PRESET_MULTI_REPETITION_RANDOMIZED) || preset.equals(PRESET_SINGLE_REPETITION_RANDOMIZED)) {
                mode = ExecutionMode.RANDOMIZED;
            } else {
                mode = ExecutionMode.ORDERED;
            }
        }

        private void setWithOptionsMap(@Nonnull Map<?, ?> optionsMap) {
            setOptionExecutionModeAndRepetition(optionsMap);
            if (optionsMap.containsKey(OPTION_SEED)) {
                this.seed = Matchers.longValue(optionsMap.get(OPTION_SEED));
            }
            if (optionsMap.containsKey(OPTION_CHECK_CACHE)) {
                this.checkCache = Matchers.bool(optionsMap.get(OPTION_CHECK_CACHE));
            }
            if (optionsMap.containsKey(OPTION_FORCE_SIMPLE_STATEMENT)) {
                this.forceSimpleStatement = Matchers.bool(optionsMap.get(OPTION_FORCE_SIMPLE_STATEMENT));
            }
            setOptionConnectionLifecycle(optionsMap);
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
            return String.format("{mode: %s, repetition: %d, seed: %d, check_cache: %s, connection_lifecycle: %s}", mode, repetition, seed, checkCache, connectionLifecycle);
        }
    }

    TestBlock(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
        super(((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "test_block").getKey()).getStartMark().getLine() + 1, executionContext);
        final var testsMap = Matchers.map(Matchers.firstEntry(document, "test_block").getValue());
        setConnectPath(testsMap.getOrDefault(BLOCK_CONNECT, null));
        // check if the preset is present, if yes, set the options according to it.
        if (testsMap.get(TEST_BLOCK_PRESET) != null) {
            options.setWithPreset(Matchers.string(testsMap.get(TEST_BLOCK_PRESET)));
        }
        // higher priority than the preset is that of options map, set the options according to that, if any is present.
        if (testsMap.get(TEST_BLOCK_OPTIONS) != null) {
            options.setWithOptionsMap(Matchers.map(testsMap.get(TEST_BLOCK_OPTIONS)));
        }
        // execution context carries the highest priority, try setting options per that if it has some options to override.
        options.setWithExecutionContext(executionContext);
        setupTests(Matchers.notNull(testsMap.get(TEST_BLOCK_TESTS), "‼️ tests not found at line " + lineNumber));
        Assert.thatUnchecked(!executables.isEmpty(), "‼️ Test block at line " + lineNumber + " have no tests to execute");
        executionContext.registerBlock(this);
    }

    @Override
    public void execute() {
        logger.info("⚪️ Executing `test` block at line {} with options {}", getLineNumber(), options);
        if (options.mode == ExecutionMode.PARALLELIZED) {
            executeInParallelizedMode(executables);
            executeInNonParallelizedMode(executableTestsWithCacheCheck);
        } else {
            final var allExecutables = new ArrayList<Consumer<RelationalConnection>>();
            allExecutables.addAll(executables);
            allExecutables.addAll(executableTestsWithCacheCheck);
            executeInNonParallelizedMode(allExecutables);
        }
    }

    private void executeInNonParallelizedMode(Collection<Consumer<RelationalConnection>> testsToExecute) {
        if (options.connectionLifecycle == ConnectionLifecycle.BLOCK) {
            // resort to the default implementation of execute.
            executeExecutables(testsToExecute);
        } else if (options.connectionLifecycle == ConnectionLifecycle.TEST) {
            testsToExecute.forEach(t -> {
                if (failureException.get() != null) {
                    logger.trace("⚠️ Aborting test as one of the test has failed.");
                    return;
                }
                connectToDatabaseAndExecute(t);
            });
        }
    }

    private void executeInParallelizedMode(Collection<Consumer<RelationalConnection>> testsToExecute) {
        final var executorService = Executors.newFixedThreadPool(executionContext.getNumThreads());
        testsToExecute.forEach(t -> executorService.submit(() -> executeInNonParallelizedMode(List.of(t))));
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(15, TimeUnit.MINUTES)) {
                failureException.set(new RuntimeException("Parallel executor did not terminate before the 15 minutes timeout."));
            }
        } catch (Exception e) {
            failureException.set(new RuntimeException("Parallel executor did not terminate."));
        }
    }

    private void setupTests(@Nonnull Object testsObject) {
        var randomGenerator = new Random(options.seed);
        executables.clear();
        executableTestsWithCacheCheck.clear();
        final var tests = Matchers.arrayList(testsObject, "tests");
        for (var testObject : tests) {
            final var test = Matchers.arrayList(testObject, "test");
            final var resolvedCommand = Objects.requireNonNull(Command.parse(test));
            Assert.thatUnchecked(resolvedCommand instanceof QueryCommand, "Illegal Format: Test is expected to start with a query.");
            var runAsPreparedMix = getRunAsPreparedMix(options.forceSimpleStatement, options.repetition, randomGenerator);
            for (int i = 0; i < options.repetition; i++) {
                executables.add(createTestExecutable((QueryCommand) resolvedCommand, resolvedCommand.getLineNumber(),
                        false, randomGenerator, runAsPreparedMix.getLeft().get(i)));
            }
            if (options.checkCache) {
                executableTestsWithCacheCheck.add(createTestExecutable((QueryCommand) resolvedCommand, resolvedCommand.getLineNumber(),
                        true, randomGenerator, runAsPreparedMix.getRight()));
            }
        }
        if (options.mode != ExecutionMode.ORDERED) {
            Collections.shuffle(executables, randomGenerator);
        }
    }

    private Pair<List<Boolean>, Boolean> getRunAsPreparedMix(boolean forceFalse, int repetitions, @Nonnull Random random) {
        if (forceFalse) {
            // if false is forced, all the instances should be false
            return Pair.of(Collections.nCopies(repetitions, false), false);
        } else if (repetitions == 1) {
            // _all_ the instances (1 test and cache check) should be the same
            var x = random.nextBoolean();
            return Pair.of(List.of(x), x);
        } else {
            // try to get a mix of both
            while (true) {
                var mix = IntStream.range(0, repetitions).mapToObj(ignore -> random.nextBoolean()).collect(Collectors.toList());
                if (mix.contains(true) && mix.contains(false)) {
                    return Pair.of(mix, random.nextBoolean());
                }
            }
        }
    }

    private Consumer<RelationalConnection> createTestExecutable(QueryCommand queryCommand, int lineNumber, boolean checkCache,
                                                              @Nonnull Random random, boolean runAsPreparedStatement) {
        return connection -> {
            try {
                queryCommand.invoke(connection, executionContext, checkCache, random, runAsPreparedStatement);
            } catch (SQLException e) {
                failureException.set(new RuntimeException(String.format("‼️ Cannot run query at line %d", lineNumber), e));
            } catch (RelationalException e) {
                failureException.set(new RuntimeException(String.format("‼️ Error executing test at line %d", lineNumber), e));
            } catch (TestAbortedException e) {
                logger.debug("⚠️ Aborting test as the assumption are not fulfilled: " + e.getMessage());
            } catch (AssertionFailedError | Exception e) {
                failureException.set(new RuntimeException(String.format("‼️ Test failed at line %d", lineNumber), e));
            }
        };
    }

    @Override
    public Optional<RuntimeException> getFailureExceptionIfPresent() {
        final var failure = super.getFailureExceptionIfPresent();
        return failure.map(throwable -> new RuntimeException("‼️ Test Block failed at line " + getLineNumber() + " with option " + options, throwable));
    }
}
