/*
 * Block.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Block is a single region in the YAMSQL file that can either be a {@link ConfigBlock} or {@link TestBlock}.
 * <ul>
 *      <li> {@link ConfigBlock}: It can be either a `setup` block or a `destruct` block. The motive of these block
 *          is to "setup" and "clean" the environment needed to run the `test-block`s. A Config block consist of a list
 *          of commands.</li>
 *      <li> {@link TestBlock}: Defines a scope for a group of tests by setting the knobs that determines how those
 *          tests are run.</li>
 * </ul>
 * <p>
 * Each block needs to be associated with a `connectPath` which provides it with the address to the database to which
 * the block connects to for running its executables. The block does so through the
 * {@link com.apple.foundationdb.relational.yamltests.YamlRunner.YamlConnectionFactory}. A block is free to implement `how` and `when`
 * it wants to use the factory to create a connection and also manages the lifecycle of the established connection.
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public abstract class Block {

    private static final Logger logger = LogManager.getLogger(Block.class);

    static final String BLOCK_CONNECT = "connect";

    int lineNumber;
    @Nonnull
    YamlRunner.YamlExecutionContext executionContext;
    @Nullable
    Throwable throwable;
    @Nullable
    URI connectPath;

    Block(int lineNumber, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
    }

    int getLineNumber() {
        return lineNumber;
    }

    /**
     * Executes the parsed block.
     */
    public abstract void execute();

    /**
     * Returns the throwable, if the block do not execute normally.
     *
     * @return An error or exception that might have been encountered while executing the block.
     */
    @Nonnull
    public final Optional<Throwable> getThrowableIfExists() {
        return Optional.ofNullable(this.throwable);
    }

    /**
     * Sets the path of the database to which the block should connect to.
     *
     * @param connectObject the connection path.
     */
    void setConnectPath(Object connectObject) {
        if (connectObject == null) {
            Assert.failUnchecked("Illegal Format: Connect not provided in block at line " + lineNumber);
        }
        this.connectPath = URI.create(Matchers.string(connectObject));
    }

    /**
     * Tries connecting to a database and execute the consumer using the established connection.
     *
     * @param consumer operations to be performed on the database using the established connection.
     */
    void connectToDatabaseAndExecute(Consumer<RelationalConnection> consumer) {
        if (connectPath == null) {
            Assert.failUnchecked("‼️ Cannot connect. Path is not provided");
        }
        logger.debug("🚠 Connecting to database: `" + connectPath + "`");
        try (var connection = executionContext.getConnectionFactory().getNewConnection(connectPath)) {
            logger.debug("✅ Connected to database: `" + connectPath + "`");
            consumer.accept(connection);
        } catch (SQLException sqle) {
            throw new RuntimeException(String.format("‼️ Error connecting to the database `%s` in block at line %d",
                    connectPath, lineNumber), sqle);
        }
    }

    public static boolean isConfigBlock(@Nonnull Object document) {
        final var blockObject = Matchers.map(document);
        if (blockObject.size() != 1) {
            return false;
        }
        final var entry = Matchers.firstEntry(blockObject, "config");
        final var key = ((CustomYamlConstructor.LinedObject) entry.getKey()).getObject();
        return blockObject.size() == 1 && (key.equals(ConfigBlock.CONFIG_BLOCK_SETUP) ||
                key.equals(ConfigBlock.CONFIG_BLOCK_DESTRUCT));
    }

    public static boolean isTestBlock(@Nonnull Object document) {
        final var blockObject = Matchers.map(document);
        if (blockObject.size() != 1) {
            return false;
        }
        final var entry = Matchers.firstEntry(blockObject, "config");
        final var key = ((CustomYamlConstructor.LinedObject) entry.getKey()).getObject();
        return blockObject.size() == 1 && key.equals(TestBlock.TEST_BLOCK);
    }

    /**
     * Looks at the block to determine if its one of the valid blocks. If it is, parses it to that one.
     *
     * @param document a region in the file
     * @param executionContext information needed to carry out the execution
     *
     * @return a parsed block
     */
    @Nonnull
    public static Block parse(@Nonnull Object document, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
        if (isConfigBlock(document)) {
            return new ConfigBlock(document, executionContext);
        } else if (isTestBlock(document)) {
            return new TestBlock(document, executionContext);
        } else {
            throw new RuntimeException("Cannot recognize the type of block");
        }
    }

    private static String getCommandString(Map.Entry<?, ?> commandAndArgument) {
        final var key = ((CustomYamlConstructor.LinedObject) Matchers.notNull(commandAndArgument, "command").getKey()).getObject();
        return Matchers.notNull(Matchers.string(Matchers.notNull(key, "command"), "command"), "command");
    }

    private static int getCommandLineNumber(Map.Entry<?, ?> commandAndArgument) {
        return ((CustomYamlConstructor.LinedObject) Matchers.notNull(commandAndArgument, "command").getKey()).getStartMark().getLine() + 1;
    }

    /**
     * Implementation of block that serves the purpose of creating the 'environment' needed to run the {@link TestBlock}
     * that follows it. In essence, it consists of a `connectPath` that is required to connect to a database and an
     * ordered list of `steps` to execute. A `step` is nothing but a query to be executed, translating to a special
     * {@link QueryCommand} that executes but doesn't verify anything.
     * </p>
     * The failure handling in case of {@link ConfigBlock} is straight-forward. It {@code throws} downstream exceptions
     * and errors to handled in the consumer. The rationale for this is that if the {@link ConfigBlock} fails at step,
     * there is no guarantee **as of now** that some following {@link Block} can run independent of this failure.
     */
    static class ConfigBlock extends Block {

        static final String CONFIG_BLOCK_SETUP = "setup";
        static final String CONFIG_BLOCK_STEPS = "steps";
        static final String CONFIG_BLOCK_DESTRUCT = "destruct";

        @Nonnull
        List<Consumer<RelationalConnection>> executableSteps = new ArrayList<>();

        private ConfigBlock(@Nonnull Object document, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
            super((((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "config").getKey()).getStartMark().getLine() + 1), executionContext);
            final var configMap = Matchers.map(Matchers.firstEntry(document, "config").getValue());
            setConnectPath(configMap.getOrDefault(BLOCK_CONNECT, null));
            final var steps = getSteps(configMap.getOrDefault(CONFIG_BLOCK_STEPS, null));
            for (final var step : steps) {
                Assert.thatUnchecked(Matchers.map(step).size() == 1, "Illegal Format: A configuration step should be a single command");
                final var commandString = getCommandString(Matchers.firstEntry(step, "configuration step"));
                final var resolvedCommand = Objects.requireNonNull(Command.resolve(commandString));
                executableSteps.add(connection -> {
                    try {
                        resolvedCommand.invoke(List.of(step), connection, executionContext);
                    } catch (Exception e) {
                        this.throwable = new RuntimeException(String.format("‼️ Error executing config step at line %d",
                                getCommandLineNumber(Matchers.firstEntry(step, "configuration step"))), e);
                        throw (RuntimeException) this.throwable;
                    }
                });
            }
        }

        private List<?> getSteps(Object steps) {
            if (steps == null) {
                Assert.failUnchecked("Illegal Format: No steps provided in block at line " + lineNumber);
            }
            return Matchers.arrayList(steps);
        }

        @Override
        public void execute() {
            connectToDatabaseAndExecute(connection -> executableSteps.forEach(e -> e.accept(connection)));
        }
    }

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
     *     <li><{@code connection_lifecycle}: Whether to use a new connection for each test or using one throughout./li>
     * </ul>
     */
    static class TestBlock extends Block {

        static final String TEST_BLOCK = "test_block";
        static final String TEST_BLOCK_TESTS = "tests";
        static final String TEST_BLOCK_OPTIONS = "options";
        static final String OPTION_EXECUTION_MODE = "mode";
        static final String OPTION_EXECUTION_MODE_ORDERED = "ordered";
        static final String OPTION_EXECUTION_MODE_RANDOMIZED = "randomized";
        static final String OPTION_REPETITION = "repetition";
        static final String OPTION_SEED = "seed";
        static final String OPTION_CHECK_CACHE = "check_cache";
        static final String OPTION_CONNECTION_LIFECYCLE = "connection_lifecycle";
        static final String OPTION_CONNECTION_LIFECYCLE_TEST = "test";
        static final String OPTION_CONNECTION_LIFECYCLE_BLOCK = "block";

        enum ExecutionMode {
            RANDOMIZED,
            ORDERED
        }

        enum ConnectionLifecycle {
            // Runs each test executable in a new connection.
            TEST,
            // Runs all the tests in the test block in a common connection that is kept alive for the entire lifecycle
            // of this test block.
            BLOCK
        }

        private int repetition = 1;
        private ExecutionMode mode = ExecutionMode.ORDERED;
        private long seed = -1;
        private boolean checkCache;
        private ConnectionLifecycle connectionLifecycle = ConnectionLifecycle.TEST;
        @Nonnull
        private final List<Consumer<RelationalConnection>> executableTests = new ArrayList<>();

        private TestBlock(@Nonnull Object document, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
            super(((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "test_block").getKey()).getStartMark().getLine() + 1, executionContext);
            final var testsMap = Matchers.map(Matchers.firstEntry(document, "test_block").getValue());
            setConnectPath(testsMap.getOrDefault(BLOCK_CONNECT, null));
            setTestBlockOptions(testsMap.getOrDefault(TEST_BLOCK_OPTIONS, null));
            setupTests(testsMap.getOrDefault(TEST_BLOCK_TESTS, null));
            Assert.thatUnchecked(!executableTests.isEmpty(), "‼️ Test block at line " + lineNumber + " have no tests to execute");
        }

        @Override
        public void execute() {
            try {
                if (connectionLifecycle == ConnectionLifecycle.BLOCK) {
                    connectToDatabaseAndExecute(connection -> {
                        for (var t : executableTests) {
                            t.accept(connection);
                            if (this.throwable != null) {
                                break;
                            }
                        }
                    });
                } else if (connectionLifecycle == ConnectionLifecycle.TEST) {
                    for (var t : executableTests) {
                        connectToDatabaseAndExecute(t);
                        if (this.throwable != null) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                this.throwable = new RuntimeException("‼️ Test Block failed at line " + getLineNumber() + " with option " + printOptions(), e);
            }
        }

        private void setTestBlockOptions(@Nullable Object options) {
            if (options == null) {
                return;
            }
            final var optionsMap = Matchers.map(options);
            setOptionExecutionModeAndSeed(optionsMap);
            setOptionRepetition(optionsMap);
            setOptionCheckCache(optionsMap);
            setOptionConnectionLifecycle(optionsMap);
        }

        private void setOptionExecutionModeAndSeed(Map<?, ?> optionsMap) {
            if (optionsMap.containsKey(OPTION_EXECUTION_MODE)) {
                final var value = Matchers.string(optionsMap.get(OPTION_EXECUTION_MODE));
                switch (value) {
                    case OPTION_EXECUTION_MODE_ORDERED:
                        this.mode = ExecutionMode.ORDERED;
                        break;
                    case OPTION_EXECUTION_MODE_RANDOMIZED:
                        this.mode = ExecutionMode.RANDOMIZED;
                        break;
                    default:
                        Assert.failUnchecked("Illegal Format: Unknown value for option mode: " + value);
                        break;
                }
            }
            if (optionsMap.containsKey(OPTION_SEED)) {
                this.seed = Matchers.longValue(optionsMap.get(OPTION_SEED));
            } else if (mode == ExecutionMode.RANDOMIZED) {
                this.seed = System.currentTimeMillis();
            }
        }

        private void setOptionRepetition(Map<?, ?> optionsMap) {
            if (optionsMap.containsKey(OPTION_REPETITION)) {
                this.repetition = Matchers.intValue(optionsMap.get(OPTION_REPETITION));
            }
        }

        private void setOptionCheckCache(Map<?, ?> optionsMap) {
            if (optionsMap.containsKey(OPTION_CHECK_CACHE)) {
                this.checkCache = Matchers.bool(optionsMap.get(OPTION_CHECK_CACHE));
            }
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

        private void setupTests(@Nullable Object testsObject) {
            if (testsObject == null) {
                return;
            }
            final var tests = Matchers.arrayList(testsObject, "tests");
            executableTests.clear();
            final var executableTestsWithCacheCheck = new ArrayList<Consumer<RelationalConnection>>();
            for (var testObject : tests) {
                final var test = Matchers.arrayList(testObject, "test");
                final var testQuery = Matchers.firstEntry(Matchers.first(test, "test query"), "test query command");
                final var resolvedCommand = Objects.requireNonNull(Command.resolve(getCommandString(testQuery)));
                Assert.thatUnchecked(resolvedCommand instanceof QueryCommand, "Illegal Format: Test is expected to start with a query.");
                for (int i = 0; i < repetition; i++) {
                    executableTests.add(createTestExecutable(test, getCommandLineNumber(testQuery), false));
                }
                if (checkCache) {
                    executableTestsWithCacheCheck.add(createTestExecutable(test, getCommandLineNumber(testQuery), true));
                }
            }
            if (mode == ExecutionMode.RANDOMIZED) {
                Collections.shuffle(executableTests, new Random(seed));
            }
            executableTests.addAll(executableTestsWithCacheCheck);
        }

        private Consumer<RelationalConnection> createTestExecutable(List<?> test, int lineNumber, boolean checkCache) {
            return connection -> {
                try {
                    new QueryCommand(checkCache).invoke(test, connection, executionContext);
                } catch (SQLException e) {
                    throw new RuntimeException(String.format("‼️ Cannot run query at line %d", lineNumber), e);
                } catch (RelationalException e) {
                    throw new RuntimeException(String.format("‼️ Error executing test at line %d", lineNumber), e);
                } catch (TestAbortedException e) {
                    logger.debug("⚠️ Aborting test as the assumption are not fulfilled: " + e.getMessage());
                } catch (AssertionFailedError | Exception e) {
                    throw new RuntimeException(String.format("‼️ Test failed at line %d", lineNumber), e);
                }
            };
        }

        private String printOptions() {
            return String.format("{mode: %s, repetition: %d, seed: %d}", mode, repetition, seed);
        }
    }
}
