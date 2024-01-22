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

import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.util.Assert;

import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

/**
 * Block is a single region in the YAMSQL file that can either be a `config` or `test-block`.
 * <ul>
 *      <li> {@link ConfigBlock}: It can be either a `setup` block or a `destruct` block. The motive of these block
 *          is to "setup" and "clean" the environment needed to run the `test-block`s. A Config block consist of a list
 *          of commands.</li>
 *      <li> {@link TestBlock}: Defines a scope for a group of tests by setting the knobs that determines how those
 *          tests are run.</li>
 * </ul>
 */
public abstract class Block {

    int lineNumber;
    @Nullable
    Throwable throwable;

    Block(int lineNumber) {
        this.lineNumber = lineNumber;
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
    public final Optional<Throwable> getThrowableIfExists() {
        return Optional.ofNullable(this.throwable);
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
     * @param cliCommandFactory interface needed to run various commands while executing the parsed block.
     *
     * @return a parsed block
     */
    public static Block parse(@Nonnull Object document, @Nonnull CliCommandFactory cliCommandFactory) {
        if (isConfigBlock(document)) {
            return new ConfigBlock(document, cliCommandFactory);
        } else if (isTestBlock(document)) {
            return new TestBlock(document, cliCommandFactory);
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

    static class ConfigBlock extends Block {

        static final String CONFIG_BLOCK_SETUP = "setup";
        static final String CONFIG_BLOCK_DESTRUCT = "destruct";

        @Nonnull
        List<Runnable> executableSteps = new ArrayList<>();

        private ConfigBlock(@Nonnull Object document, @Nonnull CliCommandFactory cliCommandFactory) {
            super((((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "config").getKey()).getStartMark().getLine() + 1));
            final var steps = Matchers.arrayList(Matchers.firstEntry(document, "config").getValue(), "configuration steps");
            for (int i = 0; i < steps.size(); i++) {
                final var step = steps.get(i);
                Assert.thatUnchecked(Matchers.map(step).size() == 1, "Illegal Format: A configuration step should be a single command");
                final var commandString = getCommandString(Matchers.firstEntry(step, "configuration step"));
                final var resolvedCommand = Objects.requireNonNull(Command.resolve(commandString));
                executableSteps.add(() -> {
                    try {
                        resolvedCommand.invoke(List.of(step), cliCommandFactory);
                    } catch (Exception e) {
                        this.throwable = new RuntimeException(String.format("‼️ Error executing config step at line %d",
                                getCommandLineNumber(Matchers.firstEntry(step, "configuration step"))), e);
                        throw (RuntimeException) this.throwable;
                    }
                });
            }
        }

        @Override
        public void execute()  {
            executableSteps.forEach(Runnable::run);
        }
    }

    static class TestBlock extends Block {

        static final String TEST_BLOCK = "test_block";
        static final String TEST_BLOCK_TESTS = "tests";
        static final String TEST_BLOCK_OPTIONS = "options";
        static final String OPTION_EXECUTION_MODE = "mode";
        static final String OPTION_EXECUTION_MODE_ORDERED = "ordered";
        static final String OPTION_EXECUTION_MODE_RANDOMIZED = "randomized";
        static final String OPTION_REPETITION = "repetition";
        static final String OPTION_SEED = "seed";

        enum ExecutionMode {
            RANDOMIZED,
            ORDERED
        }

        private int repetition = 1;
        private ExecutionMode mode = ExecutionMode.ORDERED;
        private long seed = -1;
        @Nonnull
        private final List<Runnable> executableTests = new ArrayList<>();

        private TestBlock(@Nonnull Object document, @Nonnull CliCommandFactory cliCommandFactory) {
            super(((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "test_block").getKey()).getStartMark().getLine() + 1);
            final var optionsAndTests = Matchers.map(Matchers.firstEntry(document, "test_block").getValue());
            setTestBlockOptions(optionsAndTests.getOrDefault(TEST_BLOCK_OPTIONS, null));
            setupTests(optionsAndTests.getOrDefault(TEST_BLOCK_TESTS, null), cliCommandFactory);
        }

        @Override
        public void execute() {
            for (var test : executableTests) {
                try {
                    test.run();
                } catch (Exception e) {
                    this.throwable = new RuntimeException("‼️ Illegal format", e);
                    break;
                } catch (AssertionFailedError testFailed) {
                    this.throwable = new RuntimeException("‼️ Test Result Mismatch in testBlock at line " + getLineNumber() + " with option " + printOptions(), testFailed);
                    break;
                }
            }
        }

        private void setTestBlockOptions(@Nullable Object options) {
            if (options == null) {
                return;
            }
            final var optionsMap = Matchers.map(options);
            setOptionExecutionModeAndSeed(optionsMap);
            setOptionRepetition(optionsMap);
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

        private void setupTests(@Nullable Object testsObject, @Nonnull CliCommandFactory cliCommandFactory) {
            if (testsObject == null) {
                return;
            }
            final var tests = Matchers.arrayList(testsObject, "tests");
            executableTests.clear();
            for (var testObject : tests) {
                final var test = Matchers.arrayList(testObject, "test");
                final var testQuery = Matchers.firstEntry(Matchers.first(test, "test query"), "test query command");
                final var resolvedCommand = Objects.requireNonNull(Command.resolve(getCommandString(testQuery)));
                Assert.thatUnchecked(resolvedCommand instanceof QueryCommand, "Illegal Format: Test is expected to start with a query.");
                for (int i = 0; i < repetition; i++) {
                    executableTests.add(() -> {
                        try {
                            new QueryCommand().invoke(test, cliCommandFactory);
                        } catch (Exception e) {
                            this.throwable = new RuntimeException(String.format("‼️ Error executing query at line %d",
                                    getCommandLineNumber(testQuery)), e);
                        }
                    });
                }
            }
            if (mode == ExecutionMode.RANDOMIZED) {
                Collections.shuffle(executableTests, new Random(seed));
            }
        }

        private String printOptions() {
            return String.format("{mode: %s, repetition: %d, seed: %d}", mode, repetition, seed);
        }
    }
}
