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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.command.Command;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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
    @Nonnull
    protected final AtomicReference<RuntimeException> failureException = new AtomicReference<>(null);
    URI connectPath;
    @Nonnull
    List<Consumer<RelationalConnection>> executables = new ArrayList<>();

    Block(int lineNumber, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Executes the executables from the parsed block in a single connection.
     */
    public void execute() {
        execute(executables);
    }

    protected void execute(@Nonnull Collection<Consumer<RelationalConnection>> list) {
        connectToDatabaseAndExecute(connection -> list.forEach(t -> {
            if (failureException.get() != null) {
                logger.trace("⚠️ Aborting test as one of the test has failed.");
                return;
            }
            t.accept(connection);
        }));
    }

    /**
     * Sets the path of the database to which the block should connect to.
     *
     * @param connectObject the connection path.
     */
    void setConnectPath(@Nullable Object connectObject) {
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
            failureException.set(new RuntimeException(String.format("‼️ Error connecting to the database `%s` in block at line %d",
                    connectPath, lineNumber), sqle));
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

    public Optional<RuntimeException> getFailureExceptionIfPresent() {
        return failureException.get() == null ? Optional.empty() : Optional.of(failureException.get());
    }

    /**
     * Implementation of block that serves the purpose of creating the 'environment' needed to run the {@link TestBlock}
     * that follows it. In essence, it consists of a `connectPath` that is required to connect to a database and an
     * ordered list of `steps` to execute. A `step` is nothing but a query to be executed, translating to a special
     * {@link QueryCommand} that executes but doesn't verify anything.
     * <p>
     * The failure handling in case of {@link ConfigBlock} is straight-forward. It {@code throws} downstream exceptions
     * and errors to handled in the consumer. The rationale for this is that if the {@link ConfigBlock} fails at step,
     * there is no guarantee **as of now** that some following {@link Block} can run independent of this failure.
     */
    public static class ConfigBlock extends Block {

        public static final String CONFIG_BLOCK_SETUP = "setup";
        public static final String CONFIG_BLOCK_STEPS = "steps";
        public static final String CONFIG_BLOCK_DESTRUCT = "destruct";

        private ConfigBlock(@Nonnull Object document, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
            super((((CustomYamlConstructor.LinedObject) Matchers.firstEntry(document, "config").getKey()).getStartMark().getLine() + 1), executionContext);
            final var configMap = Matchers.map(Matchers.firstEntry(document, "config").getValue());
            setConnectPath(configMap.getOrDefault(BLOCK_CONNECT, null));
            final var steps = getSteps(configMap.getOrDefault(CONFIG_BLOCK_STEPS, null));
            for (final var step : steps) {
                Assert.thatUnchecked(Matchers.map(step).size() == 1, "Illegal Format: A configuration step should be a single command");
                final var resolvedCommand = Objects.requireNonNull(Command.parse(List.of(step)));
                executables.add(connection -> {
                    try {
                        resolvedCommand.invoke(connection, executionContext);
                    } catch (Exception e) {
                        failureException.set(new RuntimeException(String.format("‼️ Error executing config step at line %d",
                                resolvedCommand.getLineNumber()), e));
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
            super.execute();
            if (getFailureExceptionIfPresent().isPresent()) {
                throw getFailureExceptionIfPresent().get();
            }
        }
    }
}
