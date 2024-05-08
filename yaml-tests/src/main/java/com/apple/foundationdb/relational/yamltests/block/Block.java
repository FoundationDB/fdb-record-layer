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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    YamlExecutionContext executionContext;
    @Nonnull
    protected final AtomicReference<RuntimeException> failureException = new AtomicReference<>(null);
    URI connectPath;
    @Nonnull
    List<Consumer<RelationalConnection>> executables = new ArrayList<>();

    Block(int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Executes the executables from the parsed block in a single connection.
     */
    public abstract void execute();

    protected final void executeExecutables(@Nonnull Collection<Consumer<RelationalConnection>> list) {
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
            this.connectPath = URI.create(executionContext.getOnlyConnectionPath());
        } else if (connectObject instanceof Integer) {
            this.connectPath = URI.create(executionContext.getConnectionPath((Integer) connectObject));
        } else {
            this.connectPath = URI.create(Matchers.string(connectObject));
        }
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

    public static boolean isManualConfigBlock(@Nonnull Object document) {
        final var blockObject = Matchers.map(document);
        if (blockObject.size() != 1) {
            return false;
        }
        final var entry = Matchers.firstEntry(blockObject, "config");
        final var key = ((CustomYamlConstructor.LinedObject) entry.getKey()).getObject();
        return blockObject.size() == 1 && key.equals(ConfigBlock.ManualConfigBlock.MANUAL_CONFIG);
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

    public static boolean isDefineTemplateBlock(@Nonnull Object document) {
        final var blockObject = Matchers.map(document);
        if (blockObject.size() != 1) {
            return false;
        }
        final var entry = Matchers.firstEntry(blockObject, "config");
        final var key = ((CustomYamlConstructor.LinedObject) entry.getKey()).getObject();
        return blockObject.size() == 1 && key.equals(ConfigBlock.SchemaTemplateBlock.DEFINE_TEMPLATE_BLOCK);
    }

    /**
     * Looks at the block to determine if its one of the valid blocks. If it is, parses it to that one.
     *
     * @param document a region in the file
     * @param executionContext information needed to carry out the execution
     */
    public static void parse(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
        if (isManualConfigBlock(document)) {
            new ConfigBlock.ManualConfigBlock(document, executionContext);
        } else if (isTestBlock(document)) {
            new TestBlock(document, executionContext);
        } else if (isDefineTemplateBlock(document)) {
            new ConfigBlock.SchemaTemplateBlock(document, executionContext);
        } else {
            throw new RuntimeException("Cannot recognize the type of block");
        }
    }

    public Optional<RuntimeException> getFailureExceptionIfPresent() {
        return failureException.get() == null ? Optional.empty() : Optional.of(failureException.get());
    }
}
