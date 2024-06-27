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
import java.net.URI;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Block is a single region in the YAMSQL file that can either be a {@link SetupBlock} or {@link TestBlock}.
 * <ul>
 *      <li> {@link SetupBlock}: It can be either a `setup` block or a `destruct` block. The motive of these block
 *          is to "setup" and "clean" the environment needed to run the `test-block`s. A Setup block consist of a list
 *          of commands.</li>
 *      <li> {@link TestBlock}: Defines a scope for a group of tests by setting the knobs that determines how those
 *          tests are run.</li>
 * </ul>
 * <p>
 * Each block needs to be associated with a `connectionURI` which provides it with the address to the database to which
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
    private final URI connectionURI;
    @Nonnull
    final List<Consumer<RelationalConnection>> executables;

    Block(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull URI connectionURI, @Nonnull YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executables = executables;
        this.connectionURI = connectionURI;
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
        connectToDatabaseAndExecute(connection -> list.forEach(t -> t.accept(connection)));
    }

    /**
     * Tries connecting to a database and execute the consumer using the established connection.
     *
     * @param consumer operations to be performed on the database using the established connection.
     */
    void connectToDatabaseAndExecute(Consumer<RelationalConnection> consumer) {
        logger.debug("🚠 Connecting to database: `" + connectionURI + "`");
        try (var connection = executionContext.getConnectionFactory().getNewConnection(connectionURI)) {
            logger.debug("✅ Connected to database: `" + connectionURI + "`");
            consumer.accept(connection);
        } catch (SQLException sqle) {
            throw executionContext.wrapContext(sqle,
                    () -> String.format("‼️ Error connecting to the database `%s` in block at line %d", connectionURI, lineNumber),
                    "connection [" + connectionURI + "]", getLineNumber());
        }
    }

    /**
     * Looks at the block to determine if its one of the valid blocks. If it is a valid one, parses it to that. This
     * method dispatches the execution to the right block which takes care of reading from the block and initializing
     * the correctly configured {@link Block} for execution.
     *
     * @param document a region in the file
     * @param executionContext information needed to carry out the execution
     */
    public static void parse(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
        final var blockObject = Matchers.map(document, "block");
        Assert.thatUnchecked(blockObject.size() == 1, "Illegal Format: A block is expected to be a map of size 1");
        final var entry = Matchers.firstEntry(blockObject, "block key-value");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(entry.getKey(), () -> "Invalid block key-value pair: " + entry);
        final var lineNumber = linedObject.getLineNumber();
        switch (Matchers.notNull(Matchers.string(linedObject.getObject(), "block key"), "block key")) {
            case SetupBlock.SETUP_BLOCK:
                SetupBlock.ManualSetupBlock.parse(lineNumber, entry.getValue(), executionContext);
                break;
            case TestBlock.TEST_BLOCK:
                TestBlock.parse(lineNumber, entry.getValue(), executionContext);
                break;
            case SetupBlock.SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK:
                SetupBlock.SchemaTemplateBlock.parse(lineNumber, entry.getValue(), executionContext);
                break;
            default:
                throw new RuntimeException("Cannot recognize the type of block");
        }
    }
}
