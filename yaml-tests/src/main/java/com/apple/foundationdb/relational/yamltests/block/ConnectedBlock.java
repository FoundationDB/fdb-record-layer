/*
 * ConnectedBlock.java
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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 * A {@link Block} that requires an active connection for execution.
 * <p>
 * Each block needs to be associated with a `connectionURI` which provides it with the address to the database to which
 * the block connects to for running its executables. The block does so through the
 * {@link com.apple.foundationdb.relational.yamltests.YamlRunner.YamlConnectionFactory}. A block is free to implement `how` and `when`
 * it wants to use the factory to create a connection and also manages the lifecycle of the established connection.
 * </p>
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public abstract class ConnectedBlock implements Block {

    private static final Logger logger = LogManager.getLogger(ConnectedBlock.class);

    static final String BLOCK_CONNECT = "connect";

    int lineNumber;
    @Nonnull
    YamlExecutionContext executionContext;
    @Nonnull
    private final URI connectionURI;
    @Nonnull
    final List<Consumer<RelationalConnection>> executables;

    ConnectedBlock(int lineNumber, @Nonnull List<Consumer<RelationalConnection>> executables, @Nonnull URI connectionURI, @Nonnull YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executables = executables;
        this.connectionURI = connectionURI;
        this.executionContext = executionContext;
    }

    @Override
    public int getLineNumber() {
        return lineNumber;
    }

    protected final void executeExecutables(@Nonnull Collection<Consumer<RelationalConnection>> list) {
        connectToDatabaseAndExecute(connection -> list.forEach(t -> t.accept(connection)));
    }

    /**
     * Tries connecting to a database and execute the consumer using the established connection.
     *
     * @param consumer operations to be performed on the database using the established connection.
     */
    void connectToDatabaseAndExecute(Consumer<RelationalConnection> consumer) {
        logger.debug("🚠 Connecting to database: `{}`", connectionURI);
        try (var connection = executionContext.getConnectionFactory().getNewConnection(connectionURI)) {
            logger.debug("✅ Connected to database: `{}`", connectionURI);
            consumer.accept(connection);
        } catch (SQLException sqle) {
            throw executionContext.wrapContext(sqle,
                    () -> String.format("‼️ Error connecting to the database `%s` in block at line %d", connectionURI, lineNumber),
                    "connection [" + connectionURI + "]", getLineNumber());
        }
    }

}
