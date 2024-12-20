/*
 * MultiServerConnectionFactory.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A connection factory that creates a connection that can be used with multiple servers.
 * This kind of connection is useful when we want to run a test that simulates multiple versions of the system
 * running concurrently, and verify that the results (e.g. Continuations) are correctly handled through the entire
 * setup.
 */
public class MultiServerConnectionFactory implements YamlRunner.YamlConnectionFactory {
    // The fixed index of the default connection
    public static final int DEFAULT_CONNECTION = 0;

    /**
     * Server selection policy.
     * - DEFAULT always uses the default connection
     * - ALTERNATE alternates between the default connection and the other connections
     */
    public enum ConnectionSelectionPolicy { DEFAULT, ALTERNATE }

    @Nonnull
    private final ConnectionSelectionPolicy connectionSelectionPolicy;
    private final int initialConnection;
    @Nonnull
    private final YamlRunner.YamlConnectionFactory defaultFactory;
    @Nullable
    private final List<YamlRunner.YamlConnectionFactory> alternateFactories;

    public MultiServerConnectionFactory(@Nonnull final YamlRunner.YamlConnectionFactory defaultFactory,
                                        @Nullable final List<YamlRunner.YamlConnectionFactory> alternateFactories) {
        this(ConnectionSelectionPolicy.DEFAULT, 0, defaultFactory, alternateFactories);
    }

    public MultiServerConnectionFactory(@Nonnull final ConnectionSelectionPolicy connectionSelectionPolicy,
                                        final int initialConnection,
                                        @Nonnull final YamlRunner.YamlConnectionFactory defaultFactory,
                                        @Nullable final List<YamlRunner.YamlConnectionFactory> alternateFactories) {
        this.connectionSelectionPolicy = connectionSelectionPolicy;
        this.initialConnection = initialConnection;
        this.defaultFactory = defaultFactory;
        this.alternateFactories = alternateFactories;
    }

    @Override
    public RelationalConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        return new MultiServerRelationalConnection(connectionSelectionPolicy, initialConnection, defaultFactory.getNewConnection(connectPath), alternateConnections(connectPath));
    }

    @Nullable
    private List<RelationalConnection> alternateConnections(URI connectPath) {
        if (alternateFactories == null) {
            return null;
        } else {
            return alternateFactories.stream().map(factory -> {
                try {
                    return factory.getNewConnection(connectPath);
                } catch (SQLException e) {
                    throw new IllegalStateException("Failed to create a connection", e);
                }
            }).collect(Collectors.toList());
        }
    }

    /**
     * A connection that wraps around multiple connections.
     */
    public static class MultiServerRelationalConnection implements RelationalConnection {
        private static final Logger logger = LogManager.getLogger(MultiServerRelationalConnection.class);

        private int currentConnectionSelector;

        @Nonnull
        private final ConnectionSelectionPolicy connectionSelectionPolicy;
        @Nonnull
        private final List<RelationalConnection> allConnections;

        public MultiServerRelationalConnection(@Nonnull ConnectionSelectionPolicy connectionSelectionPolicy,
                                             final int initialConnecion,
                                             @Nonnull final RelationalConnection defaultConnection,
                                             @Nullable List<RelationalConnection> alternateConnections) {
            this.connectionSelectionPolicy = connectionSelectionPolicy;
            this.currentConnectionSelector = initialConnecion;
            allConnections = new ArrayList<>();
            // The default connection is always the one at location 0
            allConnections.add(defaultConnection);
            if (alternateConnections != null) {
                allConnections.addAll(alternateConnections);
            }
        }

        @Override
        public RelationalStatement createStatement() throws SQLException {
            return getNextConnection("createStatement").createStatement();
        }

        @Override
        public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
            return getNextConnection("prepareStatement").prepareStatement(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            if (!autoCommit) {
                throw new UnsupportedOperationException("setAutoCommit(false) is not supported in YAML tests");
            }
            logger.info("Sending operation {} to all connections", "setAutoCommit");
            for (RelationalConnection connection: allConnections) {
                connection.setAutoCommit(autoCommit);
            }
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return getNextConnection("getAutoCommit").getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            throw new UnsupportedOperationException("commit is not supported in YAML tests");
        }

        @Override
        public void rollback() throws SQLException {
            throw new UnsupportedOperationException("rollback is not supported in YAML tests");
        }

        @Override
        public void close() throws SQLException {
            logger.info("Sending operation {} to all connections", "close");
            for (RelationalConnection connection : allConnections) {
                connection.close();
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            return getNextConnection("isClosed").isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return getNextConnection("getMetaData").getMetaData();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            getNextConnection("setTransactionIsolation").setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return getNextConnection("getTransactionIsolation").getTransactionIsolation();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return getNextConnection("createArrayOf").createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return getNextConnection("createStruct").createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            logger.info("Sending operation {} to all connections", "setSchema");
            for (RelationalConnection connection: allConnections) {
                connection.setSchema(schema);
            }
        }

        @Override
        public String getSchema() throws SQLException {
            return getNextConnection("getSchema").getSchema();
        }

        @Nonnull
        @Override
        public Options getOptions() {
            return getNextConnection("getOptions").getOptions();
        }

        @Override
        public void setOption(Options.Name name, Object value) throws SQLException {
            logger.info("Sending operation {} to all connections", "setOption");
            for (RelationalConnection connection: allConnections) {
                connection.setOption(name, value);
            }
        }

        @Override
        public URI getPath() {
            return getNextConnection("getPath").getPath();
        }

        private RelationalConnection getNextConnection(String op) {
            switch (connectionSelectionPolicy) {
                case DEFAULT:
                    if (logger.isInfoEnabled()) {
                        logger.info("Sending operation {} to connection {}", op, "DEFAULT");
                    }
                    return allConnections.get(DEFAULT_CONNECTION);
                case ALTERNATE:
                    RelationalConnection result = allConnections.get(currentConnectionSelector);
                    if (logger.isInfoEnabled()) {
                        logger.info("Sending operation {} to connection {}", op, currentConnectionSelector);
                    }
                    currentConnectionSelector = (currentConnectionSelector + 1) % allConnections.size();
                    return result;
                default:
                    throw new IllegalStateException("Unsupported selection policy " + connectionSelectionPolicy);
            }
        }
    }
}
