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
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A connection factory that creates a connection that can be used with multiple servers.
 * This kind of connection is useful when we want to run a test that simulates multiple versions of the system
 * running concurrently, and verify that the results (e.g. Continuations) are correctly handled through the entire
 * setup.
 */
public class MultiServerConnectionFactory implements YamlRunner.YamlConnectionFactory {
    // The fixed index of the default connection
    public static final int DEFAULT_CONNECTION = 0;
    private final Set<String> versionsUnderTest;

    /**
     * Server selection policy.
     * - DEFAULT always uses the default connection
     * - ALTERNATE alternates between the default connection and the other connections
     */
    public enum ConnectionSelectionPolicy { DEFAULT, ALTERNATE }

    @Nonnull
    private final ConnectionSelectionPolicy connectionSelectionPolicy;
    @Nonnull
    private final YamlRunner.YamlConnectionFactory defaultFactory;
    @Nonnull
    private final List<YamlRunner.YamlConnectionFactory> alternateFactories;
    private final int totalFactories;
    @Nonnull
    private final AtomicInteger currentConnectionSelector;

    public MultiServerConnectionFactory(@Nonnull final YamlRunner.YamlConnectionFactory defaultFactory,
                                        @Nonnull final List<YamlRunner.YamlConnectionFactory> alternateFactories) {
        this(ConnectionSelectionPolicy.DEFAULT, 0, defaultFactory, alternateFactories);
    }

    public MultiServerConnectionFactory(@Nonnull final ConnectionSelectionPolicy connectionSelectionPolicy,
                                        final int initialConnection,
                                        @Nonnull final YamlRunner.YamlConnectionFactory defaultFactory,
                                        @Nonnull final List<YamlRunner.YamlConnectionFactory> alternateFactories) {
        this.connectionSelectionPolicy = connectionSelectionPolicy;
        this.defaultFactory = defaultFactory;
        this.alternateFactories = alternateFactories;
        this.versionsUnderTest =
                Stream.concat(Stream.of(defaultFactory), alternateFactories.stream())
                        .flatMap(yamlConnectionFactory -> yamlConnectionFactory.getVersionsUnderTest().stream())
                        .collect(Collectors.toSet());
        this.totalFactories = 1 + alternateFactories.size(); // one default and N alternates
        Assertions.assertTrue(initialConnection >= 0);
        Assertions.assertTrue(initialConnection < totalFactories, "Initial connections should be <= number of factories");
        this.currentConnectionSelector = new AtomicInteger(initialConnection);
    }

    @Override
    public Connection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        return new MultiServerRelationalConnection(connectionSelectionPolicy, getNextConnectionNumber(), defaultFactory.getNewConnection(connectPath), alternateConnections(connectPath));
    }

    @Override
    public Set<String> getVersionsUnderTest() {
        return versionsUnderTest;
    }

    @Override
    public boolean isMultiServer() {
        return true;
    }

    public int getCurrentConnectionSelector() {
        return currentConnectionSelector.get();
    }

    @Nonnull
    private List<Connection> alternateConnections(URI connectPath) {
        return alternateFactories.stream().map(factory -> {
            try {
                return factory.getNewConnection(connectPath);
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to create a connection", e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Increment and return the next connection's initialConnection number.
     * This allows us to better distribute the connections positions as connections are created per query in the tests
     * and thus connections with a single query should increment their position between connection creation or else they
     * will always execute with the same initial connection.
     * @return the next initial connection number to use
     */
    private int getNextConnectionNumber() {
        switch (connectionSelectionPolicy) {
            case DEFAULT:
                return DEFAULT_CONNECTION;
            case ALTERNATE:
                return currentConnectionSelector.getAndUpdate(i -> (i + 1) % totalFactories);
            default:
                throw new IllegalArgumentException("Unknown policy");
        }
    }

    /**
     * A connection that wraps around multiple connections.
     */
    @SuppressWarnings("PMD.CloseResource") // false-positive as constituent connections are closed when object is closed
    public static class MultiServerRelationalConnection implements RelationalConnection {
        private static final Logger logger = LogManager.getLogger(MultiServerRelationalConnection.class);

        private int currentConnectionSelector;

        @Nonnull
        private final ConnectionSelectionPolicy connectionSelectionPolicy;
        @Nonnull
        private final List<RelationalConnection> relationalConnections;

        public MultiServerRelationalConnection(@Nonnull ConnectionSelectionPolicy connectionSelectionPolicy,
                                             final int initialConnecion,
                                             @Nonnull final Connection defaultConnection,
                                             @Nonnull List<Connection> alternateConnections) throws SQLException {
            this.connectionSelectionPolicy = connectionSelectionPolicy;
            this.currentConnectionSelector = initialConnecion;
            relationalConnections = new ArrayList<>();
            // The default connection is always the one at location 0
            relationalConnections.add(defaultConnection.unwrap(RelationalConnection.class));
            for (final Connection alternateConnection : alternateConnections) {
                relationalConnections.add(alternateConnection.unwrap(RelationalConnection.class));
            }
        }

        public int getCurrentConnectionSelector() {
            return currentConnectionSelector;
        }

        @Override
        public RelationalStatement createStatement() throws SQLException {
            return getCurrentConnection(true, "createStatement").createStatement();
        }

        @Override
        public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
            return getCurrentConnection(true, "prepareStatement").prepareStatement(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            if (!autoCommit) {
                throw new UnsupportedOperationException("setAutoCommit(false) is not supported in YAML tests");
            }
            logger.info("Sending operation {} to all connections", "setAutoCommit");
            for (RelationalConnection connection: relationalConnections) {
                connection.setAutoCommit(autoCommit);
            }
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return getCurrentConnection(false, "getAutoCommit").getAutoCommit();
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
            for (RelationalConnection connection : relationalConnections) {
                connection.close();
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            return getCurrentConnection(false, "isClosed").isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return getCurrentConnection(false, "getMetaData").getMetaData();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            logger.info("Sending operation {} to all connections", "setTransactionIsolation");
            for (RelationalConnection connection : relationalConnections) {
                connection.setTransactionIsolation(level);
            }
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return getCurrentConnection(false, "getTransactionIsolation").getTransactionIsolation();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return getCurrentConnection(true, "createArrayOf").createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return getCurrentConnection(true, "createStruct").createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            logger.info("Sending operation {} to all connections", "setSchema");
            for (RelationalConnection connection: relationalConnections) {
                connection.setSchema(schema);
            }
        }

        @Override
        public String getSchema() throws SQLException {
            return getCurrentConnection(false, "getSchema").getSchema();
        }

        @Nonnull
        @Override
        public Options getOptions() {
            return getCurrentConnection(false, "getOptions").getOptions();
        }

        @Override
        public void setOption(Options.Name name, Object value) throws SQLException {
            logger.info("Sending operation {} to all connections", "setOption");
            for (RelationalConnection connection: relationalConnections) {
                connection.setOption(name, value);
            }
        }

        @Override
        public URI getPath() {
            return getCurrentConnection(false, "getPath").getPath();
        }

        /**
         * Get the connection to send requests to.
         * This method conditionally advances the connection selector. This is done for ease of testing, where some
         * commands that just return information will not advance the selector, leading to better predictability
         * of the command routing.
         * @param advance whether to advance the connection selector
         * @param op the name of the operation (for logging)
         * @return the underlying connection to use
         */
        private RelationalConnection getCurrentConnection(boolean advance, String op) {
            switch (connectionSelectionPolicy) {
                case DEFAULT:
                    if (logger.isInfoEnabled()) {
                        logger.info("Sending operation {} to connection {}", op, "DEFAULT");
                    }
                    return relationalConnections.get(DEFAULT_CONNECTION);
                case ALTERNATE:
                    RelationalConnection result = relationalConnections.get(currentConnectionSelector);
                    if (logger.isInfoEnabled()) {
                        logger.info("Sending operation {} to connection {}", op, currentConnectionSelector);
                    }
                    if (advance) {
                        currentConnectionSelector = (currentConnectionSelector + 1) % relationalConnections.size();
                    }
                    return result;
                default:
                    throw new IllegalStateException("Unsupported selection policy " + connectionSelectionPolicy);
            }
        }

        @Override
        public <T> T unwrap(final Class<T> iface) throws SQLException {
            if (iface.equals(RelationalConnection.class)) {
                return iface.cast(this);
            } else {
                return RelationalConnection.super.unwrap(iface);
            }
        }
    }
}
