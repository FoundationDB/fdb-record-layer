/*
 * MultiServerConnectionFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.connectionfactory;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.command.SQLFunction;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
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
public class MultiServerConnectionFactory implements YamlConnectionFactory {
    // The fixed index of the default connection
    public static final int DEFAULT_CONNECTION = 0;
    private final Set<SemanticVersion> versionsUnderTest;

    /**
     * Server selection policy.
     * - DEFAULT always uses the default connection
     * - ALTERNATE alternates between the default connection and the other connections
     */
    public enum ConnectionSelectionPolicy { DEFAULT, ALTERNATE }

    @Nonnull
    private final ConnectionSelectionPolicy connectionSelectionPolicy;
    @Nonnull
    private final YamlConnectionFactory defaultFactory;
    @Nonnull
    private final List<YamlConnectionFactory> alternateFactories;
    private final int totalFactories;
    @Nonnull
    private final AtomicInteger currentConnectionSelector;

    public MultiServerConnectionFactory(@Nonnull final ConnectionSelectionPolicy connectionSelectionPolicy,
                                        final int initialConnection,
                                        @Nonnull final YamlConnectionFactory defaultFactory,
                                        @Nonnull final List<YamlConnectionFactory> alternateFactories) {
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
    public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        if (connectionSelectionPolicy == ConnectionSelectionPolicy.DEFAULT) {
            return defaultFactory.getNewConnection(connectPath);
        } else {
            return new MultiServerConnection(connectionSelectionPolicy, getNextConnectionNumber(),
                    defaultFactory.getNewConnection(connectPath), alternateConnections(connectPath));
        }
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return versionsUnderTest;
    }

    @Override
    public boolean isMultiServer() {
        return true;
    }

    @Nonnull
    private List<YamlConnection> alternateConnections(URI connectPath) {
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
    public static class MultiServerConnection implements YamlConnection {
        private static final Logger logger = LogManager.getLogger(MultiServerConnection.class);

        private int currentConnectionSelector;

        @Nonnull
        private final ConnectionSelectionPolicy connectionSelectionPolicy;
        @Nonnull
        private final List<YamlConnection> underlyingConnections;
        @Nonnull
        private final List<SemanticVersion> versions;

        public MultiServerConnection(@Nonnull ConnectionSelectionPolicy connectionSelectionPolicy,
                                     final int initialConnecion,
                                     @Nonnull final YamlConnection defaultConnection,
                                     @Nonnull List<YamlConnection> alternateConnections) throws SQLException {
            this.connectionSelectionPolicy = connectionSelectionPolicy;
            this.currentConnectionSelector = initialConnecion;
            underlyingConnections = new ArrayList<>();
            // The default connection is always the one at location 0
            underlyingConnections.add(defaultConnection);
            underlyingConnections.addAll(alternateConnections);

            versions = createVersionsList(initialConnecion, underlyingConnections);
        }

        @Override
        public RelationalStatement createStatement() throws SQLException {
            return getCurrentConnection(true, "createStatement").createStatement();
        }

        @Override
        public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
            return getCurrentConnection(true, "prepareStatement").prepareStatement(sql);
        }

        @Nullable
        @Override
        public MetricCollector getMetricCollector() {
            throw new UnsupportedOperationException("MultiServer does not support getting the metric collector");
        }

        @Nullable
        @Override
        public EmbeddedRelationalConnection tryGetEmbedded() {
            return null;
        }

        @Nonnull
        @Override
        public List<SemanticVersion> getVersions() {
            return this.versions;
        }

        @Nonnull
        @Override
        public SemanticVersion getInitialVersion() {
            return versions.get(0);
        }

        @Override
        public <T> T executeTransactionally(final SQLFunction<YamlConnection, T> transactionalWork)
                throws SQLException, RelationalException {
            return getCurrentConnection(true, "transactional work").executeTransactionally(transactionalWork);
        }

        @Override
        public void close() throws SQLException {
            logger.info("Sending operation {} to all connections", "close");
            for (var connection : underlyingConnections) {
                connection.close();
            }
        }

        @Override
        public void setConnectionOptions(@Nonnull final Options connectionOptions) throws SQLException {
            logger.info("Sending operation {} to all connections", "setConnectionOptions");
            for (var connection : underlyingConnections) {
                connection.setConnectionOptions(connectionOptions);
            }
        }

        @Override
        public boolean supportsMetricCollector() {
            return false;
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
        private YamlConnection getCurrentConnection(boolean advance, String op) {
            if (connectionSelectionPolicy == ConnectionSelectionPolicy.ALTERNATE) {
                YamlConnection result = underlyingConnections.get(currentConnectionSelector);
                if (logger.isInfoEnabled()) {
                    logger.info("Sending operation {} to connection: {}", op, result.toString());
                }
                if (advance) {
                    currentConnectionSelector = (currentConnectionSelector + 1) % underlyingConnections.size();
                }
                return result;
            }
            throw new IllegalStateException("Unsupported selection policy " + connectionSelectionPolicy);
        }

        private static List<SemanticVersion> createVersionsList(final int initialConnection,
                                                                final List<YamlConnection> relationalConnections) {
            List<SemanticVersion> versions = new ArrayList<>();
            for (int i = initialConnection; i < relationalConnections.size(); i++) {
                final List<SemanticVersion> underlying = relationalConnections.get(i).getVersions();
                Assert.thatUnchecked(underlying.size() == 1, "Part of multi server config has more than one version");
                versions.add(underlying.get(0));
            }
            for (int i = 0; i < initialConnection; i++) {
                final List<SemanticVersion> underlying = relationalConnections.get(i).getVersions();
                Assert.thatUnchecked(underlying.size() == 1, "Part of multi server config has more than one version");
                versions.add(underlying.get(0));
            }
            return List.copyOf(versions);
        }
    }
}
