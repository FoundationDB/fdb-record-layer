/*
 * JDBCInProcessYamlConnectionFactory.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.jdbc.JDBCURI;
import com.apple.foundationdb.relational.server.InProcessRelationalServer;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class JDBCInProcessYamlConnectionFactory implements YamlConnectionFactory {
    private static final Logger LOG = LogManager.getLogger(JDBCInProcessYamlConnectionFactory.class);
    private final InProcessRelationalServer server;
    private final String clusterFile;
    @Nonnull
    private final List<ClusterServer> additionalClusterServers;

    public JDBCInProcessYamlConnectionFactory(final InProcessRelationalServer server, final String clusterFile) {
        this(server, clusterFile, List.of());
    }

    public JDBCInProcessYamlConnectionFactory(final InProcessRelationalServer server, final String clusterFile,
                                              @Nonnull List<ClusterServer> additionalClusterServers) {
        this.server = server;
        this.clusterFile = clusterFile;
        this.additionalClusterServers = additionalClusterServers;
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        return createConnection(connectPath, server, clusterFile);
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath, int clusterIndex) throws SQLException {
        if (clusterIndex == 0) {
            return getNewConnection(connectPath);
        }
        final int idx = clusterIndex - 1;
        if (idx >= additionalClusterServers.size()) {
            throw new SQLException("Cluster index " + clusterIndex + " not available (only " +
                    (additionalClusterServers.size() + 1) + " clusters configured)");
        }
        final ClusterServer clusterServer = additionalClusterServers.get(idx);
        return createConnection(connectPath, clusterServer.server, clusterServer.clusterFile);
    }

    @Override
    public int getAvailableClusterCount() {
        return 1 + additionalClusterServers.size();
    }

    private YamlConnection createConnection(@Nonnull URI connectPath, @Nonnull InProcessRelationalServer targetServer,
                                            @Nonnull String targetClusterFile) throws SQLException {
        URI connectPathPlusServerName = JDBCURI.addQueryParameter(connectPath, JDBCURI.INPROCESS_URI_QUERY_SERVERNAME_KEY, targetServer.getServerName());
        String uriStr = connectPathPlusServerName.toString().replaceFirst("embed:", "relational://");
        if (LOG.isInfoEnabled()) {
            LOG.info(KeyValueLogMessage.of("Rewrote connection string for in-process server",
                    "original", connectPath,
                    "rewritten", uriStr,
                    "server", targetServer.getServerName()));
        }
        return new SimpleYamlConnection(DriverManager.getConnection(uriStr), SemanticVersion.current(), "JDBC In-Process", targetClusterFile);
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return Set.of(SemanticVersion.current());
    }

    /**
     * A server associated with its cluster file, for additional (non-primary) clusters.
     */
    public static class ClusterServer {
        @Nonnull
        final InProcessRelationalServer server;
        @Nonnull
        final String clusterFile;

        public ClusterServer(@Nonnull InProcessRelationalServer server, @Nonnull String clusterFile) {
            this.server = server;
            this.clusterFile = clusterFile;
        }
    }
}
