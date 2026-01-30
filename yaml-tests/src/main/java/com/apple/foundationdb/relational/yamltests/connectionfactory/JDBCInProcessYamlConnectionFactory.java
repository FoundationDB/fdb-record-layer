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
import java.util.Set;

public class JDBCInProcessYamlConnectionFactory implements YamlConnectionFactory {
    private static final Logger LOG = LogManager.getLogger(JDBCInProcessYamlConnectionFactory.class);
    private final InProcessRelationalServer server;
    private final String clusterFile;

    public JDBCInProcessYamlConnectionFactory(final InProcessRelationalServer server, final String clusterFile) {
        this.server = server;
        this.clusterFile = clusterFile;
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        // Add name of the in-process running server to the connectPath.
        URI connectPathPlusServerName = JDBCURI.addQueryParameter(connectPath, JDBCURI.INPROCESS_URI_QUERY_SERVERNAME_KEY, server.getServerName());
        String uriStr = connectPathPlusServerName.toString().replaceFirst("embed:", "relational://");
        LOG.info(KeyValueLogMessage.of("Rewrote connection string for in-process server",
                "original", connectPath,
                "rewritten", uriStr,
                "server", server.getServerName()));
        return new SimpleYamlConnection(DriverManager.getConnection(uriStr), SemanticVersion.current(), "JDBC In-Process", clusterFile);
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return Set.of(SemanticVersion.current());
    }
}
