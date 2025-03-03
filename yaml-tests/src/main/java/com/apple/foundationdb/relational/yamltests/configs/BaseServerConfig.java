/*
 * EmbeddedConfig.java
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

package com.apple.foundationdb.relational.yamltests.configs;

import com.apple.foundationdb.relational.jdbc.JDBCURI;
import com.apple.foundationdb.relational.server.InProcessRelationalServer;
import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * An abstract class for server-based configs. It holds a few common utilities for
 * external-server-based configs to use
 */
public abstract class BaseServerConfig implements YamlTestConfig {
    private static final Logger LOG = LogManager.getLogger(BaseServerConfig.class);

    protected YamlConnectionFactory createEmbeddedConnectionFactory() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()), SemanticVersion.current());
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(SemanticVersion.current());
            }

        };
    }

    protected YamlConnectionFactory createJDBCInProcessConnectionFactory(final InProcessRelationalServer server) {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                // Add name of the in-process running server to the connectPath.
                URI connectPathPlusServerName = JDBCURI.addQueryParameter(connectPath, JDBCURI.INPROCESS_URI_QUERY_SERVERNAME_KEY, server.getServerName());
                String uriStr = connectPathPlusServerName.toString().replaceFirst("embed:", "relational://");
                LOG.info("Rewrote {} as {}", connectPath, uriStr);
                return new SimpleYamlConnection(DriverManager.getConnection(uriStr), SemanticVersion.current());
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(SemanticVersion.current());
            }
        };
    }

    protected YamlConnectionFactory createExternalServerConnectionFactory(@Nonnull ExternalServer externalServer) {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + externalServer.getPort());
                LOG.info("Rewrote {} as {}", connectPath, uriStr);
                return new SimpleYamlConnection(DriverManager.getConnection(uriStr), externalServer.getVersion());
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(externalServer.getVersion());
            }

        };
    }

    protected YamlConnectionFactory createMultiServerConnectionFactory(final int initialConnection,
                                                                    @Nonnull final YamlConnectionFactory defaultFactory,
                                                                    @Nonnull final List<YamlConnectionFactory> alternateFactories) {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                defaultFactory,
                alternateFactories);
    }
}
