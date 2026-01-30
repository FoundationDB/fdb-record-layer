/*
 * ExternalServerYamlConnectionFactory.java
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
import java.util.Set;

public class ExternalServerYamlConnectionFactory implements YamlConnectionFactory {
    private static final Logger LOG = LogManager.getLogger(ExternalServerYamlConnectionFactory.class);
    private final ExternalServer externalServer;

    public ExternalServerYamlConnectionFactory(final ExternalServer externalServer) {
        this.externalServer = externalServer;
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + externalServer.getPort());
        if (LOG.isInfoEnabled()) {
            LOG.info(KeyValueLogMessage.of("Rewrote connection string for external server",
                    "original", connectPath,
                    "rewritten", uriStr,
                    "version", externalServer.getVersion()));
        }
        return new SimpleYamlConnection(DriverManager.getConnection(uriStr), externalServer.getVersion(), externalServer.getClusterFile());
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return Set.of(externalServer.getVersion());
    }

}
