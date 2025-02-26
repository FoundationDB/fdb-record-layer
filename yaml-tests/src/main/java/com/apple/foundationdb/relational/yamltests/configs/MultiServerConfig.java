/*
 * MultiServerConfig.java
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

import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Run against an embedded JDBC driver, and an external server, alternating commands that go against each.
 */
public class MultiServerConfig extends JDBCInProcessConfig {

    private final ExternalServer externalServer;
    private final int initialConnection;

    public MultiServerConfig(final int initialConnection, ExternalServer externalServer) {
        super();
        this.initialConnection = initialConnection;
        this.externalServer = externalServer;
    }

    @Override
    public void beforeAll() throws Exception {
        super.beforeAll();
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                super.createConnectionFactory(),
                List.of(createExternalServerConnection()));
    }

    YamlConnectionFactory createExternalServerConnection() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + externalServer.getPort());
                return new SimpleYamlConnection(DriverManager.getConnection(uriStr), externalServer.getVersion());
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of(externalServer.getVersion());
            }
        };
    }

    @Override
    public String toString() {
        if (initialConnection == 0) {
            return "MultiServer (Embedded then " + externalServer.getVersion() + ")";
        } else {
            return "MultiServer (" + externalServer.getVersion() + " then Embedded)";
        }
    }
}
