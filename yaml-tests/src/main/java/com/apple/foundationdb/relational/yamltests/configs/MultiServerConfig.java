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
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class MultiServerConfig implements YamlTestConfig {

    private final JDBCInProcessConfig embeddedConfig;
    private final ExternalServer externalServer;
    private int initialConnection;

    public MultiServerConfig(final int initialConnection, final int grpcPort, final int httpPort) {
        this.initialConnection = initialConnection;
        embeddedConfig = new JDBCInProcessConfig();
        externalServer = new ExternalServer(grpcPort, httpPort);
    }

    @Override
    public void beforeAll() throws Exception {
        embeddedConfig.beforeAll();
        externalServer.start();
    }

    @Override
    public void afterAll() throws Exception {
        try {
            externalServer.stop();
        } finally {
            embeddedConfig.afterAll();
        }
    }

    @Override
    public YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                embeddedConfig.createConnectionFactory(),
                List.of(createExternalServerConnection()));
    }

    YamlRunner.YamlConnectionFactory createExternalServerConnection() {
        return new YamlRunner.YamlConnectionFactory() {
            @Override
            public Connection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + externalServer.getPort());
                return DriverManager.getConnection(uriStr);
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
            return "MultiServer (Embedded then External)";
        } else {
            return "MultiServer (External then Embedded)";
        }
    }
}
