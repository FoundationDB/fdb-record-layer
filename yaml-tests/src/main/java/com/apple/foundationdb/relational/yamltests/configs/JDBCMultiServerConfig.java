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

import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.ExternalServerYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Run against an embedded JDBC driver, and an external server, alternating commands that go against each.
 */
public class JDBCMultiServerConfig extends JDBCInProcessConfig {

    private final ExternalServer externalServer;
    private final int initialConnection;

    public JDBCMultiServerConfig(final int initialConnection, ExternalServer externalServer) {
        this(initialConnection, externalServer, null);
    }

    public JDBCMultiServerConfig(final int initialConnection, ExternalServer externalServer,
                                 @Nullable final String clusterFile) {
        super(clusterFile);
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
                List.of(new ExternalServerYamlConnectionFactory(externalServer)));
    }

    @Override
    public String toString() {
        if (initialConnection == 0) {
            return "MultiServer (" + super.toString() + " then " + externalServer.getVersion() + ")";
        } else {
            return "MultiServer (" + externalServer.getVersion() + " then " + super.toString() + ")";
        }
    }
}
