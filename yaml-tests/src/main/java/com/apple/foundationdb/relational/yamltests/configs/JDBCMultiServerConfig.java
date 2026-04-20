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
import com.apple.foundationdb.relational.yamltests.connectionfactory.Clusters;
import com.apple.foundationdb.relational.yamltests.connectionfactory.ExternalServerYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Run against an embedded JDBC driver, and an external server, alternating commands that go against each.
 * <p>
 * Multi-cluster support (in-process servers for all cluster files) is inherited from
 * {@link JDBCInProcessConfig}. The external server clusters should have one entry per cluster file
 * (matching the in-process servers), so that cluster-specific connections also alternate.
 */
public class JDBCMultiServerConfig extends JDBCInProcessConfig {

    @Nonnull
    private final Clusters<ExternalServer> externalServers;
    private final int initialConnection;

    public JDBCMultiServerConfig(final int initialConnection, @Nonnull Clusters<ExternalServer> externalServers) {
        super(externalServers.clusterFiles());
        this.initialConnection = initialConnection;
        this.externalServers = externalServers;
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                super.createConnectionFactory(),
                List.of(new ExternalServerYamlConnectionFactory(externalServers)));
    }

    @Override
    public String toString() {
        final SemanticVersion externalVersion = externalServers.getInfo(ExternalServer::getVersion);
        if (initialConnection == 0) {
            return "MultiServer (" + super.toString() + " then " + externalVersion + ")";
        } else {
            return "MultiServer (" + externalVersion + " then " + super.toString() + ")";
        }
    }
}
