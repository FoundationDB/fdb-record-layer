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
import com.apple.foundationdb.relational.yamltests.connectionfactory.JDBCInProcessYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Run against an embedded JDBC driver, and an external server, alternating commands that go against each.
 * <p>
 * Multi-cluster support (additional in-process servers for non-primary cluster files) is inherited from
 * {@link JDBCInProcessConfig}. When additional cluster external servers are provided, they are passed to
 * the {@link ExternalServerYamlConnectionFactory} so that cluster-specific connections also alternate.
 */
public class JDBCMultiServerConfig extends JDBCInProcessConfig {

    private final ExternalServer externalServer;
    private final int initialConnection;
    @Nonnull
    private final List<ExternalServer> additionalClusterExternalServers;

    public JDBCMultiServerConfig(final int initialConnection, ExternalServer externalServer) {
        this(initialConnection, externalServer, null, List.of());
    }

    public JDBCMultiServerConfig(final int initialConnection, ExternalServer externalServer,
                                 @Nullable final String clusterFile) {
        this(initialConnection, externalServer, clusterFile, List.of());
    }

    public JDBCMultiServerConfig(final int initialConnection, ExternalServer externalServer,
                                 @Nullable final String clusterFile,
                                 @Nonnull List<ExternalServer> additionalClusterExternalServers) {
        super(clusterFile);
        this.initialConnection = initialConnection;
        this.externalServer = externalServer;
        this.additionalClusterExternalServers = additionalClusterExternalServers;
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        final JDBCInProcessYamlConnectionFactory jdbcFactory =
                new JDBCInProcessYamlConnectionFactory(getServer(), getClusterFile(), buildClusterServers());

        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                jdbcFactory,
                List.of(new ExternalServerYamlConnectionFactory(externalServer, additionalClusterExternalServers)));
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
