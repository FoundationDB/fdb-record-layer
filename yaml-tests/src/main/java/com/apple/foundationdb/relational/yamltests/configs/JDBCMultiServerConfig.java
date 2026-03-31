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

import com.apple.foundationdb.relational.server.InProcessRelationalServer;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.ExternalServerYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.JDBCInProcessYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;
import com.apple.foundationdb.test.FDBTestEnvironment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Run against an embedded JDBC driver, and an external server, alternating commands that go against each.
 * <p>
 * When multiple cluster files are available, starts one in-process server per additional cluster so that
 * multi-cluster tests (using {@code connect: { cluster: N }}) can route to the correct cluster.
 */
public class JDBCMultiServerConfig extends JDBCInProcessConfig {

    private final ExternalServer externalServer;
    private final int initialConnection;
    @Nonnull
    private final List<InProcessRelationalServer> additionalClusterServers = new ArrayList<>();

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
        // Start one in-process server per additional cluster file
        for (final String otherClusterFile : FDBTestEnvironment.allClusterFiles()) {
            if (!Objects.equals(otherClusterFile, getClusterFile())) {
                final InProcessRelationalServer server = new InProcessRelationalServer(otherClusterFile).start();
                additionalClusterServers.add(server);
            }
        }
    }

    @Override
    public void afterAll() throws Exception {
        for (final InProcessRelationalServer server : additionalClusterServers) {
            server.close();
        }
        additionalClusterServers.clear();
        super.afterAll();
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        // Build the JDBCInProcessYamlConnectionFactory with multi-cluster support
        final List<JDBCInProcessYamlConnectionFactory.ClusterServer> clusterServers = new ArrayList<>();
        final List<String> allClusterFiles = FDBTestEnvironment.allClusterFiles();
        for (int i = 0; i < additionalClusterServers.size(); i++) {
            final String otherClusterFile = allClusterFiles.stream()
                    .filter(cf -> !Objects.equals(cf, getClusterFile()))
                    .skip(i)
                    .findFirst()
                    .orElseThrow();
            clusterServers.add(new JDBCInProcessYamlConnectionFactory.ClusterServer(
                    additionalClusterServers.get(i), otherClusterFile));
        }
        final JDBCInProcessYamlConnectionFactory jdbcFactory =
                new JDBCInProcessYamlConnectionFactory(getServer(), getClusterFile(), clusterServers);

        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                jdbcFactory,
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
