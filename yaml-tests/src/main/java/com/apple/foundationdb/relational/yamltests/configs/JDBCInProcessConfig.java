/*
 * JDBCInProcessConfig.java
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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.connectionfactory.JDBCInProcessYamlConnectionFactory;
import com.apple.foundationdb.test.FDBTestEnvironment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Run against an embedded JDBC server.
 * <p>
 * When multiple cluster files are available, starts one in-process server per additional cluster so that
 * multi-cluster tests (using {@code connect: { cluster: N }}) can route to the correct cluster.
 */
public class JDBCInProcessConfig implements YamlTestConfig {
    @Nullable
    private InProcessRelationalServer server;
    @Nullable
    private final String clusterFile;
    @Nonnull
    private final List<InProcessRelationalServer> additionalClusterServers = new ArrayList<>();

    public JDBCInProcessConfig() {
        this(null);
    }

    public JDBCInProcessConfig(@Nullable final String clusterFile) {
        this.clusterFile = clusterFile;
    }

    @Override
    public void beforeAll() throws Exception {
        try {
            server = new InProcessRelationalServer(clusterFile).start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Start one in-process server per additional cluster file
        for (final String otherClusterFile : FDBTestEnvironment.allClusterFiles()) {
            if (!Objects.equals(otherClusterFile, clusterFile)) {
                final InProcessRelationalServer additionalServer = new InProcessRelationalServer(otherClusterFile).start();
                additionalClusterServers.add(additionalServer);
            }
        }
    }

    @Override
    public void afterAll() throws Exception {
        for (final InProcessRelationalServer additionalServer : additionalClusterServers) {
            additionalServer.close();
        }
        additionalClusterServers.clear();
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new JDBCInProcessYamlConnectionFactory(server, clusterFile, buildClusterServers());
    }

    /**
     * Build the list of {@link JDBCInProcessYamlConnectionFactory.ClusterServer} entries for the additional clusters.
     */
    @Nonnull
    protected List<JDBCInProcessYamlConnectionFactory.ClusterServer> buildClusterServers() {
        final List<JDBCInProcessYamlConnectionFactory.ClusterServer> clusterServers = new ArrayList<>();
        final List<String> allClusterFiles = FDBTestEnvironment.allClusterFiles();
        for (int i = 0; i < additionalClusterServers.size(); i++) {
            final String otherClusterFile = allClusterFiles.stream()
                    .filter(cf -> !Objects.equals(cf, clusterFile))
                    .skip(i)
                    .findFirst()
                    .orElseThrow();
            clusterServers.add(new JDBCInProcessYamlConnectionFactory.ClusterServer(
                    additionalClusterServers.get(i), otherClusterFile));
        }
        return clusterServers;
    }

    protected InProcessRelationalServer getServer() {
        return server;
    }

    @Nullable
    protected String getClusterFile() {
        return clusterFile;
    }

    @Nonnull
    @Override
    public YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

    @Override
    public String toString() {
        return "JDBC In-Process";
    }
}
