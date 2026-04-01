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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Run against an embedded JDBC server.
 * <p>
 * Starts one in-process server per cluster file so that multi-cluster tests
 * (using {@code connect: { cluster: N }}) can route to the correct cluster.
 */
public class JDBCInProcessConfig implements YamlTestConfig {
    @Nonnull
    private final List<String> clusterFiles;
    @Nonnull
    private final List<JDBCInProcessYamlConnectionFactory.ClusterServer> clusterServers = new ArrayList<>();

    public JDBCInProcessConfig(@Nonnull final List<String> clusterFiles) {
        this.clusterFiles = clusterFiles;
    }

    @Override
    public void beforeAll() throws Exception {
        for (final String clusterFile : clusterFiles) {
            final InProcessRelationalServer server = new InProcessRelationalServer(clusterFile).start();
            clusterServers.add(new JDBCInProcessYamlConnectionFactory.ClusterServer(server, clusterFile));
        }
    }

    @Override
    public void afterAll() throws Exception {
        for (final JDBCInProcessYamlConnectionFactory.ClusterServer cs : clusterServers) {
            cs.server().close();
        }
        clusterServers.clear();
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new JDBCInProcessYamlConnectionFactory(clusterServers);
    }

    @Nonnull
    protected List<JDBCInProcessYamlConnectionFactory.ClusterServer> getClusterServers() {
        return clusterServers;
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
