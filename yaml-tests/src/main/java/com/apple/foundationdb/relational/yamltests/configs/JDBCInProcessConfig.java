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
import javax.annotation.Nullable;

/**
 * Run against an embedded JDBC server.
 */
public class JDBCInProcessConfig implements YamlTestConfig {
    @Nullable
    private InProcessRelationalServer server;
    @Nullable
    private final String clusterFile;

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
    }

    @Override
    public void afterAll() throws Exception {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new JDBCInProcessYamlConnectionFactory(server);
    }

    @Override
    public @Nonnull YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

    @Override
    public String toString() {
        return "JDBC In-Process";
    }
}
