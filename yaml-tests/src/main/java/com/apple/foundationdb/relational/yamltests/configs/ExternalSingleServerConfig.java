/*
 * ExternalSingleServerConfig.java
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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.connectionfactory.ExternalServerYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nonnull;

/**
 * Run against a single external server.
 */
public class ExternalSingleServerConfig implements YamlTestConfig {

    private final ExternalServer server;

    public ExternalSingleServerConfig(ExternalServer server) {
        this.server = server;
    }

    @Override
    public void beforeAll() throws Exception {
    }

    @Override
    public void afterAll() throws Exception {
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new ExternalServerYamlConnectionFactory(server);
    }

    @Override
    public String toString() {
        return "ExternalServer (" + server.getVersion() + ")";
    }

    @Override
    public @Nonnull YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

}
