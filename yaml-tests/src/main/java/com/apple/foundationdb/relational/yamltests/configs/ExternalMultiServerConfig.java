/*
 * ExternalMultiServerConfig.java
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
import com.apple.foundationdb.relational.yamltests.connectionfactory.Clusters;
import com.apple.foundationdb.relational.yamltests.connectionfactory.ExternalServerYamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Run against multiple external servers, alternating commands that go against each.
 * Notably, even if the versions are the same, you can catch issues where something is jvm specific (e.g. planHash).
 */
public class ExternalMultiServerConfig implements YamlTestConfig {

    private final int initialConnection;
    @Nonnull
    private final Clusters<ExternalServer> servers0;
    @Nonnull
    private final Clusters<ExternalServer> servers1;

    public ExternalMultiServerConfig(final int initialConnection,
                                     @Nonnull Clusters<ExternalServer> servers0,
                                     @Nonnull Clusters<ExternalServer> servers1) {
        super();
        this.initialConnection = initialConnection;
        this.servers0 = servers0;
        this.servers1 = servers1;
    }

    @Override
    public void beforeAll() throws Exception {
    }

    @Override
    public void afterAll() throws Exception {
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                new ExternalServerYamlConnectionFactory(servers0),
                List.of(new ExternalServerYamlConnectionFactory(servers1)));
    }

    @Override
    public String toString() {
        final SemanticVersion version0 = servers0.getInfo(ExternalServer::getVersion);
        final SemanticVersion version1 = servers1.getInfo(ExternalServer::getVersion);
        if (initialConnection == 0) {
            return "MultiServer (" + version0 + " then " + version1 + ")";
        } else {
            return "MultiServer (" + version1 + " then " + version0 + ")";
        }
    }

    @Override
    public @Nonnull YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

}
