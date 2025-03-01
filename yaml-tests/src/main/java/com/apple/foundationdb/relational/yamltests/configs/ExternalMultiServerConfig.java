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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.server.ExternalServer;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Run against multiple external servers, alternating commands that go against each.
 */
public class ExternalMultiServerConfig extends BaseServerConfig {

    private final int initialConnection;
    private final ExternalServer server0;
    private final ExternalServer server1;

    public ExternalMultiServerConfig(final int initialConnection, ExternalServer server0, ExternalServer server1) {
        super();
        this.initialConnection = initialConnection;
        this.server0 = server0;
        this.server1 = server1;
    }

    @Override
    public void beforeAll() throws Exception {
    }

    @Override
    public void afterAll() throws Exception {
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return createMultiServerConnectionFactory(
                initialConnection,
                createExternalServerConnectionFactory(server0),
                List.of(createExternalServerConnectionFactory(server1)));
    }

    @Override
    public String toString() {
        if (initialConnection == 0) {
            return "MultiServer (" + server0.getVersion() + " then " + server1.getVersion() + ")";
        } else {
            return "MultiServer (" + server1.getVersion() + " then " + server0 + ")";
        }
    }

    @Override
    public @Nonnull YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

}
