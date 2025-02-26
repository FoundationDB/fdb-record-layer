/*
 * EmbeddedConfig.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;

/**
 * Run directly against an instance of {@link FRL}.
 */
public class EmbeddedConfig implements YamlTestConfig {
    private FRL frl;

    @Override
    public void beforeAll() throws Exception {
        var options = Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 10)
                .build();
        frl = new FRL(options);
    }

    @Override
    public void afterAll() throws Exception {
        if (frl != null) {
            frl.close();
            frl = null;
        }
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()), SemanticVersion.current());
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(SemanticVersion.current());
            }

        };
    }

    @Override
    public @Nonnull YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

    @Override
    public String toString() {
        return "Embedded";
    }
}
