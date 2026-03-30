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
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.connectionfactory.EmbeddedYamlConnectionFactory;
import com.apple.foundationdb.test.FDBTestEnvironment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Run directly against an instance of {@link FRL}.
 */
public class EmbeddedConfig implements YamlTestConfig {
    private FRL frl;
    @Nullable
    private final String clusterFile;
    @Nonnull
    private final List<FRL> additionalClusterFrls = new ArrayList<>();

    public EmbeddedConfig(@Nullable final String clusterFile) {
        this.clusterFile = clusterFile;
    }

    @Override
    public void beforeAll() throws Exception {
        var options = Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 10)
                .build();
        frl = new FRL(options, clusterFile);

        // Create drivers for additional clusters (without registering them in DriverManager)
        for (final String otherClusterFile : FDBTestEnvironment.allClusterFiles()) {
            if (!Objects.equals(otherClusterFile, clusterFile)) {
                additionalClusterFrls.add(new FRL(options, otherClusterFile, false));
            }
        }
    }

    @Override
    public void afterAll() throws Exception {
        for (final FRL additionalFrl : additionalClusterFrls) {
            additionalFrl.close();
        }
        additionalClusterFrls.clear();
        if (frl != null) {
            frl.close();
            frl = null;
        }
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        final List<EmbeddedYamlConnectionFactory.ClusterDriver> additionalDrivers = new ArrayList<>();
        final List<String> allClusterFiles = FDBTestEnvironment.allClusterFiles();
        for (int i = 0; i < additionalClusterFrls.size(); i++) {
            // Find the cluster file for this additional FRL
            final String otherClusterFile = allClusterFiles.stream()
                    .filter(cf -> !Objects.equals(cf, clusterFile))
                    .skip(i)
                    .findFirst()
                    .orElseThrow();
            additionalDrivers.add(new EmbeddedYamlConnectionFactory.ClusterDriver(
                    additionalClusterFrls.get(i).getDriver(), otherClusterFile));
        }
        return new EmbeddedYamlConnectionFactory(clusterFile, additionalDrivers);
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
