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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.connectionfactory.Clusters;
import com.apple.foundationdb.relational.yamltests.connectionfactory.EmbeddedYamlConnectionFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * Run directly against an instance of {@link FRL}.
 * <p>
 * Starts one FRL per cluster file so that multi-cluster tests
 * (using {@code connect: { cluster: N }}) can route to the correct cluster.
 */
public class EmbeddedConfig implements YamlTestConfig {
    @Nonnull
    private final List<String> clusterFiles;
    @Nonnull
    private Clusters<Clusters.Entry<FRL>> clusters = Clusters.empty();

    public EmbeddedConfig(@Nonnull final String clusterFile) {
        this(List.of(clusterFile));
    }

    public EmbeddedConfig(@Nonnull final List<String> clusterFiles) {
        this.clusterFiles = clusterFiles;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") // FRLs are tracked in the list and closed in afterAll()
    public void beforeAll() throws Exception {
        var options = Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 10)
                .build();
        // The primary FRL registers its driver in DriverManager; additional ones do not
        final String registeredCluster = clusterFiles.get(0);
        clusters = Clusters.fromClusterFilesAsEntries(clusterFiles,
                clusterFile -> {
                    try {
                        return new FRL(options, clusterFile, Objects.equals(clusterFile, registeredCluster));
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    }
                });
    }

    @Override
    @SuppressWarnings("PMD.CloseResource") // FRLs are being closed in this loop
    public void afterAll() throws Exception {
        for (final Clusters.Entry<FRL> cluster : clusters) {
            cluster.server().close();
        }
        clusters = Clusters.empty();
    }

    @Override
    public YamlConnectionFactory createConnectionFactory() {
        return new EmbeddedYamlConnectionFactory(clusters.map(e -> Clusters.mapEntry(e, FRL::getDriver)));
    }

    @Nonnull
    @Override
    public YamlExecutionContext.ContextOptions getRunnerOptions() {
        return YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
    }

    @Override
    public String toString() {
        return "Embedded";
    }
}
