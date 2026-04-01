/*
 * EmbeddedYamlConnectionFactory.java
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

package com.apple.foundationdb.relational.yamltests.connectionfactory;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class EmbeddedYamlConnectionFactory implements YamlConnectionFactory {
    @Nonnull
    private final List<ClusterDriver> clusterDrivers;

    public EmbeddedYamlConnectionFactory(@Nonnull List<ClusterDriver> clusterDrivers) {
        if (clusterDrivers.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster driver is required");
        }
        this.clusterDrivers = clusterDrivers;
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
        // The primary cluster's driver is registered in DriverManager, so use that path
        return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()),
                SemanticVersion.current(), "Embedded", clusterDrivers.get(0).clusterFile());
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull URI connectPath, int clusterIndex) throws SQLException {
        if (clusterIndex == 0) {
            return getNewConnection(connectPath);
        }
        if (clusterIndex < 0 || clusterIndex >= clusterDrivers.size()) {
            throw new SQLException("Cluster index " + clusterIndex + " not available (only " +
                    clusterDrivers.size() + " clusters configured)");
        }
        // Non-primary clusters are not registered in DriverManager, so connect via the driver directly
        final ClusterDriver clusterDriver = clusterDrivers.get(clusterIndex);
        return new SimpleYamlConnection(
                clusterDriver.driver().connect(connectPath, Options.NONE),
                SemanticVersion.current(),
                "Embedded[cluster=" + clusterIndex + "]",
                clusterDriver.clusterFile());
    }

    @Override
    public int getAvailableClusterCount() {
        return clusterDrivers.size();
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return Set.of(SemanticVersion.current());
    }

    /**
     * A driver associated with its cluster file.
     */
    public static class ClusterDriver {
        @Nonnull
        private final RelationalDriver driver;
        @Nonnull
        private final String clusterFile;

        public ClusterDriver(@Nonnull RelationalDriver driver, @Nonnull String clusterFile) {
            this.driver = driver;
            this.clusterFile = clusterFile;
        }

        @Nonnull
        public RelationalDriver driver() {
            return driver;
        }

        @Nonnull
        public String clusterFile() {
            return clusterFile;
        }
    }
}
