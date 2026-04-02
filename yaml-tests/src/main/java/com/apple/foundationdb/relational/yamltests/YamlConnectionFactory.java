/*
 * YamlConnectionFactory.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Set;

/**
 * Connection factory to support yaml tests (see {@link YamlRunner}.
 */
public interface YamlConnectionFactory {
    /**
     * Convert a connection uri into an actual connection on a specific cluster.
     *
     * @param connectPath the path to connect to
     * @param clusterIndex the cluster to connect to (0 is the default cluster)
     *
     * @return A new {@link RelationalConnection} for the given path on the specified cluster
     *
     * @throws SQLException if we cannot connect or the cluster index is not supported
     */
    YamlConnection getNewConnection(@Nonnull URI connectPath, int clusterIndex) throws SQLException;

    /**
     * The versions that the connection has, other than the current code.
     * <p>
     * If we are just testing against the current code, this will be empty, but otherwise it will include the
     * versions that we're testing. In the future we may want to support tests that don't run against the
     * current version, but that's not currently needed, so not supported.
     * </p>
     *
     * @return A set of versions that we are testing against, or an empty set if just testing against the current
     * version
     */
    Set<SemanticVersion> getVersionsUnderTest();

    /**
     * Whether the connection supports multiple servers.
     * There are some changes to the behavior that are to be expected when running the tests in multi-server mode,
     * this method allows the system to make that decision.
     *
     * @return TRUE if this connection factory can support multiple servers, false otherwise.
     */
    default boolean isMultiServer() {
        return false;
    }

    /**
     * Returns the number of clusters available for testing.
     *
     * @return the number of available clusters (1 means only the default cluster)
     */
    default int getAvailableClusterCount() {
        return 1;
    }
}
