/*
 * ConnectionTarget.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * A resolved connection target consisting of a URI and a cluster index.
 *
 * <p>The cluster index identifies which FDB cluster to connect to. Index 0 is the default.
 * Additional clusters can be accessed in YAMSQL files using the map form
 * of the {@code connect} directive:
 * <pre>{@code
 * connect: { cluster: 1, uri: 0 }
 * }</pre>
 * The uri component can be a few different things:
 * <ul>
 *     <li>An index, in which case {@code 0} is the catalog, and other positive numbers refer to the schemas created
 *     automatically with the {@code schema_template:} block</li>
 *     <li>A fully qualified uri such as {@code "jdbc:embed:/FRL/MCI_DB?schema=S1"} or
 *     {@code "jdbc:embed:/__SYS?schema=CATALOG"}. The scheme should always be {@code jdbc:embed:}, and the framework
 *     will update it to control whether it goes to the embedded connection, or one of the servers.</li>
 * </ul>
 */
public class ConnectionTarget {
    @Nonnull
    private final URI uri;
    private final int clusterIndex;

    public ConnectionTarget(@Nonnull URI uri, int clusterIndex) {
        this.uri = uri;
        this.clusterIndex = clusterIndex;
    }

    @Nonnull
    public URI getUri() {
        return uri;
    }

    public int getClusterIndex() {
        return clusterIndex;
    }

    @Override
    public String toString() {
        if (clusterIndex == 0) {
            return uri.toString();
        }
        return uri + " [cluster=" + clusterIndex + "]";
    }
}
