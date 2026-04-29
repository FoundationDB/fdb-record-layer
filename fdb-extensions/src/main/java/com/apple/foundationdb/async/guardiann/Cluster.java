/*
 * Cluster.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

class Cluster {
    @Nonnull
    private final ClusterMetadata clusterMetadata;
    @Nonnull
    private final Transformed<RealVector> centroid;
    @Nonnull
    private final List<VectorReference> vectorReferences;

    public Cluster(@Nonnull final ClusterMetadata clusterMetadata, @Nonnull final Transformed<RealVector> centroid,
                   @Nonnull final List<VectorReference> vectorReferences) {
        this.clusterMetadata = clusterMetadata;
        this.centroid = centroid;
        this.vectorReferences = vectorReferences;
    }

    @Nonnull
    public ClusterMetadata getClusterMetadata() {
        return clusterMetadata;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    public List<VectorReference> getVectorReferences() {
        return vectorReferences;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Cluster cluster = (Cluster)o;
        return Objects.equals(getClusterMetadata(), cluster.getClusterMetadata()) &&
                Objects.equals(getCentroid(), cluster.getCentroid()) &&
                Objects.equals(getVectorReferences(), cluster.getVectorReferences());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterMetadata(), getCentroid(), getVectorReferences());
    }

    @Override
    public String toString() {
        return "C[" + clusterMetadata + "]";
    }
}
