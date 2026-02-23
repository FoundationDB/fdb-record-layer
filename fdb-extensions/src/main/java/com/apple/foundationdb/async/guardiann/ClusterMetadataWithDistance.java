/*
 * ClusterMetadataWithDistance.java
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
import java.util.Objects;

class ClusterMetadataWithDistance {
    @Nonnull
    private final ClusterMetadata clusterMetadata;

    @Nonnull
    private final Transformed<RealVector> centroid;
    private final double distance;

    public ClusterMetadataWithDistance(@Nonnull final ClusterMetadata clusterMetadata,
                                       @Nonnull final Transformed<RealVector> centroid,
                                       final double distance) {
        this.centroid = centroid;
        this.distance = distance;
        this.clusterMetadata = clusterMetadata;
    }

    @Nonnull
    public ClusterMetadata getClusterMetadata() {
        return clusterMetadata;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    public double getDistance() {
        return distance;
    }

    @Nonnull
    public ClusterMetadataWithDistance withNewDistance(final double newDistance) {
        return new ClusterMetadataWithDistance(getClusterMetadata(), getCentroid(), newDistance);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadataWithDistance that = (ClusterMetadataWithDistance)o;
        return  Objects.equals(getClusterMetadata(), that.getClusterMetadata()) &&
                Objects.equals(getCentroid(), that.getCentroid());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCentroid(), getClusterMetadata());
    }
}
