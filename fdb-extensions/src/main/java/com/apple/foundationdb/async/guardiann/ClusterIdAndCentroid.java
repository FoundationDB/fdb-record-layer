/*
 * ClusterIdAndCentroid.java
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
import com.apple.foundationdb.util.Lens;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

class ClusterIdAndCentroid {
    static Lens<ClusterMetadataWithDistance, ClusterIdAndCentroid> FROM_CLUSTER_METADATA_AND_DISTANCE =
            new Lens<>() {
                @Override
                public ClusterIdAndCentroid get(@Nonnull final ClusterMetadataWithDistance clusterMetadataWithDistance) {
                    return new ClusterIdAndCentroid(clusterMetadataWithDistance.getClusterMetadata().getId(),
                            clusterMetadataWithDistance.getCentroid());
                }

                @Nonnull
                @Override
                public ClusterMetadataWithDistance set(@Nullable final ClusterMetadataWithDistance clusterMetadataWithDistance,
                                                       @Nullable final ClusterIdAndCentroid clusterIdAndCentroid) {
                    throw new UnsupportedOperationException("unsupported");
                }
            };

    @Nonnull
    private final UUID clusterId;

    @Nonnull
    private final Transformed<RealVector> centroid;

    public ClusterIdAndCentroid(@Nonnull final UUID clusterId,
                                @Nonnull final Transformed<RealVector> centroid) {
        this.clusterId = clusterId;
        this.centroid = centroid;
    }

    @Nonnull
    public UUID getClusterId() {
        return clusterId;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterIdAndCentroid that = (ClusterIdAndCentroid)o;
        return  getClusterId().equals(that.getClusterId()) &&
                getCentroid().equals(that.getCentroid());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterId(), getCentroid());
    }

    @Nonnull
    static List<ClusterIdAndCentroid> fromClusterMetadataAndDistances(@Nonnull List<ClusterMetadataWithDistance> clusterMetadataWithDistances) {
        return Lens.extract(FROM_CLUSTER_METADATA_AND_DISTANCE, clusterMetadataWithDistances);
    }
}
