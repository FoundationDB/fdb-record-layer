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

/**
 * A {@link ClusterMetadata} paired with its centroid and the distance from a reference point (typically the query
 * vector, or a target cluster's centroid during repartitioning) to that centroid. Carrying the distance alongside
 * the metadata lets search and split/merge logic process clusters in order of increasing distance.
 *
 * @param clusterMetadata the cluster's metadata
 * @param centroid the transformed centroid of the cluster
 * @param distance the distance from the reference point to {@code centroid}
 */
record ClusterMetadataWithDistance(@Nonnull ClusterMetadata clusterMetadata,
                                   @Nonnull Transformed<RealVector> centroid,
                                   double distance) {
    @Nonnull
    public ClusterMetadataWithDistance withNewDistance(final double newDistance) {
        return new ClusterMetadataWithDistance(clusterMetadata(), centroid(), newDistance);
    }
}
