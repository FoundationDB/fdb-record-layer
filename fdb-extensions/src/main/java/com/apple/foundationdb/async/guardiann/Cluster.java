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

/**
 * Represents a fully materialized cluster: its metadata, its centroid in the transformed coordinate space,
 * and the list of vector references it contains (both primary and replicated). Instances are created by
 * fetching a cluster's metadata and vector references from storage, typically during split, merge, or
 * reassign operations that need to inspect and repartition the cluster's contents.
 *
 * @param clusterMetadata the cluster's metadata (ID, vector counts, running standard deviation, state flags)
 * @param centroid the cluster's centroid in the transformed coordinate space
 * @param vectorReferences all vector references stored in this cluster (primary and replicated)
 */
record Cluster(@Nonnull ClusterMetadata clusterMetadata,
               @Nonnull Transformed<RealVector> centroid,
               @Nonnull List<VectorReference> vectorReferences) {
    @Override
    @Nonnull
    public String toString() {
        return "C[" + clusterMetadata + "]";
    }
}
