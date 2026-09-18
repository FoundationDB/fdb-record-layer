/*
 * ClusterView.java
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
import java.util.Set;
import java.util.UUID;

/**
 * Per-cluster view captured by {@link Search#snapshotStructure}. The vector-reference sets are keyed by
 * {@link VectorId} (primary key + UUID), which uniquely identifies a vector across the structure and supports
 * cross-cluster cross-referencing (e.g. linking a replica to its primary).
 *
 * @param clusterId the cluster's UUID
 * @param centroid the centroid in the client (untransformed) coordinate space, taken from the centroid HNSW.
 *        Useful for stable cross-snapshot comparison
 * @param transformedCentroid the centroid in the stored (transformed) coordinate space, paired with
 *        {@link StructureSnapshot#estimator()} to rank vectors against centroids for the deep invariants
 * @param metadata the cluster's stored {@link ClusterMetadata}
 * @param primaries primary-copy vector references (whether or not underreplicated; collapsed primaries are
 *        excluded from this set and appear in {@code collapsedRefs} instead)
 * @param replicas replicated copies held in this cluster for search recall
 * @param collapsedRefs collapsed references; each carries its signature in its {@code id()}'s primary-key tuple
 * @param references every vector reference stored in the cluster (primaries, replicas and collapsed), retaining
 *        each reference's flags and stored {@code replicationPriority}
 */
record ClusterView(@Nonnull UUID clusterId,
                   @Nonnull RealVector centroid,
                   @Nonnull Transformed<RealVector> transformedCentroid,
                   @Nonnull ClusterMetadata metadata,
                   @Nonnull Set<VectorId> primaries,
                   @Nonnull Set<VectorId> replicas,
                   @Nonnull Set<VectorId> collapsedRefs,
                   @Nonnull List<VectorReference> references) {
}
