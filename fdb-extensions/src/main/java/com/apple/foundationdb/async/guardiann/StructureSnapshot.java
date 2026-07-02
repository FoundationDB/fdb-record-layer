/*
 * StructureSnapshot.java
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

import com.apple.foundationdb.linear.DistanceEstimator;
import com.google.common.base.Verify;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

/**
 * Compact, immutable view of a Guardiann's cluster topology at a point in time: per-cluster metadata, primary
 * vectors, replicas, and collapsed references, plus the distance estimator for the stored coordinate space. Built
 * by {@link Search#snapshotStructure} and consumed by diagnostics and the test invariant helpers. The same
 * snapshots are intended for diff-based scenario checks ("after this op, exactly these clusters changed"); the
 * diff API isn't built yet.
 *
 * @param clusters per-cluster views, keyed by cluster id (immutable)
 * @param estimator the distance estimator for the stored (transformed) coordinate space, paired with each
 *        {@link ClusterView#transformedCentroid()} by {@link #computeAssignmentRanking}; {@code null} only when
 *        the structure is empty (no clusters)
 */
record StructureSnapshot(@Nonnull Map<UUID, ClusterView> clusters,
                         @Nullable DistanceEstimator estimator) {
    /** Returns the number of clusters in this snapshot. */
    public int numClusters() {
        return clusters.size();
    }

    /** Returns the total primary-copy count across all clusters. */
    public int totalPrimaries() {
        return clusters.values().stream().mapToInt(c -> c.primaries().size()).sum();
    }

    /** Returns the total replica count across all clusters (counts each replica copy once). */
    public int totalReplicas() {
        return clusters.values().stream().mapToInt(c -> c.replicas().size()).sum();
    }

    /** Returns the total collapsed-reference count across all clusters. */
    public int totalCollapsedRefs() {
        return clusters.values().stream().mapToInt(c -> c.collapsedRefs().size()).sum();
    }

    /**
     * Returns the reverse mapping primary {@link VectorId} → owning cluster id. The build itself fails via
     * {@link Verify} if a primary appears in more than one cluster, so the returned map is guaranteed unique by
     * construction.
     */
    @Nonnull
    public Map<VectorId, UUID> primaryOwners() {
        final Map<VectorId, UUID> owners = Maps.newHashMapWithExpectedSize(totalPrimaries());
        for (final ClusterView cv : clusters.values()) {
            for (final VectorId id : cv.primaries()) {
                final UUID prior = owners.put(id, cv.clusterId());
                Verify.verify(prior == null,
                        "primary %s appears in clusters %s and %s", id, prior, cv.clusterId());
            }
        }
        return owners;
    }

    /**
     * Ranking-derived statistics behind soft invariant 3, produced by {@link #computeAssignmentRanking}.
     *
     * @param numPrimaries total number of primary vectors ranked
     * @param numWrongAssignments primaries whose nearest cluster (by centroid distance) is not their owning cluster
     */
    record AssignmentRanking(int numPrimaries,
                             int numWrongAssignments) {
    }

    /**
     * Ranks every primary in the snapshot against every cluster centroid (using this snapshot's {@link #estimator()}
     * in the stored coordinate space) and counts how many sit off their nearest centroid — i.e. whose owning cluster
     * is not the nearest one (soft invariant 3). This is O(primaries × clusters), so callers gate it on structure
     * size. An empty structure (no estimator) yields an all-zero result.
     *
     * @return the ranking-derived statistics
     */
    @Nonnull
    public AssignmentRanking computeAssignmentRanking() {
        if (estimator == null) {
            return new AssignmentRanking(0, 0);
        }
        int numPrimaries = 0;
        int numWrongAssignments = 0;
        for (final ClusterView homeCluster : clusters.values()) {
            for (final VectorReference reference : homeCluster.references()) {
                if (!reference.isPrimaryCopy()) {
                    continue;
                }
                numPrimaries++;
                ClusterView nearest = null;
                double nearestDistance = Double.POSITIVE_INFINITY;
                for (final ClusterView candidate : clusters.values()) {
                    final double distance =
                            estimator.distance(candidate.transformedCentroid(), reference.vector());
                    // Break ties on cluster id so the chosen "nearest" cluster is deterministic.
                    if (distance < nearestDistance
                            || (distance == nearestDistance && nearest != null
                                    && candidate.clusterId().compareTo(nearest.clusterId()) < 0)) {
                        nearestDistance = distance;
                        nearest = candidate;
                    }
                }
                if (nearest != null && !nearest.clusterId().equals(homeCluster.clusterId())) {
                    numWrongAssignments++;
                }
            }
        }
        return new AssignmentRanking(numPrimaries, numWrongAssignments);
    }
}
