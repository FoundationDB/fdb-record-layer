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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
     * Ranking-derived statistics behind the deep replication invariants (2 and 3), produced by
     * {@link #computeAssignmentRanking}.
     *
     * @param numPrimaries total number of primary vectors ranked
     * @param numWrongAssignments primaries whose nearest cluster (by centroid distance) is not their owning
     *        cluster (invariant 3)
     * @param replicationDemand number of (primary, neighboring-cluster) pairs whose replication priority reaches
     *        {@link Config#replicationPriorityMin()} — i.e. should be replicated (invariant 2)
     * @param replicationSatisfied of those demanded pairs, how many are actually replicated into the neighbor
     */
    record AssignmentRanking(int numPrimaries,
                             int numWrongAssignments,
                             int replicationDemand,
                             int replicationSatisfied) {
    }

    /**
     * Ranks every primary in the snapshot against every cluster centroid (using this snapshot's
     * {@link #estimator()} in the stored coordinate space) to derive the deep replication invariants: how many
     * primaries sit off their nearest centroid (invariant 3), and how much of the score-derived replication demand
     * is actually met (invariant 2). This is O(primaries × clusters), so callers gate it on structure size. An
     * empty structure (no estimator) yields an all-zero result.
     *
     * @param config the configuration whose {@link Config#replicationPriorityMin()} and
     *        {@link Config#insertMaxCandidateClusters()} govern the replication-demand decision
     *
     * @return the ranking-derived statistics
     */
    @Nonnull
    public AssignmentRanking computeAssignmentRanking(@Nonnull final Config config) {
        if (estimator == null) {
            return new AssignmentRanking(0, 0, 0, 0);
        }
        final int maxCandidates = config.insertMaxCandidateClusters();
        final double minPriority = config.replicationPriorityMin();
        final List<ClusterView> clusterList = new ArrayList<>(clusters.values());
        // Per cluster, the set of vector UUIDs it currently holds as replicas — used to check whether a
        // demanded replica is actually present.
        final Map<UUID, Set<UUID>> replicaUuidsByCluster = Maps.newHashMapWithExpectedSize(clusterList.size());
        for (final ClusterView cv : clusterList) {
            replicaUuidsByCluster.put(cv.clusterId(),
                    cv.replicas().stream().map(VectorId::getUuid).collect(Collectors.toSet()));
        }

        int numPrimaries = 0;
        int numWrongAssignments = 0;
        int replicationDemand = 0;
        int replicationSatisfied = 0;
        for (final ClusterView homeCluster : clusterList) {
            for (final VectorReference reference : homeCluster.references()) {
                if (!reference.isPrimaryCopy()) {
                    continue;
                }
                numPrimaries++;
                final Map<UUID, Double> distanceByCluster =
                        Maps.newHashMapWithExpectedSize(clusterList.size());
                for (final ClusterView candidate : clusterList) {
                    distanceByCluster.put(candidate.clusterId(),
                            estimator.distance(candidate.transformedCentroid(), reference.vector()));
                }
                final List<ClusterView> ranked = new ArrayList<>(clusterList);
                ranked.sort(Comparator.comparingDouble((ClusterView c) -> distanceByCluster.get(c.clusterId()))
                        .thenComparing(ClusterView::clusterId));
                if (!ranked.get(0).clusterId().equals(homeCluster.clusterId())) {
                    numWrongAssignments++;
                }
                final double distToHome = distanceByCluster.get(ranked.get(0).clusterId());
                final int candidateLimit = Math.min(ranked.size(), maxCandidates);
                for (int j = 1; j < candidateLimit; j++) {
                    final ClusterView candidate = ranked.get(j);
                    final ClusterMetadata candidateMetadata = candidate.metadata();
                    final double score = StorageAdapter.replicationPriority(config,
                            distanceByCluster.get(candidate.clusterId()), distToHome,
                            candidateMetadata.getNumPrimaryVectors(),
                            candidateMetadata.meanDistance(),
                            candidateMetadata.standardDeviation());
                    if (score >= minPriority) {
                        replicationDemand++;
                        if (replicaUuidsByCluster.getOrDefault(candidate.clusterId(), Set.of())
                                .contains(reference.id().getUuid())) {
                            replicationSatisfied++;
                        }
                    }
                }
            }
        }
        return new AssignmentRanking(numPrimaries, numWrongAssignments, replicationDemand, replicationSatisfied);
    }
}
