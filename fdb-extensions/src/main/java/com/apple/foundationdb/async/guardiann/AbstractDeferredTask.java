/*
 * AbstractDeferredTask.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class AbstractDeferredTask {
    @Nonnull
    private final Locator locator;
    @Nonnull
    private final AccessInfo accessInfo;
    @Nonnull
    private final UUID taskId;
    @Nonnull
    private final Set<UUID> targetClusterIds;

    AbstractDeferredTask(@Nonnull final Locator locator,
                         @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId,
                         @Nonnull final Set<UUID> targetClusterId) {
        this.locator = locator;
        this.accessInfo = accessInfo;
        this.taskId = taskId;
        this.targetClusterIds = ImmutableSet.copyOf(targetClusterId);
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    @Nonnull
    Primitives primitives() {
        return getLocator().primitives();
    }

    @Nonnull
    OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    @Nonnull
    OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    AccessInfo getAccessInfo() {
        return accessInfo;
    }

    @Nonnull
    public UUID getTaskId() {
        return taskId;
    }

    @Nonnull
    public Set<UUID> getTargetClusterIds() {
        return targetClusterIds;
    }

    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    @Nonnull
    public abstract Tuple valueTuple();

    @Nonnull
    public abstract CompletableFuture<Void> runTask(@Nonnull Transaction transaction);

    protected void logStart(@Nonnull final Logger logger) {
        if (logger.isDebugEnabled()) {
            logger.debug("executing task kind={}, taskId={}, targetClusterId={}", getKind(), taskIdToString(getTaskId()),
                    getTargetClusterIds());
        }
    }

    protected void logSuccessful(@Nonnull final Logger logger) {
        if (logger.isDebugEnabled()) {
            logger.debug("successfully finished executing task kind={}, taskId={}", getKind(),
                    taskIdToString(getTaskId()));
        }
    }

    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        primitives().writeDeferredTask(transaction, this.getTaskId(), valueTuple());
        getOnWriteListener().onTaskEnqueued(this.getKind(), this.getTaskId(), this.getTargetClusterIds());
    }

    @Nonnull
    public abstract Kind getKind();

    boolean enqueueCollapseIfNecessary(@Nonnull final Transaction transaction,
                                       @Nonnull final SplittableRandom random,
                                       @Nonnull final List<VectorReference> primaryVectorReferences,
                                       @Nonnull final UUID targetClusterId,
                                       @Nonnull final Transformed<RealVector> centroid) {
        final Config config = getConfig();
        final Primitives primitives = primitives();
        final Map<UUID, Integer> collapsibleVectorsCountersMap =
                CollapseTask.collapsibleVectorsCountersMap(primaryVectorReferences);
        final int maximumNumberCollapsibleVectorsPerDuplicate =
                CollapseTask.maxDuplicateCount(collapsibleVectorsCountersMap);
        if (maximumNumberCollapsibleVectorsPerDuplicate > 10) {
            final UUID collapseTaskId = AbstractDeferredTask.randomHighPriorityTaskId(random,
                    config.deterministicRandomness());
            final CollapseTask collapseTask =
                    CollapseTask.of(getLocator(), getAccessInfo(), collapseTaskId,
                            targetClusterId, centroid);
            final UUID bounceTaskId = randomHighPriorityTaskId(random,
                    config.deterministicRandomness());
            collapseTask.writeDeferredTask(transaction);
            final BounceTask bounceTask =
                    BounceTask.of(getLocator(), getAccessInfo(), bounceTaskId,
                            ImmutableSet.of(targetClusterId),
                            ImmutableSet.of(collapseTaskId),
                            AbstractDeferredTask.Kind.SPLIT_MERGE);
            bounceTask.writeDeferredTask(transaction);
            return true;
        }
        return false;
    }

    @Nonnull
    static AbstractDeferredTask newFromTuples(@Nonnull final Locator locator,
                                              @Nonnull final AccessInfo accessInfo,
                                              @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        final Kind kind = Kind.fromValueTuple(valueTuple);
        return kind.create(locator, accessInfo, keyTuple, valueTuple);
    }

    @Nonnull
    protected static UUID randomHighPriorityTaskId(@Nonnull final SplittableRandom random, final boolean isDeterministic) {
        return uuidToHighPriorityTaskId(isDeterministic ? RandomHelpers.randomUuid(random) : UUID.randomUUID());
    }

    @Nonnull
    private static UUID uuidToHighPriorityTaskId(@Nonnull final UUID uuid) {
        return new UUID(uuid.getMostSignificantBits() & 0x7fffffffffffffffL,
                uuid.getLeastSignificantBits());
    }

    @Nonnull
    protected static UUID randomNormalPriorityTaskId(@Nonnull final SplittableRandom random, final boolean isDeterministic) {
        return uuidToNormalPriorityTaskId(isDeterministic ? RandomHelpers.randomUuid(random) : UUID.randomUUID());
    }

    @Nonnull
    private static UUID uuidToNormalPriorityTaskId(@Nonnull final UUID uuid) {
        return new UUID(uuid.getMostSignificantBits() | 0x8000000000000000L,
                uuid.getLeastSignificantBits());
    }

    @Nonnull
    static String taskIdToString(@Nonnull final UUID taskId) {
        return (isHighPriority(taskId) ? "NORMAL" : "HIGH") + ":" + taskId;
    }

    static boolean isHighPriority(@Nonnull final UUID taskId) {
        return (taskId.getMostSignificantBits() & 0x8000000000000000L) != 0;
    }

    @CanIgnoreReturnValue
    static <T> int incrementCounter(@Nonnull final Map<T, Integer> countersMap, @Nonnull final T key) {
        return  countersMap.compute(key, (ignoredKey, oldCounter) -> {
            if (oldCounter == null) {
                return 1;
            }
            return oldCounter + 1;
        });
    }

    @CanIgnoreReturnValue
    static <T> RunningStats
            updateRunningStatssMap(@Nonnull final Map<T, RunningStats> map,
                                               @Nonnull final T key, final double distance) {
        return map.compute(key, (ignoredKey, old) -> {
            if (old == null) {
                return RunningStats.of(distance);
            }
            return old.add(distance);
        });
    }

    /**
     * Computes the diff between the old cluster contents and the new assigned vectors. Vectors present
     * in both old and new are checked for status changes (primary/replicated/underreplicated toggling);
     * vectors only in the old cluster are marked for deletion.
     *
     * @param targetCluster the cluster as it currently exists
     * @param newAssignments the new vector references that should be in the cluster
     * @return the delta of writes and deletes
     */
    @Nonnull
    static TargetClusterDelta computeTargetClusterDelta(@Nonnull final Cluster targetCluster,
                                                        @Nonnull final List<VectorReference> newAssignments) {
        final ImmutableMap.Builder<Tuple, VectorReference> assignedByPrimaryKeyBuilder = ImmutableMap.builder();
        for (final VectorReference assignedVector : newAssignments) {
            assignedByPrimaryKeyBuilder.put(assignedVector.id().getPrimaryKey(), assignedVector);
        }
        final ImmutableMap<Tuple, VectorReference> assignedByPrimaryKey = assignedByPrimaryKeyBuilder.build();

        final ImmutableList.Builder<Tuple> toDeleteBuilder = ImmutableList.builder();
        final ImmutableList.Builder<VectorReference> toWriteBuilder = ImmutableList.builder();

        for (final VectorReference vectorReference : targetCluster.vectorReferences()) {
            final Tuple primaryKey = vectorReference.id().getPrimaryKey();
            final VectorReference assignedVectorReference = assignedByPrimaryKey.get(primaryKey);

            if (assignedVectorReference != null) {
                Verify.verify(assignedVectorReference.id().getUuid()
                        .equals(vectorReference.id().getUuid()));

                if (assignedVectorReference.isPrimaryCopy() != vectorReference.isPrimaryCopy() ||
                        assignedVectorReference.isUnderreplicated() != vectorReference.isUnderreplicated()) {
                    toWriteBuilder.add(assignedVectorReference);
                }
            } else {
                toDeleteBuilder.add(primaryKey);
            }
        }

        return new TargetClusterDelta(toWriteBuilder.build(), toDeleteBuilder.build());
    }

    record TargetClusterDelta(@Nonnull ImmutableList<VectorReference> toWrite,
                              @Nonnull ImmutableList<Tuple> toDelete) {
    }

    /**
     * Computes, for each primary vector, the k nearest clusters (sorted by distance) from the given
     * candidate cluster map. Non-primary vector references in the input are silently skipped.
     * Returns a result containing an inverted assignments map (keyed by vector UUID, values are nearest
     * clusters in ascending distance order) and a map of running standard deviation updates accumulated
     * from each vector's primary (nearest) cluster assignment.
     *
     * <p>
     * The caller is responsible for merging the returned standard deviation updates into its own
     * tracking map via {@link #mergeStandardDeviationUpdates}.
     * </p>
     *
     * @param estimator distance estimator for the vector space
     * @param vectorReferences vector references to process (only primary copies are considered)
     * @param candidateClusters all clusters (new + outer) that vectors may be assigned to
     * @return the nearest cluster assignments and accumulated standard deviation updates
     */
    @Nonnull
    static NearestClustersResult computeNearestClusters(@Nonnull final DistanceEstimator estimator,
                                                        @Nonnull final List<VectorReference> vectorReferences,
                                                        @Nonnull final Map<UUID, ClusterMetadataWithDistance> candidateClusters) {
        final ImmutableListMultimap.Builder<UUID, ClusterMetadataWithDistance> invertedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, RunningStats> standardDeviationUpdates = Maps.newHashMap();
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                continue;
            }

            final TopK<ClusterMetadataWithDistance> nearestClusters =
                    TopK.min(Comparator.comparing(ClusterMetadataWithDistance::distance), 32);
            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : candidateClusters.values()) {
                final double distance =
                        estimator.distance(vectorReference.vector(), clusterMetadataWithDistance.centroid());
                nearestClusters.add(clusterMetadataWithDistance.withNewDistance(distance));
            }

            final List<ClusterMetadataWithDistance> sortedNearestClusters = nearestClusters.toSortedList();
            Verify.verify(!sortedNearestClusters.isEmpty());

            final ClusterMetadataWithDistance primaryClusterMetadataWithDistance = sortedNearestClusters.get(0);
            updateRunningStatssMap(standardDeviationUpdates,
                    primaryClusterMetadataWithDistance.clusterMetadata().id(),
                    primaryClusterMetadataWithDistance.distance());

            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : sortedNearestClusters) {
                invertedAssignmentsMapBuilder.put(vectorReference.id().getUuid(),
                        clusterMetadataWithDistance);
            }
        }
        return new NearestClustersResult(invertedAssignmentsMapBuilder.build(), standardDeviationUpdates);
    }

    /**
     * Merges standard deviation updates into an existing tracking map. For each entry in the updates
     * map, combines it with the existing value (or inserts it if absent).
     *
     * @param target the mutable map to merge into
     * @param updates the updates to merge (as returned by {@link #computeNearestClusters})
     */
    static void mergeStandardDeviationUpdates(@Nonnull final Map<UUID, RunningStats> target,
                                              @Nonnull final Map<UUID, RunningStats> updates) {
        for (final Map.Entry<UUID, RunningStats> entry : updates.entrySet()) {
            target.merge(entry.getKey(), entry.getValue(), RunningStats::combine);
        }
    }

    /**
     * Partitions the given cluster neighborhood into inner and outer neighborhoods. The inner
     * neighborhood contains the clusters closest to the target that will be dissolved/repartitioned;
     * the outer neighborhood contains clusters that may receive overflow vectors.
     *
     * <p>
     * Handles the rare edge case where the target cluster itself is not found in the HNSW results
     * by synthesizing it at position 0 of the inner neighborhood.
     * </p>
     */
    @Nonnull
    static Neighborhoods neighborhoods(@Nonnull final List<ClusterMetadataWithDistance> clusterMetadataWithDistances,
                                       @Nonnull final ClusterMetadata targetClusterMetadata,
                                       @Nonnull final Transformed<RealVector> targetClusterCentroid,
                                       final int numInnerNeighborhood,
                                       final int numOuterNeighborhood) {
        boolean foundPrimaryCluster = false;
        for (final ClusterMetadataWithDistance clusterMetadata : clusterMetadataWithDistances) {
            if (clusterMetadata.clusterMetadata().id().equals(targetClusterMetadata.id())) {
                foundPrimaryCluster = true;
                break;
            }
        }

        if (foundPrimaryCluster) {
            final int cappedNumInnerNeighborhood = Math.min(numInnerNeighborhood, clusterMetadataWithDistances.size());
            return new Neighborhoods(clusterMetadataWithDistances.subList(0, cappedNumInnerNeighborhood),
                    clusterMetadataWithDistances.subList(cappedNumInnerNeighborhood, clusterMetadataWithDistances.size()));
        }

        final ImmutableList.Builder<ClusterMetadataWithDistance> innerNeighborhoodBuilder = ImmutableList.builder();
        innerNeighborhoodBuilder.add(
                new ClusterMetadataWithDistance(targetClusterMetadata, targetClusterCentroid, 0.0d));
        final int cappedNumInnerNeighborhood = Math.min(numInnerNeighborhood - 1, clusterMetadataWithDistances.size());

        innerNeighborhoodBuilder.addAll(clusterMetadataWithDistances.subList(0, cappedNumInnerNeighborhood));
        final List<ClusterMetadataWithDistance> innerNeighborhood = innerNeighborhoodBuilder.build();

        final int cappedNumOuterNeighborhood = Math.min(numOuterNeighborhood,
                clusterMetadataWithDistances.size() - cappedNumInnerNeighborhood);
        final List<ClusterMetadataWithDistance> outerNeighborhood = clusterMetadataWithDistances.subList(
                cappedNumInnerNeighborhood, cappedNumInnerNeighborhood + cappedNumOuterNeighborhood);
        return new Neighborhoods(innerNeighborhood, outerNeighborhood);
    }

    record NearestClustersResult(@Nonnull ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignments,
                                 @Nonnull Map<UUID, RunningStats> standardDeviationUpdates) {
    }

    record Neighborhoods(@Nonnull List<ClusterMetadataWithDistance> innerNeighborhood,
                         @Nonnull List<ClusterMetadataWithDistance> outerNeighborhood) {
    }

    public enum Kind {
        SPLIT_MERGE(0, SplitMergeTask::fromTuples),
        REASSIGN(1, ReassignTask::fromTuples),
        BOUNCE(2, BounceTask::fromTuples),
        COLLAPSE(3, CollapseTask::fromTuples);

        private static final Map<Integer, Kind> BY_CODE =
                Arrays.stream(values())
                        .collect(Collectors.toMap(s -> s.code, s -> s));

        private final int code;
        private final TaskCreationFunction taskCreationFunction;

        Kind(final int code, @Nonnull final TaskCreationFunction taskCreationFunction) {
            this.code = code;
            this.taskCreationFunction = taskCreationFunction;
        }

        public int getCode() {
            return code;
        }

        @Nonnull
        private AbstractDeferredTask create(@Nonnull final Locator locator,
                                            @Nonnull final AccessInfo accessInfo,
                                            @Nonnull final Tuple keyTuple,
                                            @Nonnull final Tuple valueTuple) {
            return taskCreationFunction.create(locator, accessInfo, keyTuple, valueTuple);
        }

        public static Kind fromValueTuple(@Nonnull final Tuple valueTuple) {
            return Kind.ofCode(Math.toIntExact(valueTuple.getLong(0)));
        }

        @Nonnull
        public static Kind ofCode(final int code) {
            return Objects.requireNonNull(BY_CODE.getOrDefault(code, null));
        }
    }

    @FunctionalInterface
    private interface TaskCreationFunction {
        AbstractDeferredTask create(@Nonnull Locator locator,
                                    @Nonnull AccessInfo accessInfo,
                                    @Nonnull Tuple keyTuple,
                                    @Nonnull Tuple valueTuple);
    }
}
