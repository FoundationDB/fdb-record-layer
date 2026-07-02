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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Base class for the deferred (background) maintenance tasks Guardiann enqueues to keep its cluster structure
 * healthy: {@link SplitMergeTask}, {@link ReassignTask}, {@link CollapseTask}, and {@link BounceTask}.
 *
 * <p>
 * Each task is persisted in a task queue and carries:
 * <ul>
 *   <li>a {@linkplain #getTaskId() task id} under which the task is stored in the queue; the id's high bit encodes
 *       the task's scheduling priority, so high-priority ids sort ahead of normal-priority ones;</li>
 *   <li>the {@link AccessInfo access context} it runs against; and</li>
 *   <li>the set of {@linkplain #getTargetClusterIds() target cluster ids} it operates on.</li>
 * </ul>
 * A persisted task is rehydrated from its tuple representation via its {@link Kind}, and subclasses implement the
 * actual maintenance work.
 */
public abstract class AbstractDeferredTask {
    @Nonnull
    private final Locator locator;
    @Nonnull
    private final AccessInfo accessInfo;
    @Nonnull
    private final UUID taskId;
    @Nonnull
    private final Set<UUID> targetClusterIds;

    /**
     * Common initialization shared by every deferred task: binds the task to its {@link Locator} (storage, config,
     * executor), the {@link AccessInfo} context it runs against, its queue {@linkplain #getTaskId() id}, and the
     * cluster ids it targets. Package-private because tasks are only ever created by subclass factories or rehydrated
     * via {@link Kind}.
     *
     * @param locator the locator providing storage, configuration and execution context
     * @param accessInfo the access/coordinate-transform context the task operates in
     * @param taskId the id under which this task is stored in the queue; its high bit encodes scheduling priority
     * @param targetClusterId the cluster ids this task operates on (defensively copied)
     */
    AbstractDeferredTask(@Nonnull final Locator locator,
                         @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId,
                         @Nonnull final Set<UUID> targetClusterId) {
        this.locator = locator;
        this.accessInfo = accessInfo;
        this.taskId = taskId;
        this.targetClusterIds = ImmutableSet.copyOf(targetClusterId);
    }

    /**
     * The {@link Locator} binding this task to its stored data, configuration, and executor.
     *
     * @return the locator
     */
    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * The low-level storage/graph {@link Primitives} this task reads and writes through.
     *
     * @return the primitives for the underlying structure
     */
    @Nonnull
    Primitives primitives() {
        return getLocator().primitives();
    }

    /**
     * The listener notified of writes (e.g. task enqueues) this task performs; a hook for metrics and tests.
     *
     * @return the on-write listener
     */
    @Nonnull
    OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * The listener notified of reads this task performs; a hook for metrics and tests.
     *
     * @return the on-read listener
     */
    @Nonnull
    OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    /**
     * The {@link AccessInfo} — the coordinate-transform/quantization context — this task runs against. A task is
     * pinned to the access info current when it was enqueued so it decodes stored vectors the same way they were
     * written.
     *
     * @return the access context
     */
    @Nonnull
    AccessInfo getAccessInfo() {
        return accessInfo;
    }

    /**
     * The id under which this task is stored in the deferred-task queue. It doubles as the queue's ordering key: the
     * id's high bit encodes scheduling priority, so high-priority ids sort ahead of normal-priority ones.
     *
     * @return the task id
     */
    @Nonnull
    public UUID getTaskId() {
        return taskId;
    }

    /**
     * The cluster ids this task operates on (e.g. the cluster to split, or the clusters a merge dissolves).
     *
     * @return the immutable set of target cluster ids
     */
    @Nonnull
    public Set<UUID> getTargetClusterIds() {
        return targetClusterIds;
    }

    /**
     * The Guardiann {@link Config} in effect for this task.
     *
     * @return the configuration
     */
    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Formats this task's kind-specific payload into the {@link Tuple} stored as the task's value in the queue
     * (which is then serialized). The inverse is the subclass {@code fromTuples} factory selected by {@link Kind},
     * which recreates the task from its stored key and value tuples.
     *
     * @return the value tuple to persist for this task
     */
    @Nonnull
    public abstract Tuple valueTuple();

    /**
     * Executes this deferred task against the given transaction.
     *
     * @param transaction the transaction to perform the task's work within
     *
     * @return a future that completes when the task has finished
     */
    @Nonnull
    public abstract CompletableFuture<Void> runTask(@Nonnull Transaction transaction);

    /**
     * Emits a debug log line marking the start of this task's execution (kind, id, targets). A no-op unless debug
     * logging is enabled.
     *
     * @param logger the subclass logger to emit through
     */
    protected void logStart(@Nonnull final Logger logger) {
        if (logger.isDebugEnabled()) {
            logger.debug("executing task kind={}, taskId={}, targetClusterIds={}", getKind(), taskIdToString(getTaskId()),
                    getTargetClusterIds());
        }
    }

    /**
     * Emits a debug log line marking this task's successful completion. A no-op unless debug logging is enabled.
     *
     * @param logger the subclass logger to emit through
     */
    protected void logSuccessful(@Nonnull final Logger logger) {
        if (logger.isDebugEnabled()) {
            logger.debug("successfully finished executing task kind={}, taskId={}", getKind(),
                    taskIdToString(getTaskId()));
        }
    }

    /**
     * Persists this task into the deferred-task queue and notifies the write listener that it was enqueued.
     *
     * @param transaction the transaction to write the task within
     */
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        primitives().writeDeferredTask(transaction, this.getTaskId(), valueTuple());
        getOnWriteListener().onTaskEnqueued(this.getKind(), this.getTaskId(), this.getTargetClusterIds());
    }

    /**
     * The {@link Kind} discriminant for this task, used to tag its serialized form and to select the right factory
     * when rehydrating it from tuples.
     *
     * @return this task's kind
     */
    @Nonnull
    public abstract Kind getKind();

    /**
     * Enqueues a collapse (plus a follow-up bounce) for {@code targetClusterId} if it holds enough duplicate primary
     * vectors to be worth deduplicating, returning whether it did so. This method is called by implementing tasks
     * as a last resort effort. For instance, {@link SplitMergeTask} may call this method to probe if vectors can be
     * collapsed (if the split evaluation was yielding invalid partitionings).
     *
     * <p>
     * Guardiann keeps identical vectors as separate references until there are enough of them that collapsing them
     * into a single representative pays off; {@link Config#collapseMinDuplicates()} is that threshold. When the most
     * duplicated content signature in the cluster exceeds it, this schedules two high-priority tasks: a
     * {@link CollapseTask} that performs the collapse, and a {@link BounceTask} that — once the collapse commits —
     * re-evaluates the cluster as a {@code SPLIT_MERGE}, since collapsing changes the cluster's effective size. Both
     * run at high priority so deduplication happens promptly rather than waiting behind normal work.
     *
     * @param transaction the transaction to enqueue the tasks within
     * @param random the RNG source for the new tasks' ids
     * @param primaryVectorReferences the cluster's primary vector references, scanned for duplicate signatures
     * @param targetClusterId the cluster to collapse
     * @param centroid the target cluster's centroid, carried to the collapse task
     * @return {@code true} if a collapse (and bounce) were enqueued, {@code false} if the cluster was below threshold
     */
    boolean enqueueCollapseIfNecessary(@Nonnull final Transaction transaction,
                                       @Nonnull final SplittableRandom random,
                                       @Nonnull final List<VectorReference> primaryVectorReferences,
                                       @Nonnull final UUID targetClusterId,
                                       @Nonnull final Transformed<RealVector> centroid) {
        final Config config = getConfig();
        final Map<UUID, Integer> collapsibleVectorsCountersMap =
                CollapseTask.collapsibleVectorsCountersMap(primaryVectorReferences);
        final int maximumNumberCollapsibleVectorsPerDuplicate =
                CollapseTask.maxDuplicateCount(collapsibleVectorsCountersMap);
        if (maximumNumberCollapsibleVectorsPerDuplicate > config.collapseMinDuplicates()) {
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

    /**
     * Rehydrates a persisted task from its stored key and value tuples, dispatching on the {@link Kind} encoded in the
     * value tuple to the matching subclass factory.
     *
     * @param locator the locator to bind the reconstructed task to
     * @param accessInfo the access context to bind the reconstructed task to
     * @param keyTuple the task's stored key tuple (its queue id)
     * @param valueTuple the task's stored value tuple (its kind plus payload)
     * @return the reconstructed task
     */
    @Nonnull
    static AbstractDeferredTask newFromTuples(@Nonnull final Locator locator,
                                              @Nonnull final AccessInfo accessInfo,
                                              @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        final Kind kind = Kind.fromValueTuple(valueTuple);
        return kind.create(locator, accessInfo, keyTuple, valueTuple);
    }

    /**
     * Generates a task id that sorts as high priority: a (optionally deterministic) random UUID with its
     * most-significant bit cleared, so that high-priority tasks order ahead of normal-priority ones in the queue.
     *
     * @param random the random source used to derive the id
     * @param isDeterministic whether to derive the id deterministically from {@code random} rather than randomly
     *
     * @return a high-priority task id
     */
    @Nonnull
    protected static UUID randomHighPriorityTaskId(@Nonnull final SplittableRandom random, final boolean isDeterministic) {
        return uuidToHighPriorityTaskId(isDeterministic ? RandomHelpers.randomUuid(random) : UUID.randomUUID());
    }

    /**
     * Clears the most-significant bit of {@code uuid} so the resulting id sorts (as an unsigned 128-bit value) ahead
     * of normal-priority ids, which set that bit.
     *
     * @param uuid the base uuid
     * @return the high-priority task id
     */
    @Nonnull
    private static UUID uuidToHighPriorityTaskId(@Nonnull final UUID uuid) {
        return new UUID(uuid.getMostSignificantBits() & 0x7fffffffffffffffL,
                uuid.getLeastSignificantBits());
    }

    /**
     * Generates a task id that sorts as normal priority: a (optionally deterministic) random UUID with its
     * most-significant bit set, so that normal-priority tasks order after high-priority ones in the queue.
     *
     * @param random the random source used to derive the id
     * @param isDeterministic whether to derive the id deterministically from {@code random} rather than randomly
     *
     * @return a normal-priority task id
     */
    @Nonnull
    protected static UUID randomNormalPriorityTaskId(@Nonnull final SplittableRandom random, final boolean isDeterministic) {
        return uuidToNormalPriorityTaskId(isDeterministic ? RandomHelpers.randomUuid(random) : UUID.randomUUID());
    }

    /**
     * Sets the most-significant bit of {@code uuid} so the resulting id sorts (as an unsigned 128-bit value) after
     * high-priority ids, which clear that bit.
     *
     * @param uuid the base uuid
     * @return the normal-priority task id
     */
    @Nonnull
    private static UUID uuidToNormalPriorityTaskId(@Nonnull final UUID uuid) {
        return new UUID(uuid.getMostSignificantBits() | 0x8000000000000000L,
                uuid.getLeastSignificantBits());
    }

    /**
     * Renders a task id for logging as {@code HIGH:<uuid>} or {@code NORMAL:<uuid>}, surfacing the priority encoded in
     * its high bit.
     *
     * @param taskId the task id
     * @return a human-readable, priority-prefixed rendering
     */
    @Nonnull
    static String taskIdToString(@Nonnull final UUID taskId) {
        return (isNormalPriority(taskId) ? "NORMAL" : "HIGH") + ":" + taskId;
    }

    /**
     * Whether {@code taskId} is a normal-priority id — i.e. its most-significant bit is set (see
     * {@link #randomNormalPriorityTaskId}). High-priority ids clear that bit and therefore sort ahead in the queue.
     *
     * @param taskId the task id to test
     * @return {@code true} if the id is normal priority, {@code false} if high priority
     */
    static boolean isNormalPriority(@Nonnull final UUID taskId) {
        return (taskId.getMostSignificantBits() & 0x8000000000000000L) != 0;
    }

    /**
     * Increments the counter stored under {@code key} in {@code countersMap}, treating an absent key as zero, and
     * returns the new count.
     *
     * @param countersMap the mutable counter map
     * @param key the key whose counter to increment
     * @param <T> the key type
     * @return the incremented counter value
     */
    @CanIgnoreReturnValue
    static <T> int incrementCounter(@Nonnull final Map<T, Integer> countersMap, @Nonnull final T key) {
        return  countersMap.compute(key, (ignoredKey, oldCounter) -> {
            if (oldCounter == null) {
                return 1;
            }
            return oldCounter + 1;
        });
    }

    /**
     * Folds a new {@code distance} sample into the {@link RunningStats} stored under {@code key}, starting fresh
     * statistics if the key is absent, and returns the updated statistics.
     *
     * @param map the mutable per-key running-statistics map
     * @param key the key whose statistics to update
     * @param distance the distance sample to add
     * @param <T> the key type
     * @return the updated running statistics for {@code key}
     */
    @CanIgnoreReturnValue
    static <T> RunningStats
               updateRunningStatsMap(@Nonnull final Map<T, RunningStats> map,
                                     @Nonnull final T key, final double distance) {
        return map.compute(key, (ignoredKey, old) -> {
            if (old == null) {
                return RunningStats.of(distance);
            }
            return old.add(distance);
        });
    }

    /**
     * Computes the diff between the old and new contents of a single cluster. Vectors present in both old and new
     * are checked for changes (a primary/replicated/underreplicated toggle, or a changed replication priority);
     * vectors only in the old cluster are marked for deletion; assignments whose primary key was not in the
     * old cluster (e.g. a freshly created collapsed reference) are insertions and are added to the write list.
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
            assignedByPrimaryKeyBuilder.put(assignedVector.id().primaryKey(), assignedVector);
        }
        final ImmutableMap<Tuple, VectorReference> assignedByPrimaryKey = assignedByPrimaryKeyBuilder.build();

        final ImmutableList.Builder<Tuple> toDeleteBuilder = ImmutableList.builder();
        final ImmutableList.Builder<VectorReference> toWriteBuilder = ImmutableList.builder();

        final ImmutableSet.Builder<Tuple> oldPrimaryKeysBuilder = ImmutableSet.builder();
        for (final VectorReference vectorReference : targetCluster.vectorReferences()) {
            final Tuple primaryKey = vectorReference.id().primaryKey();
            oldPrimaryKeysBuilder.add(primaryKey);
            final VectorReference assignedVectorReference = assignedByPrimaryKey.get(primaryKey);

            if (assignedVectorReference != null) {
                Verify.verify(assignedVectorReference.id().uuid()
                        .equals(vectorReference.id().uuid()));

                // Rewrite on a role transition (primary<->replica or underreplication toggle), or when the
                // replicationPriority changed. A replica's priority is stable while its primary stays put and only
                // shifts when that primary is reassigned to another cluster, so this rarely fires.
                if (assignedVectorReference.isPrimaryCopy() != vectorReference.isPrimaryCopy() ||
                        assignedVectorReference.isUnderreplicated() != vectorReference.isUnderreplicated() ||
                        vectorReference.replicationPriorityChanged(assignedVectorReference)) {
                    toWriteBuilder.add(assignedVectorReference);
                }
            } else {
                toDeleteBuilder.add(primaryKey);
            }
        }

        // Insertions: assignments whose primary key was not previously in the cluster (e.g. a freshly created
        // collapsed reference) must be written; the loop above only visits keys that already existed.
        final ImmutableSet<Tuple> oldPrimaryKeys = oldPrimaryKeysBuilder.build();
        for (final Map.Entry<Tuple, VectorReference> entry : assignedByPrimaryKey.entrySet()) {
            if (!oldPrimaryKeys.contains(entry.getKey())) {
                toWriteBuilder.add(entry.getValue());
            }
        }

        return new TargetClusterDelta(toWriteBuilder.build(), toDeleteBuilder.build());
    }

    /**
     * The change to apply to a single cluster's stored vectors: the vector references to write (insert or update)
     * and the primary keys to delete.
     *
     * @param toWrite the vector references to write to the cluster
     * @param toDelete the primary keys of vectors to delete from the cluster
     */
    record TargetClusterDelta(@Nonnull List<VectorReference> toWrite,
                              @Nonnull List<Tuple> toDelete) {
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
                                                        @Nonnull final Collection<ClusterMetadataWithDistance> candidateClusters) {
        final ImmutableListMultimap.Builder<UUID, ClusterMetadataWithDistance> invertedAssignmentsMapBuilder =
                ImmutableListMultimap.builder();
        final Map<UUID, RunningStats> standardDeviationUpdates = Maps.newHashMap();
        for (final VectorReference vectorReference : vectorReferences) {
            if (!vectorReference.isPrimaryCopy()) {
                continue;
            }

            // Rank every candidate cluster by distance to this vector (ascending). The candidate set is already
            // bounded by the caller's nearest clusters (see reassignNumNeighboringClusters / splitNumNearestClusters), and
            // the eventual replication fan-out is bounded downstream by replicatedClusterTarget + occlusion, so there
            // is no separate cap here: position 0 is the new primary owner, the rest are replication candidates.
            final List<ClusterMetadataWithDistance> sortedNearestClusters = new ArrayList<>(candidateClusters.size());
            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : candidateClusters) {
                final double distance =
                        estimator.distance(vectorReference.vector(), clusterMetadataWithDistance.centroid());
                sortedNearestClusters.add(clusterMetadataWithDistance.withNewDistance(distance));
            }
            sortedNearestClusters.sort(Comparator.comparing(ClusterMetadataWithDistance::distance));
            Verify.verify(!sortedNearestClusters.isEmpty());

            final ClusterMetadataWithDistance primaryClusterMetadataWithDistance = sortedNearestClusters.get(0);
            updateRunningStatsMap(standardDeviationUpdates,
                    primaryClusterMetadataWithDistance.clusterMetadata().id(),
                    primaryClusterMetadataWithDistance.distance());

            for (final ClusterMetadataWithDistance clusterMetadataWithDistance : sortedNearestClusters) {
                invertedAssignmentsMapBuilder.put(vectorReference.id().uuid(),
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
     * Partitions the given nearest clusters into core and neighboring clusters. The core clusters
     * contain the clusters closest to the target that will be dissolved/repartitioned;
     * the neighboring clusters contain clusters that may receive overflow vectors.
     *
     * <p>
     * Handles the rare edge case where the target cluster itself is not found in the HNSW results
     * by synthesizing it at position 0 of the core clusters.
     * </p>
     *
     * @param clusterMetadataWithDistances the candidate clusters ordered by ascending distance to the target centroid
     * @param targetClusterMetadata the cluster being repartitioned
     * @param targetClusterCentroid the transformed centroid of the target cluster, used when the target is
     *        synthesized at position 0
     * @param numCoreClusters the number of closest clusters to place in the core clusters (must be {@code >= 1})
     * @param numNeighboringClusters the number of subsequent clusters to place in the neighboring clusters
     *
     * @return the core/neighboring {@link ClusterClassification} partition of the given clusters
     */
    @Nonnull
    static ClusterClassification classifyClusters(@Nonnull final List<ClusterMetadataWithDistance> clusterMetadataWithDistances,
                                       @Nonnull final ClusterMetadata targetClusterMetadata,
                                       @Nonnull final Transformed<RealVector> targetClusterCentroid,
                                       final int numCoreClusters,
                                       final int numNeighboringClusters) {
        Verify.verify(numCoreClusters >= 1, "numCoreClusters must be >= 1, got %s", numCoreClusters);
        boolean foundPrimaryCluster = false;
        for (final ClusterMetadataWithDistance clusterMetadata : clusterMetadataWithDistances) {
            if (clusterMetadata.clusterMetadata().id().equals(targetClusterMetadata.id())) {
                foundPrimaryCluster = true;
                break;
            }
        }

        if (foundPrimaryCluster) {
            final int cappedNumCoreClusters = Math.min(numCoreClusters, clusterMetadataWithDistances.size());
            final int cappedNumNeighboringClusters = Math.min(numNeighboringClusters,
                    clusterMetadataWithDistances.size() - cappedNumCoreClusters);
            return new ClusterClassification(clusterMetadataWithDistances.subList(0, cappedNumCoreClusters),
                    clusterMetadataWithDistances.subList(cappedNumCoreClusters,
                            cappedNumCoreClusters + cappedNumNeighboringClusters));
        }

        final ImmutableList.Builder<ClusterMetadataWithDistance> coreClustersBuilder = ImmutableList.builder();
        coreClustersBuilder.add(
                new ClusterMetadataWithDistance(targetClusterMetadata, targetClusterCentroid, 0.0d));
        final int cappedNumCoreClusters = Math.min(numCoreClusters - 1, clusterMetadataWithDistances.size());

        coreClustersBuilder.addAll(clusterMetadataWithDistances.subList(0, cappedNumCoreClusters));
        final List<ClusterMetadataWithDistance> coreClusters = coreClustersBuilder.build();

        final int cappedNumNeighboringClusters = Math.min(numNeighboringClusters,
                clusterMetadataWithDistances.size() - cappedNumCoreClusters);
        final List<ClusterMetadataWithDistance> neighboringClusters = clusterMetadataWithDistances.subList(
                cappedNumCoreClusters, cappedNumCoreClusters + cappedNumNeighboringClusters);
        return new ClusterClassification(coreClusters, neighboringClusters);
    }

    /**
     * The result of computing each primary vector's nearest clusters: an inverted assignments multimap (keyed by
     * vector UUID, with the nearest clusters in ascending distance order) together with the per-cluster running
     * standard-deviation updates accumulated while doing so.
     *
     * @param invertedAssignments a multimap from vector UUID to its nearest clusters, in ascending distance order
     * @param standardDeviationUpdates a map from cluster id to the running statistics update for that cluster
     */
    record NearestClustersResult(@Nonnull ImmutableListMultimap<UUID, ClusterMetadataWithDistance> invertedAssignments,
                                 @Nonnull Map<UUID, RunningStats> standardDeviationUpdates) {
    }

    /**
     * A classification of a cluster's nearest clusters into core clusters and neighboring clusters. The core clusters hold
     * the closest clusters (those dissolved or repartitioned by an operation); the neighboring clusters hold the next
     * clusters out, which may receive overflow vectors.
     *
     * @param coreClusters the closest clusters, to be dissolved or repartitioned
     * @param neighboringClusters the surrounding clusters that may receive overflow vectors
     */
    record ClusterClassification(@Nonnull List<ClusterMetadataWithDistance> coreClusters,
                                 @Nonnull List<ClusterMetadataWithDistance> neighboringClusters) {
    }

    /**
     * The kinds of deferred task. Each constant carries a stable integer {@linkplain #getCode() code} used in the
     * task's serialized form and a factory that reconstructs the corresponding {@link AbstractDeferredTask} from its
     * stored key and value tuples.
     */
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

        /**
         * The stable integer code persisted for this kind as the first element of a task's value tuple.
         *
         * @return the serialized code
         */
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

        /**
         * Reads the {@link Kind} from a persisted task's value tuple, whose first element is the kind code.
         *
         * @param valueTuple the task's stored value tuple
         * @return the decoded kind
         */
        public static Kind fromValueTuple(@Nonnull final Tuple valueTuple) {
            return Kind.ofCode(Math.toIntExact(valueTuple.getLong(0)));
        }

        /**
         * Returns the kind for a persisted {@linkplain #getCode() code}.
         *
         * @param code the serialized kind code
         * @return the matching kind
         * @throws NullPointerException if the code does not correspond to any kind
         */
        @Nonnull
        public static Kind ofCode(final int code) {
            return Objects.requireNonNull(BY_CODE.getOrDefault(code, null));
        }
    }

    /**
     * Factory that reconstructs an {@link AbstractDeferredTask} from its persisted key and value tuples.
     */
    @FunctionalInterface
    private interface TaskCreationFunction {
        AbstractDeferredTask create(@Nonnull Locator locator,
                                    @Nonnull AccessInfo accessInfo,
                                    @Nonnull Tuple keyTuple,
                                    @Nonnull Tuple valueTuple);
    }
}
