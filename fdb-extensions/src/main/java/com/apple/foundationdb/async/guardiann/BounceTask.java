/*
 * BounceTask.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A deferred task that acts as a join (barrier): it waits until a set of dependent tasks has completed and then
 * enqueues a follow-up task of {@link Kind} {@code finalTaskKind} (for example a {@link ReassignTask}) for its
 * target clusters. Guardiann uses it after a split or merge so that the reassignment of the newly created clusters
 * only runs once all tasks they depend on have finished.
 *
 * <p>
 * Rather than passively re-enqueueing itself until its dependencies happen to drain, a bounce actively drives
 * progress: each time it reaches the top of the task queue it runs one still-outstanding dependency itself (chosen at
 * random, see {@link #runTask}) before re-enqueueing itself for the rest. A dependent task therefore completes in one
 * of two ways:
 * <ul>
 *   <li>it was enqueued <em>before</em> this bounce, so ordinary queue processing runs it first; or</li>
 *   <li>it was enqueued <em>after</em> this bounce (and so sits behind it in the queue), in which case the bounce
 *       picks it up and runs it.</li>
 * </ul>
 * Handling the second case is what keeps the bounce from starving: if it relied on the first alone it could keep
 * finding unfinished dependencies, re-enqueue itself, and spin forever without making progress. Once no dependencies
 * remain, the bounce enqueues its follow-up work (see {@link #enqueueFollowUpTasks}).
 */
public class BounceTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(BounceTask.class);

    @Nonnull
    private final Set<UUID> dependentTaskIds;
    @Nonnull
    private final Kind finalTaskKind;

    private BounceTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                       @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds,
                       @Nonnull final Set<UUID> dependentTaskIds, @Nonnull final Kind finalTaskKind) {
        super(locator, accessInfo, taskId, targetClusterIds);
        this.dependentTaskIds = ImmutableSet.copyOf(dependentTaskIds);
        this.finalTaskKind = finalTaskKind;
    }

    @Nonnull
    public Set<UUID> getDependentTaskIds() {
        return dependentTaskIds;
    }

    @Nonnull
    public Kind getFinalTaskKind() {
        return finalTaskKind;
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        return Tuple.from(getKind().getCode(), StorageAdapter.tupleFromClusterIds(getTargetClusterIds()),
                StorageAdapter.tupleFromTaskIds(getDependentTaskIds()), finalTaskKind.name());
    }

    @Override
    protected void writeDeferredTask(@Nonnull final Transaction transaction) {
        super.writeDeferredTask(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("enqueuing BOUNCE; taskId={}; targetClusterIds={}; newDependentTaskIds={}",
                    taskIdToString(getTaskId()), getTargetClusterIds(), getDependentTaskIds());
        }
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.BOUNCE;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);
        final SplittableRandom splittableRandom = RandomHelpers.random(getTaskId());

        final Config config = getConfig();
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();

        return MoreAsyncUtil.forEach(getDependentTaskIds(),
                        dependentTaskId -> primitives.fetchDeferredTask(transaction, accessInfo, dependentTaskId),
                        config.bounceConcurrency(), executor)
                .thenCompose(tasks -> {
                    final Random random = new Random(splittableRandom.split().nextLong());
                    final List<AbstractDeferredTask> outstandingTasks = Lists.newArrayList();
                    for (final AbstractDeferredTask task : tasks) {
                        //
                        // fetchDeferredTask returns null for a dependent task that is no longer in the task subspace,
                        // i.e. one that has already run and been removed by ordinary queue processing. Keeping only
                        // the non-null entries both drops those completed dependencies and leaves the still-outstanding
                        // ones.
                        //
                        if (task != null) {
                            outstandingTasks.add(task);
                        }
                    }

                    if (outstandingTasks.isEmpty()) {
                        // Every dependency has completed: this bounce is done, so enqueue the follow-up work.
                        return enqueueFollowUpTasks(transaction, splittableRandom);
                    }

                    //
                    // At least one dependency is still outstanding. Run one of them ourselves -- rather than only
                    // waiting for the queue to drain them -- so the bounce always makes progress instead of possibly
                    // re-enqueueing itself forever (see the class javadoc). The pick is random so that we do not keep
                    // favoring (and repeatedly retrying) the same task across successive bounces.
                    //
                    final int pick = random.nextInt(outstandingTasks.size());
                    final AbstractDeferredTask bounceTask = outstandingTasks.get(pick);

                    if (logger.isDebugEnabled()) {
                        logger.debug("bouncing task; taskKind={}, taskId={}",
                                bounceTask.getKind().name(), bounceTask.getTaskId());
                    }

                    return primitives.executeSingleDeferredTask(transaction, bounceTask)
                            .thenCompose(ignored -> {
                                if (outstandingTasks.size() > 1) {
                                    //
                                    // Re-enqueue a bounce that waits on the dependencies we did not just run. The set
                                    // is rebuilt (rather than mutated) because dependentTaskIds is immutable; it also
                                    // no longer contains any dependency that had already completed (filtered above).
                                    //
                                    final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
                                    for (int i = 0; i < outstandingTasks.size(); i++) {
                                        if (i != pick) { // skip the one we just executed
                                            newDependentTaskIdsBuilder.add(outstandingTasks.get(i).getTaskId());
                                        }
                                    }

                                    final ImmutableSet<UUID> newDependentTaskIds = newDependentTaskIdsBuilder.build();
                                    final BounceTask newBounceTask =
                                            BounceTask.of(getLocator(), accessInfo,
                                                    getTaskId(),
                                                    getTargetClusterIds(), newDependentTaskIds, getFinalTaskKind());
                                    newBounceTask.writeDeferredTask(transaction);
                                    return AsyncUtil.DONE;
                                }

                                Verify.verify(outstandingTasks.size() == 1); // the one we just executed
                                return enqueueFollowUpTasks(transaction, splittableRandom);
                            });
                });
    }

    /**
     * Enqueues this bounce's follow-up work, now that all of its dependencies have completed: one
     * {@code finalTaskKind} task (a {@link ReassignTask} or {@link SplitMergeTask}) per target cluster. It can
     * therefore schedule several tasks -- for example one reassign per cluster produced by a split. The follow-up for
     * a given target cluster is skipped when the cluster's centroid can no longer be found (the cluster was deleted in
     * the meantime) or when the cluster already carries a {@code SPLIT_MERGE}/{@code REASSIGN}/{@code COLLAPSE} state
     * (a maintenance task for it is already pending); see {@link #enqueueFollowUpTaskForCluster} for the per-cluster
     * decision.
     *
     * @param transaction the transaction to enqueue the follow-up tasks into
     * @param random source of randomness used to derive the generated task ids
     * @return a future that completes once the follow-up tasks have been enqueued
     */
    @SuppressWarnings("checkstyle:Indentation")
    @Nonnull
    private CompletableFuture<Void> enqueueFollowUpTasks(@Nonnull final Transaction transaction,
                                                         @Nonnull final SplittableRandom random) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final StorageTransform storageTransform = primitives.storageTransform(getAccessInfo());

        return RandomHelpers.forEach(random, getTargetClusterIds(),
                        (targetClusterId, nestedRandom) ->
                                centroidsHnsw.fetch(transaction, StorageAdapter.tupleFromClusterId(targetClusterId))
                                        .thenCombine(primitives.fetchClusterMetadata(transaction, targetClusterId),
                                                (resultEntry, targetClusterMetadata) -> {
                                                    enqueueFollowUpTaskForCluster(transaction, targetClusterId,
                                                            nestedRandom, storageTransform, resultEntry,
                                                            targetClusterMetadata);
                                                    return null;
                                                }), getConfig().bounceConcurrency(), executor)
                .thenCompose(ignored2 -> AsyncUtil.DONE);
    }

    /**
     * Enqueues the follow-up task for a single target cluster, if appropriate. {@link #enqueueFollowUpTasks} invokes
     * this once per target cluster, passing that cluster's freshly fetched centroid entry and metadata.
     *
     * <p>
     * The follow-up is a {@code finalTaskKind} task ({@link SplitMergeTask} or {@link ReassignTask}) seeded at the
     * cluster's centroid. It is skipped in two cases:
     * <ul>
     *   <li>{@code resultEntry} is {@code null} — the centroid is gone from the centroid HNSW because the cluster was
     *       deleted (e.g. merged away) between the time this bounce was scheduled and now, so there is nothing left to
     *       target; or</li>
     *   <li>the cluster already carries a {@code SPLIT_MERGE}/{@code REASSIGN}/{@code COLLAPSE} state — a maintenance
     *       task for it is already pending (Guardiann keeps at most one such task per cluster), so a second would
     *       duplicate work, and if it is mid-split we would rather let that finish than layer a reassign on top.</li>
     * </ul>
     * When it does enqueue, it also flips the cluster into the matching state so the now-pending task is recorded on
     * the cluster.
     *
     * @param transaction the transaction to enqueue the follow-up task into
     * @param targetClusterId the cluster to enqueue a follow-up task for
     * @param random source of randomness used to derive the generated task id
     * @param storageTransform the transform that maps a stored centroid back into the working coordinate space
     * @param resultEntry the cluster's centroid entry fetched from the centroid HNSW, or {@code null} if it is gone
     * @param targetClusterMetadata the cluster's current metadata
     */
    private void enqueueFollowUpTaskForCluster(@Nonnull final Transaction transaction,
                                               @Nonnull final UUID targetClusterId,
                                               @Nonnull final SplittableRandom random,
                                               @Nonnull final StorageTransform storageTransform,
                                               @Nullable final ResultEntry resultEntry,
                                               @Nonnull final ClusterMetadata targetClusterMetadata) {
        if (resultEntry == null) {
            // Centroid gone: the target cluster was deleted before this bounce ran its follow-up (see javadoc).
            if (logger.isDebugEnabled()) {
                logger.debug("unable to enqueue final task; kind={}; targetClusterId={}",
                        getFinalTaskKind(), targetClusterId);
            }
            return;
        }

        final Primitives primitives = getLocator().primitives();
        final AccessInfo accessInfo = getAccessInfo();
        final Transformed<RealVector> transformedCentroid =
                storageTransform.transform(Objects.requireNonNull(resultEntry.vector()));

        final UUID taskId = randomNormalPriorityTaskId(random, getConfig().deterministicRandomness());
        final AbstractDeferredTask finalTask = switch (getFinalTaskKind()) {
            case SPLIT_MERGE -> SplitMergeTask.of(getLocator(), accessInfo, taskId, targetClusterId,
                    transformedCentroid);
            case REASSIGN -> ReassignTask.of(getLocator(), accessInfo, taskId, targetClusterId, transformedCentroid,
                    ImmutableSet.of());
            default -> throw new UnsupportedOperationException("unsupported kind for final task");
        };

        // Skip if the cluster is not idle: a pending SPLIT_MERGE/REASSIGN/COLLAPSE already covers it (see javadoc).
        final EnumSet<ClusterMetadata.State> oldStates = targetClusterMetadata.states();
        if (oldStates.contains(ClusterMetadata.State.REASSIGN) ||
                oldStates.contains(ClusterMetadata.State.SPLIT_MERGE) ||
                oldStates.contains(ClusterMetadata.State.COLLAPSE)) {
            return;
        }

        final ClusterMetadata.State newState = switch (getFinalTaskKind()) {
            case SPLIT_MERGE -> ClusterMetadata.State.SPLIT_MERGE;
            case REASSIGN -> ClusterMetadata.State.REASSIGN;
            default -> throw new UnsupportedOperationException("unsupported kind for final task");
        };
        primitives.writeClusterMetadata(transaction, targetClusterMetadata.withNewStates(EnumSet.of(newState)));

        finalTask.writeDeferredTask(transaction);
        if (logger.isDebugEnabled()) {
            logger.debug("enqueued final task; taskId={}", taskIdToString(finalTask.getTaskId()));
        }
    }

    @Nonnull
    static BounceTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                 @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.BOUNCE);

        final Set<UUID> targetClusterIds = StorageAdapter.clusterIdsFromTuple(valueTuple.getNestedTuple(1));
        final Set<UUID> dependentTaskIds = StorageAdapter.taskIdsFromTuple(valueTuple.getNestedTuple(2));
        final Kind finalTaskKind = Kind.valueOf(valueTuple.getString(3));
        return new BounceTask(locator, accessInfo, keyTuple.getUUID(0),
                targetClusterIds, dependentTaskIds, finalTaskKind);
    }

    @Nonnull
    static BounceTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds,
                         @Nonnull final Set<UUID> dependentTaskIds, @Nonnull final Kind finalTaskKind) {
        return new BounceTask(locator, accessInfo, taskId, targetClusterIds, dependentTaskIds, finalTaskKind);
    }
}
