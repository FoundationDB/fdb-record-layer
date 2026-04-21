/*
 * SplitMergeTask.java
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
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.BOUNCE;
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);
        final SplittableRandom splittableRandom = Objects.requireNonNull(
                RandomHelpers.random(Iterables.getFirst(getTargetClusterIds(), null)));

        final Config config = getConfig();
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();

        return MoreAsyncUtil.forEach(getDependentTaskIds(),
                        dependentTaskId -> primitives.fetchDeferredTask(transaction, accessInfo, dependentTaskId),
                        10, executor)
                .thenCompose(tasks -> {
                    final Random random = new Random(splittableRandom.split().nextLong());
                    final ArrayList<AbstractDeferredTask> shuffledTasks = Lists.newArrayList();
                    for (final AbstractDeferredTask task : tasks) {
                        if (task != null) {
                            shuffledTasks.add(task);
                        }
                    }

                    if (shuffledTasks.isEmpty()) {
                        return enqueueFinal(transaction, splittableRandom);
                    }

                    // there is at least one task, bounce and the rewrite a new bounce task afterward

                    // shuffle for good measure
                    Collections.shuffle(shuffledTasks, random);

                    final AbstractDeferredTask bounceTask = shuffledTasks.get(0);

                    if (logger.isInfoEnabled()) {
                        logger.info("bouncing task; taskKind={}, taskId={}", bounceTask.getKind().name(), bounceTask.getTaskId());
                    }

                    return bounceTask.runTask(transaction)
                            .thenCompose(ignored -> {
                                if (shuffledTasks.size() > 1) {
                                    final ImmutableSet.Builder<UUID> newDependentTaskIdsBuilder = ImmutableSet.builder();
                                    for (int i = 1; i < shuffledTasks.size(); i++) { // skip the first item
                                        final AbstractDeferredTask task = shuffledTasks.get(i);
                                        newDependentTaskIdsBuilder.add(task.getTaskId());
                                    }

                                    final ImmutableSet<UUID> newDependentTaskIds = newDependentTaskIdsBuilder.build();
                                    final BounceTask newBounceTask =
                                            BounceTask.of(getLocator(), accessInfo,
                                                    randomNormalPriorityTaskId(splittableRandom,
                                                            config.isDeterministicRandomness()),
                                                    getTargetClusterIds(), newDependentTaskIds, getFinalTaskKind());
                                    primitives.writeDeferredTask(transaction, newBounceTask);

                                    if (logger.isInfoEnabled()) {
                                        logger.info("re-enqueuing BOUNCE_REASSIGN; taskId={}; targetClusterIds={}; newDependentTaskIds={}",
                                                taskIdToString(newBounceTask.getTaskId()),
                                                newBounceTask.getTargetClusterIds(),
                                                newBounceTask.getDependentTaskIds());
                                    }
                                    return AsyncUtil.DONE;
                                }

                                Verify.verify(shuffledTasks.size() == 1); // the one we just executed
                                return enqueueFinal(transaction, splittableRandom);
                            });
                });
    }

    @Nonnull
    private CompletableFuture<Void> enqueueFinal(@Nonnull final Transaction transaction,
                                                 @Nonnull final SplittableRandom random) {
        final Config config = getConfig();
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();

        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);

        return RandomHelpers.forEach(random, getTargetClusterIds(),
                        (targetClusterId, nestedRandom) ->
                                centroidsHnsw.fetch(transaction, StorageAdapter.tupleFromClusterId(targetClusterId))
                                        .thenAccept(resultEntry -> {
                                            if (resultEntry == null) {
                                                if (logger.isInfoEnabled()) {
                                                    logger.info("unable to enqueue final task; kind={}; targetClusterIds={}",
                                                            getFinalTaskKind(), targetClusterId);
                                                }
                                                return;
                                            }
                                            final Transformed<RealVector> transformedCentroid =
                                                    storageTransform.transform(Objects.requireNonNull(resultEntry.getVector()));

                                            final UUID taskId = randomNormalPriorityTaskId(nestedRandom, config.isDeterministicRandomness());
                                            final AbstractDeferredTask finalTask;
                                            switch (getFinalTaskKind()) {
                                                case SPLIT_MERGE:
                                                    finalTask =
                                                            SplitMergeTask.of(getLocator(), accessInfo, taskId,
                                                                    targetClusterId, transformedCentroid);
                                                    break;
                                                case REASSIGN:
                                                    finalTask =
                                                            ReassignTask.of(getLocator(), accessInfo,
                                                                    taskId, targetClusterId, transformedCentroid,
                                                                    ImmutableSet.of());
                                                    break;
                                                default:
                                                    throw new UnsupportedOperationException("unsupported kind for final task");
                                            }

                                            primitives.writeDeferredTask(transaction, finalTask);
                                            if (logger.isInfoEnabled()) {
                                                logger.info("enqueuing final task; kind={}; taskId={}; targetClusterIds={}",
                                                        finalTask.getKind(),
                                                        taskIdToString(finalTask.getTaskId()),
                                                        finalTask.getTargetClusterIds());
                                            }

                                        }), 10, executor)
                .thenCompose(ignored2 -> AsyncUtil.DONE);
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
