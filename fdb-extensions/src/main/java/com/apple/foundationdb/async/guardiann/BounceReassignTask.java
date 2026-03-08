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

public class BounceReassignTask extends AbstractDeferredTask {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(BounceReassignTask.class);

    @Nonnull
    private final Set<UUID> dependentTaskIds;

    private BounceReassignTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                               @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds,
                               @Nonnull final Set<UUID> dependentTaskIds) {
        super(locator, accessInfo, taskId, targetClusterIds);
        this.dependentTaskIds = ImmutableSet.copyOf(dependentTaskIds);
    }

    @Nonnull
    public Set<UUID> getDependentTaskIds() {
        return dependentTaskIds;
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        return Tuple.from(getKind().getCode(), StorageAdapter.tupleFromClusterIds(getTargetClusterIds()),
                StorageAdapter.tupleFromTaskIds(getDependentTaskIds()));
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Kind.BOUNCE_REASSIGN;
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        logStart(logger);
        final SplittableRandom splittableRandom = Objects.requireNonNull(
                RandomHelpers.random(Iterables.getFirst(getTargetClusterIds(), null)));

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
                        return enqueueReassign(transaction, splittableRandom);
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
                                    final BounceReassignTask newBounceReassignTask =
                                            BounceReassignTask.of(getLocator(), accessInfo,
                                                    RandomHelpers.randomUUID(splittableRandom), getTargetClusterIds(),
                                                    newDependentTaskIds);
                                    primitives.writeDeferredTask(transaction, newBounceReassignTask);

                                    if (logger.isInfoEnabled()) {
                                        logger.info("enqueuing BOUNCE_REASSIGN; taskId={}; targetClusterIds={}; newDependentTaskIds={}",
                                                newBounceReassignTask.getTaskId(),
                                                newBounceReassignTask.getTargetClusterIds(),
                                                newBounceReassignTask.getDependentTaskIds());
                                    }
                                    return AsyncUtil.DONE;
                                }

                                Verify.verify(shuffledTasks.size() == 1); // the one we just executed
                                return enqueueReassign(transaction, splittableRandom);
                            });
                });
    }

    @Nonnull
    private CompletableFuture<Void> enqueueReassign(@Nonnull final Transaction transaction,
                                                    @Nonnull final SplittableRandom random) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();

        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);

        return MoreAsyncUtil.forEach(getTargetClusterIds(),
                        targetClusterId -> centroidsHnsw.fetch(transaction, StorageAdapter.tupleFromClusterId(targetClusterId))
                                .thenAccept(resultEntry -> {
                                    final Transformed<RealVector> transformedCentroid =
                                            storageTransform.transform(Objects.requireNonNull(resultEntry.getVector()));
                                    final ReassignTask reassignTask =
                                            ReassignTask.of(getLocator(), accessInfo,
                                                    RandomHelpers.randomUUID(random.split()),
                                                    targetClusterId,
                                                    transformedCentroid,
                                                    ImmutableSet.of());
                                    primitives.writeDeferredTask(transaction, reassignTask);
                                    if (logger.isInfoEnabled()) {
                                        logger.info("enqueuing final REASSIGN; taskId={}; targetClusterIds={}",
                                                reassignTask.getTaskId(),
                                                reassignTask.getTargetClusterIds());
                                    }

                                }), 10, executor)
                .thenCompose(ignored2 -> AsyncUtil.DONE);
    }

    @Nonnull
    static BounceReassignTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                         @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.BOUNCE_REASSIGN);

        final Set<UUID> targetClusterIds = StorageAdapter.clusterIdsFromTuple(valueTuple.getNestedTuple(1));
        final Set<UUID> dependentTaskIds = StorageAdapter.taskIdsFromTuple(valueTuple.getNestedTuple(2));
        return new BounceReassignTask(locator, accessInfo, keyTuple.getUUID(0),
                targetClusterIds, dependentTaskIds);
    }

    @Nonnull
    static BounceReassignTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                 @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds,
                                 @Nonnull final Set<UUID> dependentTaskIds) {
        return new BounceReassignTask(locator, accessInfo, taskId, targetClusterIds, dependentTaskIds);
    }
}
