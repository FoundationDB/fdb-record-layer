/*
 * Delete.java
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
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.MoreAsyncUtil.limitIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;

/**
 * Handles deletion of vectors from the Guardiann index. Removes the vector's references from all
 * clusters (primary and replicated), updates cluster metadata counts, and enqueues maintenance tasks
 * if a cluster drops below its minimum size threshold.
 */
class Delete {
    private static final Logger logger = LoggerFactory.getLogger(Delete.class);

    private final Locator locator;

    public Delete(final Locator locator) {
        this.locator = locator;
    }

    public Locator getLocator() {
        return locator;
    }

    public Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    public Config getConfig() {
        return getLocator().getConfig();
    }

    private Primitives primitives() {
        return getLocator().primitives();
    }

    /**
     * Deletes a vector identified by its primary key from the Guardiann index.
     * <p>
     * Locates the vector's references in nearby clusters by querying the HNSW centroid index,
     * removes them, adjusts cluster metadata, and enqueues a merge task via
     * {@link Primitives#updateClusterMetadataAndEnqueueMergeTaskMaybe} if the primary cluster's size drops below the
     * configured minimum (and a mergeable neighbor exists). Finally, removes the vector's metadata entry.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param primaryKey the unique {@link Tuple} primary key of the vector to delete
     * @param vector the {@link RealVector} data of the vector being deleted (needed to locate its clusters)
     * @param maintainInTransaction when {@code true}, drain one deferred maintenance task inside this writing
     *        transaction; when {@code false} tasks accumulate for a background merge
     * @return a {@link CompletableFuture} that completes when the deletion is finished, or completes
     *         immediately if the vector does not exist
     */
    public CompletableFuture<Void> delete(final Transaction transaction,
                                          final Tuple primaryKey,
                                          final RealVector vector,
                                          final boolean maintainInTransaction) {
        final SplittableRandom random = RandomHelpers.random(primaryKey);
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(transaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return AsyncUtil.DONE;
                    }

                    return primitives.fetchVectorMetadata(transaction, primaryKey)
                            .thenCompose(vectorMetadata -> {
                                if (vectorMetadata == null) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("vector not found for deletion; primaryKey={}", primaryKey);
                                    }
                                    return AsyncUtil.DONE;
                                }

                                // Optionally drain a deferred task in this transaction; when disabled (the default)
                                // tasks accumulate and are left for the background merge process.
                                if (!maintainInTransaction) {
                                    return deleteFromClusters(transaction, random, accessInfo, primaryKey, vector);
                                }
                                return primitives.executeDeferredTasks(transaction, accessInfo, 1)
                                        .thenCompose(ignored ->
                                                deleteFromClusters(transaction, random, accessInfo, primaryKey,
                                                        vector));
                            });
                });
    }

    private CompletableFuture<Void> deleteFromClusters(final Transaction transaction,
                                                       final SplittableRandom random,
                                                       final AccessInfo accessInfo,
                                                       final Tuple primaryKey,
                                                       final RealVector vector) {
        final Config config = getConfig();
        final Primitives primitives = primitives();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);

        return AsyncUtil.collect(
                        limitIterable(
                                mapIterablePipelined(getExecutor(),
                                        MoreAsyncUtil.iterableOf(() ->
                                                        primitives.centroidsOrderedByDistance(transaction, vector, 0.0d, null,
                                                                config.constructionSearchConfig().centroidEfRingSearch(),
                                                                config.constructionSearchConfig().centroidEfOutwardSearch()),
                                                getExecutor()),
                                        resultEntry ->
                                                primitives.fetchClusterMetadataWithDistance(transaction,
                                                        StorageAdapter.clusterIdFromTuple(resultEntry.primaryKey()),
                                                        storageTransform.transform(
                                                                Objects.requireNonNull(resultEntry.vector())),
                                                        resultEntry.distance())
                                                        .thenApply(clusterMetadataWithDistance -> Objects.requireNonNull(
                                                                clusterMetadataWithDistance,
                                                                "cluster found in centroid HNSW is missing its metadata")),
                                        1),
                                config.deleteMaxCandidateClusters(),
                                getExecutor()),
                        getExecutor())
                .thenCompose(clusterMetadataWithDistances ->
                        MoreAsyncUtil.forEach(clusterMetadataWithDistances,
                                clusterMetadataWithDistance -> {
                                    final UUID clusterId = clusterMetadataWithDistance.clusterMetadata().id();
                                    return primitives.fetchVectorReference(transaction, storageTransform,
                                            clusterId, primaryKey);
                                },
                                config.deleteConcurrency(), getExecutor())
                        .thenCompose(vectorReferences -> {
                            boolean foundPrimary = false;
                            // Deleting a primary may drop its cluster below primaryClusterMin and trigger a
                            // merge; whether a merge is possible depends on the centroid index, so that step is
                            // asynchronous and is threaded through this future.
                            CompletableFuture<Void> primaryUpdateFuture = AsyncUtil.DONE;

                            for (int i = 0; i < clusterMetadataWithDistances.size(); i++) {
                                final VectorReference vectorReference = vectorReferences.get(i);
                                if (vectorReference == null) {
                                    continue;
                                }

                                final ClusterMetadataWithDistance clusterMetadataWithDistance =
                                        clusterMetadataWithDistances.get(i);
                                final ClusterMetadata clusterMetadata = clusterMetadataWithDistance.clusterMetadata();
                                final UUID clusterId = clusterMetadata.id();

                                primitives.deleteVectorReference(transaction, clusterId, primaryKey);

                                if (vectorReference.isPrimaryCopy()) {
                                    foundPrimary = true;
                                    final RunningStats updatedStandardDeviation =
                                            clusterMetadata.runningStandardDeviation().remove(
                                                    clusterMetadataWithDistance.distance());

                                    primaryUpdateFuture = primaryUpdateFuture.thenCompose(ignored ->
                                            primitives.updateClusterMetadataAndEnqueueMergeTaskMaybe(transaction,
                                                    random.split(), clusterMetadata,
                                                    clusterMetadataWithDistance.centroid(), accessInfo,
                                                    updatedStandardDeviation));
                                } else {
                                    primitives.updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe(transaction,
                                            random.split(), clusterMetadata,
                                            clusterMetadataWithDistance.centroid(), accessInfo,
                                            0, 0, -1,
                                            clusterMetadata.runningStandardDeviation(),
                                            ImmutableSet.of());
                                }
                            }

                            final boolean foundPrimaryFinal = foundPrimary;
                            return primaryUpdateFuture.thenRun(() -> {
                                if (!foundPrimaryFinal) {
                                    //
                                    // The vector was not found as a regular reference — it must be collapsed, or
                                    // we just didn't find it which should be rare. In any case, this gives us enough
                                    // ammunition to assume the vector is in a collapsed set.
                                    //
                                    final UUID signature = StorageAdapter.signatureUuid(storageTransform.transform(vector));
                                    primitives.deleteCollapsedVectorId(transaction, signature, primaryKey);
                                }

                                primitives.deleteVectorMetadata(transaction, primaryKey);
                            });
                        }));
    }
}
