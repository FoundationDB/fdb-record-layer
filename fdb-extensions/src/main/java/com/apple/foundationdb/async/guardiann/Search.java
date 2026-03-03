/*
 * Search.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.ResultEntry;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.async.MoreAsyncUtil.limitIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapConcatIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;
import static com.apple.foundationdb.async.MoreAsyncUtil.takeWhileIterable;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class Search {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Search.class);

    @Nonnull
    private final Locator locator;

    /**
     * This constructor initializes a new insert operations object with the necessary components for storage,
     * execution, configuration, and event handling.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Search(@Nonnull final Locator locator) {
        this.locator = locator;
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    private Primitives primitives() {
        return getLocator().primitives();
    }

    @Nonnull
    private StorageAdapter getStorageAdapter() {
        return getLocator().getStorageAdapter();
    }

    @Nonnull
    private Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    AsyncIterator<ResultEntry> orderByDistance(@Nonnull final ReadTransaction readTransaction,
                                               final int efOutwardSearch,
                                               final boolean includeVectors,
                                               @Nonnull final RealVector centerVector,
                                               final double minimumRadius,
                                               @Nullable final Tuple minimumPrimaryKey,
                                               final boolean shouldQuickStart) {
        return null;
    }

    @SuppressWarnings("checkstyle:MethodName")
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        final Config config = getConfig();
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return AsyncUtil.DONE;
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final Estimator estimator = primitives.quantizer(accessInfo).estimator();

                    final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                            MoreAsyncUtil.iterableOf(() ->
                                    primitives.centroidsOrderedByDistance(readTransaction, queryVector,
                                            0.0d, null), getExecutor());

                    final AsyncIterable<ClusterMetadataWithDistance> clusterMetadataIterable =
                            mapIterablePipelined(getExecutor(), clusterCentroidEntriesByDistanceIterable,
                                    resultEntry ->
                                            primitives.fetchClusterMetadata(readTransaction,
                                                            StorageAdapter.clusterIdFromTuple(resultEntry.getPrimaryKey()))
                                                    .thenApply(clusterMetadata -> {
                                                        final Transformed<RealVector> transformedCentroid =
                                                                storageTransform.transform(Objects.requireNonNull(resultEntry.getVector()));
                                                        return new ClusterMetadataWithDistance(clusterMetadata,
                                                                transformedCentroid,
                                                                resultEntry.getDistance());
                                                    }),
                                    10);

                    final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable =
                            AsyncUtil.mapIterable(mapConcatIterable(getExecutor(), clusterMetadataIterable,
                                    clusterMetadataWithDistance ->
                                            primitives.fetchVectorReferencesIterable(readTransaction,
                                                    storageTransform,
                                                    clusterMetadataWithDistance.getClusterMetadata().getId()),
                                    10),
                                    vectorReference -> {
                                        final double distance =
                                                estimator.distance(transformedQueryVector, vectorReference.getVector());
                                        return new VectorReferenceAndDistance(vectorReference, distance);
                                    });

                    final AsyncIterable<VectorReferenceAndDistance> almostSortedVectorReferencesIterable =
                            almostSortedVectorReferencesIterable(vectorReferenceAndDistancesIterable,
                                    1000, getExecutor());

                    final Map<Tuple, CompletableFuture<UUID>> primaryKeyToVectorMetadataUuidFutureMap =
                            Maps.newConcurrentMap();
                    final AsyncIterable<VectorReferenceAndDistance> filteredByCurrentMetadataIterable =
                            MoreAsyncUtil.filterIterablePipelined(getExecutor(), almostSortedVectorReferencesIterable,
                                    vectorReferenceAndDistance -> {
                                        final VectorId vectorReferenceId =
                                                vectorReferenceAndDistance.getVectorReference().getId();
                                        final CompletableFuture<UUID> vectorMetadataFuture =
                                                primaryKeyToVectorMetadataUuidFutureMap.computeIfAbsent(
                                                        vectorReferenceId.getPrimaryKey(),
                                                        primaryKey ->
                                                                primitives.fetchVectorMetadata(readTransaction, primaryKey)
                                                                        .thenApply(VectorId::getUuid));
                                        return vectorMetadataFuture.thenApply(vectorMetadataUuid ->
                                                vectorMetadataUuid.equals(vectorReferenceId.getUuid()));
                                    }, 10);

                    final Set<Tuple> seenPrimaryKeys = Sets.newHashSet();
                    final var dedupedVectorReferenceAndDistances =
                            MoreAsyncUtil.filterIterable(getExecutor(), filteredByCurrentMetadataIterable,
                                    vectorReferenceAndDistance ->
                                            seenPrimaryKeys.add(vectorReferenceAndDistance.getVectorReference()
                                                    .getId().getPrimaryKey()));


                    return AsyncUtil.collect(updatedNeighborhood, getExecutor())
                            .thenCompose(results -> {
                                Verify.verify(!results.isEmpty());
                                return addToStatsIfNecessary(random, transaction, accessInfo, transformedNewVector);
                            });
                });
    }

    @Nonnull
    private static AsyncIterable<VectorReferenceAndDistance>
            almostSortedVectorReferencesIterable(@Nonnull final AsyncIterable<VectorReferenceAndDistance> iterable,
                                                 final int maxQueueSize, @Nonnull final Executor executor) {
        return MoreAsyncUtil.iterableOf(() -> new AlmostSortedAsyncIterator<>(iterable.iterator(),
                Comparator.comparing(VectorReferenceAndDistance::getDistance), maxQueueSize, executor),
                executor);
    }
}
