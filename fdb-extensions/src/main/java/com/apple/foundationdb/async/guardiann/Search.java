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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.apple.foundationdb.async.MoreAsyncUtil.mapConcatIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;

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

    @SuppressWarnings("checkstyle:MethodName")
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search(readTransaction, k, efSearch, queryVector)
                .thenApply(searchResult ->
                        postProcessSearchResult(searchResult.getStorageTransform(),
                                searchResult.getNearestReferences(), includeVectors));
    }

    @SuppressWarnings("checkstyle:MethodName")
    CompletableFuture<SearchResult> search(@Nonnull final ReadTransaction readTransaction,
                                           final int k,
                                           final int efSearch,
                                           @Nonnull final RealVector queryVector) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
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
                                                                storageTransform.transform(
                                                                        Objects.requireNonNull(resultEntry.getVector()));
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

                    final var l = AsyncUtil.collect(vectorReferenceAndDistancesIterable, getExecutor()).join();
                    System.out.println((long)l.size());
                    final var s = l.stream()
                            .map(item -> item.getVectorReference().getId().getPrimaryKey())
                            .collect(Collectors.toSet());
                    System.out.println((long)s.size());

                    final AsyncIterable<VectorReferenceAndDistance> almostSortedVectorReferencesIterable =
                            almostSortedVectorReferencesIterable(vectorReferenceAndDistancesIterable,
                                    efSearch, getExecutor());

                    final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataUuidFutureMap =
                            Maps.newConcurrentMap();
                    final AsyncIterable<VectorReferenceAndDistance> filteredByCurrentMetadataIterable =
                            MoreAsyncUtil.filterIterablePipelined(getExecutor(), almostSortedVectorReferencesIterable,
                                    vectorReferenceAndDistance -> {
                                        final VectorId vectorReferenceId =
                                                vectorReferenceAndDistance.getVectorReference().getId();
                                        final CompletableFuture<VectorMetadata> vectorMetadataFuture =
                                                primaryKeyToVectorMetadataUuidFutureMap.computeIfAbsent(
                                                        vectorReferenceId.getPrimaryKey(),
                                                        primaryKey ->
                                                                primitives.fetchVectorMetadata(readTransaction, primaryKey));
                                        return vectorMetadataFuture.thenApply(vectorMetadata ->
                                                vectorMetadata.getUuid().equals(vectorReferenceId.getUuid()));
                                    }, 10);

                    final Set<Tuple> seenPrimaryKeys = Sets.newHashSet();
                    final AsyncIterable<VectorReferenceAndDistance> dedupedVectorReferenceAndDistancesIterable =
                            MoreAsyncUtil.filterIterable(getExecutor(), filteredByCurrentMetadataIterable,
                                    vectorReferenceAndDistance ->
                                            seenPrimaryKeys.add(vectorReferenceAndDistance.getVectorReference()
                                                    .getId().getPrimaryKey()));
//                    final var l =
//                            Lists.newArrayList(AsyncUtil.collect(dedupedVectorReferenceAndDistancesIterable, getExecutor()).join());
//                    l.sort(Comparator.comparing(VectorReferenceAndDistance::getDistance));
//                    return CompletableFuture.completedFuture(new SearchResult(accessInfo, storageTransform, l.subList(0, k)));

                    //System.out.println(l.size());
                    //l.forEach(x -> System.out.println(x.getDistance()));

                    final CompletableFuture<List<VectorReferenceAndDistance>> nearestKReferencesFuture =
                            AsyncUtil.collect(
                                    AsyncUtil.mapIterable(
                                            MoreAsyncUtil.limitIterable(dedupedVectorReferenceAndDistancesIterable,
                                                    k, getExecutor()),
                                            vectorReferenceAndDistance ->
                                                    new VectorReferenceAndDistance(enrichVectorReference(primaryKeyToVectorMetadataUuidFutureMap,
                                                            vectorReferenceAndDistance.getVectorReference()),
                                                            vectorReferenceAndDistance.getDistance())), getExecutor());
                    return nearestKReferencesFuture.thenApply(nearestKReferences ->
                            new SearchResult(accessInfo, storageTransform, nearestKReferences));

                });
    }

    @Nonnull
    private ImmutableList<ResultEntry> postProcessSearchResult(@Nonnull final StorageTransform storageTransform,
                                                               @Nonnull List<VectorReferenceAndDistance> nearestReferences,
                                                               final boolean includeVectors) {
        final ImmutableList.Builder<ResultEntry> resultBuilder = ImmutableList.builder();

        for (int i = 0; i < nearestReferences.size(); i++) {
            final VectorReferenceAndDistance vectorReferenceAndDistance = nearestReferences.get(i);
            final VectorReference vectorReference = vectorReferenceAndDistance.getVectorReference();
            @Nullable final RealVector reconstructedVector =
                    includeVectors ? storageTransform.untransform(vectorReference.getVector()) : null;

            final VectorId vectorId = vectorReference.getId();
            @Nullable final Tuple additionalValues =
                    vectorId instanceof VectorMetadata ? ((VectorMetadata)vectorId).getAdditionalValues() : null;
            resultBuilder.add(
                    new ResultEntry(vectorReference.getId().getPrimaryKey(),
                            reconstructedVector, additionalValues, vectorReferenceAndDistance.getDistance(),
                            i));
        }
        return resultBuilder.build();
    }

    @SuppressWarnings("checkstyle:MethodName")
    CompletableFuture<Map<UUID, Integer>> globalAssignmentCheck(@Nonnull final ReadTransaction readTransaction,
                                                                @Nonnull final List<ResultEntry> centroids) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Estimator estimator = primitives.quantizer(accessInfo).estimator();

                    return MoreAsyncUtil.forEach(centroids,
                                    centroid -> {
                                        final UUID clusterId = StorageAdapter.clusterIdFromTuple(centroid.getPrimaryKey());
                                        return primitives.fetchClusterMetadata(readTransaction, clusterId)
                                                .thenCompose(clusterMetadata -> {
                                                    final Transformed<RealVector> transformedCentroid =
                                                            storageTransform.transform(
                                                                    Objects.requireNonNull(centroid.getVector()));
                                                    return primitives.fetchCluster(readTransaction,
                                                            storageTransform, clusterId, transformedCentroid);
                                                });
                                    },
                                    10, getExecutor())
                            .thenApply(clusters -> {
                                final Map<UUID, Integer> wrongAssignmentsMap = Maps.newHashMap();
                                for (final Cluster currentCluster : clusters) {
                                    final UUID currentClusterId = currentCluster.getClusterMetadata().getId();
                                    int allPrimaryAssignments = 0;
                                    int wrongAssignments = 0;
                                    for (final VectorReference vectorReference : currentCluster.getVectorReferences()) {
                                        if (vectorReference.isPrimaryCopy()) {
                                            final TreeSet<ClusterMetadataWithDistance> trueClusterDistances =
                                                    new TreeSet<>(Comparator.comparing(ClusterMetadataWithDistance::getDistance));
                                            for (final Cluster cluster : clusters) {
                                                final double distance =
                                                        estimator.distance(cluster.getCentroid(), vectorReference.getVector());
                                                trueClusterDistances.add(new ClusterMetadataWithDistance(cluster.getClusterMetadata(), cluster.getCentroid(), distance));
                                            }
                                            if (!trueClusterDistances.isEmpty()) {
                                                final UUID trueBestClusterId = Objects.requireNonNull(trueClusterDistances.pollFirst()).getClusterMetadata().getId();
                                                allPrimaryAssignments++;
                                                if (!trueBestClusterId.equals(currentClusterId)) {
                                                    wrongAssignments++;
                                                }
                                            }
                                        }
                                    }
                                    wrongAssignmentsMap.put(currentClusterId, wrongAssignments);
                                    if (logger.isInfoEnabled()) {
                                        logger.info("bad assignment; clusterId={}, allPrimaryAssignments={}, wrongAssignments={}", currentClusterId, allPrimaryAssignments, wrongAssignments);
                                    }
                                }
                                return wrongAssignmentsMap;
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

    @Nonnull
    private static VectorReference enrichVectorReference(@Nonnull final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataUuidFutureMap,
                                                         @Nonnull final VectorReference vectorReference) {
        final var vectorMetadata =
                Objects.requireNonNull(
                        Objects.requireNonNull(
                                        primaryKeyToVectorMetadataUuidFutureMap.get(vectorReference.getId().getPrimaryKey())).getNow(null));
        return vectorReference.withVectorId(vectorMetadata);
    }

    static class SearchResult {
        @Nullable
        private final AccessInfo accessInfo;
        @Nonnull
        private final StorageTransform storageTransform;
        @Nonnull
        private final List<VectorReferenceAndDistance> nearestReferences;

        public SearchResult(@Nullable final AccessInfo accessInfo,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final List<VectorReferenceAndDistance> nearestReferences) {
            this.accessInfo = accessInfo;
            this.storageTransform = storageTransform;
            this.nearestReferences = ImmutableList.copyOf(nearestReferences);
        }

        @Nullable
        public AccessInfo getAccessInfo() {
            return accessInfo;
        }

        @Nonnull
        public StorageTransform getStorageTransform() {
            return storageTransform;
        }

        @Nonnull
        public List<VectorReferenceAndDistance> getNearestReferences() {
            return nearestReferences;
        }
    }
}
