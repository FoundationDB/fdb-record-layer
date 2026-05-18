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
import com.google.common.base.Verify;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
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

import static com.apple.foundationdb.async.MoreAsyncUtil.mapConcatIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;

/**
 * Implements approximate nearest neighbor search over the Guardiann vector structure. The search
 * pipeline proceeds through the following stages:
 * <ol>
 *   <li><b>Candidate cluster selection</b> — queries the HNSW centroid index for the nearest cluster
 *       centroids to the query vector, up to {@link Config#searchMaxClusters()}.</li>
 *   <li><b>Distance-ratio pruning</b> — discards clusters whose centroid distance exceeds
 *       {@link Config#searchDistanceRatioCutoff()} times the nearest centroid distance, retaining
 *       at least {@link Config#searchMinClustersBeforePruning()} clusters.</li>
 *   <li><b>Vector reference retrieval</b> — scans the surviving clusters for individual vector
 *       references, computing distances to the query vector and collecting the top-{@code efSearch}
 *       closest candidates.</li>
 *   <li><b>Collapsed reference expansion</b> — if any top candidates represent collapsed (deduplicated)
 *       vectors, expands them back into their constituent vector IDs.</li>
 *   <li><b>Validation and enrichment</b> — verifies each candidate against current vector metadata
 *       (filtering stale references whose UUID no longer matches) and attaches additional stored
 *       values before trimming to the final {@code k} results.</li>
 * </ol>
 */
@API(API.Status.EXPERIMENTAL)
public class Search {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Search.class);

    @Nonnull
    private final Locator locator;

    /**
     * Initializes a new search operations instance.
     *
     * @param locator the {@link Locator} providing access to storage, configuration, and execution context
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

    /**
     * Performs a k-nearest neighbor search and returns the results as {@link ResultEntry} instances,
     * optionally including the reconstructed vectors.
     *
     * @param readTransaction the read transaction context
     * @param k the number of nearest neighbors to return
     * @param efSearch the size of the candidate pool (larger values improve recall at the cost of latency)
     * @param searchMaxClusters maximum number of clusters to probe
     * @param searchMinClustersBeforePruning minimum clusters before distance-ratio pruning kicks in
     * @param searchDistanceRatioCutoff distance ratio threshold for cluster pruning
     * @param includeVectors whether to include reconstructed vector data in the results
     * @param queryVector the query vector to search for
     * @return a future completing with up to {@code k} nearest neighbors sorted by distance
     */
    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    final int searchMaxClusters,
                                    final int searchMinClustersBeforePruning,
                                    final double searchDistanceRatioCutoff,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search(readTransaction, k, efSearch, searchMaxClusters, searchMinClustersBeforePruning,
                searchDistanceRatioCutoff, queryVector)
                .thenApply(searchResult ->
                        postProcessSearchResult(searchResult.storageTransform(),
                                searchResult.nearestReferences(), includeVectors));
    }

    /**
     * Executes the full search pipeline: fetches access info, then runs the five-stage search
     * (candidate selection, pruning, retrieval, expansion, enrichment) and wraps the results
     * in a {@link SearchResult}.
     *
     * @param readTransaction the read transaction context
     * @param k the number of final results to return
     * @param efSearch the candidate pool size for the top-K collection stages
     * @param searchMaxClusters maximum number of clusters to probe
     * @param searchMinClustersBeforePruning minimum clusters before pruning
     * @param searchDistanceRatioCutoff distance ratio threshold for pruning
     * @param queryVector the query vector
     * @return a future completing with the search result, or {@code null} if no vectors exist yet
     */
    @Nonnull
    CompletableFuture<SearchResult> search(@Nonnull final ReadTransaction readTransaction,
                                           final int k,
                                           final int efSearch,
                                           final int searchMaxClusters,
                                           final int searchMinClustersBeforePruning,
                                           final double searchDistanceRatioCutoff,
                                           @Nonnull final RealVector queryVector) {
        final Primitives primitives = primitives();
        final Config config = getConfig();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final Estimator estimator = primitives.quantizer(accessInfo).estimator();

                    return fetchCandidateClusters(readTransaction, primitives, storageTransform, queryVector, searchMaxClusters)
                            .thenApply(clusters -> pruneClusters(clusters, searchMinClustersBeforePruning, searchDistanceRatioCutoff))
                            .thenCompose(clusters ->
                                    retrieveVectorReferencesFromClusters(readTransaction, primitives,
                                            storageTransform, estimator, transformedQueryVector, clusters, efSearch))
                            .thenCompose(topReferences ->
                                    expandCollapsedReferencesIfNecessary(readTransaction, primitives, topReferences, efSearch))
                            .thenCompose(expanded ->
                                    enrichResults(readTransaction, primitives, expanded, k))
                            .thenApply(results -> new SearchResult(accessInfo, storageTransform, results));
                });
    }

    /**
     * Queries the HNSW centroid index for the nearest cluster centroids to the query vector, fetches
     * their metadata, and returns up to {@code searchMaxClusters} candidates sorted by centroid distance.
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param storageTransform the transform for converting raw vectors into the stored coordinate space
     * @param queryVector the query vector in its original coordinate space
     * @param searchMaxClusters maximum number of clusters to probe
     * @return a future completing with the candidate clusters sorted by centroid distance (ascending)
     */
    @Nonnull
    private CompletableFuture<List<ClusterMetadataWithDistance>>
            fetchCandidateClusters(@Nonnull final ReadTransaction readTransaction,
                                   @Nonnull final Primitives primitives,
                                   @Nonnull final StorageTransform storageTransform,
                                   @Nonnull final RealVector queryVector,
                                   final int searchMaxClusters) {
        final Config config = getConfig();

        final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                        primitives.centroidsOrderedByDistance(readTransaction, queryVector,
                                0.0d, null), getExecutor()), searchMaxClusters, getExecutor());

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
                        config.searchConcurrency());

        return AsyncUtil.collect(clusterMetadataIterable, getExecutor());
    }

    /**
     * Prunes the candidate cluster list by removing clusters whose centroid distance exceeds the
     * given ratio relative to the nearest centroid. Always retains at least
     * {@code searchMinClustersBeforePruning} clusters regardless of the ratio.
     *
     * @param clusterMetadataWithDistances the candidate clusters sorted by centroid distance (ascending)
     * @param searchMinClustersBeforePruning minimum clusters to retain before pruning
     * @param searchDistanceRatioCutoff distance ratio threshold
     * @return the pruned sublist
     */
    @Nonnull
    private List<ClusterMetadataWithDistance>
            pruneClusters(@Nonnull final List<ClusterMetadataWithDistance> clusterMetadataWithDistances,
                          final int searchMinClustersBeforePruning,
                          final double searchDistanceRatioCutoff) {
        if (clusterMetadataWithDistances.size() <= searchMinClustersBeforePruning) {
            if (logger.isInfoEnabled()) {
                logger.info("querying numClusters={}", clusterMetadataWithDistances.size());
            }
            return clusterMetadataWithDistances;
        }

        final double nearestCentroidDistance = clusterMetadataWithDistances.get(0).distance();
        int i;
        for (i = searchMinClustersBeforePruning; i < clusterMetadataWithDistances.size(); i++) {
            final ClusterMetadataWithDistance currentClusterMetadata = clusterMetadataWithDistances.get(i);
            if (currentClusterMetadata.distance() / nearestCentroidDistance > searchDistanceRatioCutoff) {
                break;
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("limiting query to numClusters={}", i);
        }
        return clusterMetadataWithDistances.subList(0, i);
    }

    /**
     * Scans all vector references in the given clusters, computes their distance to the query vector,
     * and collects the top-{@code efSearch} closest candidates using a distinct top-K structure
     * (deduplicating by vector ID).
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param storageTransform the transform applied to stored vectors
     * @param estimator the distance estimator
     * @param transformedQueryVector the query vector in the transformed coordinate space
     * @param clusters the pruned candidate clusters to scan
     * @param efSearch the candidate pool size
     * @return a future completing with the top candidates sorted by distance (best first)
     */
    @Nonnull
    private CompletableFuture<List<VectorReferenceAndDistance>>
            retrieveVectorReferencesFromClusters(@Nonnull final ReadTransaction readTransaction,
                                                 @Nonnull final Primitives primitives,
                                                 @Nonnull final StorageTransform storageTransform,
                                                 @Nonnull final Estimator estimator,
                                                 @Nonnull final Transformed<RealVector> transformedQueryVector,
                                                 @Nonnull final List<ClusterMetadataWithDistance> clusters,
                                                 final int efSearch) {
        final Config config = getConfig();

        final var boundedClusterMetadataIterable =
                MoreAsyncUtil.iterableFromCollection(
                        CompletableFuture.completedFuture(clusters), getExecutor());

        final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable =
                AsyncUtil.mapIterable(mapConcatIterable(getExecutor(), boundedClusterMetadataIterable,
                                clusterMetadataWithDistance ->
                                        primitives.fetchVectorReferencesIterable(readTransaction,
                                                storageTransform,
                                                clusterMetadataWithDistance.clusterMetadata().id()),
                                config.searchConcurrency()),
                        vectorReference -> {
                            final double distance =
                                    estimator.distance(transformedQueryVector, vectorReference.getVector());
                            return new VectorReferenceAndDistance(vectorReference, distance);
                        });

        final DistinctTopK<VectorReferenceAndDistance> distinctTopK =
                DistinctTopK.min(Comparator.comparing(VectorReferenceAndDistance::getDistance)
                        .thenComparing(d -> d.getVectorReference().getId()), efSearch);

        return distinctTopK.collect(vectorReferenceAndDistancesIterable, getExecutor());
    }

    /**
     * If any of the top references represent collapsed (deduplicated) vectors, expands them by
     * fetching the individual vector IDs from the collapsed set and re-running the top-K selection
     * over the expanded pool. Returns the original list unchanged if no collapsed references are present.
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param topReferences the current top-K candidates (may contain collapsed entries)
     * @param efSearch the candidate pool size for the expanded top-K
     * @return a future completing with the expanded top candidates
     */
    @Nonnull
    private CompletableFuture<List<VectorReferenceAndDistance>>
            expandCollapsedReferencesIfNecessary(@Nonnull final ReadTransaction readTransaction,
                                                 @Nonnull final Primitives primitives,
                                                 @Nonnull final List<VectorReferenceAndDistance> topReferences,
                                                 final int efSearch) {
        final Config config = getConfig();

        boolean foundCollapsedReferences = false;
        for (final VectorReferenceAndDistance referenceAndDistance : topReferences) {
            if (referenceAndDistance.getVectorReference().isCollapsed()) {
                foundCollapsedReferences = true;
                break;
            }
        }
        if (!foundCollapsedReferences) {
            return CompletableFuture.completedFuture(topReferences);
        }

        final var topReferencesIterable =
                MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(topReferences), getExecutor());

        final AsyncIterable<VectorReferenceAndDistance> expandedTopReferencesIterable =
                MoreAsyncUtil.mapConcatIterable(getExecutor(), topReferencesIterable,
                        vectorReferenceAndDistance -> {
                            final VectorReference vectorReference = vectorReferenceAndDistance.getVectorReference();
                            if (!vectorReference.isCollapsed()) {
                                return MoreAsyncUtil.mapToIterable(vectorReferenceAndDistance,
                                        item -> CompletableFuture.completedFuture(vectorReferenceAndDistance));
                            }

                            final UUID signature = StorageAdapter.signatureUuid(vectorReference.getVector());
                            return AsyncUtil.mapIterable(
                                    primitives.fetchCollapsedVectorIdsIterable(readTransaction, signature),
                                    vectorId -> vectorReferenceAndDistance.withVectorReference(
                                            vectorReference.withVectorId(vectorId)));
                        }, config.searchConcurrency());

        final DistinctTopK<VectorReferenceAndDistance> expandedDistinctTopK =
                DistinctTopK.min(Comparator.comparing(VectorReferenceAndDistance::getDistance)
                        .thenComparing(d -> d.getVectorReference().getId()), efSearch);

        return expandedDistinctTopK.collect(expandedTopReferencesIterable, getExecutor());
    }

    /**
     * Filters candidates by verifying that each reference's UUID matches the current vector metadata,
     * discarding stale references from vectors that have been updated or deleted since the reference
     * was written. Enriches surviving candidates with additional stored values from the metadata and
     * trims the result to the final {@code k} entries.
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param topReferences the candidates to filter and enrich
     * @param k the maximum number of results to return
     * @return a future completing with the filtered and enriched results
     */
    @Nonnull
    private CompletableFuture<List<VectorReferenceAndDistance>>
            enrichResults(@Nonnull final ReadTransaction readTransaction,
                          @Nonnull final Primitives primitives,
                          @Nonnull final List<VectorReferenceAndDistance> topReferences,
                          final int k) {
        final Config config = getConfig();
        final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataFutureMap =
                Maps.newConcurrentMap();

        return MoreAsyncUtil.forEach(topReferences,
                        vectorReferenceAndDistance -> {
                            final VectorId vectorReferenceId =
                                    vectorReferenceAndDistance.getVectorReference().getId();
                            final CompletableFuture<VectorMetadata> vectorMetadataFuture =
                                    primaryKeyToVectorMetadataFutureMap.computeIfAbsent(
                                            vectorReferenceId.getPrimaryKey(),
                                            primaryKey ->
                                                    primitives.fetchVectorMetadata(readTransaction, primaryKey));
                            return vectorMetadataFuture.thenApply(vectorMetadata -> {
                                if (vectorMetadata.getUuid().equals(vectorReferenceId.getUuid())) {
                                    return vectorReferenceAndDistance;
                                } else {
                                    return null;
                                }
                            });
                        }, config.searchConcurrency(), getExecutor())
                .thenApply(vectorReferenceAndDistances -> {
                    final ImmutableList.Builder<VectorReferenceAndDistance> enrichedResultsBuilder =
                            ImmutableList.builder();
                    int numEnrichedResults = 0;
                    for (final VectorReferenceAndDistance vectorReferenceAndDistance : vectorReferenceAndDistances) {
                        if (numEnrichedResults >= k) {
                            break;
                        }
                        enrichedResultsBuilder.add(new VectorReferenceAndDistance(
                                enrichVectorReference(primaryKeyToVectorMetadataFutureMap,
                                        vectorReferenceAndDistance.getVectorReference()),
                                vectorReferenceAndDistance.getDistance()));
                        numEnrichedResults++;
                    }
                    if (numEnrichedResults < k) {
                        logger.warn("Insufficient data to form result set (numResults={}). Consider increasing efSearch for k={}",
                                numEnrichedResults, k);
                    }
                    return enrichedResultsBuilder.build();
                });
    }

    @Nonnull
    CompletableFuture<SearchResult> searchOrderedByDistance(@Nonnull final ReadTransaction readTransaction,
                                                            final int k,
                                                            final int searchMaxClusters,
                                                            final int efSearch,
                                                            @Nonnull final RealVector queryVector,
                                                            final double minimumRadiusCluster,
                                                            final double minimumRadius,
                                                            @Nullable final Tuple minimumPrimaryKey) {
        final Primitives primitives = primitives();
        final Config config = getConfig();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final Estimator estimator = primitives.quantizer(accessInfo).estimator();

                    final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                            MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                                    primitives.centroidsOrderedByDistance(readTransaction, queryVector,
                                            minimumRadiusCluster, null), getExecutor()),
                                    searchMaxClusters, getExecutor());

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
                                                                transformedCentroid, resultEntry.getDistance());
                                                    }),
                                    config.searchConcurrency());

                    final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable =
                            AsyncUtil.mapIterable(mapConcatIterable(getExecutor(), clusterMetadataIterable,
                                            clusterMetadataWithDistance ->
                                                    primitives.fetchVectorReferencesIterable(readTransaction,
                                                            storageTransform,
                                                            clusterMetadataWithDistance.clusterMetadata().id()),
                                            config.searchConcurrency()),
                                    vectorReference -> {
                                        final double distance =
                                                estimator.distance(transformedQueryVector, vectorReference.getVector());
                                        return new VectorReferenceAndDistance(vectorReference, distance);
                                    });

                    // Expand collapsed references inline: a collapsed reference becomes one entry
                    // per vector ID behind the signature, all sharing the same distance.
                    final AsyncIterable<VectorReferenceAndDistance> expandedVectorReferenceAndDistancesIterable =
                            MoreAsyncUtil.mapConcatIterable(getExecutor(), vectorReferenceAndDistancesIterable,
                                    vectorReferenceAndDistance -> {
                                        final VectorReference vectorReference = vectorReferenceAndDistance.getVectorReference();
                                        if (!vectorReference.isCollapsed()) {
                                            return MoreAsyncUtil.mapToIterable(vectorReferenceAndDistance,
                                                    item -> CompletableFuture.completedFuture(vectorReferenceAndDistance));
                                        }
                                        final UUID signature = StorageAdapter.signatureUuid(vectorReference.getVector());
                                        return AsyncUtil.mapIterable(
                                                primitives.fetchCollapsedVectorIdsIterable(readTransaction, signature),
                                                vectorId -> vectorReferenceAndDistance.withVectorReference(
                                                        vectorReference.withVectorId(vectorId)));
                                    }, config.searchConcurrency());

                    final AsyncIterable<VectorReferenceAndDistance> filteredVectorReferenceAndDistancesIterable =
                            MoreAsyncUtil.filterIterable(getExecutor(), expandedVectorReferenceAndDistancesIterable,
                                    vectorReferenceAndDistance -> {
                                        final int distanceComparison = Double.compare(vectorReferenceAndDistance.getDistance(), minimumRadius);
                                        if (distanceComparison != 0) {
                                            return distanceComparison > 0; // keep the ones that are bigger than minimum radius
                                        }
                                        if (minimumPrimaryKey == null) {
                                            return true;
                                        }
                                        final int tupleComparison =
                                                Objects.compare(vectorReferenceAndDistance.getVectorReference().getId().getPrimaryKey(),
                                                        minimumPrimaryKey, Comparator.naturalOrder());
                                        return tupleComparison > 0;
                                    });

                    final AsyncIterable<VectorReferenceAndDistance> almostSortedVectorReferencesIterable =
                            almostSortedVectorReferencesIterable(filteredVectorReferenceAndDistancesIterable,
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
                                    }, config.searchConcurrency());

                    final Set<Tuple> seenPrimaryKeys = Sets.newHashSet();
                    final AsyncIterable<VectorReferenceAndDistance> dedupedVectorReferenceAndDistancesIterable =
                            MoreAsyncUtil.filterIterable(getExecutor(), filteredByCurrentMetadataIterable,
                                    vectorReferenceAndDistance ->
                                            seenPrimaryKeys.add(vectorReferenceAndDistance.getVectorReference()
                                                    .getId().getPrimaryKey()));

                    final CompletableFuture<List<VectorReferenceAndDistance>> nearestKReferencesFuture =
                            AsyncUtil.collect(
                                    AsyncUtil.mapIterable(
                                            MoreAsyncUtil.limitIterable(dedupedVectorReferenceAndDistancesIterable,
                                                    k, getExecutor()),
                                            vectorReferenceAndDistance ->
                                                    new VectorReferenceAndDistance(
                                                            enrichVectorReference(primaryKeyToVectorMetadataUuidFutureMap,
                                                                    vectorReferenceAndDistance.getVectorReference()),
                                                            vectorReferenceAndDistance.getDistance())), getExecutor());
                    return nearestKReferencesFuture.thenApply(nearestKReferences ->
                            new SearchResult(accessInfo, storageTransform, nearestKReferences));
                });
    }

    /**
     * Converts internal search results into the public {@link ResultEntry} format, optionally
     * reconstructing the original vector by un-transforming from the stored representation.
     *
     * @param storageTransform the transform used to reconstruct vectors into their original coordinate space
     * @param nearestReferences the internal search results to convert
     * @param includeVectors whether to include the reconstructed vector data in each result entry
     * @return the search results as a list of {@link ResultEntry} instances
     */
    @Nonnull
    private ImmutableList<ResultEntry> postProcessSearchResult(@Nonnull final StorageTransform storageTransform,
                                                               @Nonnull final List<VectorReferenceAndDistance> nearestReferences,
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
    CompletableFuture<ListMultimap<UUID, Tuple>> globalAssignmentCheck(@Nonnull final ReadTransaction readTransaction,
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
                                final ListMultimap<UUID, Tuple> assignmentsMap = ArrayListMultimap.create();
                                for (final Cluster currentCluster : clusters) {
                                    final ClusterMetadata clusterMetadata = currentCluster.clusterMetadata();
                                    final UUID currentClusterId = clusterMetadata.id();
                                    int numAllPrimaryAssignments = 0;
                                    int numWrongAssignments = 0;
                                    int numReplicatedVectors = 0;
                                    final Map<Integer, Integer> wrongAssignmentsByRankMap = Maps.newHashMap();
                                    for (final VectorReference vectorReference : currentCluster.vectorReferences()) {
                                        if (vectorReference.isPrimaryCopy()) {
                                            assignmentsMap.put(currentClusterId, vectorReference.getId().getPrimaryKey());
                                            numAllPrimaryAssignments++;
                                            final TreeSet<ClusterMetadataWithDistance> trueClusterDistances =
                                                    new TreeSet<>(Comparator.comparing(ClusterMetadataWithDistance::distance));
                                            for (final Cluster cluster : clusters) {
                                                final double distance =
                                                        estimator.distance(cluster.centroid(), vectorReference.getVector());
                                                trueClusterDistances.add(
                                                        new ClusterMetadataWithDistance(cluster.clusterMetadata(),
                                                                cluster.centroid(), distance));
                                            }
                                            boolean found = false;
                                            int rank = 0;
                                            while (!trueClusterDistances.isEmpty()) {
                                                final UUID nextClusterId =
                                                        Objects.requireNonNull(trueClusterDistances.pollFirst())
                                                                .clusterMetadata().id();
                                                if (nextClusterId.equals(currentClusterId)) {
                                                    wrongAssignmentsByRankMap.compute(rank, (r, oldCount) ->
                                                            Objects.requireNonNullElse(oldCount, 0) + 1);
                                                    if (rank != 0) {
                                                        numWrongAssignments++;
                                                    }
                                                    found = true;
                                                    break;
                                                }
                                                rank ++;
                                            }
                                            Verify.verify(found);
                                        } else {
                                            numReplicatedVectors ++;
                                        }
                                    }
                                    if (numAllPrimaryAssignments != clusterMetadata.getNumPrimaryVectors()) {
                                        if (logger.isErrorEnabled()) {
                                            logger.error("""
                                                    cluster metadata count of primary vectors is wrong; \
                                                    exected={}; actual={}
                                                    """,
                                                    clusterMetadata.getNumPrimaryVectors(), numAllPrimaryAssignments);
                                        }
                                    }
                                    if (logger.isInfoEnabled()) {
                                        logger.info(
                                                """
                                                assignment stats; clusterId={}, numAllPrimaryAssignments={}, \
                                                numWrongAssignments={}, numReplicated={}, stdDev={}, \
                                                wrongAssignmentsByRankMap={} \
                                                """,
                                                currentClusterId, numAllPrimaryAssignments, numWrongAssignments,
                                                numReplicatedVectors, clusterMetadata.standardDeviation(),
                                                wrongAssignmentsByRankMap);
                                    }
                                }
                                return assignmentsMap;
                            });
                });
    }

    /**
     * Diagnostic method that computes cluster overlap statistics for a set of query vectors. For each
     * query vector, counts how many clusters "overlap" — i.e., the query falls within the cluster's ball
     * defined by {@code dist(query, centroid) <= maxEver}. Logs per-query overlap counts and aggregate
     * statistics (min, max, mean).
     *
     * @param readTransaction the read transaction context
     * @param queryVectors the query vectors to evaluate overlap for
     * @param centroids the list of all cluster centroids (as returned by an HNSW scan)
     * @return a future completing with a list of overlap counts (one per query vector)
     */
    @Nonnull
    CompletableFuture<List<Integer>> clusterOverlapDiagnostics(@Nonnull final ReadTransaction readTransaction,
                                                               @Nonnull final List<RealVector> queryVectors,
                                                               @Nonnull final List<ResultEntry> centroids) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(ImmutableList.of());
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Estimator estimator = primitives.quantizer(accessInfo).estimator();

                    return MoreAsyncUtil.forEach(centroids,
                                    centroid -> {
                                        final UUID clusterId = StorageAdapter.clusterIdFromTuple(centroid.getPrimaryKey());
                                        return primitives.fetchClusterMetadata(readTransaction, clusterId)
                                                .thenApply(clusterMetadata -> {
                                                    final Transformed<RealVector> transformedCentroid =
                                                            storageTransform.transform(
                                                                    Objects.requireNonNull(centroid.getVector()));
                                                    return new ClusterMetadataWithDistance(clusterMetadata,
                                                            transformedCentroid, 0.0d);
                                                });
                                    },
                                    getConfig().searchConcurrency(), getExecutor())
                            .thenApply(clusterMetadataList -> {
                                final ImmutableList.Builder<Integer> overlapCountsBuilder = ImmutableList.builder();
                                int totalOverlaps = 0;
                                int minOverlap = Integer.MAX_VALUE;
                                int maxOverlap = 0;

                                for (final RealVector queryVector : queryVectors) {
                                    final Transformed<RealVector> transformedQuery = storageTransform.transform(queryVector);
                                    int overlapCount = 0;

                                    for (final ClusterMetadataWithDistance clusterMetadataWithDistance : clusterMetadataList) {
                                        final double maxEver = clusterMetadataWithDistance.clusterMetadata()
                                                .runningStandardDeviation().maxEver();
                                        if (Double.isNaN(maxEver)) {
                                            continue;
                                        }
                                        final double distanceToCentroid = estimator.distance(
                                                transformedQuery, clusterMetadataWithDistance.centroid());
                                        if (distanceToCentroid <= maxEver) {
                                            overlapCount++;
                                        }
                                    }

                                    overlapCountsBuilder.add(overlapCount);
                                    totalOverlaps += overlapCount;
                                    minOverlap = Math.min(minOverlap, overlapCount);
                                    maxOverlap = Math.max(maxOverlap, overlapCount);
                                }

                                final int numQueries = queryVectors.size();
                                if (logger.isInfoEnabled() && numQueries > 0) {
                                    logger.info("cluster overlap diagnostics; numQueries={}, numClusters={}, " +
                                                    "minOverlap={}, maxOverlap={}, meanOverlap={}",
                                            numQueries, clusterMetadataList.size(),
                                            minOverlap, maxOverlap,
                                            (double) totalOverlaps / numQueries);
                                }

                                return overlapCountsBuilder.build();
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

    /**
     * Replaces a vector reference's ID with the full {@link VectorMetadata} (including additional values)
     * from the already-resolved metadata future map.
     *
     * @param primaryKeyToVectorMetadataUuidFutureMap the map of already-fetched metadata futures
     * @param vectorReference the reference to enrich
     * @return the vector reference with its ID replaced by the full metadata
     */
    @Nonnull
    private static VectorReference enrichVectorReference(@Nonnull final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataUuidFutureMap,
                                                         @Nonnull final VectorReference vectorReference) {
        final var vectorMetadata =
                Objects.requireNonNull(
                        Objects.requireNonNull(
                                        primaryKeyToVectorMetadataUuidFutureMap.get(vectorReference.getId().getPrimaryKey())).getNow(null));
        return vectorReference.withVectorId(vectorMetadata);
    }

    /**
     * Internal result of the search pipeline, carrying the access context, the storage transform needed
     * to reconstruct vectors, and the list of nearest references sorted by distance.
     *
     * @param accessInfo the access info used during this search (provides quantizer/transform context)
     * @param storageTransform the transform for reconstructing vectors into their original coordinate space
     * @param nearestReferences the search results sorted by distance (best first), defensively copied
     */
    record SearchResult(@Nullable AccessInfo accessInfo,
                        @Nonnull StorageTransform storageTransform,
                        @Nonnull List<VectorReferenceAndDistance> nearestReferences) {
        SearchResult(@Nullable final AccessInfo accessInfo,
                     @Nonnull final StorageTransform storageTransform,
                     @Nonnull final List<VectorReferenceAndDistance> nearestReferences) {
            this.accessInfo = accessInfo;
            this.storageTransform = storageTransform;
            this.nearestReferences = ImmutableList.copyOf(nearestReferences);
        }
    }
}
