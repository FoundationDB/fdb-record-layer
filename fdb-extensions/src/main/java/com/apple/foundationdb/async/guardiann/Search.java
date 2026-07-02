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
import com.apple.foundationdb.async.common.DistinctTopK;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.DistanceEstimator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.MoreAsyncUtil.mapConcatIterable;
import static com.apple.foundationdb.async.MoreAsyncUtil.mapIterablePipelined;

/**
 * Implements approximate nearest neighbor search over the Guardiann vector structure. All performance/recall
 * tuning knobs ({@code searchMaxClusters}, the candidate-pool size, the pruning thresholds, concurrency) are
 * supplied as a single {@link SearchConfig}; the semantic inputs ({@code k}, the query, {@code includeVectors})
 * stay direct arguments. The search pipeline proceeds through the following stages:
 * <ol>
 *   <li><b>Candidate cluster selection</b> — queries the HNSW centroid index for the nearest cluster
 *       centroids to the query vector, up to {@link SearchConfig#searchMaxClusters()}.</li>
 *   <li><b>Distance-ratio pruning</b> — discards clusters whose centroid distance exceeds
 *       {@link SearchConfig#searchDistanceRatioCutoff()} times the nearest centroid distance, retaining
 *       at least {@link SearchConfig#searchMinClustersBeforePruning()} clusters.</li>
 *   <li><b>Vector reference retrieval</b> — scans the surviving clusters for individual vector
 *       references, computing distances to the query vector and collecting the top-{@link
 *       SearchConfig#candidatePoolSize() candidatePoolSize} closest candidates.</li>
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

    /**
     * Performs a k-nearest neighbor search and returns the results as {@link ResultEntry} instances,
     * optionally including the reconstructed vectors.
     *
     * @param readTransaction the read transaction context
     * @param k the number of nearest neighbors to return
     * @param searchConfig the performance/recall tuning knobs for this search (see {@link SearchConfig})
     * @param includeVectors whether to include reconstructed vector data in the results
     * @param queryVector the query vector to search for
     * @return a future completing with up to {@code k} nearest neighbors sorted by distance
     */
    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    @Nonnull final SearchConfig searchConfig,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search(readTransaction, k, searchConfig, queryVector)
                .thenApply(searchResult ->
                        searchResult == null
                        ? ImmutableList.of()
                        : postProcessSearchResult(searchResult.storageTransform(),
                                searchResult.nearestRecords(), includeVectors));
    }

    /**
     * Executes the full search pipeline: fetches access info, then runs the five-stage search
     * (candidate selection, pruning, retrieval, expansion, enrichment) and wraps the results
     * in a {@link SearchResult}.
     *
     * @param readTransaction the read transaction context
     * @param k the number of final results to return
     * @param searchConfig the performance/recall tuning knobs for this search (see {@link SearchConfig})
     * @param queryVector the query vector
     * @return a future completing with the search result, or {@code null} if no vectors exist yet
     */
    @Nonnull
    CompletableFuture<SearchResult> search(@Nonnull final ReadTransaction readTransaction,
                                           final int k,
                                           @Nonnull final SearchConfig searchConfig,
                                           @Nonnull final RealVector queryVector) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final DistanceEstimator estimator = primitives.quantizer(accessInfo).estimator();

                    return fetchCandidateClusters(readTransaction, primitives, storageTransform, queryVector, searchConfig)
                            .thenApply(clusters -> pruneClusters(clusters, searchConfig))
                            .thenCompose(clusters ->
                                    retrieveVectorReferencesFromClusters(readTransaction, primitives,
                                            storageTransform, estimator, transformedQueryVector, clusters, searchConfig))
                            .thenCompose(topReferences ->
                                    expandCollapsedReferencesIfNecessary(readTransaction, primitives, topReferences, searchConfig))
                            .thenCompose(expanded ->
                                    enrichResults(readTransaction, primitives, expanded, k, searchConfig))
                            .thenApply(results -> new SearchResult(accessInfo, storageTransform, results));
                });
    }

    /**
     * Queries the HNSW centroid index for the nearest cluster centroids to the query vector, fetches
     * their metadata, and returns up to {@link SearchConfig#searchMaxClusters()} candidates sorted by
     * centroid distance.
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param storageTransform the transform for converting raw vectors into the stored coordinate space
     * @param queryVector the query vector in its original coordinate space
     * @param searchConfig the search tuning knobs (probed-cluster cap and centroid-walk exploration factors)
     * @return a future completing with the candidate clusters sorted by centroid distance (ascending)
     */
    @Nonnull
    private CompletableFuture<List<ClusterMetadataWithDistance>>
            fetchCandidateClusters(@Nonnull final ReadTransaction readTransaction,
                                   @Nonnull final Primitives primitives,
                                   @Nonnull final StorageTransform storageTransform,
                                   @Nonnull final RealVector queryVector,
                                   @Nonnull final SearchConfig searchConfig) {
        final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                        primitives.centroidsOrderedByDistance(readTransaction, queryVector, 0.0d, null,
                                searchConfig.centroidEfRingSearch(), searchConfig.centroidEfOutwardSearch()),
                        getExecutor()), searchConfig.searchMaxClusters(), getExecutor());

        final AsyncIterable<ClusterMetadataWithDistance> clusterMetadataIterable =
                mapIterablePipelined(getExecutor(), clusterCentroidEntriesByDistanceIterable,
                        resultEntry ->
                                primitives.fetchClusterMetadata(readTransaction,
                                                StorageAdapter.clusterIdFromTuple(resultEntry.primaryKey()))
                                        .thenApply(clusterMetadata -> {
                                            final Transformed<RealVector> transformedCentroid =
                                                    storageTransform.transform(
                                                            Objects.requireNonNull(resultEntry.vector()));
                                            return new ClusterMetadataWithDistance(clusterMetadata,
                                                    transformedCentroid,
                                                    resultEntry.distance());
                                        }),
                        searchConfig.searchConcurrency());

        return AsyncUtil.collect(clusterMetadataIterable, getExecutor());
    }

    /**
     * Prunes the candidate cluster list by removing clusters whose centroid distance exceeds the
     * given ratio relative to the nearest centroid. Always retains at least
     * {@link SearchConfig#searchMinClustersBeforePruning()} clusters regardless of the ratio.
     *
     * @param clusterMetadataWithDistances the candidate clusters sorted by centroid distance (ascending)
     * @param searchConfig the search tuning knobs (the min-clusters floor and distance-ratio cutoff)
     * @return the pruned sublist
     */
    @Nonnull
    private List<ClusterMetadataWithDistance>
            pruneClusters(@Nonnull final List<ClusterMetadataWithDistance> clusterMetadataWithDistances,
                          @Nonnull final SearchConfig searchConfig) {
        final int searchMinClustersBeforePruning = searchConfig.searchMinClustersBeforePruning();
        final double searchDistanceRatioCutoff = searchConfig.searchDistanceRatioCutoff();
        if (clusterMetadataWithDistances.size() <= searchMinClustersBeforePruning) {
            if (logger.isTraceEnabled()) {
                logger.trace("querying numClusters={}", clusterMetadataWithDistances.size());
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
        if (logger.isTraceEnabled()) {
            logger.trace("limiting query to numClusters={}", i);
        }
        return clusterMetadataWithDistances.subList(0, i);
    }

    /**
     * Scans all vector references in the given clusters, computes their distance to the query vector,
     * and collects the top-{@link SearchConfig#candidatePoolSize()} closest candidates using a distinct
     * top-K structure (deduplicating by vector ID).
     *
     * @param readTransaction the read transaction context
     * @param primitives primitives for database access
     * @param storageTransform the transform applied to stored vectors
     * @param estimator the distance estimator
     * @param transformedQueryVector the query vector in the transformed coordinate space
     * @param clusters the pruned candidate clusters to scan
     * @param searchConfig the search tuning knobs (candidate-pool size and fan-out concurrency)
     * @return a future completing with the top candidates sorted by distance (best first)
     */
    @Nonnull
    private CompletableFuture<List<VectorReferenceAndDistance>>
            retrieveVectorReferencesFromClusters(@Nonnull final ReadTransaction readTransaction,
                                                 @Nonnull final Primitives primitives,
                                                 @Nonnull final StorageTransform storageTransform,
                                                 @Nonnull final DistanceEstimator estimator,
                                                 @Nonnull final Transformed<RealVector> transformedQueryVector,
                                                 @Nonnull final List<ClusterMetadataWithDistance> clusters,
                                                 @Nonnull final SearchConfig searchConfig) {
        final AsyncIterable<ClusterMetadataWithDistance> boundedClusterMetadataIterable =
                MoreAsyncUtil.iterableFromCollection(
                        CompletableFuture.completedFuture(clusters), getExecutor());

        final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable =
                AsyncUtil.mapIterable(mapConcatIterable(getExecutor(), boundedClusterMetadataIterable,
                                clusterMetadataWithDistance ->
                                        primitives.fetchVectorReferencesIterable(readTransaction,
                                                storageTransform,
                                                clusterMetadataWithDistance.clusterMetadata().id()),
                                searchConfig.searchConcurrency()),
                        vectorReference -> {
                            final double distance =
                                    estimator.distance(transformedQueryVector, vectorReference.vector());
                            return new VectorReferenceAndDistance(vectorReference, distance);
                        });

        final DistinctTopK<VectorReferenceAndDistance> distinctTopK =
                DistinctTopK.min(Comparator.comparing(VectorReferenceAndDistance::distance)
                        .thenComparing(d -> d.vectorReference().id()), searchConfig.candidatePoolSize());

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
     * @param searchConfig the search tuning knobs (candidate-pool size for the expanded top-K and concurrency)
     * @return a future completing with the expanded top candidates
     */
    @Nonnull
    private CompletableFuture<List<VectorReferenceAndDistance>>
            expandCollapsedReferencesIfNecessary(@Nonnull final ReadTransaction readTransaction,
                                                 @Nonnull final Primitives primitives,
                                                 @Nonnull final List<VectorReferenceAndDistance> topReferences,
                                                 @Nonnull final SearchConfig searchConfig) {
        boolean foundCollapsedReferences = false;
        for (final VectorReferenceAndDistance referenceAndDistance : topReferences) {
            if (referenceAndDistance.vectorReference().isCollapsed()) {
                foundCollapsedReferences = true;
                break;
            }
        }
        if (!foundCollapsedReferences) {
            return CompletableFuture.completedFuture(topReferences);
        }

        final AsyncIterable<VectorReferenceAndDistance> topReferencesIterable =
                MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(topReferences), getExecutor());

        final AsyncIterable<VectorReferenceAndDistance> expandedTopReferencesIterable =
                mapConcatIterable(getExecutor(), topReferencesIterable,
                        vectorReferenceAndDistance -> {
                            final VectorReference vectorReference = vectorReferenceAndDistance.vectorReference();
                            if (!vectorReference.isCollapsed()) {
                                return MoreAsyncUtil.mapToIterable(vectorReferenceAndDistance,
                                        item -> CompletableFuture.completedFuture(vectorReferenceAndDistance));
                            }

                            final UUID signature = StorageAdapter.signatureUuid(vectorReference.vector());
                            return AsyncUtil.mapIterable(
                                    primitives.fetchCollapsedVectorIdsIterable(readTransaction, signature),
                                    vectorId -> vectorReferenceAndDistance.withVectorReference(
                                            vectorReference.withVectorId(vectorId)));
                        }, searchConfig.searchConcurrency());

        final DistinctTopK<VectorReferenceAndDistance> expandedDistinctTopK =
                DistinctTopK.min(Comparator.comparing(VectorReferenceAndDistance::distance)
                        .thenComparing(d -> d.vectorReference().id()), searchConfig.candidatePoolSize());

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
     * @param searchConfig the search tuning knobs (fan-out concurrency)
     * @return a future completing with the filtered and enriched results
     */
    @Nonnull
    private CompletableFuture<List<VectorRecord>>
            enrichResults(@Nonnull final ReadTransaction readTransaction,
                          @Nonnull final Primitives primitives,
                          @Nonnull final List<VectorReferenceAndDistance> topReferences,
                          final int k,
                          @Nonnull final SearchConfig searchConfig) {
        final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataFutureMap =
                Maps.newConcurrentMap();

        return MoreAsyncUtil.forEach(topReferences,
                        vectorReferenceAndDistance -> {
                            final VectorId vectorReferenceId =
                                    vectorReferenceAndDistance.vectorReference().id();
                            final CompletableFuture<VectorMetadata> vectorMetadataFuture =
                                    primaryKeyToVectorMetadataFutureMap.computeIfAbsent(
                                            vectorReferenceId.primaryKey(),
                                            primaryKey ->
                                                    primitives.fetchVectorMetadata(readTransaction, primaryKey));
                            return vectorMetadataFuture.thenApply(vectorMetadata -> {
                                if (vectorMetadata != null &&
                                        vectorMetadata.vectorId().uuid().equals(vectorReferenceId.uuid())) {
                                    return vectorReferenceAndDistance;
                                } else {
                                    return null;
                                }
                            });
                        }, searchConfig.searchConcurrency(), getExecutor())
                .thenApply(vectorReferenceAndDistances -> {
                    final ImmutableList.Builder<VectorRecord> enrichedResultsBuilder =
                            ImmutableList.builder();
                    int numEnrichedResults = 0;
                    for (final VectorReferenceAndDistance vectorReferenceAndDistance : vectorReferenceAndDistances) {
                        if (numEnrichedResults >= k) {
                            break;
                        }
                        if (vectorReferenceAndDistance == null) {
                            continue;
                        }
                        enrichedResultsBuilder.add(
                                enrichVectorReference(primaryKeyToVectorMetadataFutureMap,
                                        vectorReferenceAndDistance.vectorReference(),
                                        vectorReferenceAndDistance.distance()));
                        numEnrichedResults++;
                    }
                    if (numEnrichedResults < k) {
                        logger.warn("Insufficient data to form result set (numResults={}). Consider increasing candidatePoolSize for k={}",
                                numEnrichedResults, k);
                    }
                    return enrichedResultsBuilder.build();
                });
    }

    @Nonnull
    CompletableFuture<SearchResult> searchOrderedByDistance(@Nonnull final ReadTransaction readTransaction,
                                                            final int k,
                                                            @Nonnull final SearchConfig searchConfig,
                                                            @Nonnull final RealVector queryVector,
                                                            final double minimumRadiusCluster,
                                                            final double minimumRadius,
                                                            @Nullable final Tuple minimumPrimaryKey) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final DistanceEstimator estimator = primitives.quantizer(accessInfo).estimator();

                    final AsyncIterable<ResultEntry> clusterCentroidEntriesByDistanceIterable =
                            MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                                    primitives.centroidsOrderedByDistance(readTransaction, queryVector,
                                            minimumRadiusCluster, null, searchConfig.centroidEfRingSearch(),
                                            searchConfig.centroidEfOutwardSearch()), getExecutor()),
                                    searchConfig.searchMaxClusters(), getExecutor());

                    final AsyncIterable<ClusterMetadataWithDistance> clusterMetadataIterable =
                            mapIterablePipelined(getExecutor(), clusterCentroidEntriesByDistanceIterable,
                                    resultEntry ->
                                            primitives.fetchClusterMetadata(readTransaction,
                                                            StorageAdapter.clusterIdFromTuple(resultEntry.primaryKey()))
                                                    .thenApply(clusterMetadata -> {
                                                        final Transformed<RealVector> transformedCentroid =
                                                                storageTransform.transform(
                                                                        Objects.requireNonNull(resultEntry.vector()));
                                                        return new ClusterMetadataWithDistance(clusterMetadata,
                                                                transformedCentroid, resultEntry.distance());
                                                    }),
                                    searchConfig.searchConcurrency());

                    final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable =
                            AsyncUtil.mapIterable(mapConcatIterable(getExecutor(), clusterMetadataIterable,
                                            clusterMetadataWithDistance ->
                                                    primitives.fetchVectorReferencesIterable(readTransaction,
                                                            storageTransform,
                                                            clusterMetadataWithDistance.clusterMetadata().id()),
                                            searchConfig.searchConcurrency()),
                                    vectorReference -> {
                                        final double distance =
                                                estimator.distance(transformedQueryVector, vectorReference.vector());
                                        return new VectorReferenceAndDistance(vectorReference, distance);
                                    });

                    return collectNearestKOrderedByDistance(readTransaction, primitives,
                            vectorReferenceAndDistancesIterable, k, searchConfig, minimumRadius, minimumPrimaryKey)
                            .thenApply(nearestVectorRecords ->
                                    new SearchResult(accessInfo, storageTransform, nearestVectorRecords));
                });
    }

    /**
     * {@link #searchOrderedByDistance} followed by {@link #postProcessSearchResult}: runs the distance-ordered
     * search and converts the {@link SearchResult} into {@link ResultEntry} instances, mirroring how
     * {@link #kNearestNeighborsSearch} wraps {@link #search}. Returns an empty list when the structure has no
     * access info yet (an empty index).
     *
     * @param readTransaction the read transaction context
     * @param k the maximum number of results to return
     * @param searchConfig the performance/recall tuning knobs for this search (see {@link SearchConfig})
     * @param queryVector the query vector
     * @param minimumRadiusCluster exclusive lower bound on cluster-centroid distance (cluster-level cursor)
     * @param minimumRadius exclusive lower bound on result distance (result-level cursor)
     * @param minimumPrimaryKey tie-breaker applied at exactly {@code minimumRadius} (may be {@code null})
     * @param includeVectors whether to include reconstructed vector data in the results
     * @return a future completing with up to {@code k} results in ascending distance order
     */
    @Nonnull
    CompletableFuture<List<? extends ResultEntry>>
            searchOrderedByDistanceResults(@Nonnull final ReadTransaction readTransaction,
                                           final int k,
                                           @Nonnull final SearchConfig searchConfig,
                                           @Nonnull final RealVector queryVector,
                                           final double minimumRadiusCluster,
                                           final double minimumRadius,
                                           @Nullable final Tuple minimumPrimaryKey,
                                           final boolean includeVectors) {
        return searchOrderedByDistance(readTransaction, k, searchConfig, queryVector,
                minimumRadiusCluster, minimumRadius, minimumPrimaryKey)
                .thenApply(searchResult ->
                        searchResult == null
                        ? ImmutableList.of()
                        : postProcessSearchResult(searchResult.storageTransform(),
                                searchResult.nearestRecords(), includeVectors));
    }

    /**
     * Tail of {@link #searchOrderedByDistance}: from the per-cluster {@link VectorReferenceAndDistance} stream,
     * expands collapsed references, applies the minimum-radius / minimum-primary-key cutoff, reorders into
     * (approximate) distance order, drops references whose metadata is stale, de-duplicates by primary key, then
     * takes the nearest {@code k} and enriches each with its full metadata.
     */
    @Nonnull
    private CompletableFuture<List<VectorRecord>> collectNearestKOrderedByDistance(
            @Nonnull final ReadTransaction readTransaction,
            @Nonnull final Primitives primitives,
            @Nonnull final AsyncIterable<VectorReferenceAndDistance> vectorReferenceAndDistancesIterable,
            final int k,
            @Nonnull final SearchConfig searchConfig,
            final double minimumRadius,
            @Nullable final Tuple minimumPrimaryKey) {
        // Expand collapsed references inline: a collapsed reference becomes one entry
        // per vector ID behind the signature, all sharing the same distance.
        final AsyncIterable<VectorReferenceAndDistance> expandedVectorReferenceAndDistancesIterable =
                mapConcatIterable(getExecutor(), vectorReferenceAndDistancesIterable,
                        vectorReferenceAndDistance -> {
                            final VectorReference vectorReference = vectorReferenceAndDistance.vectorReference();
                            if (!vectorReference.isCollapsed()) {
                                return MoreAsyncUtil.mapToIterable(vectorReferenceAndDistance,
                                        item -> CompletableFuture.completedFuture(vectorReferenceAndDistance));
                            }
                            final UUID signature = StorageAdapter.signatureUuid(vectorReference.vector());
                            return AsyncUtil.mapIterable(
                                    primitives.fetchCollapsedVectorIdsIterable(readTransaction, signature),
                                    vectorId -> vectorReferenceAndDistance.withVectorReference(
                                            vectorReference.withVectorId(vectorId)));
                        }, searchConfig.searchConcurrency());

        final AsyncIterable<VectorReferenceAndDistance> filteredVectorReferenceAndDistancesIterable =
                MoreAsyncUtil.filterIterable(getExecutor(), expandedVectorReferenceAndDistancesIterable,
                        vectorReferenceAndDistance -> {
                            final int distanceComparison = Double.compare(vectorReferenceAndDistance.distance(), minimumRadius);
                            if (distanceComparison != 0) {
                                return distanceComparison > 0; // keep the ones that are bigger than minimum radius
                            }
                            if (minimumPrimaryKey == null) {
                                return true;
                            }
                            final int tupleComparison =
                                    Objects.compare(vectorReferenceAndDistance.vectorReference().id().primaryKey(),
                                            minimumPrimaryKey, Comparator.naturalOrder());
                            return tupleComparison > 0;
                        });

        final AsyncIterable<VectorReferenceAndDistance> almostSortedVectorReferencesIterable =
                almostSortedVectorReferencesIterable(filteredVectorReferenceAndDistancesIterable,
                        searchConfig.candidatePoolSize(), getExecutor());

        final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataUuidFutureMap =
                Maps.newConcurrentMap();
        final AsyncIterable<VectorReferenceAndDistance> filteredByCurrentMetadataIterable =
                MoreAsyncUtil.filterIterablePipelined(getExecutor(), almostSortedVectorReferencesIterable,
                        vectorReferenceAndDistance -> {
                            final VectorId vectorReferenceId =
                                    vectorReferenceAndDistance.vectorReference().id();
                            final CompletableFuture<VectorMetadata> vectorMetadataFuture =
                                    primaryKeyToVectorMetadataUuidFutureMap.computeIfAbsent(
                                            vectorReferenceId.primaryKey(),
                                            primaryKey ->
                                                    primitives.fetchVectorMetadata(readTransaction, primaryKey));
                            return vectorMetadataFuture.thenApply(vectorMetadata ->
                                    vectorMetadata != null
                                            && vectorMetadata.vectorId().uuid().equals(vectorReferenceId.uuid()));
                        }, searchConfig.searchConcurrency());

        final Set<Tuple> seenPrimaryKeys = Sets.newHashSet();
        final AsyncIterable<VectorReferenceAndDistance> dedupedVectorReferenceAndDistancesIterable =
                MoreAsyncUtil.filterIterable(getExecutor(), filteredByCurrentMetadataIterable,
                        vectorReferenceAndDistance ->
                                seenPrimaryKeys.add(vectorReferenceAndDistance.vectorReference()
                                        .id().primaryKey()));

        return AsyncUtil.collect(
                AsyncUtil.mapIterable(
                        MoreAsyncUtil.limitIterable(dedupedVectorReferenceAndDistancesIterable,
                                k, getExecutor()),
                        vectorReferenceAndDistance ->
                                enrichVectorReference(primaryKeyToVectorMetadataUuidFutureMap,
                                        vectorReferenceAndDistance.vectorReference(),
                                        vectorReferenceAndDistance.distance())), getExecutor());
    }

    /**
     * Converts internal search results into the public {@link ResultEntry} format, optionally
     * reconstructing the original vector by un-transforming from the stored representation.
     *
     * @param storageTransform the transform used to reconstruct vectors into their original coordinate space
     * @param nearestRecords the internal search results to convert
     * @param includeVectors whether to include the reconstructed vector data in each result entry
     * @return the search results as a list of {@link ResultEntry} instances
     */
    @Nonnull
    private ImmutableList<ResultEntry> postProcessSearchResult(@Nonnull final StorageTransform storageTransform,
                                                               @Nonnull final List<VectorRecord> nearestRecords,
                                                               final boolean includeVectors) {
        final ImmutableList.Builder<ResultEntry> resultBuilder = ImmutableList.builder();

        for (int i = 0; i < nearestRecords.size(); i++) {
            final VectorRecord vectorRecord = nearestRecords.get(i);
            @Nullable final RealVector reconstructedVector =
                    includeVectors ? storageTransform.untransform(vectorRecord.vector()) : null;

            final VectorMetadata vectorMetadata = vectorRecord.vectorMetadata();
            @Nullable final Tuple additionalValues = vectorMetadata.additionalValues();

            resultBuilder.add(
                    new ResultEntry(vectorMetadata.vectorId().primaryKey(),
                            reconstructedVector, additionalValues, vectorRecord.distance(),
                            i));
        }
        return resultBuilder.build();
    }

    /**
     * Diagnostic method that computes cluster overlap statistics for a set of query vectors. For each
     * query vector, counts how many clusters "overlap" — i.e., the query falls within the cluster's sphere
     * defined by {@code dist(query, centroid) <= maxEver}. Logs per-query overlap counts and aggregate
     * statistics (min, max, mean).
     *
     * @param readTransaction the read transaction context
     * @param queryVectors the query vectors to evaluate overlap for
     * @param centroids the list of all cluster centroids (as returned by an HNSW scan)
     * @param searchConfig the search tuning knobs (fan-out concurrency)
     * @return a future completing with a list of overlap counts (one per query vector)
     */
    @Nonnull
    CompletableFuture<List<Integer>> clusterOverlapDiagnostics(@Nonnull final ReadTransaction readTransaction,
                                                               @Nonnull final List<RealVector> queryVectors,
                                                               @Nonnull final List<ResultEntry> centroids,
                                                               @Nonnull final SearchConfig searchConfig) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(ImmutableList.of());
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final DistanceEstimator estimator = primitives.quantizer(accessInfo).estimator();

                    return MoreAsyncUtil.forEach(centroids,
                                    centroid -> {
                                        final UUID clusterId = StorageAdapter.clusterIdFromTuple(centroid.primaryKey());
                                        return primitives.fetchClusterMetadata(readTransaction, clusterId)
                                                .thenApply(clusterMetadata -> {
                                                    final Transformed<RealVector> transformedCentroid =
                                                            storageTransform.transform(
                                                                    Objects.requireNonNull(centroid.vector()));
                                                    return new ClusterMetadataWithDistance(clusterMetadata,
                                                            transformedCentroid, 0.0d);
                                                });
                                    },
                                    searchConfig.searchConcurrency(), getExecutor())
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

    /**
     * Snapshots the cluster topology for the given centroids: fetches every cluster's metadata and vector
     * references, buckets them into primaries / replicas / collapsed, and captures the distance estimator so the
     * snapshot can later rank vectors against centroids (see {@link StructureSnapshot#computeAssignmentRanking}).
     * Follows the same diagnostic shape as {@link #clusterOverlapDiagnostics}.
     *
     * @param readTransaction the read transaction context
     * @param centroids the cluster centroids (as returned by an HNSW scan)
     * @param searchConfig the search tuning knobs (fan-out concurrency)
     * @return a future completing with the topology snapshot (an empty snapshot if the structure has no access
     *         info yet)
     */
    @Nonnull
    CompletableFuture<StructureSnapshot> snapshotStructure(@Nonnull final ReadTransaction readTransaction,
                                                           @Nonnull final List<ResultEntry> centroids,
                                                           @Nonnull final SearchConfig searchConfig) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(readTransaction)
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        return CompletableFuture.completedFuture(new StructureSnapshot(Map.of(), null));
                    }
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final DistanceEstimator estimator = primitives.quantizer(accessInfo).estimator();

                    return MoreAsyncUtil.forEach(centroids,
                                    centroid -> {
                                        final UUID clusterId = StorageAdapter.clusterIdFromTuple(centroid.primaryKey());
                                        final RealVector untransformedCentroid =
                                                Objects.requireNonNull(centroid.vector(),
                                                        "centroid HNSW must yield vectors");
                                        return primitives.fetchCluster(readTransaction, storageTransform,
                                                        clusterId, untransformedCentroid)
                                                .thenApply(cluster ->
                                                        buildClusterView(clusterId, untransformedCentroid, cluster));
                                    },
                                    searchConfig.searchConcurrency(), getExecutor())
                            .thenApply(clusterViews -> {
                                final Map<UUID, ClusterView> built =
                                        Maps.newHashMapWithExpectedSize(clusterViews.size());
                                for (final ClusterView clusterView : clusterViews) {
                                    built.put(clusterView.clusterId(), clusterView);
                                }
                                return new StructureSnapshot(Map.copyOf(built), estimator);
                            });
                });
    }

    /**
     * Builds a single {@link ClusterView} from a fetched {@link Cluster}, bucketing its references into primaries,
     * replicas and collapsed (a reference is collapsed first, then primary, else a replica).
     */
    @Nonnull
    private static ClusterView buildClusterView(@Nonnull final UUID clusterId,
                                                @Nonnull final RealVector untransformedCentroid,
                                                @Nonnull final Cluster cluster) {
        final ImmutableSet.Builder<VectorId> primariesBuilder = ImmutableSet.builder();
        final ImmutableSet.Builder<VectorId> replicasBuilder = ImmutableSet.builder();
        final ImmutableSet.Builder<VectorId> collapsedBuilder = ImmutableSet.builder();
        for (final VectorReference reference : cluster.vectorReferences()) {
            if (reference.isCollapsed()) {
                collapsedBuilder.add(reference.id());
            } else if (reference.isPrimaryCopy()) {
                primariesBuilder.add(reference.id());
            } else {
                replicasBuilder.add(reference.id());
            }
        }
        return new ClusterView(clusterId, untransformedCentroid, cluster.centroid(), cluster.clusterMetadata(),
                primariesBuilder.build(), replicasBuilder.build(), collapsedBuilder.build(),
                ImmutableList.copyOf(cluster.vectorReferences()));
    }

    @Nonnull
    private static AsyncIterable<VectorReferenceAndDistance>
            almostSortedVectorReferencesIterable(@Nonnull final AsyncIterable<VectorReferenceAndDistance> iterable,
                                                 final int maxQueueSize, @Nonnull final Executor executor) {
        return MoreAsyncUtil.iterableOf(() -> new AlmostSortedAsyncIterator<>(iterable.iterator(),
                        Comparator.comparing(VectorReferenceAndDistance::distance)
                                .thenComparing(d -> d.vectorReference().id()), maxQueueSize, executor),
                executor);
    }

    /**
     * Builds the enriched {@link VectorRecord} for a candidate, pairing the reference's stored vector and the given
     * distance with the vector's full {@link VectorMetadata} (including any additional values), taken from the
     * already-resolved metadata future map.
     *
     * @param primaryKeyToVectorMetadataUuidFutureMap the map of already-fetched metadata futures, keyed by primary key
     * @param vectorReference the reference whose metadata is being resolved
     * @param distance the computed distance between this vector and the query vector
     * @return the enriched record carrying the metadata, the stored vector, and the distance
     */
    @Nonnull
    private static VectorRecord enrichVectorReference(@Nonnull final Map<Tuple, CompletableFuture<VectorMetadata>> primaryKeyToVectorMetadataUuidFutureMap,
                                                      @Nonnull final VectorReference vectorReference,
                                                      final double distance) {
        // Every metadata future was already awaited by the enclosing forEach(...).thenApply(...), so it is complete.
        final CompletableFuture<VectorMetadata> metadataFuture =
                Objects.requireNonNull(
                        primaryKeyToVectorMetadataUuidFutureMap.get(vectorReference.id().primaryKey()));
        Verify.verify(metadataFuture.isDone(), "metadata future must already be complete during enrichment");
        final VectorMetadata vectorMetadata = Objects.requireNonNull(metadataFuture.join());
        return new VectorRecord(vectorMetadata, vectorReference.vector(), distance);
    }

    /**
     * Internal result of the search pipeline, carrying the access context, the storage transform needed
     * to reconstruct vectors, and the list of nearest enriched records sorted by distance.
     *
     * @param accessInfo the access info used during this search (provides quantizer/transform context)
     * @param storageTransform the transform for reconstructing vectors into their original coordinate space
     * @param nearestRecords the enriched search results sorted by distance (best first), defensively copied
     */
    record SearchResult(@Nullable AccessInfo accessInfo,
                        @Nonnull StorageTransform storageTransform,
                        @Nonnull List<VectorRecord> nearestRecords) {
        SearchResult(@Nullable final AccessInfo accessInfo,
                     @Nonnull final StorageTransform storageTransform,
                     @Nonnull final List<VectorRecord> nearestRecords) {
            this.accessInfo = accessInfo;
            this.storageTransform = storageTransform;
            this.nearestRecords = ImmutableList.copyOf(nearestRecords);
        }
    }
}
