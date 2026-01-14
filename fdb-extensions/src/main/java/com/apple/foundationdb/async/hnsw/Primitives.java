/*
 * Primitives.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;

/**
 * An implementation of primitives for the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 */
@API(API.Status.EXPERIMENTAL)
public class Primitives {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Primitives.class);

    @Nonnull
    private final Locator locator;

    /**
     * Constructs a new HNSW primitives instance.
     * <p>
     * This constructor initializes the HNSW graph with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Primitives(@Nonnull final Locator locator) {
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
    private Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    private Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
     */
    @Nonnull
    private Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    private OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    private OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return new StorageTransform(accessInfo.getRotatorSeed(),
                getConfig().getNumDimensions(), Objects.requireNonNull(accessInfo.getNegatedCentroid()));
    }

    @Nonnull
    Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(getConfig().getMetric());
        }

        final Config config = getConfig();
        return config.isUseRaBitQ()
               ? new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits())
               : Quantizer.noOpQuantizer(config.getMetric());
    }

    /**
     * Gets a node from the cache or throws an exception.
     *
     * @param <N> the type of the node reference, which must extend {@link NodeReference}
     * @param primaryKey the {@link Tuple} representing the primary key of the node
     * @param nodeCache the cache to check for the node
     *
     * @return a {@link CompletableFuture} that will be completed with the cached {@link AbstractNode}
     * @throws IllegalArgumentException if the node is not already present in the cache
     */
    @Nonnull
    <N extends NodeReference> AbstractNode<N> nodeFromCache(@Nonnull final Tuple primaryKey,
                                                            @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final AbstractNode<N> nodeFromCache = nodeCache.get(primaryKey);
        if (nodeFromCache == null) {
            throw new IllegalStateException("node should already have been fetched: " + primaryKey);
        }
        return nodeFromCache;
    }

    /**
     * Asynchronously fetches a node if it is not already present in the cache.
     * <p>
     * This method first attempts to retrieve the node from the provided {@code nodeCache} using the
     * primary key of the {@code nodeReference}. If the node is not found in the cache, it is
     * fetched from the underlying storage using the {@code storageAdapter}. Once fetched, the node
     * is added to the {@code nodeCache} before the future is completed.
     * <p>
     * This is a convenience method that delegates to {@link #fetchNodeIfNecessaryAndApply}.
     *
     * @param <N> the type of the node reference, which must extend {@link NodeReference}
     * @param storageAdapter the storage adapter used to fetch the node from persistent storage
     * @param readTransaction the transaction to use for reading from storage
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer the layer index where the node is located
     * @param nodeReference the reference to the node to fetch
     * @param nodeCache the cache to check for the node and to which the node will be added if fetched
     *
     * @return a {@link CompletableFuture} that will be completed with the fetched or cached {@link AbstractNode}
     */
    @Nonnull
    <N extends NodeReference> CompletableFuture<AbstractNode<N>>
            fetchNodeIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                 @Nonnull final ReadTransaction readTransaction,
                                 @Nonnull final StorageTransform storageTransform,
                                 final int layer,
                                 @Nonnull final NodeReference nodeReference,
                                 @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        return fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction, storageTransform, layer, nodeReference,
                nR -> nodeCache.get(nR.getPrimaryKey()),
                (nR, node) -> {
                    // TODO maybe use a placeholder instance for null so we won't try multiple times
                    if (node != null) {
                        nodeCache.put(nR.getPrimaryKey(), node);
                    }
                    return node;
                });
    }

    /**
     * Conditionally fetches a node from storage and applies a function to it.
     * <p>
     * This method first attempts to generate a result by applying the {@code fetchBypassFunction}.
     * If this function returns a non-null value, that value is returned immediately in a
     * completed {@link CompletableFuture}, and no storage access occurs. This provides an
     * optimization path, for example, if the required data is already available in a cache.
     * <p>
     * If the bypass function returns {@code null}, the method proceeds to asynchronously fetch the
     * node from the given {@code StorageAdapter}. Once the node is retrieved, the
     * {@code biMapFunction} is applied to the original {@code nodeReference} and the fetched
     * {@code Node} to produce the final result.
     *
     * @param <R> The type of the input node reference.
     * @param <N> The type of the node reference used by the storage adapter.
     * @param <U> The type of the result.
     * @param storageAdapter The storage adapter used to fetch the node if necessary.
     * @param readTransaction The read transaction context for the storage operation.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer index from which to fetch the node.
     * @param nodeReference The reference to the node that may need to be fetched.
     * @param fetchBypassFunction A function that provides a potential shortcut. If it returns a
     * non-null value, the node fetch is bypassed.
     * @param biMapFunction A function to be applied after a successful node fetch, combining the
     * original reference and the fetched node to produce the final result.
     *
     * @return A {@link CompletableFuture} that will complete with the result from either the
     * {@code fetchBypassFunction} or the {@code biMapFunction}.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<U>
            fetchNodeIfNecessaryAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
                                         @Nonnull final ReadTransaction readTransaction,
                                         @Nonnull final StorageTransform storageTransform,
                                         final int layer,
                                         @Nonnull final R nodeReference,
                                         @Nonnull final Function<R, U> fetchBypassFunction,
                                         @Nonnull final BiFunction<R, AbstractNode<N>, U> biMapFunction) {
        final U bypass = fetchBypassFunction.apply(nodeReference);
        if (bypass != null) {
            return CompletableFuture.completedFuture(bypass);
        }

        return getOnReadListener().onAsyncRead(
                        storageAdapter.fetchNode(readTransaction, storageTransform, layer,
                                nodeReference.getPrimaryKey()))
                .thenApply(node -> biMapFunction.apply(nodeReference, node));
    }

    /**
     * Asynchronously fetches neighborhood nodes and returns them as {@link NodeReferenceWithVector} instances,
     * which include the node's vector.
     * <p>
     * This method efficiently retrieves node data by first checking an in-memory {@code nodeCache}. If a node is not
     * in the cache, it is fetched from the {@link StorageAdapter}. Fetched nodes are then added to the cache to
     * optimize subsequent lookups. It also handles cases where the input {@code neighborReferences} may already
     * contain {@link NodeReferenceWithVector} instances, avoiding redundant work.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes from if they are not in the cache
     * @param readTransaction the transaction context for database read operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer the graph layer from which to fetch the nodes
     * @param neighborReferences an iterable of references to the neighbor nodes to be fetched
     * @param nodeCache a map serving as an in-memory cache for nodes. This map will be populated with any
     * nodes fetched from storage.
     *
     * @return a {@link CompletableFuture} that, upon completion, will contain a list of
     * {@link NodeReferenceWithVector} objects for the specified neighbors
     */
    @Nonnull
    <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>>
            fetchNeighborhoodReferences(@Nonnull final StorageAdapter<N> storageAdapter,
                                        @Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final StorageTransform storageTransform,
                                        final int layer,
                                        @Nonnull final Iterable<? extends NodeReference> neighborReferences,
                                        @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, storageTransform, layer, neighborReferences,
                neighborReference -> {
                    if (neighborReference.isNodeReferenceWithVector()) {
                        return neighborReference.asNodeReferenceWithVector();
                    }
                    final AbstractNode<N> neighborNode = nodeCache.get(neighborReference.getPrimaryKey());
                    if (neighborNode == null) {
                        return null;
                    }
                    return new NodeReferenceWithVector(neighborReference.getPrimaryKey(),
                            neighborNode.asCompactNode().getVector());
                },
                (neighborReference, neighborNode) -> {
                    if (neighborNode != null) {
                        //
                        // At this point we know that the node needed to be fetched, which means this branch cannot be
                        // reached for INLINING nodes as they never have to be fetched. Therefore, we can safely treat
                        // the nodes as compact nodes.
                        //
                        nodeCache.put(neighborReference.getPrimaryKey(), neighborNode);
                        return new NodeReferenceWithVector(neighborReference.getPrimaryKey(),
                                neighborNode.asCompactNode().getVector());
                    }
                    return null;
                });
    }

    /**
     * Fetches a collection of nodes, attempting to retrieve them from a cache first before
     * accessing the underlying storage.
     * <p>
     * This method iterates through the provided {@code nodeReferences}. For each reference, it
     * first checks the {@code nodeCache}. If the corresponding {@link AbstractNode} is found, it is
     * used directly. If not, the node is fetched from the {@link StorageAdapter}. Any nodes
     * fetched from storage are then added to the {@code nodeCache} to optimize subsequent lookups.
     * The entire operation is performed asynchronously.
     *
     * @param <N> The type of the node reference, which must extend {@link NodeReference}.
     * @param storageAdapter The storage adapter used to fetch nodes from storage if they are not in the cache.
     * @param readTransaction The transaction context for the read operation.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer from which to fetch the nodes.
     * @param nodeReferences An {@link Iterable} of {@link NodeReferenceWithDistance} objects identifying the nodes to
     * be fetched.
     * @param nodeCache A map used as a cache. It is checked for existing nodes and updated with any newly fetched
     * nodes.
     *
     * @return A {@link CompletableFuture} which will complete with a {@link List} of {@link NodeReferenceAndNode}
     *         objects, pairing each requested reference with its corresponding node.
     */
    @Nonnull
    <T extends NodeReferenceWithVector, N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<T, N>>>
            fetchSomeNodesIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                      @Nonnull final ReadTransaction readTransaction,
                                      @Nonnull final StorageTransform storageTransform,
                                      final int layer,
                                      @Nonnull final Iterable<T> nodeReferences,
                                      @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, storageTransform, layer, nodeReferences,
                nodeReference -> {
                    final AbstractNode<N> node = nodeCache.get(nodeReference.getPrimaryKey());
                    if (node == null) {
                        return null;
                    }
                    return new NodeReferenceAndNode<>(nodeReference, node);
                },
                (nodeReference, node) -> {
                    if (node != null) {
                        nodeCache.put(nodeReference.getPrimaryKey(), node);
                        return new NodeReferenceAndNode<>(nodeReference, node);
                    }
                    return null;
                });
    }

    /**
     * Asynchronously fetches a collection of nodes from storage and applies a function to each.
     * <p>
     * For each {@link NodeReference} in the provided iterable, this method concurrently fetches the corresponding
     * {@code Node} using the given {@link StorageAdapter}. The logic delegates to
     * {@code fetchNodeIfNecessaryAndApply}, which determines whether a full node fetch is required.
     * If a node is fetched from storage, the {@code biMapFunction} is applied. If the fetch is bypassed
     * (e.g., because the reference itself contains sufficient information), the {@code fetchBypassFunction} is used
     * instead.
     *
     * @param <R> The type of the node references to be processed, extending {@link NodeReference}.
     * @param <N> The type of the key references within the nodes, extending {@link NodeReference}.
     * @param <U> The type of the result after applying one of the mapping functions.
     * @param storageAdapter The {@link StorageAdapter} used to fetch nodes from the underlying storage.
     * @param readTransaction The {@link ReadTransaction} context for the read operations.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param layer The layer index from which the nodes are being fetched.
     * @param nodeReferences An {@link Iterable} of {@link NodeReference}s for the nodes to be fetched and processed.
     * @param fetchBypassFunction The function to apply to a node reference when the actual node fetch is bypassed,
     * mapping the reference directly to a result of type {@code U}.
     * @param biMapFunction The function to apply when a node is successfully fetched, mapping the original
     * reference and the fetched {@link AbstractNode} to a result of type {@code U}.
     *
     * @return A {@link CompletableFuture} that, upon completion, will hold a {@link List} of non-null results
     * of type {@code U}
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<List<U>>
            fetchSomeNodesAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
                                   @Nonnull final ReadTransaction readTransaction,
                                   @Nonnull final StorageTransform storageTransform,
                                   final int layer,
                                   @Nonnull final Iterable<R> nodeReferences,
                                   @Nonnull final Function<R, U> fetchBypassFunction,
                                   @Nonnull final BiFunction<R, AbstractNode<N>, U> biMapFunction) {
        return forEach(nodeReferences,
                currentNeighborReference -> fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction,
                        storageTransform, layer, currentNeighborReference, fetchBypassFunction, biMapFunction),
                getConfig().getMaxNumConcurrentNodeFetches(),
                getExecutor())
                .thenApply(results -> {
                    final ImmutableList.Builder<U> filteredListBuilder = ImmutableList.builder();
                    for (final U result : results) {
                        if (result != null) {
                            filteredListBuilder.add(result);
                        }
                    }
                    return filteredListBuilder.build();
                });
    }

    @Nonnull
    <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
            filterExisting(@Nonnull final StorageAdapter<N> storageAdapter,
                           @Nonnull final ReadTransaction readTransaction,
                           @Nonnull final StorageTransform storageTransform,
                           @Nonnull final Iterable<NodeReferenceAndNode<NodeReferenceWithVector, N>> nodeReferenceAndNodes) {
        if (!storageAdapter.isInliningStorageAdapter()) {
            return CompletableFuture.completedFuture(ImmutableList.copyOf(nodeReferenceAndNodes));
        }

        return forEach(nodeReferenceAndNodes,
                nodeReferenceAndNode -> {
                    final AbstractNode<N> node = nodeReferenceAndNode.getNode();
                    final NodeReferenceWithVector nodeReference = nodeReferenceAndNode.getNodeReference();
                    return fetchBaseNode(readTransaction, storageTransform, nodeReference.getPrimaryKey())
                            .thenApply(baseCompactNode -> {
                                if (baseCompactNode == null) {
                                    return null;
                                }

                                //
                                // The node does exist on layer 0 meaning the base node is a compact node, and we
                                // can use its vector going forward. This may be necessary if this is a dangling
                                // reference and the record has been reinserted after deletion.
                                //
                                final NodeReferenceWithVector updatedNodeReference =
                                        new NodeReferenceWithVector(baseCompactNode.getPrimaryKey(),
                                                baseCompactNode.getVector());
                                return new NodeReferenceAndNode<>(updatedNodeReference, node);
                            });
                },
                getConfig().getMaxNumConcurrentNodeFetches(),
                getExecutor())
                .thenApply(results -> {
                    final ImmutableList.Builder<NodeReferenceAndNode<NodeReferenceWithVector, N>> filteredListBuilder =
                            ImmutableList.builder();
                    for (final NodeReferenceAndNode<NodeReferenceWithVector, N> result : results) {
                        if (result != null) {
                            filteredListBuilder.add(result);
                        }
                    }
                    return filteredListBuilder.build();
                });
    }

    @Nonnull
    CompletableFuture<Boolean> exists(@Nonnull final ReadTransaction readTransaction,
                                      @Nonnull final Tuple primaryKey) {
        //
        // Call fetchBaseNode() to check for the node's existence; we are handing in the identity operator,
        // since we do not care about the vector itself at all.
        //
        return fetchBaseNode(readTransaction, StorageTransform.identity(), primaryKey)
                .thenApply(Objects::nonNull);
    }

    @Nonnull
    CompletableFuture<CompactNode> fetchBaseNode(@Nonnull final ReadTransaction readTransaction,
                                                 @Nonnull final StorageTransform storageTransform,
                                                 @Nonnull final Tuple primaryKey) {
        final StorageAdapter<? extends NodeReference> storageAdapter = storageAdapterForLayer(0);

        return storageAdapter.fetchNode(readTransaction, storageTransform, 0, primaryKey)
                .thenApply(node -> {
                    if (node == null) {
                        return null;
                    }
                    return node.asCompactNode();
                });
    }

    /**
     * Prunes the neighborhood of a given node if its number of connections exceeds the maximum allowed ({@code mMax}).
     * <p>
     * This is a maintenance operation for the HNSW graph. When new nodes are added, an existing node's neighborhood
     * might temporarily grow beyond its limit. This method identifies such cases and trims the neighborhood back down
     * to the {@code mMax} best connections, based on the configured distance metric. If the neighborhood size is
     * already within the limit, this method does nothing.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes from the database
     * @param transaction the transaction context for database operations
     * @param estimator an estimator to estimate distances
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param nodeReferenceWithVector the node reference of the node whose neighborhood is being considered for pruning
     * @param layer the graph layer on which the operation is performed
     * @param mMax the maximum number of neighbors a node is allowed to have on this layer
     * @param neighborChangeSet a set of pending changes to the neighborhood that must be included in the pruning
     *        calculation
     * @param nodeCache a cache of nodes to avoid redundant database fetches
     *
     * @return a {@link CompletableFuture} which completes with a list of the newly selected neighbors for the pruned node.
     * If no pruning was necessary, it completes with {@code null}.
     */
    @Nonnull
    <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            pruneNeighborsIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                      @Nonnull final Transaction transaction,
                                      @Nonnull final StorageTransform storageTransform,
                                      @Nonnull final Estimator estimator,
                                      final int layer,
                                      @Nonnull final NodeReferenceWithVector nodeReferenceWithVector,
                                      final int mMax,
                                      @Nonnull final NeighborsChangeSet<N> neighborChangeSet,
                                      @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final int numNeighbors =
                Iterables.size(neighborChangeSet.merge()); // this is a view over the iterable neighbors in the set
        if (numNeighbors < mMax) {
            return CompletableFuture.completedFuture(null);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("pruning neighborhood of key={} which has numNeighbors={} out of mMax={}",
                        nodeReferenceWithVector.getPrimaryKey(), numNeighbors, mMax);
            }
            return fetchNeighborhoodReferences(storageAdapter, transaction, storageTransform, layer, neighborChangeSet.merge(), nodeCache)
                    .thenApply(neighborReferenceWithVectors -> {
                        final ImmutableList.Builder<NodeReferenceWithDistance> nodeReferencesWithDistancesBuilder =
                                ImmutableList.builder();
                        for (final NodeReferenceWithVector neighborReferenceWithVector : neighborReferenceWithVectors) {
                            final var neighborVector = neighborReferenceWithVector.getVector();
                            final double distance = estimator.distance(neighborVector, nodeReferenceWithVector.getVector());
                            nodeReferencesWithDistancesBuilder.add(
                                    new NodeReferenceWithDistance(neighborReferenceWithVector.getPrimaryKey(),
                                            neighborVector, distance));
                        }
                        return nodeReferencesWithDistancesBuilder.build();
                    })
                    .thenCompose(nodeReferencesAndNodes ->
                            selectCandidates(storageAdapter, transaction, storageTransform, estimator,
                                    nodeReferencesAndNodes, layer,
                                    mMax, nodeCache));
        }
    }

    /**
     * Selects the {@code m} best neighbors for a new node from a set of candidates using the HNSW selection heuristic.
     * <p>
     * This method implements the core logic for neighbor selection within a layer of the HNSW graph. It starts with an
     * initial set of candidates ({@code nearestNeighbors}), which can be optionally extended by fetching their own
     * neighbors.
     * It then iteratively refines this set using a greedy best-first search.
     * <p>
     * The selection heuristic ensures diversity among neighbors. A candidate is added to the result set only if it is
     * closer to the query {@code vector} than to any node already in the result set. This prevents selecting neighbors
     * that are clustered together. If the {@code keepPrunedConnections} configuration is enabled, candidates that are
     * pruned by this heuristic are kept and may be added at the end if the result set is not yet full.
     * <p>
     * The process is asynchronous and returns a {@link CompletableFuture} that will eventually contain the list of
     * selected neighbors with their full node data.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter to fetch nodes and their neighbors
     * @param readTransaction the transaction for performing database reads
     * @param estimator the estimator in use
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param initialCandidates the initial pool of candidate neighbors, typically from a search in a higher layer
     * @param layer the layer in the HNSW graph where the selection is being performed
     * @param m the maximum number of neighbors to select
     * neighbors of the {@code nearestNeighbors}
     * @param nodeCache a cache of nodes to avoid redundant storage lookups
     *
     * @return a {@link CompletableFuture} which will complete with a list of the selected neighbors,
     * each represented as a {@link NodeReferenceAndNode}
     */
    <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            selectCandidates(@Nonnull final StorageAdapter<N> storageAdapter,
                             @Nonnull final ReadTransaction readTransaction,
                             @Nonnull final StorageTransform storageTransform,
                             @Nonnull final Estimator estimator,
                             @Nonnull final Iterable<NodeReferenceWithDistance> initialCandidates,
                             final int layer,
                             final int m,
                             @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final Metric metric = getConfig().getMetric();

        final List<NodeReferenceWithDistance> selected = Lists.newArrayListWithExpectedSize(m);
        final Queue<NodeReferenceWithDistance> candidates =
                new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator());
        initialCandidates.forEach(candidates::add);
        final Queue<NodeReferenceWithDistance> discardedCandidates =
                getConfig().isKeepPrunedConnections()
                ? new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator()) : null;

        while (!candidates.isEmpty() && selected.size() < m) {
            final NodeReferenceWithDistance nearestCandidate = candidates.poll();
            boolean shouldSelect = true;
            // if the metric does not support triangle inequality, we shold not use the heuristic
            if (metric.satisfiesTriangleInequality()) {
                for (final NodeReferenceWithDistance alreadySelected : selected) {
                    if (estimator.distance(nearestCandidate.getVector(),
                            alreadySelected.getVector()) < nearestCandidate.getDistance()) {
                        shouldSelect = false;
                        break;
                    }
                }
            }
            if (shouldSelect) {
                selected.add(nearestCandidate);
            } else if (discardedCandidates != null) {
                discardedCandidates.add(nearestCandidate);
            }
        }

        if (discardedCandidates != null) { // isKeepPrunedConnections is set to true
            while (!discardedCandidates.isEmpty() && selected.size() < m) {
                selected.add(discardedCandidates.poll());
            }
        }

        return fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                selected, nodeCache)
                .thenApply(selectedNeighbors -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("selected neighbors={}",
                                selectedNeighbors.stream()
                                        .map(selectedNeighbor ->
                                                "(primaryKey=" + selectedNeighbor.getNodeReference().getPrimaryKey() +
                                                        ",distance=" + selectedNeighbor.getNodeReference().getDistance() + ")")
                                        .collect(Collectors.joining(",")));
                    }
                    return selectedNeighbors;
                });
    }

    /**
     * Conditionally extends a set of candidate nodes by fetching and evaluating their neighbors.
     * <p>
     * If {@code isExtendCandidates} is {@code true}, this method gathers the neighbors of the provided
     * {@code candidates}, fetches their full node data, and calculates their distance to the given
     * {@code vector}. The resulting list will contain both the original candidates and their newly
     * evaluated neighbors.
     * <p>
     * If {@code isExtendCandidates} is {@code false}, the method simply returns a list containing
     * only the original candidates. This operation is asynchronous and returns a {@link CompletableFuture}.
     *
     * @param <N> the type of the {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} used to access node data from storage
     * @param readTransaction the active {@link ReadTransaction} for database access
     * @param estimator the estimator
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param candidates an {@link Collection} of initial candidate nodes, which have already been evaluated
     * @param layer the graph layer from which to fetch nodes
     * @param isExtendCandidates a boolean flag; if {@code true}, the candidate set is extended with neighbors
     * @param nodeCache a cache mapping primary keys to {@link AbstractNode} objects to avoid redundant fetches
     * @param vector the query vector used to calculate distances for any new neighbor nodes
     *
     * @return a {@link CompletableFuture} which will complete with a list of {@link NodeReferenceWithDistance},
     * containing the original candidates and potentially their neighbors
     */
    <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>>
            extendCandidatesIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                        @Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final StorageTransform storageTransform,
                                        @Nonnull final Estimator estimator,
                                        @Nonnull final Collection<NodeReferenceAndNode<NodeReferenceWithDistance, N>> candidates,
                                        final int layer,
                                        final boolean isExtendCandidates,
                                        @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache,
                                        @Nonnull final Transformed<RealVector> vector) {
        final ImmutableList.Builder<NodeReferenceWithDistance> resultBuilder = ImmutableList.builder();

        if (isExtendCandidates) {
            return neighborReferences(storageAdapter, readTransaction, storageTransform, null, candidates,
                    CandidatePredicate.tautology(), layer, nodeCache)
                    .thenApply(neighborsOfCandidates -> {
                        for (final NodeReferenceWithVector nodeReferenceWithVector : neighborsOfCandidates) {
                            final double distance = estimator.distance(nodeReferenceWithVector.getVector(), vector);
                            resultBuilder.add(new NodeReferenceWithDistance(nodeReferenceWithVector.getPrimaryKey(),
                                    nodeReferenceWithVector.getVector(), distance));
                        }
                        return resultBuilder.build();
                    });
        } else {
            //
            // Add all given candidates to the result.
            //
            for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> candidate : candidates) {
                resultBuilder.add(candidate.getNodeReference());
            }

            return CompletableFuture.completedFuture(resultBuilder.build());
        }
    }

    /**
     * Compute and if necessary fetch the neighbor references (with vectors) and the neighboring nodes of an iterable
     * of initial nodes that is passed in. Note that the neighbor of an initial node might be another initial node.
     * If that is the case the node is returned.
     *
     * @param <N> the type of the {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} used to access node data from storage
     * @param readTransaction the active {@link ReadTransaction} for database access
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param random the random to be used for sampling
     * @param initialNodeReferenceAndNodes an {@link Iterable} of initial candidate nodes, which have already been evaluated
     * @param layer the graph layer from which to fetch nodes
     * @param nodeCache a cache mapping primary keys to {@link AbstractNode} objects to avoid redundant fetches
     *
     * @return a {@link CompletableFuture} which will complete with a list of fetched nodes
     */
    <T extends NodeReference, N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
            neighbors(@Nonnull final StorageAdapter<N> storageAdapter,
                      @Nonnull final ReadTransaction readTransaction,
                      @Nonnull final StorageTransform storageTransform,
                      @Nonnull final SplittableRandom random,
                      @Nonnull final Collection<NodeReferenceAndNode<T, N>> initialNodeReferenceAndNodes,
                      @Nonnull final CandidatePredicate samplingPredicate,
                      final int layer,
                      @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        return neighborReferences(storageAdapter, readTransaction, storageTransform, random,
                initialNodeReferenceAndNodes, samplingPredicate, layer, nodeCache)
                .thenCompose(neighbors ->
                        fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                                neighbors, nodeCache))
                .thenCompose(neighbors ->
                        filterExisting(storageAdapter, readTransaction, storageTransform, neighbors));
    }

    /**
     * Compute and if necessary fetch the neighbor references (with vectors) of an iterable of initial nodes that is
     * passed in. Note that the neighbor of an initial node might be another initial node. If that is the case the node
     * is returned.
     *
     * @param <N> the type of the {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} used to access node data from storage
     * @param readTransaction the active {@link ReadTransaction} for database access
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param random a {@link SplittableRandom} to be used for sampling
     * @param initialNodeReferenceAndNodes an {@link Iterable} of initial candidate nodes, which have already been
     *        evaluated
     * @param samplingPredicate a predicate that restricts the number of neighbors to be fetched
     * @param layer the graph layer from which to fetch nodes
     * @param nodeCache a cache mapping primary keys to {@link AbstractNode} objects to avoid redundant fetches
     *
     * @return a {@link CompletableFuture} which will complete with a list of {@link NodeReferenceWithVector}
     */
    private <T extends NodeReference, N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>>
            neighborReferences(@Nonnull final StorageAdapter<N> storageAdapter,
                               @Nonnull final ReadTransaction readTransaction,
                               @Nonnull final StorageTransform storageTransform,
                               @Nullable final SplittableRandom random,
                               @Nonnull final Collection<NodeReferenceAndNode<T, N>> initialNodeReferenceAndNodes,
                               @Nonnull final CandidatePredicate samplingPredicate,
                               final int layer,
                               @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final Iterable<NodeReference> toBeFetched =
                findNeighborReferences(initialNodeReferenceAndNodes, random, samplingPredicate);
        return fetchNeighborhoodReferences(storageAdapter, readTransaction, storageTransform, layer, toBeFetched,
                nodeCache);
    }

    /**
     * Return the union of the nodes passed in and their neighbors.
     *
     * @param <N> the type of the {@link NodeReference} storage space that is currently being used
     * @param initialNodeReferenceAndNodes an {@link Iterable} of initial candidate nodes
     *
     * @return a {@link CompletableFuture} which will complete with a set of {@link NodeReference}s
     */
    private <T extends NodeReference, N extends NodeReference> Set<NodeReference>
            findNeighborReferences(@Nonnull final Collection<NodeReferenceAndNode<T, N>> initialNodeReferenceAndNodes,
                                   @Nullable final SplittableRandom random,
                                   @Nonnull final CandidatePredicate candidatePredicate) {
        final Set<NodeReference> neighborReferences = Sets.newLinkedHashSet();
        final ImmutableMap.Builder<Tuple, NodeReferenceAndNode<T, N>> initialNodesMapBuilder = ImmutableMap.builder();
        for (final NodeReferenceAndNode<T, N> nodeReferenceAndNode : initialNodeReferenceAndNodes) {
            initialNodesMapBuilder.put(nodeReferenceAndNode.getNode().getPrimaryKey(), nodeReferenceAndNode);
            neighborReferences.add(nodeReferenceAndNode.getNodeReference());
        }

        final ImmutableMap<Tuple, NodeReferenceAndNode<T, N>> initialNodesMap = initialNodesMapBuilder.build();
        final Set<Tuple> nodeReferencesSeen = Sets.newHashSet();

        for (final NodeReferenceAndNode<T, N> nodeReferenceAndNode : initialNodeReferenceAndNodes) {
            for (final N neighbor : nodeReferenceAndNode.getNode().getNeighbors()) {
                final Tuple neighborPrimaryKey = neighbor.getPrimaryKey();

                //
                // We need to distinguish between initial node references and non-initial node references:
                // Initial nodes references are of type T (and sometimes already contain a vector in which case
                // we do not want to refetch the node later if we don't have to). The initial nodes already have been
                // added earlier in this method (with or without a vector). The neighbors that are not initial most
                // likely do not contain a vector which is fine but if T != N, we need to be careful in order to not
                // create duplicates in this set.
                //
                @Nullable final NodeReferenceAndNode<T, N> initialNode = initialNodesMap.get(neighborPrimaryKey);
                if (initialNode == null && !nodeReferencesSeen.contains(neighborPrimaryKey)) {
                    //
                    // This is a node that is currently not known to us. It is not an initial node. We need to fetch it,
                    // and we need to mark it as seen so we won't consider it more than once.
                    //
                    neighborReferences.add(neighbor);
                    nodeReferencesSeen.add(neighborPrimaryKey);
                }
            }
        }

        // sample down the set of neighbors by testing the candidate predicate
        final ImmutableSet.Builder<NodeReference> resultBuilder = ImmutableSet.builder();
        for (final NodeReference neighborReference : neighborReferences) {
            if (candidatePredicate.test(random, initialNodesMap.keySet(),
                    neighborReferences.size(), neighborReference)) {
                resultBuilder.add(neighborReference);
            }
        }

        return resultBuilder.build();
    }

    /**
     * Writes lonely nodes for a given key across a specified range of layers.
     * <p>
     * A "lonely node" is a node in the layered structure that does not have a
     * sibling. This method iterates downwards from the {@code highestLayerInclusive}
     * to the {@code lowestLayerExclusive}. For each layer in this range, it
     * retrieves the appropriate {@link StorageAdapter} and calls
     * {@link #writeLonelyNodeOnLayer} to persist the node's information.
     *
     * @param quantizer the quantizer
     * @param transaction the transaction to use for writing to the database
     * @param primaryKey the primary key of the record for which lonely nodes are being written
     * @param vector the search path vector that was followed to find this key
     * @param highestLayerInclusive the highest layer (inclusive) to begin writing lonely nodes on
     * @param lowestLayerExclusive the lowest layer (exclusive) at which to stop writing lonely nodes
     */
    void writeLonelyNodes(@Nonnull final Quantizer quantizer,
                          @Nonnull final Transaction transaction,
                          @Nonnull final Tuple primaryKey,
                          @Nonnull final Transformed<RealVector> vector,
                          final int highestLayerInclusive,
                          final int lowestLayerExclusive) {
        for (int layer = highestLayerInclusive; layer > lowestLayerExclusive; layer --) {
            final StorageAdapter<?> storageAdapter = storageAdapterForLayer(layer);
            writeLonelyNodeOnLayer(quantizer, storageAdapter, transaction, layer, primaryKey, vector);
        }
    }

    /**
     * Writes a new, isolated ('lonely') node to a specified layer within the graph.
     * <p>
     * This method uses the provided {@link StorageAdapter} to create a new node with the
     * given primary key and vector but with an empty set of neighbors. The write
     * operation is performed as part of the given {@link Transaction}. This is typically
     * used to insert the very first node into an empty graph layer.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param quantizer the quantizer
     * @param storageAdapter the {@link StorageAdapter} used to access the data store and create nodes; must not be null
     * @param transaction the {@link Transaction} context for the write operation; must not be null
     * @param layer the layer index where the new node will be written
     * @param primaryKey the primary key for the new node; must not be null
     * @param vector the vector data for the new node; must not be null
     */
    <N extends NodeReference> void writeLonelyNodeOnLayer(@Nonnull final Quantizer quantizer,
                                                          @Nonnull final StorageAdapter<N> storageAdapter,
                                                          @Nonnull final Transaction transaction,
                                                          final int layer,
                                                          @Nonnull final Tuple primaryKey,
                                                          @Nonnull final Transformed<RealVector> vector) {
        storageAdapter.writeNode(transaction, quantizer,
                layer, storageAdapter.getNodeFactory()
                        .create(primaryKey, vector, ImmutableList.of()),
                new BaseNeighborsChangeSet<>(ImmutableList.of()));
        if (logger.isTraceEnabled()) {
            logger.trace("written lonely node at key={} on layer={}", primaryKey, layer);
        }
    }

    /**
     * Compile a list of node references that definitely exist. The neighbor list of a node may contain node
     * references to neighbors that don't exist anymore (stale reference). The (non-existing) nodes that these node
     * references might refer to must not be repaired as that may resurrect a node.
     * <p>
     * We know that the candidate change set map only contains keys for nodes that exist AND that the candidate change
     * set map contains all primary neighbors (if they exist). Therefore, we filter the neighbors list from the node by
     * cross-referencing the change set map.
     * @param <N> type parameter extending {@link NodeReference}
     * @param toBeDeletedNode the node that is being deleted.
     * @param candidateChangeSetMap the initialized candidate change set map.
     * @return a list of existing primary neighbors
     */
    @Nonnull
    <N extends NodeReference> ImmutableList<N>
            primaryNeighbors(@Nonnull final AbstractNode<N> toBeDeletedNode,
                             @Nonnull final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap) {
        //
        // All entries in the change set map definitely exist and the candidate change set map hold all keys for all
        // existing primary candidates.
        //
        final ImmutableList.Builder<N> primaryNeighborsBuilder = ImmutableList.builder();
        for (final N potentialPrimaryNeighbor : toBeDeletedNode.getNeighbors()) {
            if (candidateChangeSetMap.containsKey(potentialPrimaryNeighbor.getPrimaryKey())) {
                primaryNeighborsBuilder.add(potentialPrimaryNeighbor);
            }
        }
        return primaryNeighborsBuilder.build();
    }

    /**
     * Calculates the delta between a current set of neighbors and a new set, producing a
     * {@link NeighborsChangeSet} that represents the required insertions and deletions.
     * <p>
     * This method compares the neighbors present in the initial {@code beforeChangeSet} with
     * the provided {@code afterNeighbors}. It identifies which neighbors from the "before" state
     * are missing in the "after" state (to be deleted) and which new neighbors are present in the
     * "after" state but not in the "before" state (to be inserted). It then constructs a new
     * {@code NeighborsChangeSet} by wrapping the original one with {@link DeleteNeighborsChangeSet}
     * and {@link InsertNeighborsChangeSet} as needed.
     *
     * @param <N> the type of the node reference, which must extend {@link NodeReference}
     * @param beforeChangeSet the change set representing the state of neighbors before the update.
     * This is used as the base for calculating changes. Must not be null.
     * @param afterNeighbors an iterable collection of the desired neighbors after the update.
     * Must not be null.
     *
     * @return a new {@code NeighborsChangeSet} that includes the necessary deletion and insertion
     * operations to transform the neighbors from the "before" state to the "after" state.
     */
    <N extends NodeReference> NeighborsChangeSet<N>
            resolveChangeSetFromNewNeighbors(@Nonnull final NeighborsChangeSet<N> beforeChangeSet,
                                             @Nonnull final Iterable<NodeReferenceAndNode<NodeReferenceWithDistance, N>> afterNeighbors) {
        final Map<Tuple, N> beforeNeighborsMap = Maps.newLinkedHashMap();
        for (final N n : beforeChangeSet.merge()) {
            beforeNeighborsMap.put(n.getPrimaryKey(), n);
        }

        final Map<Tuple, N> afterNeighborsMap = Maps.newLinkedHashMap();
        for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> nodeReferenceAndNode : afterNeighbors) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReference();

            afterNeighborsMap.put(nodeReferenceWithDistance.getPrimaryKey(),
                    nodeReferenceAndNode.getNode().getSelfReference(nodeReferenceWithDistance.getVector()));
        }

        final ImmutableList.Builder<Tuple> toBeDeletedBuilder = ImmutableList.builder();
        for (final Map.Entry<Tuple, N> beforeNeighborEntry : beforeNeighborsMap.entrySet()) {
            if (!afterNeighborsMap.containsKey(beforeNeighborEntry.getKey())) {
                toBeDeletedBuilder.add(beforeNeighborEntry.getValue().getPrimaryKey());
            }
        }
        final List<Tuple> toBeDeleted = toBeDeletedBuilder.build();

        final ImmutableList.Builder<N> toBeInsertedBuilder = ImmutableList.builder();
        for (final Map.Entry<Tuple, N> afterNeighborEntry : afterNeighborsMap.entrySet()) {
            if (!beforeNeighborsMap.containsKey(afterNeighborEntry.getKey())) {
                toBeInsertedBuilder.add(afterNeighborEntry.getValue());
            }
        }
        final List<N> toBeInserted = toBeInsertedBuilder.build();

        NeighborsChangeSet<N> changeSet = beforeChangeSet;

        if (!toBeDeleted.isEmpty()) {
            changeSet = new DeleteNeighborsChangeSet<>(changeSet, toBeDeleted);
        }
        if (!toBeInserted.isEmpty()) {
            changeSet = new InsertNeighborsChangeSet<>(changeSet, toBeInserted);
        }
        return changeSet;
    }

    /**
     * Gets the appropriate storage adapter for a given layer.
     * <p>
     * This method selects a {@link StorageAdapter} implementation based on the layer number. The logic is intended to
     * use an {@code InliningStorageAdapter} for layers greater than {@code 0} and a {@code CompactStorageAdapter} for
     * layer 0. Note that we will only use inlining at all if the config indicates we should use inlining.
     *
     * @param layer the layer number for which to get the storage adapter
     * @return a non-null {@link StorageAdapter} instance
     */
    @Nonnull
    StorageAdapter<? extends NodeReference> storageAdapterForLayer(final int layer) {
        return storageAdapterForLayer(getConfig(), getSubspace(), getOnWriteListener(), getOnReadListener(), layer);
    }

    @Nonnull
    static SplittableRandom random(@Nonnull final Tuple primaryKey) {
        return new SplittableRandom(splitMixLong(primaryKey.hashCode()));
    }

    /**
     * Calculates a layer for a new element to be inserted or for an element to be deleted from.
     * <p>
     * The layer is selected according to a logarithmic distribution, which ensures that the probability of choosing
     * a higher layer decreases exponentially. This is achieved by applying the inverse transform sampling method.
     * The specific formula is {@code floor(-ln(u) * lambda)}, where {@code u} is a uniform random number and
     * {@code lambda} is a normalization factor derived from a system configuration parameter {@code M}.
     * @param primaryKey the primary key of the record to be inserted/updated/deleted
     * @return a non-negative integer representing the randomly selected layer
     */
    int topLayer(@Nonnull final Tuple primaryKey) {
        double lambda = 1.0 / Math.log(getConfig().getM());
        double u = 1.0 - splitMixDouble(primaryKey.hashCode());  // Avoid log(0)
        return (int) Math.floor(-Math.log(u) * lambda);
    }

    /**
     * Scans all nodes within a given layer of the database.
     * <p>
     * The scan is performed transactionally in batches to avoid loading the entire layer into memory at once. Each
     * discovered node is passed to the provided {@link Consumer} for processing. The operation continues fetching
     * batches until all nodes in the specified layer have been processed.
     *
     * @param db the non-null {@link Database} instance to run the scan against.
     * @param layer the specific layer index to scan.
     * @param batchSize the number of nodes to retrieve and process in each batch.
     * @param nodeConsumer the non-null {@link Consumer} that will accept each {@link AbstractNode}
     * found in the layer.
     */
    @VisibleForTesting
    static void scanLayer(@Nonnull final Config config,
                          @Nonnull final Subspace subspace,
                          @Nonnull final Database db,
                          final int layer,
                          final int batchSize,
                          @Nonnull final Consumer<AbstractNode<? extends NodeReference>> nodeConsumer) {
        final StorageAdapter<? extends NodeReference> storageAdapter =
                storageAdapterForLayer(config, subspace, OnWriteListener.NOOP, OnReadListener.NOOP, layer);
        final AtomicReference<Tuple> lastPrimaryKeyAtomic = new AtomicReference<>();
        Tuple newPrimaryKey;
        do {
            final Tuple lastPrimaryKey = lastPrimaryKeyAtomic.get();
            lastPrimaryKeyAtomic.set(null);
            newPrimaryKey = db.run(tr -> {
                Streams.stream(storageAdapter.scanLayer(tr, layer, lastPrimaryKey, batchSize))
                        .forEach(node -> {
                            nodeConsumer.accept(Objects.requireNonNull(node));
                            lastPrimaryKeyAtomic.set(node.getPrimaryKey());
                        });
                return lastPrimaryKeyAtomic.get();
            });
        } while (newPrimaryKey != null);
    }

    /**
     * Gets the appropriate storage adapter for a given layer.
     * <p>
     * This method selects a {@link StorageAdapter} implementation based on the layer number. The logic is intended to
     * use an {@code InliningStorageAdapter} for layers greater than {@code 0} and a {@code CompactStorageAdapter} for
     * layer 0. Note that we will only use inlining at all if the config indicates we should use inlining.
     *
     * @param config the config to use
     * @param subspace the subspace of the HNSW object itself
     * @param onWriteListener a listener that the new {@link StorageAdapter} will call back for any write events
     * @param onReadListener a listener that the new {@link StorageAdapter} will call back for any read events
     * @param layer the layer number for which to get the storage adapter
     * @return a non-null {@link StorageAdapter} instance
     */
    @Nonnull
    @VisibleForTesting
    static StorageAdapter<? extends NodeReference>
            storageAdapterForLayer(@Nonnull final Config config,
                                   @Nonnull final Subspace subspace,
                                   @Nonnull final OnWriteListener onWriteListener,
                                   @Nonnull final OnReadListener onReadListener,
                                   final int layer) {
        return config.isUseInlining() && layer > 0
               ? new InliningStorageAdapter(config, InliningNode.factory(), subspace, onWriteListener, onReadListener)
               : new CompactStorageAdapter(config, CompactNode.factory(), subspace, onWriteListener, onReadListener);
    }

    /**
     * Returns a good double hash code for the argument of type {@code long}. It uses {@link #splitMixLong(long)}
     * internally and then maps the {@code long} result to a {@code double} between {@code 0} and {@code 1}.
     * This method is directly used in {@link #topLayer(Tuple)} to determine the top layer of a record given its
     * primary key.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x} as a {@code double} in the range {@code [0.0d, 1.0d)}.
     */
    static double splitMixDouble(final long x) {
        return (splitMixLong(x) >>> 11) * 0x1.0p-53;
    }

    /**
     * Returns a good long hash code for the argument of type {@code long}. It is an implementation of the
     * output mixing function {@code SplitMix64} as employed by many PRNG such as {@link SplittableRandom}.
     * See <a href="https://en.wikipedia.org/wiki/Linear_congruential_generator">Linear congruential generator</a> for
     * more information.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x}
     */
    static long splitMixLong(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >>> 31);
        return x;
    }

    @Nonnull
    static <T> List<T> drain(@Nonnull Queue<T> queue) {
        final ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();
        while (!queue.isEmpty()) {
            resultBuilder.add(queue.poll());
        }
        return resultBuilder.build();
    }

    @SuppressWarnings("SameParameterValue")
    static int clamp(final int x, final int low, final int high) {
        Verify.verify(low <= high);
        return Math.max(low, Math.min(x, high));
    }

    @FunctionalInterface
    interface CandidatePredicate {
        @Nonnull
        static CandidatePredicate tautology() {
            return (random, initialNodeKeys, size, nodeReference) -> true;
        }

        boolean test(@Nullable SplittableRandom random, @Nonnull Set<Tuple> initialNodeKeys, int size, NodeReference nodeReference);
    }

    static class AccessInfoAndNodeExistence {
        @Nullable
        private final AccessInfo accessInfo;
        private final boolean nodeExists;

        public AccessInfoAndNodeExistence(@Nullable final AccessInfo accessInfo, final boolean nodeExists) {
            this.accessInfo = accessInfo;
            this.nodeExists = nodeExists;
        }

        @Nullable
        public AccessInfo getAccessInfo() {
            return accessInfo;
        }

        public boolean isNodeExists() {
            return nodeExists;
        }
    }
}
