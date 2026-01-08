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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of the search operations of the hierarchical Navigable Small World (HNSW) algorithm for efficient
 * approximate nearest neighbor (ANN) search.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class Search {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Search.class);

    @Nonnull
    private final Locator locator;

    /**
     * This constructor initializes a new search operations object with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
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
    private Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
     * @return hnsw configuration
     */
    @Nonnull
    private Config getConfig() {
        return getLocator().getConfig();
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
    private Primitives primitives() {
        return locator.primitives();
    }

    /**
     * Performs a search for the k-nearest neighbors for a given query vector.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the number of nearest neighbors to return
     * @param efSearch the size of the dynamic candidate list for the search. A larger value increases accuracy
     *        at the cost of performance.
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                    final int k,
                                    final int efSearch,
                                    final boolean includeVectors,
                                    @Nonnull final RealVector queryVector) {
        return search(readTransaction, queryVector,
                layer -> layer > 0 ? 1 : efSearch,
                Search::distanceToTargetVector)
                .thenApply(searchResult ->
                        postProcessSearchResult(searchResult.getStorageTransform(), k,
                                NodeReferenceAndNode.references(searchResult.getNearestReferenceAndNodes()),
                                includeVectors));
    }

    /**
     * Performs a search for the k-nearest neighbors of a ring around a given query vector at a given radius.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param k the number of nearest neighbors to return
     * @param efSearch the size of the dynamic candidate list for the search. A larger value increases accuracy
     *        at the cost of performance.
     * @param includeVectors indicator if the caller would like the search to also include vectors in the result set
     * @param queryVector the vector to find the nearest neighbors for
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<List<? extends ResultEntry>>
            kNearestNeighborsRingSearch(@Nonnull final ReadTransaction readTransaction,
                                        final int k,
                                        final int efSearch,
                                        final boolean includeVectors,
                                        @Nonnull final RealVector queryVector,
                                        final double radius) {
        return search(readTransaction, queryVector,
                layer ->
                        layer > 0
                        ? Primitives.clamp((int)Math.floor(Math.sqrt(efSearch)), 8, 32)
                        : efSearch,
                (estimator, targetVector) ->
                        distanceToSphericalSurface(estimator, targetVector, radius))
                .thenApply(searchResult ->
                        postProcessSearchResult(searchResult.getStorageTransform(), k,
                                NodeReferenceAndNode.references(searchResult.getNearestReferenceAndNodes()),
                                includeVectors));
    }

    /**
     * Performs a k-nearest neighbors (k-NN) search for a given query vector.
     * <p>
     * This method implements the search algorithm for an HNSW graph. The search begins at an entry point in the
     * highest layer and greedily traverses down through the layers. In each layer, it finds the node closest to the
     * {@code queryVector}. This node then serves as the entry point for the search in the layer below.
     * <p>
     * Once the search reaches the base layer (layer 0), it performs a more exhaustive search starting from the
     * determined entry point. It explores the graph, maintaining a dynamic list of the best candidates found so far.
     * The size of this candidate list is controlled by the {@code efSearch} parameter. Finally, the method selects
     * the top {@code k} nodes from the search results, sorted by their distance to the query vector.
     *
     * @param readTransaction the transaction to use for reading from the database
     * @param queryVector the vector to find the nearest neighbors for
     * @param efSearchFunction a function that returns the exploration factor for a layer that is passed in
     * @param objectiveFunctionCreator a functional that creates the objective function for this search
     *
     * @return a {@link CompletableFuture} that will complete with a list of the {@code k} nearest neighbors,
     *         sorted by distance in ascending order.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    CompletableFuture<SearchResult> search(@Nonnull final ReadTransaction readTransaction,
                                           @Nonnull final RealVector queryVector,
                                           @Nonnull final IntUnaryOperator efSearchFunction,
                                           @Nonnull final ObjectiveFunctionCreator objectiveFunctionCreator) {
        return StorageAdapter.fetchAccessInfo(getConfig(), readTransaction, getSubspace(), getOnReadListener())
                .thenCompose(accessInfo -> {
                    if (accessInfo == null) {
                        // not a single node in the index
                        return CompletableFuture.completedFuture(new SearchResult(null,
                                StorageTransform.identity(), ImmutableList.of(), Maps.newConcurrentMap()));
                    }
                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();

                    final Primitives primitives = primitives();
                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final Quantizer quantizer = primitives.quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();
                    final ToDoubleFunction<Transformed<RealVector>> objectiveFunction =
                            objectiveFunctionCreator.create(estimator, transformedQueryVector);

                    final List<NodeReferenceWithDistance> entryState =
                            ImmutableList.of(new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    objectiveFunction.applyAsDouble(entryNodeReference.getVector())));

                    final int topLayer = entryNodeReference.getLayer();
                    return forLoop(topLayer, entryState,
                            layer -> layer > 0,
                            layer -> layer - 1,
                            (layer, previousNodeReferences) -> {
                                final int efSearchForLayer = efSearchFunction.applyAsInt(layer);
                                final StorageAdapter<?> storageAdapter = primitives.storageAdapterForLayer(layer);
                                return searchLayer(storageAdapter, readTransaction, storageTransform,
                                        previousNodeReferences, layer, efSearchForLayer, objectiveFunction);
                            }, getExecutor())
                            .thenCompose(previousNodeReferences -> {
                                final int efSearchForLayer = efSearchFunction.applyAsInt(0);
                                final CompactStorageAdapter storageAdapter =
                                        primitives.storageAdapterForLayer(0).asCompactStorageAdapter();
                                final ConcurrentMap<Tuple, AbstractNode<NodeReference>> nodeCache = Maps.newConcurrentMap();
                                return beamSearchLayer(storageAdapter, readTransaction, storageTransform,
                                        previousNodeReferences, 0, efSearchForLayer, objectiveFunction, nodeCache)
                                        .thenApply(nearestNodeReferenceAndNodes ->
                                                new SearchResult(accessInfo, storageTransform,
                                                        nearestNodeReferenceAndNodes, nodeCache));
                            });
                });
    }

    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>>
            searchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                        @Nonnull final ReadTransaction readTransaction,
                        @Nonnull final StorageTransform storageTransform,
                        @Nonnull final List<NodeReferenceWithDistance> nodeReferenceWithDistances,
                        final int layer,
                        final int efSearch,
                        @Nonnull final ToDoubleFunction<Transformed<RealVector>> objectiveFunction) {
        if (efSearch == 1 && nodeReferenceWithDistances.size() == 1) {
            return greedySearchLayer(storageAdapter, readTransaction, storageTransform,
                    Iterables.getOnlyElement(nodeReferenceWithDistances),
                    layer, objectiveFunction).thenApply(ImmutableList::of);
        } else {
            return beamSearchLayer(storageAdapter, readTransaction, storageTransform,
                    nodeReferenceWithDistances, layer, efSearch, objectiveFunction,
                    Maps.newConcurrentMap())
                    .thenApply(NodeReferenceAndNode::references);
        }
    }

    @Nonnull
    private ImmutableList<ResultEntry> postProcessSearchResult(@Nonnull final StorageTransform storageTransform,
                                                               final int k,
                                                               @Nonnull final List<NodeReferenceWithDistance> nearestReferences,
                                                               final boolean includeVectors) {
        final int lastIndex = Math.max(nearestReferences.size() - k, 0);

        final ImmutableList.Builder<ResultEntry> resultBuilder =
                ImmutableList.builder();

        for (int i = nearestReferences.size() - 1; i >= lastIndex; i --) {
            final var nodeReference = nearestReferences.get(i);
            @Nullable final RealVector reconstructedVector =
                    includeVectors ? storageTransform.untransform(nodeReference.getVector()) : null;

            resultBuilder.add(
                    new ResultEntry(nodeReference.getPrimaryKey(),
                            reconstructedVector, nodeReference.getDistance(),
                            nearestReferences.size() - i - 1));
        }
        return resultBuilder.build();
    }

    /**
     * Performs a greedy search on a single layer of the HNSW graph.
     * <p>
     * This method finds the node on the specified layer that is closest to the given query vector,
     * starting the search from a designated entry point. The search is "greedy" because it aims to find
     * only the single best neighbor.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the {@link StorageAdapter} for accessing the graph data
     * @param readTransaction the {@link ReadTransaction} to use for the search
     * @param storageTransform the storage transform that needs to be applied
     * @param nodeReferenceWithDistance the starting point for the search on this layer, which includes the node and its distance to
     *        the query vector
     * @param layer the zero-based index of the layer to search within
     * @param objectiveFunction the objective function that is to be minimized
     * @return a {@link CompletableFuture} that, upon completion, will contain the closest node found on the layer,
     *         represented as a {@link NodeReferenceWithDistance}
     */
    @Nonnull
    <N extends NodeReference> CompletableFuture<NodeReferenceWithDistance>
            greedySearchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                              @Nonnull final ReadTransaction readTransaction,
                              @Nonnull final StorageTransform storageTransform,
                              @Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance,
                              final int layer,
                              @Nonnull final ToDoubleFunction<Transformed<RealVector>> objectiveFunction) {
        if (storageAdapter.isInliningStorageAdapter()) {
            return greedySearchInliningLayer(storageAdapter.asInliningStorageAdapter(), readTransaction,
                    storageTransform, nodeReferenceWithDistance, layer, objectiveFunction);
        } else {
            return beamSearchLayer(storageAdapter, readTransaction, storageTransform,
                    ImmutableList.of(nodeReferenceWithDistance), layer, 1, objectiveFunction,
                    Maps.newConcurrentMap())
                    .thenApply(searchResult ->
                            Iterables.getOnlyElement(searchResult).getNodeReference());
        }
    }

    /**
     * Method to perform the greedy nearest neighbor search. This method is only suited to traverse a layer using the
     * {@link InliningStorageAdapter}. Because of the inlining approach it is possible to avoid most of the fetches
     * that would be necessary when processing a layer in compact storage layout.
     *
     * @param storageAdapter the {@link StorageAdapter} for accessing the graph data
     * @param readTransaction the {@link ReadTransaction} to use for the search
     * @param storageTransform the storage transform that needs to be applied
     * @param nodeReferenceWithDistance the starting point for the search on this layer, which includes the node and its distance to
     *        the query vector
     * @param layer the zero-based index of the layer to search within
     * @param objectiveFunction the objective function that is to be minimized
     * @return a {@link CompletableFuture} that, upon completion, will contain the closest node found on the layer,
     *         represented as a {@link NodeReferenceWithDistance}
     */
    @Nonnull
    private CompletableFuture<NodeReferenceWithDistance>
            greedySearchInliningLayer(@Nonnull final InliningStorageAdapter storageAdapter,
                                      @Nonnull final ReadTransaction readTransaction,
                                      @Nonnull final StorageTransform storageTransform,
                                      @Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance,
                                      final int layer,
                                      @Nonnull final ToDoubleFunction<Transformed<RealVector>> objectiveFunction) {
        final Primitives primitives = primitives();
        final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();
        final Map<Tuple, AbstractNode<NodeReferenceWithVector>> nodeCache = Maps.newHashMap();
        final Map<Tuple, AbstractNode<NodeReferenceWithVector>> updatedNodes = Maps.newHashMap();

        final AtomicReference<NodeReferenceWithDistance> nearestNodeReferenceAtomic =
                new AtomicReference<>(null);

        final Queue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily a limit,
                // but it gives us a number that is better than the default.
                new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator());
        candidates.add(nodeReferenceWithDistance);

        return AsyncUtil.whileTrue(() -> {
            final NodeReferenceWithDistance candidateReference = Objects.requireNonNull(candidates.poll());
            return getOnReadListener().onAsyncRead(
                            primitives.fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                                    candidateReference, nodeCache))
                    .thenCompose(node -> {
                        if (node == null) {
                            //
                            // This cannot happen under normal circumstances as the storage adapter returns a node with no
                            // neighbors if it already has been deleted. Therefore, it is correct to throw here.
                            //
                            throw new IllegalStateException("unable to fetch node");
                        }
                        final InliningNode candidateNode = node.asInliningNode();

                        if (updatedNodes.containsKey(candidateReference.getPrimaryKey())) {
                            return CompletableFuture.completedFuture(updatedNodes.get(candidateReference.getPrimaryKey()));
                        }

                        return primitives.fetchBaseNode(readTransaction, storageTransform, candidateReference.getPrimaryKey())
                                .thenAccept(baseCompactNode -> {
                                    if (baseCompactNode == null) {
                                        // node does not exist on layer 0
                                        return;
                                    }

                                    //
                                    // Node does still exist or an updated version exists -- create new reference
                                    // and push it back into the queue
                                    //
                                    final Transformed<RealVector> baseVector = baseCompactNode.getVector();

                                    final double distance = objectiveFunction.applyAsDouble(baseVector);

                                    final NodeReferenceWithDistance updatedNodeReference =
                                            new NodeReferenceWithDistance(baseCompactNode.getPrimaryKey(),
                                                    baseVector,
                                                    distance);
                                    candidates.add(updatedNodeReference);
                                    updatedNodes.put(candidateReference.getPrimaryKey(),
                                            nodeFactory.create(candidateReference.getPrimaryKey(),
                                                    baseCompactNode.getVector(), candidateNode.getNeighbors()));
                                })
                                .thenApply(ignored -> null); // keep Java happy about the return type

                    })
                    .thenApply(candidateNode -> {
                        if (candidateNode != null) {
                            //
                            // This node definitely does exist. And it's the nearest one.
                            //
                            nearestNodeReferenceAtomic.set(candidateReference);
                            candidates.clear();

                            //
                            // Find some new candidates.
                            //
                            double minDistance = candidateReference.getDistance();

                            for (final NodeReferenceWithVector neighbor : candidateNode.getNeighbors()) {
                                final double distance = objectiveFunction.applyAsDouble(neighbor.getVector());
                                if (distance < minDistance) {
                                    candidates.add(
                                            new NodeReferenceWithDistance(neighbor.getPrimaryKey(), neighbor.getVector(),
                                                    distance));
                                }
                            }
                        }
                        return !candidates.isEmpty();
                    });
        }, getExecutor()).thenApply(ignored -> nearestNodeReferenceAtomic.get());
    }

    /**
     * Searches a single layer of the graph to find the nearest neighbors to a query vector.
     * <p>
     * This method implements the search algorithm used in HNSW (Hierarchical Navigable Small World)
     * graphs for a specific layer. It begins with a set of entry points and iteratively explores the graph,
     * always moving towards nodes that are closer to the {@code queryVector}.
     * <p>
     * It maintains a priority queue of candidates to visit and a result set of the nearest neighbors found so far.
     * The size of the dynamic candidate list is controlled by the {@code efSearch} parameter, which balances
     * search quality and performance. The entire process is asynchronous, leveraging
     * {@link CompletableFuture}
     * to handle I/O operations (fetching nodes) without blocking.
     *
     * @param <N> The type of the node reference, extending {@link NodeReference}.
     * @param storageAdapter The storage adapter for accessing node data from the underlying storage.
     * @param readTransaction The transaction context for all database read operations.
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param nodeReferences A collection of starting node references for the search in this layer, with their distances
     * to the query vector already calculated.
     * @param layer The zero-based index of the layer to search.
     * @param efSearch The size of the dynamic candidate list. A larger value increases recall at the
     * cost of performance.
     * @param objectiveFunction the objective function that is to be minimized
     * @param nodeCache A cache of nodes that have already been fetched from storage to avoid redundant I/O.
     *
     * @return A {@link CompletableFuture} that, upon completion, will contain a list of the
     * best candidate nodes found in this layer, paired with their full node data.
     */
    @Nonnull
    <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            beamSearchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final ReadTransaction readTransaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Collection<NodeReferenceWithDistance> nodeReferences,
                            final int layer,
                            final int efSearch,
                            @Nonnull final ToDoubleFunction<Transformed<RealVector>> objectiveFunction,
                            @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final Primitives primitives = primitives();
        final Queue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily a limit,
                // but it gives us a number that is better than the default.
                new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator());
        candidates.addAll(nodeReferences);
        final Set<Tuple> visited = Sets.newConcurrentHashSet(NodeReference.primaryKeys(nodeReferences));
        final Queue<NodeReferenceWithDistance> nearestNeighbors =
                new PriorityQueue<>(efSearch + 1, // prevent reallocation further down
                        NodeReferenceWithDistance.reversedComparator());
        nearestNeighbors.addAll(nodeReferences);

        return AsyncUtil.whileTrue(() -> {
            if (candidates.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }

            final NodeReferenceWithDistance candidate = candidates.poll();
            final NodeReferenceWithDistance furthestNeighbor = Objects.requireNonNull(nearestNeighbors.peek());

            if (candidate.getDistance() > furthestNeighbor.getDistance()) {
                return AsyncUtil.READY_FALSE;
            }

            return primitives.fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            candidateNode == null
                            ? ImmutableList.<N>of()
                            : Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(Objects.requireNonNull(neighbor).getPrimaryKey())))
                    .thenCompose(neighborReferences -> primitives.fetchNeighborhoodReferences(storageAdapter, readTransaction,
                            storageTransform, layer, neighborReferences, nodeCache))
                    .thenApply(neighborReferences -> {
                        for (final NodeReferenceWithVector current : neighborReferences) {
                            visited.add(current.getPrimaryKey());
                            final double furthestDistance =
                                    Objects.requireNonNull(nearestNeighbors.peek()).getDistance();

                            final double currentDistance = objectiveFunction.applyAsDouble(current.getVector());
                            if (currentDistance < furthestDistance || nearestNeighbors.size() < efSearch) {
                                final NodeReferenceWithDistance currentWithDistance =
                                        new NodeReferenceWithDistance(current.getPrimaryKey(), current.getVector(),
                                                currentDistance);
                                candidates.add(currentWithDistance);
                                nearestNeighbors.add(currentWithDistance);
                                if (nearestNeighbors.size() > efSearch) {
                                    nearestNeighbors.poll();
                                }
                            }
                        }
                        return true;
                    });
        })
        .thenCompose(ignored ->
                primitives.fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                        Primitives.drain(nearestNeighbors), nodeCache))
        .thenApply(searchResult -> {
            if (logger.isTraceEnabled()) {
                logger.trace("searched layer={} for efSearch={} with result=={}", layer, efSearch,
                        searchResult.stream()
                                .map(nodeReferenceAndNode ->
                                        "(primaryKey=" +
                                                nodeReferenceAndNode.getNodeReference().getPrimaryKey() +
                                                ",distance=" +
                                                nodeReferenceAndNode.getNodeReference().getDistance() + ")")
                                .collect(Collectors.joining(",")));
            }
            return searchResult;
        });
    }

    /**
     * Returns an async iterator that returns results ordered by their distance from a given center vector.
     * <p>
     * This method initiates an outward traversal from the {@code centerVector}, effectively performing a k-NN
     * (k-Nearest Neighbor) or beam search. The results are returned as an {@link AsyncIterator}, with items
     * yielded in increasing order of their distance from the center. The search can be started or resumed from a
     * specific point defined by {@code minimumRadius} and {@code minimumPrimaryKey}, allowing for pagination.
     *
     * @param readTransaction the transaction to use for reading data
     * @param efRingSearch the exploration factor for the initial ring search phase of the HNSW algorithm
     * @param efOutwardSearch the exploration factor for the main outward search phase, determining the size of the
     * candidate queue
     * @param includeVectors a boolean flag indicating whether the full vectors should be reconstructed and included in
     *        the results. If {@code false}, the vector in each {@link ResultEntry} will be {@code null}.
     * @param centerVector the vector to search around. Results will be ordered by their distance to this vector
     * @param minimumRadius the minimum distance from the {@code centerVector}. Only results with a distance greater
     *        than will be returned.
     * @param minimumPrimaryKey the primary key of the last item from a previous scan, used for pagination. If provided
     *        along with {@code minimumRadius}, the scan will resume after the item with this key at that radius. Can be
     *        {@code null} to start from the beginning.
     * @return an {@link AsyncIterator} of {@link ResultEntry} objects, ordered by increasing distance from the
     *         {@code centerVector}
     */
    AsyncIterator<ResultEntry> orderByDistance(@Nonnull final ReadTransaction readTransaction,
                                               final int efRingSearch,
                                               final int efOutwardSearch,
                                               final boolean includeVectors,
                                               @Nonnull final RealVector centerVector,
                                               final double minimumRadius,
                                               @Nullable final Tuple minimumPrimaryKey) {
        final OutwardTraversalIterator it =
                searchAndIterateOutward(readTransaction, efRingSearch, efOutwardSearch, centerVector,
                        minimumRadius, minimumPrimaryKey);
        return AsyncUtil.mapIterator(it,
                nodeReferenceAndNode -> {
                    final var nodeReference = nodeReferenceAndNode.getNodeReference();
                    @Nullable final RealVector reconstructedVector;
                    if (includeVectors) {
                        final StorageTransform storageTransform = it.getTraversalState().getStorageTransform();
                        reconstructedVector = storageTransform.untransform(nodeReference.getVector());
                    } else {
                        reconstructedVector = null;
                    }

                    return new ResultEntry(nodeReference.getPrimaryKey(),
                            reconstructedVector, nodeReference.getDistance(), -1);
                });
    }

    @Nonnull
    private OutwardTraversalIterator searchAndIterateOutward(@Nonnull final ReadTransaction readTransaction,
                                                             final int efRingSearch,
                                                             final int efOutwardSearch,
                                                             @Nonnull final RealVector centerVector,
                                                             final double minimumRadius,
                                                             @Nullable final Tuple minimumPrimaryKey) {
        final CompletableFuture<SearchResult> zoomInResultFuture =
                search(readTransaction, centerVector,
                        layer ->
                                layer > 0
                                ? Primitives.clamp((int)Math.floor(Math.sqrt(efRingSearch)), 8, 32)
                                : efRingSearch,
                        (estimator, targetVector) ->
                                distanceToSphericalSurface(estimator, targetVector, minimumRadius));
        final CompactStorageAdapter storageAdapter = primitives().storageAdapterForLayer(0).asCompactStorageAdapter();
        return new OutwardTraversalIterator(getLocator(), storageAdapter, readTransaction, zoomInResultFuture, centerVector,
                minimumRadius, minimumPrimaryKey, efOutwardSearch);
    }

    @Nonnull
    static ToDoubleFunction<Transformed<RealVector>> distanceToTargetVector(@Nonnull final Estimator estimator,
                                                                            @Nonnull final Transformed<RealVector> targetVector) {
        return vector -> estimator.distance(targetVector, vector);
    }

    @Nonnull
    static ToDoubleFunction<Transformed<RealVector>> distanceToSphericalSurface(@Nonnull final Estimator estimator,
                                                                                @Nonnull final Transformed<RealVector> targetVector,
                                                                                final double radius) {
        return vector -> Math.abs(estimator.distance(targetVector, vector) - radius);
    }

    static class SearchResult {
        @Nullable
        private final AccessInfo accessInfo;
        @Nonnull
        private final StorageTransform storageTransform;
        @Nonnull
        private final List<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> nearestReferenceAndNodes;
        @Nonnull
        private final Map<Tuple, AbstractNode<NodeReference>> nodeCache;

        public SearchResult(@Nullable final AccessInfo accessInfo,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final List<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> nearestReferenceWithNodes,
                            @Nonnull final Map<Tuple, AbstractNode<NodeReference>> nodeCache) {
            this.accessInfo = accessInfo;
            this.storageTransform = storageTransform;
            this.nearestReferenceAndNodes = nearestReferenceWithNodes;
            this.nodeCache = nodeCache;
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
        public List<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> getNearestReferenceAndNodes() {
            return nearestReferenceAndNodes;
        }

        @Nonnull
        public Map<Tuple, AbstractNode<NodeReference>> getNodeCache() {
            return nodeCache;
        }
    }

    @FunctionalInterface
    private interface ObjectiveFunctionCreator {
        @Nonnull
        ToDoubleFunction<Transformed<RealVector>> create(@Nonnull Estimator estimator, @Nonnull Transformed<RealVector> targetVector);
    }
}
