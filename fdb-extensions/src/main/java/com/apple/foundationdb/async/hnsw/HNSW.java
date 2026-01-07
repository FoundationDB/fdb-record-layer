/*
 * HNSW.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;
import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;

/**
 * An implementation of the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 * <p>
 * HNSW constructs a multi-layer graph, where each layer is a subset of the one below it.
 * The top layers serve as fast entry points to navigate the graph, while the bottom layer
 * contains all the data points. This structure allows for logarithmic-time complexity
 * for search operations, making it suitable for large-scale, high-dimensional datasets.
 * <p>
 * This class provides methods for building the graph ({@link #insert(Transaction, Tuple, RealVector)})
 * and performing k-NN searches ({@link #kNearestNeighborsSearch(ReadTransaction, int, int, boolean, RealVector)}).
 * It is designed to be used with a transactional storage backend, managed via a {@link Subspace}.
 *
 * @see <a href="https://arxiv.org/abs/1603.09320">Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HNSW {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(HNSW.class);

    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Config config;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link HNSW}
     * @see Config.ConfigBuilder#build
     */
    public static Config.ConfigBuilder newConfigBuilder() {
        return new Config.ConfigBuilder();
    }

    /**
     * Returns a default {@link Config}.
     * @param numDimensions number of dimensions
     * @return a new default {@code Config}.
     * @see Config.ConfigBuilder#build
     */
    @Nonnull
    public static Config defaultConfig(int numDimensions) {
        return new Config.ConfigBuilder().build(numDimensions);
    }

    /**
     * Constructs a new HNSW graph instance.
     * <p>
     * This constructor initializes the HNSW graph with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
     *
     * @param subspace the {@link Subspace} where the graph data is stored.
     * @param executor the {@link Executor} service to use for concurrent operations.
     * @param config the {@link Config} object containing HNSW algorithm parameters.
     * @param onWriteListener a listener to be notified of write events on the graph.
     * @param onReadListener a listener to be notified of read events on the graph.
     *
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public HNSW(@Nonnull final Subspace subspace,
                @Nonnull final Executor executor,
                @Nonnull final Config config,
                @Nonnull final OnWriteListener onWriteListener,
                @Nonnull final OnReadListener onReadListener) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }


    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return config;
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Nonnull
    private StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return new StorageTransform(accessInfo.getRotatorSeed(),
                getConfig().getNumDimensions(), Objects.requireNonNull(accessInfo.getNegatedCentroid()));
    }

    @Nonnull
    private Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(config.getMetric());
        }

        final Config config = getConfig();
        return config.isUseRaBitQ()
               ? new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits())
               : Quantizer.noOpQuantizer(config.getMetric());
    }

    //
    // Read Path
    //

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
                HNSW::distanceToTargetVector)
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
                        ? clamp((int)Math.floor(Math.sqrt(efSearch)), 8, 32)
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
    private CompletableFuture<SearchResult>
            search(@Nonnull final ReadTransaction readTransaction,
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

                    final StorageTransform storageTransform = storageTransform(accessInfo);
                    final Transformed<RealVector> transformedQueryVector = storageTransform.transform(queryVector);
                    final Quantizer quantizer = quantizer(accessInfo);
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
                                final StorageAdapter<?> storageAdapter = storageAdapterForLayer(layer);
                                return searchLayer(storageAdapter, readTransaction, storageTransform,
                                        previousNodeReferences, layer, efSearchForLayer, objectiveFunction);
                            }, executor)
                            .thenCompose(previousNodeReferences -> {
                                final int efSearchForLayer = efSearchFunction.applyAsInt(0);
                                final CompactStorageAdapter storageAdapter =
                                        storageAdapterForLayer(0).asCompactStorageAdapter();
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
    private <N extends NodeReference> CompletableFuture<NodeReferenceWithDistance>
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
        final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();
        final Map<Tuple, AbstractNode<NodeReferenceWithVector>> nodeCache = Maps.newHashMap();
        final Map<Tuple, AbstractNode<NodeReferenceWithVector>> updatedNodes = Maps.newHashMap();

        final AtomicReference<NodeReferenceWithDistance> nearestNodeReferenceAtomic =
                new AtomicReference<>(null);

        final Queue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily a limit,
                // but it gives us a number that is better than the default.
                new PriorityQueue<>(config.getM(), NodeReferenceWithDistance.comparator());
        candidates.add(nodeReferenceWithDistance);

        return AsyncUtil.whileTrue(() -> {
            final NodeReferenceWithDistance candidateReference = Objects.requireNonNull(candidates.poll());
            return onReadListener.onAsyncRead(
                            fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
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

                        return fetchBaseNode(readTransaction, storageTransform, candidateReference.getPrimaryKey())
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
        }, executor).thenApply(ignored -> nearestNodeReferenceAtomic.get());
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
     * {@link java.util.concurrent.CompletableFuture}
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
     * @return A {@link java.util.concurrent.CompletableFuture} that, upon completion, will contain a list of the
     * best candidate nodes found in this layer, paired with their full node data.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            beamSearchLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final ReadTransaction readTransaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Collection<NodeReferenceWithDistance> nodeReferences,
                            final int layer,
                            final int efSearch,
                            @Nonnull final ToDoubleFunction<Transformed<RealVector>> objectiveFunction,
                            @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {
        final Queue<NodeReferenceWithDistance> candidates =
                // This initial capacity is somewhat arbitrary as m is not necessarily a limit,
                // but it gives us a number that is better than the default.
                new PriorityQueue<>(config.getM(), NodeReferenceWithDistance.comparator());
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

            return fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            candidateNode == null
                            ? ImmutableList.<N>of()
                            : Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(Objects.requireNonNull(neighbor).getPrimaryKey())))
                    .thenCompose(neighborReferences -> fetchNeighborhoodReferences(storageAdapter, readTransaction,
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
                fetchSomeNodesIfNotCached(storageAdapter, readTransaction, storageTransform, layer,
                        drain(nearestNeighbors), nodeCache))
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
    private <N extends NodeReference> AbstractNode<N>
            nodeFromCache(@Nonnull final Tuple primaryKey,
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
    private <N extends NodeReference> CompletableFuture<AbstractNode<N>>
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

        return onReadListener.onAsyncRead(
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
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>>
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
    private <T extends NodeReferenceWithVector, N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<T, N>>>
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
     * @return A {@link CompletableFuture} that, upon completion, will hold a {@link java.util.List} of non-null results
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

    /**
     * Inserts a new vector with its associated primary key into the HNSW graph.
     * <p>
     * The method first determines a layer for the new node, called the {@code top layer}.
     * It then traverses the graph from the entry point downwards, greedily searching for the nearest
     * neighbors to the {@code newVector} at each layer. This search identifies the optimal
     * connection points for the new node.
     * <p>
     * Once the nearest neighbors are found, the new node is linked into the graph structure at all
     * layers up to its {@code top layer}. Special handling is included for inserting the
     * first-ever node into the graph or when a new node's layer is higher than any existing node,
     * which updates the graph's entry point. All operations are performed asynchronously.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param newPrimaryKey the unique {@link Tuple} primary key for the new node being inserted
     * @param newVector the {@link RealVector} data to be inserted into the graph
     *
     * @return a {@link CompletableFuture} that completes when the insertion operation is finished
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final RealVector newVector) {
        final SplittableRandom random = random(newPrimaryKey);
        final int insertionLayer = topLayer(newPrimaryKey);
        if (logger.isTraceEnabled()) {
            logger.trace("new node with key={} selected to be inserted into layer={}", newPrimaryKey, insertionLayer);
        }

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCombine(exists(transaction, newPrimaryKey),
                        (accessInfo, nodeAlreadyExists) -> {
                            if (nodeAlreadyExists) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("new record already exists in HNSW with key={} on layer={}",
                                            newPrimaryKey, insertionLayer);
                                }
                            }
                            return new AccessInfoAndNodeExistence(accessInfo, nodeAlreadyExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    if (accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    final StorageTransform storageTransform = storageTransform(accessInfo);
                    final Transformed<RealVector> transformedNewVector = storageTransform.transform(newVector);
                    final Quantizer quantizer = quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    final AccessInfo currentAccessInfo;
                    if (accessInfo == null) {
                        // this is the first node
                        writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
                                insertionLayer, -1);
                        currentAccessInfo = new AccessInfo(
                                new EntryNodeReference(newPrimaryKey, transformedNewVector, insertionLayer),
                                -1L, null);
                        StorageAdapter.writeAccessInfo(transaction, getSubspace(), currentAccessInfo,
                                getOnWriteListener());
                        if (logger.isTraceEnabled()) {
                            logger.trace("written initial entry node reference with key={} on layer={}",
                                    newPrimaryKey, insertionLayer);
                        }
                        return AsyncUtil.DONE;
                    } else {
                        final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                        final int lMax = entryNodeReference.getLayer();
                        if (insertionLayer > lMax) {
                            writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
                                    insertionLayer, lMax);
                            currentAccessInfo = accessInfo.withNewEntryNodeReference(
                                    new EntryNodeReference(newPrimaryKey, transformedNewVector,
                                            insertionLayer));
                            StorageAdapter.writeAccessInfo(transaction, getSubspace(), currentAccessInfo,
                                    getOnWriteListener());
                            if (logger.isTraceEnabled()) {
                                logger.trace("written higher entry node reference with key={} on layer={}",
                                        newPrimaryKey, insertionLayer);
                            }
                        } else {
                            currentAccessInfo = accessInfo;
                        }
                    }
                    
                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                    final int lMax = entryNodeReference.getLayer();
                    if (logger.isTraceEnabled()) {
                        logger.trace("entry node read with key {} at layer {}", entryNodeReference.getPrimaryKey(), lMax);
                    }

                    final ToDoubleFunction<Transformed<RealVector>> objectiveFunction =
                            distanceToTargetVector(estimator, transformedNewVector);
                    final NodeReferenceWithDistance initialNodeReference =
                            new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    objectiveFunction.applyAsDouble(entryNodeReference.getVector()));

                    return forLoop(lMax, initialNodeReference,
                            layer -> layer > insertionLayer,
                            layer -> layer - 1,
                            (layer, previousNodeReference) -> {
                                final StorageAdapter<? extends NodeReference> storageAdapter = storageAdapterForLayer(layer);
                                return greedySearchLayer(storageAdapter, transaction, storageTransform,
                                        previousNodeReference, layer, objectiveFunction);
                            }, executor)
                            .thenCompose(nodeReference ->
                                    insertIntoLayers(transaction, storageTransform, quantizer, newPrimaryKey,
                                            transformedNewVector, nodeReference, lMax, insertionLayer))
                            .thenCompose(ignored ->
                                    addToStatsIfNecessary(random, transaction, currentAccessInfo, transformedNewVector));
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
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
    private CompletableFuture<Boolean> exists(@Nonnull final ReadTransaction readTransaction,
                                              @Nonnull final Tuple primaryKey) {
        //
        // Call fetchBaseNode() to check for the node's existence; we are handing in the identity operator,
        // since we do not care about the vector itself at all.
        //
        return fetchBaseNode(readTransaction, StorageTransform.identity(), primaryKey)
                .thenApply(Objects::nonNull);
    }

    @Nonnull
    private CompletableFuture<CompactNode> fetchBaseNode(@Nonnull final ReadTransaction readTransaction,
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
     * Method to keep stats if necessary. Stats need to be kept and maintained when the client would like to use
     * e.g. RaBitQ as RaBitQ needs a stable somewhat correct centroid in order to function properly.
     * <p>
     * Specifically for RaBitQ, we add vectors to a set of sampled vectors in a designated subspace of the HNSW
     * structure. The parameter {@link Config#getSampleVectorStatsProbability()} governs when we do sample. Another
     * parameter, {@link Config#getMaintainStatsProbability()}, determines how many times we add-up/replace (consume)
     * vectors from this sampled-vector space and aggregate them in the typical running count/running sum scheme
     * in order to finally compute the centroid if {@link Config#getStatsThreshold()} number of vectors have been
     * sampled and aggregated. That centroid is then used to update the access info.
     *
     * @param random a random to use
     * @param transaction the transaction
     * @param currentAccessInfo this current access info that was fetched as part of an insert
     * @param transformedNewVector the new vector (in the transformed coordinate system) that may be added
     * @return a future that returns {@code null} when completed
     */
    @Nonnull
    private CompletableFuture<Void> addToStatsIfNecessary(@Nonnull final SplittableRandom random,
                                                          @Nonnull final Transaction transaction,
                                                          @Nonnull final AccessInfo currentAccessInfo,
                                                          @Nonnull final Transformed<RealVector> transformedNewVector) {
        if (getConfig().isUseRaBitQ() && !currentAccessInfo.canUseRaBitQ()) {
            if (shouldSampleVector(random)) {
                StorageAdapter.appendSampledVector(transaction, getSubspace(),
                        1, transformedNewVector, onWriteListener);
            }
            if (shouldMaintainStats(random)) {
                return StorageAdapter.consumeSampledVectors(transaction, getSubspace(),
                                50, onReadListener)
                        .thenApply(sampledVectors -> {
                            final AggregatedVector aggregatedSampledVector =
                                    aggregateVectors(sampledVectors);

                            if (aggregatedSampledVector != null) {
                                final int partialCount = aggregatedSampledVector.getPartialCount();
                                final Transformed<RealVector> partialVector = aggregatedSampledVector.getPartialVector();
                                StorageAdapter.appendSampledVector(transaction, getSubspace(),
                                        partialCount, partialVector, onWriteListener);
                                if (logger.isTraceEnabled()) {
                                    logger.trace("updated stats with numVectors={}, partialCount={}, partialVector={}",
                                            sampledVectors.size(), partialCount, partialVector);
                                }

                                if (partialCount >= getConfig().getStatsThreshold()) {
                                    final long rotatorSeed = random.nextLong();
                                    final FhtKacRotator rotator =
                                            new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

                                    final Transformed<RealVector> centroid =
                                            partialVector.multiply(-1.0d / partialCount);
                                    final RealVector rotatedCentroid =
                                            rotator.apply(centroid.getUnderlyingVector());
                                    final StorageTransform storageTransform =
                                            new StorageTransform(rotator, rotatedCentroid);

                                    //
                                    // The entry node reference is expressed in a transformation that has so-far been
                                    // the identity-transformation. We now need to get the underlying identical vector
                                    // and, for the first time, transform that vector into the new rotated and
                                    // translated coordinate system. In this way we guarantee, that the entry node is
                                    // always expressed in the internal system, while data vectors may be a mix of
                                    // vectors.
                                    //
                                    final Transformed<RealVector> transformedEntryNodeVector =
                                            storageTransform.transform(currentAccessInfo.getEntryNodeReference()
                                                    .getVector().getUnderlyingVector());

                                    final AccessInfo newAccessInfo =
                                            new AccessInfo(currentAccessInfo.getEntryNodeReference().withVector(transformedEntryNodeVector),
                                                    rotatorSeed, rotatedCentroid);
                                    StorageAdapter.writeAccessInfo(transaction, getSubspace(), newAccessInfo, getOnWriteListener());
                                    StorageAdapter.deleteAllSampledVectors(transaction, getSubspace(), getOnWriteListener());
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("established rotatorSeed={}, centroid with count={}, centroid={}",
                                                rotatorSeed, partialCount, rotatedCentroid);
                                    }
                                }
                            }
                            return null;
                        });
            }
        }
        return AsyncUtil.DONE;
    }

    @Nullable
    private AggregatedVector aggregateVectors(@Nonnull final Iterable<AggregatedVector> vectors) {
        Transformed<RealVector> partialVector = null;
        int partialCount = 0;
        for (final AggregatedVector vector : vectors) {
            partialVector = partialVector == null
                            ? vector.getPartialVector() : partialVector.add(vector.getPartialVector());
            partialCount += vector.getPartialCount();
        }
        return partialCount == 0 ? null : new AggregatedVector(partialCount, partialVector);
    }

    /**
     * Inserts a new vector into the HNSW graph across multiple layers, starting from a given entry point.
     * <p>
     * This method implements the second phase of the HNSW insertion algorithm. It begins at a starting layer, which is
     * the minimum of the graph's maximum layer ({@code lMax}) and the new node's randomly assigned
     * {@code layer}. It then iterates downwards to layer 0. In each layer, it invokes
     * {@link #insertIntoLayer(StorageAdapter, Transaction, StorageTransform, Quantizer, List, int, Tuple, Transformed)}
     * to perform the search and connect the new node. The set of nearest neighbors found at layer {@code L} serves as
     * the entry points for the search at layer {@code L-1}.
     * </p>
     *
     * @param transaction the transaction to use for database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param quantizer the quantizer to be used for this insert
     * @param newPrimaryKey the primary key of the new node being inserted
     * @param newVector the vector data of the new node
     * @param nodeReference the initial entry point for the search, typically the nearest neighbor found in the highest
     * layer
     * @param lMax the maximum layer number in the HNSW graph
     * @param insertionLayer the randomly determined layer for the new node. The node will be inserted into all layers
     * from this layer down to 0.
     *
     * @return a {@link CompletableFuture} that completes when the new node has been successfully inserted into all
     * its designated layers
     */
    @Nonnull
    private CompletableFuture<Void> insertIntoLayers(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageTransform storageTransform,
                                                     @Nonnull final Quantizer quantizer,
                                                     @Nonnull final Tuple newPrimaryKey,
                                                     @Nonnull final Transformed<RealVector> newVector,
                                                     @Nonnull final NodeReferenceWithDistance nodeReference,
                                                     final int lMax,
                                                     final int insertionLayer) {
        if (logger.isTraceEnabled()) {
            logger.trace("nearest entry point at lMax={} is at key={}", lMax, nodeReference.getPrimaryKey());
        }
        return MoreAsyncUtil.<List<NodeReferenceWithDistance>>forLoop(Math.min(lMax, insertionLayer), ImmutableList.of(nodeReference),
                layer -> layer >= 0,
                layer -> layer - 1,
                (layer, previousNodeReferences) -> {
                    final StorageAdapter<? extends NodeReference> storageAdapter = storageAdapterForLayer(layer);
                    return insertIntoLayer(storageAdapter, transaction, storageTransform, quantizer,
                            previousNodeReferences, layer, newPrimaryKey, newVector)
                            .thenApply(NodeReferenceAndNode::references);
                }, executor).thenCompose(ignored -> AsyncUtil.DONE);
    }

    /**
     * Inserts a new node into a specified layer of the HNSW graph.
     * <p>
     * This method orchestrates the complete insertion process for a single layer. It begins by performing a search
     * within the given layer, starting from the provided {@code nearestNeighbors} as entry points, to find a set of
     * candidate neighbors for the new node. From this candidate set, it selects the best connections based on the
     * graph's parameters (M).
     * </p>
     * <p>
     * After selecting the neighbors, it creates the new node and links it to them. It then reciprocally updates
     * the selected neighbors to link back to the new node. If adding this new link causes a neighbor to exceed its
     * maximum allowed connections, its connections are pruned. All changes, including the new node and the updated
     * neighbors, are persisted to storage within the given transaction.
     * </p>
     * <p>
     * The operation is asynchronous and returns a {@link CompletableFuture}. The future completes with the list of
     * nodes found during the initial search phase, which are then used as the entry points for insertion into the
     * next lower layer.
     * </p>
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter for reading from and writing to the graph
     * @param transaction the transaction context for the database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param quantizer the quantizer for this insert
     * @param nearestNeighbors the list of nearest neighbors from the layer above, used as entry points for the search
     * in this layer
     * @param layer the layer number to insert the new node into
     * @param newPrimaryKey the primary key of the new node to be inserted
     * @param newVector the vector associated with the new node
     *
     * @return a {@code CompletableFuture} that completes with a list of the nearest neighbors found during the
     *         initial search phase. This list serves as the entry point for insertion into the next lower layer
     *         (i.e., {@code layer - 1}).
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
            insertIntoLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final List<NodeReferenceWithDistance> nearestNeighbors,
                            final int layer,
                            @Nonnull final Tuple newPrimaryKey,
                            @Nonnull final Transformed<RealVector> newVector) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin insert key={} at layer={}", newPrimaryKey, layer);
        }
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        final Estimator estimator = quantizer.estimator();

        return beamSearchLayer(storageAdapter, transaction, storageTransform,
                nearestNeighbors, layer, config.getEfConstruction(),
                distanceToTargetVector(estimator, newVector), nodeCache)
                .thenCompose(searchResult ->
                        extendCandidatesIfNecessary(storageAdapter, transaction, storageTransform, estimator,
                                searchResult, layer, getConfig().isExtendCandidates(), nodeCache, newVector)
                                .thenCompose(extendedCandidates ->
                                        selectCandidates(storageAdapter, transaction, storageTransform, estimator,
                                                extendedCandidates, layer, getConfig().getM(), nodeCache))
                                .thenCompose(selectedNeighbors -> {
                                    final NodeFactory<N> nodeFactory = storageAdapter.getNodeFactory();

                                    final AbstractNode<N> newNode =
                                            nodeFactory.create(newPrimaryKey, newVector,
                                                    NodeReferenceAndNode.references(selectedNeighbors));

                                    final NeighborsChangeSet<N> newNodeChangeSet =
                                            new InsertNeighborsChangeSet<>(
                                                    new BaseNeighborsChangeSet<>(ImmutableList.of()),
                                                    newNode.getNeighbors());

                                    storageAdapter.writeNode(transaction, quantizer, layer, newNode,
                                            newNodeChangeSet);

                                    // create change sets for each selected neighbor and insert new node into them
                                    final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap =
                                            Maps.newLinkedHashMap();
                                    for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedNeighbor : selectedNeighbors) {
                                        final NeighborsChangeSet<N> baseSet =
                                                new BaseNeighborsChangeSet<>(
                                                        selectedNeighbor.getNode().getNeighbors());
                                        final NeighborsChangeSet<N> insertSet =
                                                new InsertNeighborsChangeSet<>(baseSet,
                                                        ImmutableList.of(newNode.getSelfReference(newVector)));
                                        neighborChangeSetMap.put(selectedNeighbor.getNode().getPrimaryKey(),
                                                insertSet);
                                    }

                                    final int currentMMax =
                                            layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();

                                    return forEach(selectedNeighbors,
                                            selectedNeighbor -> {
                                                final NodeReferenceWithDistance selectedNeighborReference =
                                                        selectedNeighbor.getNodeReference();
                                                final AbstractNode<N> selectedNeighborNode = selectedNeighbor.getNode();
                                                final NeighborsChangeSet<N> changeSet =
                                                        Objects.requireNonNull(neighborChangeSetMap.get(selectedNeighborNode.getPrimaryKey()));
                                                return pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                        storageTransform, estimator, layer, selectedNeighborReference,
                                                        currentMMax, changeSet, nodeCache)
                                                        .thenApply(nodeReferencesAndNodes -> {
                                                            if (nodeReferencesAndNodes == null) {
                                                                return changeSet;
                                                            }
                                                            return resolveChangeSetFromNewNeighbors(changeSet, nodeReferencesAndNodes);
                                                        });
                                            }, getConfig().getMaxNumConcurrentNeighborhoodFetches(), getExecutor())
                                            .thenApply(changeSets -> {
                                                for (int i = 0; i < selectedNeighbors.size(); i++) {
                                                    final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedNeighbor =
                                                            selectedNeighbors.get(i);
                                                    final NeighborsChangeSet<N> changeSet = changeSets.get(i);
                                                    if (changeSet.hasChanges()) {
                                                        storageAdapter.writeNode(transaction, quantizer,
                                                                layer, selectedNeighbor.getNode(), changeSet);
                                                    }
                                                }
                                                return ImmutableList.copyOf(searchResult);
                                            });
                                }))
                .thenApply(nodeReferencesWithDistances -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end insert key={} at layer={}", newPrimaryKey, layer);
                    }
                    return nodeReferencesWithDistances;
                });
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
    private <N extends NodeReference> NeighborsChangeSet<N>
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
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
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
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithDistance, N>>>
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
                ? new PriorityQueue<>(config.getM(), NodeReferenceWithDistance.comparator()) : null;

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
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>>
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
    private <T extends NodeReference, N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
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
    private void writeLonelyNodes(@Nonnull final Quantizer quantizer,
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
    private <N extends NodeReference> void writeLonelyNodeOnLayer(@Nonnull final Quantizer quantizer,
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
     * Deletes a record using its associated primary key from the HNSW graph.
     * <p>
     * This method implements a multi-layer deletion algorithm that maintains the structural integrity of the HNSW
     * graph. The deletion process consists of several key phases:
     * <ul>
     *     <li><b>Layer Determination:</b> First determines the top layer for the node using the same deterministic
     *         algorithm used during insertion, ensuring consistent layer assignment across operations.
     *     </li>
     *     <li><b>Existence Verification:</b> Checks whether the node actually exists in the graph before attempting
     *          deletion. If the node doesn't exist, the operation completes immediately without error.
     *     </li>
     *     <li><b>Multi-Layer Deletion:</b> Removes the node from all layers spanning from layer 0 (base layer
     *         containing all nodes) up to and including the node's top layer. The deletion is performed in parallel
     *         across all layers for optimal performance.
     *     </li>
     *     <li><b>Graph Repair:</b> For each layer where the node is deleted, the algorithm repairs the local graph
     *         structure by identifying the deleted node's neighbors and reconnecting them appropriately. This process:
     *         <ul>
     *             <li>Finds candidate replacement connections among the neighbors of neighbors</li>
     *             <li>Selects optimal new connections using the HNSW distance heuristics</li>
     *             <li>Updates neighbor lists to maintain graph connectivity and search performance</li>
     *             <li>Applies connection limits (M, MMax) and prunes excess connections if necessary</li>
     *         </ul>
     *     </li>
     *     <li><b>Entry Point Management:</b> If the deleted node was serving as the graph's entry point (the starting
     *         node for search operations), the method automatically selects a new entry point from the remaining nodes
     *         at the highest available layer. If no nodes remain after deletion, the access information is cleared,
     *         effectively resetting the graph to an empty state.
     *     </li>
     * </ul>
     * All operations are performed transactionally and asynchronously, ensuring consistency and enabling
     * non-blocking execution in concurrent environments.
     *
     * @param transaction the {@link Transaction} context for all database operations, ensuring atomicity
     *        and consistency of the deletion and repair operations
     * @param primaryKey the unique {@link Tuple} primary key identifying the node to be deleted from the graph
     *
     * @return a {@link CompletableFuture} that completes when the deletion operation is fully finished,
     *         including all graph repairs and entry point updates. The future completes with {@code null}
     *         on successful deletion.
     */
    @Nonnull
    public CompletableFuture<Void> delete(@Nonnull final Transaction transaction, @Nonnull final Tuple primaryKey) {
        final SplittableRandom random = random(primaryKey);
        final int topLayer = topLayer(primaryKey);
        if (logger.isTraceEnabled()) {
            logger.trace("node with key={} to be deleted form layer={}", primaryKey, topLayer);
        }

        return StorageAdapter.fetchAccessInfo(getConfig(), transaction, getSubspace(), getOnReadListener())
                .thenCombine(exists(transaction, primaryKey),
                        (accessInfo, nodeExists) -> {
                            if (!nodeExists) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("record does not exists in HNSW with key={} on layer={}",
                                            primaryKey, topLayer);
                                }
                            }
                            return new AccessInfoAndNodeExistence(accessInfo, nodeExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    if (!accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    final EntryNodeReference entryNodeReference =
                            accessInfo == null ? null : accessInfo.getEntryNodeReference();
                    final StorageTransform storageTransform = storageTransform(accessInfo);
                    final Quantizer quantizer = quantizer(accessInfo);

                    return deleteFromLayers(transaction, storageTransform, quantizer, random, primaryKey, topLayer)
                            .thenCompose(potentialEntryNodeReferences -> {
                                if (entryNodeReference != null && primaryKey.equals(entryNodeReference.getPrimaryKey())) {
                                    // find (and store) a new entry reference
                                    for (int i = potentialEntryNodeReferences.size() - 1; i >= 0; i --) {
                                        final EntryNodeReference potentialEntyNodeReference =
                                                potentialEntryNodeReferences.get(i);
                                        if (potentialEntyNodeReference != null) {
                                            StorageAdapter.writeAccessInfo(transaction, getSubspace(),
                                                    accessInfo.withNewEntryNodeReference(potentialEntyNodeReference),
                                                    getOnWriteListener());
                                            // early out
                                            return AsyncUtil.DONE;
                                        }
                                    }

                                    // there is no data in the structure, delete access info to start new
                                    StorageAdapter.deleteAccessInfo(transaction, getSubspace(), getOnWriteListener());
                                }
                                return AsyncUtil.DONE;
                            });
                });
    }

    /**
     * Deletes a node from the HNSW graph across multiple layers, using a primary key and a given top layer.
     *
     * @param transaction the transaction to use for database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     * storage space that is currently being used
     * @param quantizer the quantizer to be used for this insert
     * @param primaryKey the primary key of the new node being inserted
     * @param topLayer the top layer for the node.
     *
     * @return a {@link CompletableFuture} that completes when the new node has been successfully inserted into all
     *         its designated layers and contains an existing neighboring entry node reference on that layer.
     */
    @Nonnull
    private CompletableFuture<List<EntryNodeReference>> deleteFromLayers(@Nonnull final Transaction transaction,
                                                                         @Nonnull final StorageTransform storageTransform,
                                                                         @Nonnull final Quantizer quantizer,
                                                                         @Nonnull final SplittableRandom random,
                                                                         @Nonnull final Tuple primaryKey,
                                                                         final int topLayer) {
        // delete the node from all layers in parallel (inside layer in [0, topLayer])
        return forEach(() -> IntStream.rangeClosed(0, topLayer).iterator(),
                layer ->
                        deleteFromLayer(storageAdapterForLayer(layer), transaction, storageTransform, quantizer,
                                random.split(), layer, primaryKey),
                getConfig().getMaxNumConcurrentDeleteFromLayer(),
                executor);
    }

    /**
     * Deletes a node from a specified layer of the HNSW graph. This method orchestrates the complete deletion process
     * for a single layer.
     *
     * @param <N> the type of the node reference, extending {@link NodeReference}
     * @param storageAdapter the storage adapter for reading from and writing to the graph
     * @param transaction the transaction context for the database operations
     * @param storageTransform an affine transformation operator that is used to transform the fetched vector into the
     *        storage space that is currently being used
     * @param quantizer the quantizer for this insert
     * @param layer the layer number to insert the new node into
     * @param toBeDeletedPrimaryKey the primary key of the new node to be inserted
     *
     * @return a {@code CompletableFuture} that completes with a {@code null}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<EntryNodeReference>
            deleteFromLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                            @Nonnull final Transaction transaction,
                            @Nonnull final StorageTransform storageTransform,
                            @Nonnull final Quantizer quantizer,
                            @Nonnull final SplittableRandom random,
                            final int layer,
                            @Nonnull final Tuple toBeDeletedPrimaryKey) {
        if (logger.isTraceEnabled()) {
            logger.trace("begin delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
        }
        final Estimator estimator = quantizer.estimator();
        final Map<Tuple, AbstractNode<N>> nodeCache = Maps.newConcurrentMap();
        final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> candidateChangeSetMap =
                Maps.newConcurrentMap();

        return storageAdapter.fetchNode(transaction, storageTransform, layer, toBeDeletedPrimaryKey)
                .thenCompose(toBeDeletedNode -> {
                    final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode =
                            new NodeReferenceAndNode<>(new NodeReference(toBeDeletedPrimaryKey), toBeDeletedNode);

                    return findDeletionRepairCandidates(storageAdapter, transaction, storageTransform, random, layer,
                            toBeDeletedNodeReferenceAndNode, nodeCache)
                            .thenCompose(candidates -> {
                                initializeCandidateChangeSetMap(toBeDeletedPrimaryKey, toBeDeletedNode, candidates,
                                        candidateChangeSetMap);
                                // resolve the actually existing direct neighbors
                                final ImmutableList<N> primaryNeighbors =
                                        primaryNeighbors(toBeDeletedNode, candidateChangeSetMap);

                                //
                                // Repair each primary neighbor in parallel, there should not be much actual I/O,
                                // except in edge cases, but we should still parallelize it.
                                //
                                return forEach(primaryNeighbors,
                                        neighborReference ->
                                                repairNeighbor(storageAdapter, transaction,
                                                        storageTransform, estimator, layer, neighborReference,
                                                        candidates, candidateChangeSetMap, nodeCache),
                                        getConfig().getMaxNumConcurrentNeighborhoodFetches(), executor)
                                        .thenApply(ignored -> {
                                            final ImmutableMap.Builder<Tuple, NodeReferenceWithVector> candidateReferencesMapBuilder =
                                                    ImmutableMap.builder();
                                            for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                                                final var candidatePrimaryKey = candidate.getNodeReference().getPrimaryKey();
                                                if (candidateChangeSetMap.containsKey(candidatePrimaryKey)) {
                                                    candidateReferencesMapBuilder.put(candidatePrimaryKey, candidate.getNodeReference());
                                                }
                                            }
                                            return candidateReferencesMapBuilder.build();
                                        });
                            })
                            .thenCompose(candidateReferencesMap -> {
                                final int currentMMax =
                                        layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();

                                //
                                // If we previously went beyond the mMax/mMax0, we need to prune the neighbors.
                                // Pruning is independent among different nodes -- we can therefore prune in
                                // parallel.
                                //
                                return forEach(candidateChangeSetMap.entrySet(), // for each modified neighbor set
                                        changeSetEntry -> {
                                            final NodeReferenceWithVector candidateReference =
                                                    Objects.requireNonNull(candidateReferencesMap.get(changeSetEntry.getKey()));
                                            final NeighborsChangeSet<N> candidateChangeSet = changeSetEntry.getValue();
                                            return pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                    storageTransform, estimator, layer, candidateReference,
                                                    currentMMax, candidateChangeSet, nodeCache)
                                                    .thenApply(nodeReferencesAndNodes -> {
                                                        if (nodeReferencesAndNodes == null) {
                                                            return candidateChangeSet;
                                                        }

                                                        final var prunedCandidateChangeSet =
                                                                resolveChangeSetFromNewNeighbors(candidateChangeSet,
                                                                        nodeReferencesAndNodes);
                                                        candidateChangeSetMap.put(changeSetEntry.getKey(), prunedCandidateChangeSet);
                                                        return prunedCandidateChangeSet;
                                                    });
                                        },
                                        getConfig().getMaxNumConcurrentNeighborhoodFetches(), executor)
                                        .thenApply(ignored -> candidateReferencesMap);
                            })
                            .thenApply(candidateReferencesMap -> {
                                //
                                // Finally delete the node we set out to delete and persist the change sets for all
                                // repaired nodes.
                                //
                                storageAdapter.deleteNode(transaction, layer, toBeDeletedPrimaryKey);

                                for (final Map.Entry<Tuple, NeighborsChangeSet<N>> changeSetEntry : candidateChangeSetMap.entrySet()) {
                                    final NeighborsChangeSet<N> changeSet = changeSetEntry.getValue();
                                    if (changeSet.hasChanges()) {
                                        final AbstractNode<N> candidateNode =
                                                nodeFromCache(changeSetEntry.getKey(), nodeCache);
                                        storageAdapter.writeNode(transaction, quantizer,
                                                layer, candidateNode, changeSet);
                                    }
                                }

                                //
                                // Return the first item in the candidates reference map as a potential new
                                // entry node reference in order to avoid a costly search for a new global entry point.
                                // This reference is guaranteed to exist.
                                //
                                final Tuple firstPrimaryKey =
                                        Iterables.getFirst(candidateReferencesMap.keySet(), null);
                                return firstPrimaryKey == null
                                       ? null
                                       : new EntryNodeReference(firstPrimaryKey,
                                        Objects.requireNonNull(candidateReferencesMap.get(firstPrimaryKey)).getVector(),
                                        layer);
                            });
                }).thenApply(result -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("end delete key={} at layer={}", toBeDeletedPrimaryKey, layer);
                    }
                    return result;
                });
    }

    private <N extends NodeReference> void initializeCandidateChangeSetMap(@Nonnull final Tuple toBeDeletedPrimaryKey,
                                                                           @Nonnull final AbstractNode<N> toBeDeletedNode,
                                                                           @Nonnull final List<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                                                                           @Nonnull final Map<Tuple, NeighborsChangeSet<N>> candidateChangeSetMap) {
        for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
            final AbstractNode<N> candidateNode = candidate.getNode();
            boolean foundToBeDeleted = false;
            for (final N neighborOfCandidate : candidateNode.getNeighbors()) {
                if (neighborOfCandidate.getPrimaryKey().equals(toBeDeletedPrimaryKey)) {
                    //
                    // Make sure a neighbor pointing to the node being deleted is deleted as well.
                    //
                    candidateChangeSetMap.put(candidateNode.getPrimaryKey(),
                            new DeleteNeighborsChangeSet<>(
                                    new BaseNeighborsChangeSet<>(candidateNode.getNeighbors()),
                                    ImmutableList.of(toBeDeletedPrimaryKey)));
                    foundToBeDeleted = true;
                    break;
                }
            }
            if (!foundToBeDeleted) {
                // if there is no reference back to the node being deleted, just create the base set
                candidateChangeSetMap.put(candidateNode.getPrimaryKey(),
                        new BaseNeighborsChangeSet<>(candidateNode.getNeighbors()));
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("number of neighbors to repair={}", toBeDeletedNode.getNeighbors().size());
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
    private <N extends NodeReference> ImmutableList<N>
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
     * Find candidates starting from the node to be deleted. To this end we find all the existing first degree (primary)
     * and second-degree (secondary) neighbors. As that set is too big to consider for the repair we rely on sampling
     * to eventually compile a list of roughly {@code efRepair} number of candidates.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param random a {@link SplittableRandom} used for sampling the candidate set
     * @param layer the layer
     * @param toBeDeletedNodeReferenceAndNode the node that is about to be deleted
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<NodeReferenceWithVector, N>>>
             findDeletionRepairCandidates(final @Nonnull StorageAdapter<N> storageAdapter,
                                          final @Nonnull Transaction transaction,
                                          final @Nonnull StorageTransform storageTransform,
                                          final @Nonnull SplittableRandom random,
                                          final int layer,
                                          final NodeReferenceAndNode<NodeReference, N> toBeDeletedNodeReferenceAndNode,
                                          final Map<Tuple, AbstractNode<N>> nodeCache) {
        return neighbors(storageAdapter, transaction, storageTransform, random,
                ImmutableList.of(toBeDeletedNodeReferenceAndNode),
                ((r, initialNodeKeys, size, nodeReference) ->
                         shouldUsePrimaryCandidateForRepair(nodeReference,
                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())), layer, nodeCache)
                .thenCompose(candidates ->
                        neighbors(storageAdapter, transaction, storageTransform, random,
                                candidates,
                                ((r, initialNodeKeys, size, nodeReference) ->
                                         shouldUseSecondaryCandidateForRepair(r, initialNodeKeys, size, nodeReference,
                                                 toBeDeletedNodeReferenceAndNode.getNodeReference().getPrimaryKey())),
                                layer, nodeCache))
                .thenApply(candidates -> {
                    if (logger.isTraceEnabled()) {
                        final ImmutableList.Builder<String> candidateStringsBuilder = ImmutableList.builder();
                        for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                            candidateStringsBuilder.add(candidate.getNode().getPrimaryKey().toString());
                        }
                        logger.trace("found at layer={} num={} candidates={}", layer, candidates.size(),
                                String.join(",", candidateStringsBuilder.build()));
                    }
                    return candidates;
                });
    }

    /**
     * Repair a neighbor node of the node that is being deleted using a set of candidates. All candidates contain only
     * the vector (in addition to identifying information like the primary key). The logic in
     * computes distances between the neighbor vector and each candidate vector which is required by
     * {@link #repairInsForNeighborNode}.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param estimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> @Nonnull CompletableFuture<Void>
            repairNeighbor(@Nonnull final StorageAdapter<N> storageAdapter,
                           @Nonnull final Transaction transaction,
                           @Nonnull final StorageTransform storageTransform,
                           @Nonnull final Estimator estimator,
                           final int layer,
                           @Nonnull final N neighborReference,
                           @Nonnull final Collection<NodeReferenceAndNode<NodeReferenceWithVector, N>> candidates,
                           @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                           @Nonnull final Map<Tuple, AbstractNode<N>> nodeCache) {

        return fetchNodeIfNotCached(storageAdapter, transaction,
                storageTransform, layer, neighborReference, nodeCache)
                .thenCompose(neighborNode -> {
                    final ImmutableList.Builder<NodeReferenceWithDistance> candidatesReferencesBuilder =
                            ImmutableList.builder();
                    final Transformed<RealVector> neighborVector =
                            storageAdapter.getVector(neighborReference, neighborNode);
                    // transform the NodeReferencesWithVectors into NodeReferencesWithDistance
                    for (final NodeReferenceAndNode<NodeReferenceWithVector, N> candidate : candidates) {
                        // do not add the candidate if that candidate is in fact the neighbor itself
                        if (!candidate.getNodeReference().getPrimaryKey().equals(neighborReference.getPrimaryKey())) {
                            final Transformed<RealVector> candidateVector =
                                    candidate.getNodeReference().getVector();
                            final double distance =
                                    estimator.distance(candidateVector, neighborVector);
                            candidatesReferencesBuilder.add(new NodeReferenceWithDistance(
                                    candidate.getNode().getPrimaryKey(), candidateVector, distance));
                        }
                    }
                    return repairInsForNeighborNode(storageAdapter, transaction, storageTransform, estimator,
                            layer, neighborReference, candidatesReferencesBuilder.build(),
                            neighborChangeSetMap, nodeCache);
                });
    }

    /**
     * Repairs the ins of a neighbor node of the node that is being deleted using a set of candidates. Each such
     * neighbor is part of a set that is referred to as {@code p_out} in literature. In this method we only repair
     * incoming references to this node. As this method is called once per direct neighbor and all direct neighbors are
     * in the candidate set, outgoing references from this node to other nodes (in {@code p_out}) are repaired when this
     * method is called for the respective neighbors.
     *
     * @param <N> type parameter extending {@link NodeReference}
     * @param storageAdapter the storage adapter for the layer
     * @param transaction the transaction
     * @param storageTransform the storage transform
     * @param estimator an estimator for distances
     * @param layer the layer
     * @param neighborReference the reference for which this method repairs incoming references
     * @param candidates the set of candidates
     * @param neighborChangeSetMap the change set map which records all changes to all nodes that are being repaired
     * @param nodeCache the node cache to avoid repeated fetches
     * @return a future that if successful completes with {@code null}
     */
    private <N extends NodeReference> CompletableFuture<Void>
            repairInsForNeighborNode(@Nonnull final StorageAdapter<N> storageAdapter,
                                     @Nonnull final Transaction transaction,
                                     @Nonnull final StorageTransform storageTransform,
                                     @Nonnull final Estimator estimator,
                                     final int layer,
                                     @Nonnull final N neighborReference,
                                     @Nonnull final Iterable<NodeReferenceWithDistance> candidates,
                                     @Nonnull final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap,
                                     final Map<Tuple, AbstractNode<N>> nodeCache) {
        return selectCandidates(storageAdapter, transaction, storageTransform, estimator, candidates,
                layer, getConfig().getM(), nodeCache)
                .thenApply(selectedCandidates -> {
                    if (logger.isTraceEnabled()) {
                        final ImmutableList.Builder<String> candidateStringsBuilder = ImmutableList.builder();
                        for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> candidate : selectedCandidates) {
                            candidateStringsBuilder.add(candidate.getNode().getPrimaryKey().toString());
                        }
                        logger.trace("selected for neighbor={}, candidates={}",
                                neighborReference.getPrimaryKey(),
                                String.join(",", candidateStringsBuilder.build()));
                    }
                    return selectedCandidates;
                })
                .thenCompose(selectedCandidates -> {
                    // create change sets for each selected neighbor and insert new node into them
                    for (final NodeReferenceAndNode<NodeReferenceWithDistance, N> selectedCandidate : selectedCandidates) {
                        neighborChangeSetMap.compute(selectedCandidate.getNode().getPrimaryKey(),
                                (ignored, oldChangeSet) -> {
                                    Objects.requireNonNull(oldChangeSet);
                                    // insert a reference to the neighbor
                                    return new InsertNeighborsChangeSet<>(oldChangeSet, ImmutableList.of(neighborReference));
                                });
                    }
                    return AsyncUtil.DONE;
                });
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
    private StorageAdapter<? extends NodeReference> storageAdapterForLayer(final int layer) {
        return storageAdapterForLayer(getConfig(), getSubspace(), getOnWriteListener(), getOnReadListener(), layer);
    }

    @Nonnull
    private SplittableRandom random(@Nonnull final Tuple primaryKey) {
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
    private int topLayer(@Nonnull final Tuple primaryKey) {
        double lambda = 1.0 / Math.log(getConfig().getM());
        double u = 1.0 - splitMixDouble(primaryKey.hashCode());  // Avoid log(0)
        return (int) Math.floor(-Math.log(u) * lambda);
    }

    /**
     * Predicate to determine if a potential candidate is to be used as a candidate for repairing the HNSW.
     * The predicate rejects the candidate reference if it is referring to the node that is being deleted, otherwise the
     * predicate accepts the candidate reference.
     * @param candidateReference a potential candidate that is either accepted or rejected
     * @param toBeDeletedPrimaryKey the {@link Tuple} representing the node that is being deleted
     * @return {@code true} iff {@code candidateReference} is accepted as an actual candidate for repair.
     */
    private boolean shouldUsePrimaryCandidateForRepair(@Nonnull final NodeReference candidateReference,
                                                       @Nonnull final Tuple toBeDeletedPrimaryKey) {
        final Tuple candidatePrimaryKey = candidateReference.getPrimaryKey();

        //
        // If the node reference is the record we are trying to delete we must reject it here as it is not a suitable
        // candidate.
        //
        return !candidatePrimaryKey.equals(toBeDeletedPrimaryKey);
    }

    /**
     * Predicate to determine if a potential candidate is to be used ad a candidate for repairing the HNSW.
     * <ol>
     *    <li> The predicate rejects the candidate reference if it is referring to the node that is being deleted. </li>
     *    <li> The predicate always accepts a direct neighbor of the node that is about to be deleted.</li>
     *    <li> Sample the remaining potential candidates such that eventually the repair algorithm can use
     *         roughly {@code efRepair} actual candidates.</li>
     * </ol>
     * @param random the PRNG to be used (splittable)
     * @param initialNodeKeys a set of {@link Tuple}s that hold the primary neighbors of the node being deleted.
     * @param numberOfCandidates the number of potential candidates the repair algorithm compiled
     * @param candidateReference a potential candidate that is either accepted or rejected
     * @param toBeDeletedPrimaryKey the {@link Tuple} representing the node that is being deleted
     * @return {@code true} iff {@code candidateReference} is accepted as an actual candidate for repair.
     */
    private boolean shouldUseSecondaryCandidateForRepair(@Nullable final SplittableRandom random,
                                                         @Nonnull final Set<Tuple> initialNodeKeys,
                                                         final int numberOfCandidates,
                                                         @Nonnull final NodeReference candidateReference,
                                                         @Nonnull final Tuple toBeDeletedPrimaryKey) {
        final Tuple candidatePrimaryKey = candidateReference.getPrimaryKey();

        //
        // If the node reference is the record we are trying to delete we must reject it here as it is not a suitable
        // candidate.
        //
        if (candidatePrimaryKey.equals(toBeDeletedPrimaryKey)) {
            return false;
        }

        //
        // If the node reference is among the initial nodes we must accept it as they are very likely the best
        // candidates.
        //
        if (initialNodeKeys.contains(candidatePrimaryKey)) {
            return true;
        }

        //
        // Sample all the rest -- For the sampling rate, subtract the size of initialNodeKeys so that we get roughly
        // efRepair nodes.
        //
        final double sampleRate = (double)(getConfig().getEfRepair() - initialNodeKeys.size()) / numberOfCandidates;
        if (sampleRate >= 1) {
            return true;
        }
        return Objects.requireNonNull(random).nextDouble() < sampleRate;
    }

    private boolean shouldSampleVector(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getSampleVectorStatsProbability();
    }

    private boolean shouldMaintainStats(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getMaintainStatsProbability();
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
    public AsyncIterator<ResultEntry>
            orderByDistance(@Nonnull final ReadTransaction readTransaction,
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
                                ? clamp((int)Math.floor(Math.sqrt(efRingSearch)), 8, 32)
                                : efRingSearch,
                        (estimator, targetVector) ->
                                distanceToSphericalSurface(estimator, targetVector, minimumRadius));
        final CompactStorageAdapter storageAdapter = storageAdapterForLayer(0).asCompactStorageAdapter();
        return new OutwardTraversalIterator(storageAdapter, readTransaction, zoomInResultFuture, centerVector,
                minimumRadius, minimumPrimaryKey, efOutwardSearch);
    }

    /**
     * Async iterator to iterate outwards starting from a given {@code (minimumRadius, minimumPrimaryKey)} (exclusive)
     * where {@code minimumRadius} is measured as the distance of a vector to the given {@code centerVector}.
     */
    private class OutwardTraversalIterator implements AsyncIterator<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> {
        @Nonnull
        private final StorageAdapter<NodeReference> storageAdapter;
        @Nonnull
        private final ReadTransaction readTransaction;
        @Nonnull
        private final CompletableFuture<SearchResult> zoomInResultFuture;
        @Nonnull
        private final RealVector centerVector;
        private final double minimumRadius;
        @Nullable
        private final Tuple minimumPrimaryKey;
        private final int efOutwardSearch;

        /**
         * State of the iteration. All structures within the state are mutable (and in fact updates frequently)
         * as part of {@link  #computeNextRecord()}.
         */
        @Nullable
        private OutwardTraversalState traversalState;

        @Nullable
        private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> nextFuture;

        @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
        public OutwardTraversalIterator(@Nonnull final StorageAdapter<NodeReference> storageAdapter,
                                        @Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final CompletableFuture<SearchResult> zoomInResultFuture,
                                        @Nonnull final RealVector centerVector,
                                        final double minimumRadius,
                                        @Nullable final Tuple minimumPrimaryKey,
                                        final int efOutwardSearch) {
            this.storageAdapter = storageAdapter;
            this.readTransaction = readTransaction;
            this.zoomInResultFuture = zoomInResultFuture;
            this.centerVector = centerVector;
            this.minimumRadius = minimumRadius;
            this.minimumPrimaryKey = minimumPrimaryKey;
            this.efOutwardSearch = efOutwardSearch;

            this.traversalState = null;
            this.nextFuture = null;
        }

        @Nonnull
        public OutwardTraversalState getTraversalState() {
            return Objects.requireNonNull(traversalState);
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (nextFuture == null) {
                if (traversalState == null) {
                    nextFuture =
                            zoomInResultFuture
                                    .thenAccept(zoomInResult ->
                                            this.traversalState = initialTravelState(zoomInResult))
                                    .thenCompose(ignored -> computeNextRecord());
                } else {
                    nextFuture = computeNextRecord();
                }
            }
            return nextFuture.thenApply(Objects::nonNull);
        }

        @Nonnull
        private OutwardTraversalState initialTravelState(@Nonnull final SearchResult zoomInResult) {
            final StorageTransform storageTransform = zoomInResult.getStorageTransform();
            final Transformed<RealVector> transformedCenterVector = storageTransform.transform(centerVector);
            final PriorityQueue<NodeReferenceWithDistance> candidates =
                    // This initial capacity is somewhat arbitrary as m is not necessarily
                    // a limit, but it gives us a number that is better than the default.
                    new PriorityQueue<>(getConfig().getM(), NodeReferenceWithDistance.comparator());
            final SpatialRestrictions visited = new SpatialRestrictions(1, minimumRadius, minimumPrimaryKey);

            final PriorityQueue<NodeReferenceWithDistance> out =
                    new PriorityQueue<>(efOutwardSearch + 1, // prevent reallocation further down
                            NodeReferenceWithDistance.comparator());

            final Estimator estimator = quantizer(zoomInResult.getAccessInfo()).estimator();

            // rekey the distances to distance around the center
            for (final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> referenceAndNode : zoomInResult.getNearestReferenceAndNodes()) {
                final Transformed<RealVector> vector = referenceAndNode.getNodeReference().getVector();
                final double distance = estimator.distance(transformedCenterVector, vector);
                final Tuple primaryKey = referenceAndNode.getNode().getPrimaryKey();

                final NodeReferenceWithDistance nodeReferenceWithDistance =
                        new NodeReferenceWithDistance(primaryKey, vector, distance);
                visited.add(nodeReferenceWithDistance);
                // do not add to out if the distance is less than the minimum
                //if (isGreaterThanMinimum(nodeReferenceWithDistance)) {
                candidates.add(nodeReferenceWithDistance);
                //}
            }

            return new OutwardTraversalState(storageTransform, estimator,
                    transformedCenterVector, candidates, visited, out, zoomInResult.getNodeCache());
        }

        @Nonnull
        private CompletableFuture<NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference>> computeNextRecord() {
            final OutwardTraversalState localTraversalState = getTraversalState();
            final StorageTransform storageTransform = localTraversalState.getStorageTransform();
            final Estimator estimator = localTraversalState.getEstimator();
            final Transformed<RealVector> transformedCenterVector = localTraversalState.getTransformedCenterVector();
            final Queue<NodeReferenceWithDistance> candidates = localTraversalState.getCandidates();
            final SpatialRestrictions spatialRestrictions = localTraversalState.getSpatialRestrictions();
            final Queue<NodeReferenceWithDistance> out = localTraversalState.getOut();
            final Map<Tuple, AbstractNode<NodeReference>> nodeCache = localTraversalState.getNodeCache();

            return AsyncUtil.whileTrue(() -> {
                if (candidates.isEmpty() || out.size() >= efOutwardSearch) {
                    // break the refill loop
                    return AsyncUtil.READY_FALSE;
                }

                final NodeReferenceWithDistance candidate = candidates.poll();
                if (spatialRestrictions.isGreaterThanMinimum(candidate)) {
                    out.add(candidate);
                }

                return fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, 0, candidate, nodeCache)
                        .thenApply(AbstractNode::getNeighbors)
                        .thenCompose(neighborReferences -> fetchNeighborhoodReferences(storageAdapter, readTransaction,
                                storageTransform, 0, neighborReferences, nodeCache))
                        .thenApply(neighborReferences -> {
                            for (final NodeReferenceWithVector current : neighborReferences) {
                                final Tuple primaryKey = current.getPrimaryKey();
                                final double distance =
                                        estimator.distance(transformedCenterVector, current.getVector());
                                final NodeReferenceWithDistance nodeReferenceWithDistance =
                                        new NodeReferenceWithDistance(primaryKey, current.getVector(), distance);

                                if (spatialRestrictions.shouldBeAdded(nodeReferenceWithDistance)) {
                                    spatialRestrictions.add(nodeReferenceWithDistance);
                                    candidates.add(nodeReferenceWithDistance);
                                }
                            }
                            return true;
                        });
            }).thenCompose(ignored -> {
                if (out.isEmpty()) {
                    Verify.verify(candidates.isEmpty());
                    return CompletableFuture.completedFuture(null);
                }
                final NodeReferenceWithDistance nodeReference = out.poll();
                return fetchNodeIfNotCached(storageAdapter, readTransaction, storageTransform, 0, nodeReference, nodeCache)
                        .thenApply(node -> new NodeReferenceAndNode<>(nodeReference, node));
            }).thenApply(nextNodeReferenceAndNode -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("iterating for efOutwardSearch={} with result=={}", efOutwardSearch,
                            nextNodeReferenceAndNode.getNodeReference().getPrimaryKey());
                }
                return nextNodeReferenceAndNode;
            });
        }

        @Override
        public boolean hasNext() {
            return onHasNext().join();
        }

        @Override
        public NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> next() {
            if (hasNext()) {
                // underlying has already completed
                final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReference> nextNodeReferenceAndNode =
                        Objects.requireNonNull(nextFuture).join();
                nextFuture = null;
                return nextNodeReferenceAndNode;
            }
            throw new NoSuchElementException("called next() on exhausted iterator");
        }

        @Override
        public void cancel() {
            if (nextFuture != null) {
                nextFuture.cancel(false);
            }
        }
    }

    private static class OutwardTraversalState {
        @Nonnull
        private final StorageTransform storageTransform;
        @Nonnull
        private final Estimator estimator;
        @Nonnull
        private final Transformed<RealVector> transformedCenterVector;
        @Nonnull
        private final Queue<NodeReferenceWithDistance> candidates;
        @Nonnull
        private final SpatialRestrictions spatialRestrictions;
        @Nonnull
        private final Queue<NodeReferenceWithDistance> out;
        @Nonnull
        private final Map<Tuple, AbstractNode<NodeReference>> nodeCache;

        public OutwardTraversalState(@Nonnull final StorageTransform storageTransform,
                                     @Nonnull final Estimator estimator,
                                     @Nonnull final Transformed<RealVector> transformedCenterVector,
                                     @Nonnull final Queue<NodeReferenceWithDistance> candidates,
                                     @Nonnull final SpatialRestrictions spatialRestrictions,
                                     @Nonnull final Queue<NodeReferenceWithDistance> out,
                                     @Nonnull final Map<Tuple, AbstractNode<NodeReference>> nodeCache) {
            this.storageTransform = storageTransform;
            this.estimator = estimator;
            this.transformedCenterVector = transformedCenterVector;
            this.candidates = candidates;
            this.spatialRestrictions = spatialRestrictions;
            this.out = out;
            this.nodeCache = nodeCache;
        }

        @Nonnull
        public StorageTransform getStorageTransform() {
            return storageTransform;
        }

        @Nonnull
        public Estimator getEstimator() {
            return estimator;
        }

        @Nonnull
        public Transformed<RealVector> getTransformedCenterVector() {
            return transformedCenterVector;
        }

        @Nonnull
        public Queue<NodeReferenceWithDistance> getCandidates() {
            return candidates;
        }

        @Nonnull
        public SpatialRestrictions getSpatialRestrictions() {
            return spatialRestrictions;
        }

        @Nonnull
        public Queue<NodeReferenceWithDistance> getOut() {
            return out;
        }

        @Nonnull
        public Map<Tuple, AbstractNode<NodeReference>> getNodeCache() {
            return nodeCache;
        }
    }

    private static class SpatialRestrictions {
        private static final Comparator<Equivalence.Wrapper<NodeReferenceWithDistance>> COMPARATOR =
                Comparator.comparing(Equivalence.Wrapper::get,
                        Comparator.nullsFirst(
                                Comparator.comparing(NodeReferenceWithDistance::getDistance)
                                        .thenComparing(NodeReference::getPrimaryKey)));

        private static final PrimaryKeyEquivalence PRIMARY_KEY_EQUIVALENCE = new PrimaryKeyEquivalence();

        private final int insideLimit;
        private final double minimumRadius;
        @Nullable
        private final Tuple minimumPrimaryKey;

        @Nonnull
        private final NavigableSet<Equivalence.Wrapper<NodeReferenceWithDistance>> insideRadius; // inclusive
        @Nonnull
        private final NavigableSet<Equivalence.Wrapper<NodeReferenceWithDistance>> outsideRadius; // exclusive

        public SpatialRestrictions(final int insideLimit,
                                   final double minimumRadius,
                                   @Nullable final Tuple minimumPrimaryKey) {
            this.insideLimit = insideLimit;
            this.minimumRadius = minimumRadius;
            this.minimumPrimaryKey = minimumPrimaryKey;
            this.insideRadius = new TreeSet<>(COMPARATOR);
            this.outsideRadius = new TreeSet<>(COMPARATOR);
        }

        private boolean isGreaterThanMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            return compareAgainstMinimum(nodeReferenceWithDistance) < 0;
        }

        private boolean isGreaterThanOrEqualMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            return compareAgainstMinimum(nodeReferenceWithDistance) <= 0;
        }

        /**
         * Compare the given {@link NodeReferenceWithDistance} against the
         * {@code (minimumRadius, minimumPrimaryKey)} that was used to initialize the iterator.
         *
         * @param nodeReferenceWithDistance the {@link NodeReferenceWithDistance} against which to compare
         *
         * @return a negative integer, zero, or a positive integer when {@code lastEmitted} is
         *         less than, equal, or greater than the parameter {@code t}.
         */
        private int compareAgainstMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            if (minimumRadius == 0.0d || minimumPrimaryKey == null) {
                return -1;
            }

            final int compare = Double.compare(minimumRadius, nodeReferenceWithDistance.getDistance());
            if (compare != 0) {
                return compare;
            }
            return minimumPrimaryKey.compareTo(nodeReferenceWithDistance.getPrimaryKey());
        }

        public boolean shouldBeAdded(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            final Equivalence.Wrapper<NodeReferenceWithDistance> wrapped =
                    PRIMARY_KEY_EQUIVALENCE.wrap(nodeReferenceWithDistance);
            if (insideRadius.size() >= insideLimit) {
                final Equivalence.Wrapper<NodeReferenceWithDistance> least = insideRadius.first();
                if (COMPARATOR.compare(wrapped, least) <= 0) {
                    return false;
                }
            }

            if (isGreaterThanOrEqualMinimum(wrapped.get())) {
                // wrapped greater than minimum
                return !outsideRadius.contains(wrapped);
            }

            return !insideRadius.contains(wrapped);
        }

        @CanIgnoreReturnValue
        public boolean add(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            final Equivalence.Wrapper<NodeReferenceWithDistance> wrapped =
                    PRIMARY_KEY_EQUIVALENCE.wrap(nodeReferenceWithDistance);

            if (isGreaterThanOrEqualMinimum(wrapped.get())) {
                return outsideRadius.add(wrapped);
            }

            if (insideRadius.size() < insideLimit) {
                return insideRadius.add(wrapped);
            }

            Verify.verify(insideRadius.size() == insideLimit);

            final Equivalence.Wrapper<NodeReferenceWithDistance> least = insideRadius.first();
            if (COMPARATOR.compare(wrapped, least) <= 0) {
                return true;
            }

            final boolean exists = insideRadius.add(wrapped);
            insideRadius.pollFirst();
            return exists;
        }
    }

    private static class PrimaryKeyEquivalence extends Equivalence<NodeReferenceWithDistance> {
        @Override
        protected boolean doEquivalent(@Nonnull final NodeReferenceWithDistance a,
                                       @Nonnull final NodeReferenceWithDistance b) {
            return a.getPrimaryKey().equals(b.getPrimaryKey());
        }

        @Override
        protected int doHash(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            return nodeReferenceWithDistance.hashCode();
        }
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

    @Nonnull
    private static ToDoubleFunction<Transformed<RealVector>> distanceToTargetVector(@Nonnull final Estimator estimator,
                                                                                    @Nonnull final Transformed<RealVector> targetVector) {
        return vector -> estimator.distance(targetVector, vector);
    }

    @Nonnull
    private static ToDoubleFunction<Transformed<RealVector>> distanceToSphericalSurface(@Nonnull final Estimator estimator,
                                                                                        @Nonnull final Transformed<RealVector> targetVector,
                                                                                        final double radius) {
        return vector -> Math.abs(estimator.distance(targetVector, vector) - radius);
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
    private static double splitMixDouble(final long x) {
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
    private static long splitMixLong(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >>> 31);
        return x;
    }

    @Nonnull
    private static <T> List<T> drain(@Nonnull Queue<T> queue) {
        final ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();
        while (!queue.isEmpty()) {
            resultBuilder.add(queue.poll());
        }
        return resultBuilder.build();
    }

    @SuppressWarnings("SameParameterValue")
    private static int clamp(final int x, final int low, final int high) {
        Verify.verify(low <= high);
        return Math.max(low, Math.min(x, high));
    }

    private static class SearchResult {
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

    @FunctionalInterface
    private interface CandidatePredicate {
        @Nonnull
        static CandidatePredicate tautology() {
            return (random, initialNodeKeys, size, nodeReference) -> true;
        }

        boolean test(@Nullable SplittableRandom random, @Nonnull Set<Tuple> initialNodeKeys, int size, NodeReference nodeReference);
    }

    private static class AccessInfoAndNodeExistence {
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
