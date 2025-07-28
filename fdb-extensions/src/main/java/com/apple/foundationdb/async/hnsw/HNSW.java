/*
 * HNSW.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class HNSW {
    private static final Logger logger = LoggerFactory.getLogger(HNSW.class);

    public static final int MAX_CONCURRENT_READS = 16;
    @Nonnull
    public static final Random DEFAULT_RANDOM = new Random(0L);
    @Nonnull
    public static final Metric DEFAULT_METRIC = new Metric.EuclideanMetric();
    public static final int DEFAULT_M = 48;
    public static final int DEFAULT_EF_SEARCH = 64;
    public static final int DEFAULT_EF_CONSTRUCTION = 100;
    @Nonnull
    public static final Config DEFAULT_CONFIG = new Config();

    @Nonnull
    private final StorageAdapter storageAdapter;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Config config;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    /**
     * Different kinds of storage layouts.
     */
    public enum Storage {
        /**
         * Every node with all its slots is serialized as one key/value pair.
         */
        BY_NODE(ByNodeStorageAdapter::new);

        @Nonnull
        private final StorageAdapterCreator storageAdapterCreator;

        Storage(@Nonnull final StorageAdapterCreator storageAdapterCreator) {
            this.storageAdapterCreator = storageAdapterCreator;
        }

        @Nonnull
        private StorageAdapter newStorageAdapter(@Nonnull final Config config, @Nonnull final Subspace subspace,
                                                 @Nonnull final Subspace nodeSlotIndexSubspace,
                                                 @Nonnull final OnWriteListener onWriteListener,
                                                 @Nonnull final OnReadListener onReadListener) {
            return storageAdapterCreator.create(config, subspace, nodeSlotIndexSubspace, onWriteListener, onReadListener);
        }
    }

    /**
     * Functional interface to create a {@link StorageAdapter}.
     */
    private interface StorageAdapterCreator {
        StorageAdapter create(@Nonnull Config config, @Nonnull Subspace subspace, @Nonnull Subspace nodeSlotIndexSubspace,
                              @Nonnull OnWriteListener onWriteListener,
                              @Nonnull OnReadListener onReadListener);
    }

    /**
     * Configuration settings for a {@link HNSW}.
     */
    public static class Config {
        @Nonnull
        private final Random random;
        @Nonnull
        private final Metric metric;
        private final int m;
        private final int efSearch;
        private final int efConstruction;

        protected Config() {
            this.random = DEFAULT_RANDOM;
            this.metric = DEFAULT_METRIC;
            this.m = DEFAULT_M;
            this.efSearch = DEFAULT_EF_SEARCH;
            this.efConstruction = DEFAULT_EF_CONSTRUCTION;
        }

        protected Config(@Nonnull final Random random, @Nonnull final Metric metric, final int m,
                         final int efSearch, final int efConstruction) {
            this.random = random;
            this.metric = metric;
            this.m = m;
            this.efSearch = efSearch;
            this.efConstruction = efConstruction;
        }

        @Nonnull
        public Random getRandom() {
            return random;
        }

        @Nonnull
        public Metric getMetric() {
            return metric;
        }

        public int getM() {
            return m;
        }

        public int getEfSearch() {
            return efSearch;
        }

        public int getEfConstruction() {
            return efConstruction;
        }

        @Nonnull
        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(getRandom(), getMetric(), getM(), getEfSearch(), getEfConstruction());
        }

        @Override
        @Nonnull
        public String toString() {
            return "Config[M=" + getM() + " , metric=" + getMetric() + "]";
        }
    }

    /**
     * Builder for {@link Config}.
     *
     * @see #newConfigBuilder
     */
    @CanIgnoreReturnValue
    public static class ConfigBuilder {
        @Nonnull
        private Random random = DEFAULT_RANDOM;
        @Nonnull
        private Metric metric = DEFAULT_METRIC;
        private int m = DEFAULT_M;
        private int efSearch = DEFAULT_EF_SEARCH;
        private int efConstruction = DEFAULT_EF_CONSTRUCTION;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull Random random, @Nonnull final Metric metric, final int m,
                             final int efSearch, final int efConstruction) {
            this.random = random;
            this.metric = metric;
            this.m = m;
            this.efSearch = efSearch;
            this.efConstruction = efConstruction;
        }

        @Nonnull
        public Random getRandom() {
            return random;
        }

        @Nonnull
        public ConfigBuilder setRandom(@Nonnull final Random random) {
            this.random = random;
            return this;
        }

        @Nonnull
        public Metric getMetric() {
            return metric;
        }

        @Nonnull
        public ConfigBuilder setMetric(@Nonnull final Metric metric) {
            this.metric = metric;
            return this;
        }

        public int getM() {
            return m;
        }

        @Nonnull
        public ConfigBuilder setM(final int m) {
            this.m = m;
            return this;
        }

        public int getEfSearch() {
            return efSearch;
        }

        public ConfigBuilder setEfSearch(final int efSearch) {
            this.efSearch = efSearch;
            return this;
        }

        public int getEfConstruction() {
            return efConstruction;
        }

        public ConfigBuilder setEfConstruction(final int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public Config build() {
            return new Config(getRandom(), getMetric(), getM(), getEfSearch(), getEfConstruction());
        }
    }

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link HNSW}
     * @see ConfigBuilder#build
     */
    public static ConfigBuilder newConfigBuilder() {
        return new ConfigBuilder();
    }

    /**
     * Initialize a new R-tree with the default configuration.
     * @param subspace the subspace where the r-tree is stored
     * @param secondarySubspace the subspace where the node index (if used is stored)
     * @param executor an executor to use when running asynchronous tasks
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Subspace secondarySubspace,
                @Nonnull final Executor executor) {
        this(subspace, secondarySubspace, executor, DEFAULT_CONFIG,
                OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * Initialize a new R-tree.
     * @param subspace the subspace where the r-tree is stored
     * @param nodeSlotIndexSubspace the subspace where the node index (if used is stored)
     * @param executor an executor to use when running asynchronous tasks
     * @param config configuration to use
     * @param onWriteListener an on-write listener to be called after writes take place
     * @param onReadListener an on-read listener to be called after reads take place
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Subspace nodeSlotIndexSubspace,
                @Nonnull final Executor executor, @Nonnull final Config config,
                @Nonnull final OnWriteListener onWriteListener,
                @Nonnull final OnReadListener onReadListener) {
        this.storageAdapter = config.getStorage()
                .newStorageAdapter(config, subspace, nodeSlotIndexSubspace, hilbertValueFunction, onWriteListener,
                        onReadListener);
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }

    /**
     * Get the {@link StorageAdapter} used to manage this r-tree.
     * @return r-tree subspace
     */
    @Nonnull
    StorageAdapter getStorageAdapter() {
        return storageAdapter;
    }

    /**
     * Get the executer used by this r-tree.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this r-tree's configuration.
     * @return r-tree configuration
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

    //
    // Read Path
    //

    /**
     * Perform a scan over the tree within the transaction passed in using a predicate that is also passed in to
     * eliminate subtrees from the scan. This predicate may be stateful which allows for dynamic adjustments of the
     * queried area while the scan is active.
     * <br>
     * A scan of the tree offers all items that pass the {@code mbrPredicate} test in Hilbert Value order using an
     * {@link AsyncIterator}. The predicate that is passed in is applied to intermediate nodes as well as leaf nodes,
     * but not to elements contained by a leaf node. The caller should filter out items in a downstream operation.
     * A scan of the tree will not prefetch the next node before the items of the current node have been consumed. This
     * guarantees that the semantics of the mbr predicate can be adapted in response to the items being consumed.
     * (this allows for efficient scans for {@code ORDER BY x, y LIMIT n} queries).
     * @param readTransaction the transaction to use
     * @param mbrPredicate a predicate on an mbr {@link Rectangle}
     * @param suffixKeyPredicate a predicate on the suffix key
     * @return an {@link AsyncIterator} of {@link ItemSlot}s.
     */
    @Nonnull
    public AsyncIterator<ItemSlot> scan(@Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final Predicate<Rectangle> mbrPredicate,
                                        @Nonnull final BiPredicate<Tuple, Tuple> suffixKeyPredicate) {
        return scan(readTransaction, null, null, mbrPredicate, suffixKeyPredicate);
    }

    /**
     * Perform a scan over the tree within the transaction passed in using a predicate that is also passed in to
     * eliminate subtrees from the scan. This predicate may be stateful which allows for dynamic adjustments of the
     * queried area while the scan is active.
     * <br>
     * A scan of the tree offers all items that pass the {@code mbrPredicate} test in Hilbert Value order using an
     * {@link AsyncIterator}. The predicate that is passed in is applied to intermediate nodes as well as leaf nodes,
     * but not to elements contained in a leaf node. The caller should filter out items in a downstream operation.
     * A scan of the tree will not prefetch the next node before the items of the current node have been consumed. This
     * guarantees that the semantics of the mbr predicate can be adapted in response to the items being consumed.
     * (this allows for efficient scans for {@code ORDER BY x, y LIMIT n} queries).
     * @param readTransaction the transaction to use
     * @param lastHilbertValue the last Hilbert value that was returned by a previous call to this method
     * @param lastKey the last key that was returned by a previous call to this method
     * @param mbrPredicate a predicate on an mbr {@link Rectangle}
     * @param suffixKeyPredicate a predicate on the suffix key
     * @return an {@link AsyncIterator} of {@link ItemSlot}s.
     */
    @Nonnull
    public AsyncIterator<ItemSlot> scan(@Nonnull final ReadTransaction readTransaction,
                                        @Nullable final BigInteger lastHilbertValue,
                                        @Nullable final Tuple lastKey,
                                        @Nonnull final Predicate<Rectangle> mbrPredicate,
                                        @Nonnull final BiPredicate<Tuple, Tuple> suffixKeyPredicate) {
        Preconditions.checkArgument((lastHilbertValue == null && lastKey == null) ||
                                    (lastHilbertValue != null && lastKey != null));
        AsyncIterator<CompactNode> leafIterator =
                new LeafIterator(readTransaction, rootId, lastHilbertValue, lastKey, mbrPredicate, suffixKeyPredicate);
        return new ItemSlotIterator(leafIterator);
    }

    /**
     * TODO.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    private CompletableFuture<SearchResult<NodeReference>> kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                                                                   @Nonnull final Vector<Half> queryVector) {
        return storageAdapter.fetchEntryNodeReference(readTransaction)
                .thenCompose(entryPointAndLayer -> {
                    if (entryPointAndLayer == null) {
                        return CompletableFuture.completedFuture(null); // not a single node in the index
                    }

                    final Metric metric = getConfig().getMetric();

                    final NodeReferenceWithDistance entryState =
                            new NodeReferenceWithDistance(entryPointAndLayer.getPrimaryKey(),
                                    Vector.comparativeDistance(metric, entryPointAndLayer.getVector(), queryVector));

                    if (entryPointAndLayer.getLayer() == 0) {
                        // entry data points to a node in layer 0 directly
                        return CompletableFuture.completedFuture(entryState);
                    }

                    final AtomicInteger layerAtomic = new AtomicInteger(entryPointAndLayer.getLayer());
                    final AtomicReference<NodeReferenceWithDistance> nodeReferenceAtomic =
                            new AtomicReference<>(entryState);

                    return MoreAsyncUtil.forLoop(entryPointAndLayer.getLayer(),
                            layer -> layer > 0,
                            layer -> layer - 1,
                            layer -> {
                        final var greedyIn = nodeReferenceAtomic.get();
                        return greedySearchLayer(InliningNode.factory(), readTransaction, greedyIn, layer,
                                queryVector)
                                .thenApply(greedyState -> {
                                    nodeReferenceAtomic.set(greedyState);
                                    return null;
                                });
                    }, executor).thenApply(ignored -> nodeReferenceAtomic.get());
                }).thenCompose(nodeReference -> {
                    if (nodeReference == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return searchLayer(CompactNode.factory(), readTransaction,
                            ImmutableList.of(nodeReference), 0, config.getEfSearch(),
                            queryVector);
                });
    }

    @Nonnull
    private <N extends NodeReference> CompletableFuture<NodeReferenceWithDistance> greedySearchLayer(@Nonnull NodeFactory<N> nodeFactory,
                                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                                     @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                                                                                     final int layer,
                                                                                                     @Nonnull final Vector<Half> queryVector) {
        if (nodeFactory.getNodeKind() == NodeKind.INLINING) {
            return greedySearchInliningLayer(readTransaction, entryNeighbor, layer, queryVector);
        } else {
            return searchLayer(nodeFactory, readTransaction, ImmutableList.of(entryNeighbor), layer, 1, queryVector)
                    .thenApply(searchResult -> Iterables.getOnlyElement(searchResult.getNodeMap().keySet()));
        }
    }

    /**
     * TODO.
     */
    @Nonnull
    private CompletableFuture<NodeReferenceWithDistance> greedySearchInliningLayer(@Nonnull final ReadTransaction readTransaction,
                                                                                   @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                                                                   final int layer,
                                                                                   @Nonnull final Vector<Half> queryVector) {
        Verify.verify(layer > 0);
        final Metric metric = getConfig().getMetric();
        final AtomicReference<NodeReferenceWithDistance> currentNodeReferenceAtomic =
                new AtomicReference<>(new NodeReferenceWithDistance(entryNeighbor.getPrimaryKey(),
                        entryNeighbor.getDistance()));

        return AsyncUtil.whileTrue(() -> onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(InliningNode.factory(), readTransaction,
                                layer, currentNodeReferenceAtomic.get().getPrimaryKey()))
                .thenApply(node -> {
                    if (node == null) {
                        throw new IllegalStateException("unable to fetch node");
                    }
                    final InliningNode inliningNode = node.asInliningNode();
                    final List<NodeReferenceWithVector> neighbors = inliningNode.getNeighbors();

                    final NodeReferenceWithDistance currentNodeReference = currentNodeReferenceAtomic.get();
                    double minDistance = currentNodeReference.getDistance();

                    NodeReferenceWithVector nearestNeighbor = null;
                    for (final NodeReferenceWithVector neighbor : neighbors) {
                        final double distance =
                                Vector.comparativeDistance(metric, neighbor.getVector(), queryVector);
                        if (distance < minDistance) {
                            minDistance = distance;
                            nearestNeighbor = neighbor;
                        }
                    }

                    if (nearestNeighbor == null) {
                        currentNodeReferenceAtomic.set(
                                new NodeReferenceWithDistance(currentNodeReference.getPrimaryKey(), minDistance));
                        return false;
                    }

                    currentNodeReferenceAtomic.set(
                            new NodeReferenceWithDistance(nearestNeighbor.getPrimaryKey(), minDistance));
                    return true;
                }), executor).thenApply(ignored -> currentNodeReferenceAtomic.get());
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<SearchResult<N>> searchLayer(@Nonnull NodeFactory<N> nodeFactory,
                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                     @Nonnull final List<NodeReferenceWithDistance> entryNeighbors,
                                                                                     final int layer,
                                                                                     final int efSearch,
                                                                                     @Nonnull final Vector<Half> queryVector) {
        final Set<Tuple> visited = Sets.newConcurrentHashSet(NodeReference.primaryKeys(entryNeighbors));
        final Queue<NodeReferenceWithDistance> candidates =
                new PriorityBlockingQueue<>(config.getM(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance));
        candidates.addAll(entryNeighbors);
        final Queue<NodeReferenceWithDistance> nearestNeighbors =
                new PriorityBlockingQueue<>(config.getM(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance).reversed());
        nearestNeighbors.addAll(entryNeighbors);
        final Map<Tuple, Node<N>> nodeCache = Maps.newConcurrentMap();
        final Metric metric = getConfig().getMetric();

        return AsyncUtil.whileTrue(() -> {
            if (candidates.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }

            final NodeReferenceWithDistance candidate = candidates.poll();
            final NodeReferenceWithDistance furthestNeighbor = Objects.requireNonNull(nearestNeighbors.peek());

            if (candidate.getDistance() > furthestNeighbor.getDistance()) {
                return AsyncUtil.READY_FALSE;
            }

            return fetchNodeIfNotCached(nodeFactory, readTransaction, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(neighbor.getPrimaryKey())))
                    .thenCompose(neighborReferences -> fetchNeighborhood(nodeFactory, readTransaction,
                            layer, neighborReferences, nodeCache))
                    .thenApply(neighborReferences -> {
                        for (final NodeReferenceWithVector current : neighborReferences) {
                            visited.add(current.getPrimaryKey());
                            final double furthestDistance =
                                    Objects.requireNonNull(nearestNeighbors.peek()).getDistance();

                            final double currentDistance =
                                    Vector.comparativeDistance(metric, current.getVector(), queryVector);
                            if (currentDistance < furthestDistance || nearestNeighbors.size() < efSearch) {
                                final NodeReferenceWithDistance currentWithDistance =
                                        new NodeReferenceWithDistance(current.getPrimaryKey(), currentDistance);
                                candidates.add(currentWithDistance);
                                nearestNeighbors.add(currentWithDistance);
                                if (nearestNeighbors.size() > efSearch) {
                                    nearestNeighbors.poll();
                                }
                            }
                        }
                        return true;
                    });
        }).thenCompose(ignored -> fetchResultsIfNecessary(nodeFactory, readTransaction, layer, nearestNeighbors,
                nodeCache));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<Node<N>> fetchNodeIfNotCached(@Nonnull final NodeFactory<N> nodeFactory,
                                                                                      @Nonnull final ReadTransaction readTransaction,
                                                                                      final int layer,
                                                                                      @Nonnull final NodeReference nodeReference,
                                                                                      @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchNodeIfNecessaryAndApply(nodeFactory, readTransaction, layer, nodeReference,
                nR -> nodeCache.get(nR.getPrimaryKey()),
                (nR, node) -> {
                    nodeCache.put(nR.getPrimaryKey(), node);
                    return node;
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<U> fetchNodeIfNecessaryAndApply(@Nonnull final NodeFactory<N> nodeFactory,
                                                                                                                    @Nonnull final ReadTransaction readTransaction,
                                                                                                                    final int layer,
                                                                                                                    @Nonnull final R nodeReference,
                                                                                                                    @Nonnull final Function<R, U> fetchBypassFunction,
                                                                                                                    @Nonnull final BiFunction<R, Node<N>, U> biMapFunction) {
        final U bypass = fetchBypassFunction.apply(nodeReference);
        if (bypass != null) {
            return CompletableFuture.completedFuture(bypass);
        }

        return onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(nodeFactory, readTransaction, layer, nodeReference.getPrimaryKey()))
                .thenApply(node -> biMapFunction.apply(nodeReference, node));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>> fetchNeighborhood(@Nonnull final NodeFactory<N> nodeFactory,
                                                                                                         @Nonnull final ReadTransaction readTransaction,
                                                                                                         final int layer,
                                                                                                         @Nonnull final Iterable<? extends NodeReference> neighborReferences,
                                                                                                         @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(nodeFactory, readTransaction, layer, neighborReferences,
                neighborReference -> {
                    if (neighborReference instanceof NodeReferenceWithVector) {
                        return (NodeReferenceWithVector)neighborReference;
                    }
                    final Node<N> neighborNode = nodeCache.get(neighborReference.getPrimaryKey());
                    if (neighborNode == null) {
                        return null;
                    }
                    return new NodeReferenceWithVector(neighborReference.getPrimaryKey(), neighborNode.asCompactNode().getVector());
                },
                (neighborReference, neighborNode) -> {
                    nodeCache.put(neighborReference.getPrimaryKey(), neighborNode);
                    return new NodeReferenceWithVector(neighborReference.getPrimaryKey(), neighborNode.asCompactNode().getVector());
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<SearchResult<N>> fetchResultsIfNecessary(@Nonnull final NodeFactory<N> creator,
                                                                                                 @Nonnull final ReadTransaction readTransaction,
                                                                                                 final int layer,
                                                                                                 @Nonnull final Iterable<NodeReferenceWithDistance> nodeReferences,
                                                                                                 @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(creator, readTransaction, layer, nodeReferences,
                nodeReference -> {
                    final Node<N> node = nodeCache.get(nodeReference.getPrimaryKey());
                    if (node == null) {
                        return null;
                    }
                    return new SearchResult.NodeReferenceWithNode<>(nodeReference, node);
                },
                (nodeReferenceWithDistance, node) -> {
                    nodeCache.put(nodeReferenceWithDistance.getPrimaryKey(), node);
                    return new SearchResult.NodeReferenceWithNode<N>(nodeReferenceWithDistance, node);
                })
                .thenApply(nodeReferencesWithNodes -> {
                    final ImmutableMap.Builder<NodeReferenceWithDistance, Node<N>> nodeMapBuilder =
                            ImmutableMap.builder();
                    for (final SearchResult.NodeReferenceWithNode<N> nodeReferenceWithNode : nodeReferencesWithNodes) {
                        nodeMapBuilder.put(nodeReferenceWithNode.getNodeReferenceWithDistance(), nodeReferenceWithNode.getNode());
                    }
                    return new SearchResult<>(nodeMapBuilder.build());
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<List<U>> fetchSomeNodesAndApply(@Nonnull final NodeFactory<N> creator,
                                                                                                                    @Nonnull final ReadTransaction readTransaction,
                                                                                                                    final int layer,
                                                                                                                    @Nonnull final Iterable<R> nodeReferences,
                                                                                                                    @Nonnull final Function<R, U> fetchBypassFunction,
                                                                                                                    @Nonnull final BiFunction<R, Node<N>, U> biMapFunction) {
        // this deque is only modified by once upon creation
        final ArrayDeque<R> toBeProcessed = new ArrayDeque<>();
        for (final var nodeReference : nodeReferences) {
            toBeProcessed.addLast(nodeReference);
        }
        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final AtomicInteger neighborIndex = new AtomicInteger(0);
        final Object[] neighborNodeArray = new Object[toBeProcessed.size()];

        return AsyncUtil.whileTrue(() -> {
            working.removeIf(CompletableFuture::isDone);

            while (working.size() <= MAX_CONCURRENT_READS) {
                final R currentNeighborReference = toBeProcessed.pollFirst();
                if (currentNeighborReference == null) {
                    break;
                }

                final int index = neighborIndex.getAndIncrement();

                working.add(fetchNodeIfNecessaryAndApply(creator, readTransaction, layer,
                                currentNeighborReference, fetchBypassFunction, biMapFunction)
                        .thenAccept(resultNode -> {
                            Objects.requireNonNull(resultNode);
                            neighborNodeArray[index] = resultNode;
                        }));
            }

            if (working.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(working).thenApply(ignored -> true);
        }, executor).thenApply(ignored -> {
            final ImmutableList.Builder<U> resultBuilder = ImmutableList.builder();
            for (final Object o : neighborNodeArray) {
                resultBuilder.add((U)o);
            }
            return resultBuilder.build();
        });
    }

    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple primaryKey,
                                          @Nonnull final Vector<Half> vector) {
        final Metric metric = getConfig().getMetric();
        final Queue<NodeReferenceWithDistance> furthestNeighbors =
                new PriorityBlockingQueue<>(config.getM(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance).reversed());

        final int l = insertionLayer(getConfig().getRandom());

        storageAdapter.fetchEntryNodeReference(transaction)
                .thenApply(entryNodeReference -> {
                    if (entryNodeReference == null) {
                        // this is the first node
                        writeLonelyNodes(InliningNode.factory(), transaction, primaryKey, vector, l, 0);
                        storageAdapter.writeNode(transaction,
                                CompactNode.factory()
                                        .create(NodeKind.COMPACT, primaryKey, vector, ImmutableList.of()),
                                0);
                        storageAdapter.writeEntryNodeReference(transaction,
                                new EntryNodeReference(primaryKey, vector, l));
                    } else {
                        final int entryNodeLayer = entryNodeReference.getLayer();
                        if (l > entryNodeLayer) {
                            writeLonelyNodes(InliningNode.factory(), transaction, primaryKey, vector, l, entryNodeLayer);
                            storageAdapter.writeEntryNodeReference(transaction,
                                    new EntryNodeReference(primaryKey, vector, l));
                        }
                    }
                    return entryNodeReference;
                }).thenCompose(entryNodeReference -> {
                    if (entryNodeReference == null) {
                        return AsyncUtil.DONE;
                    }

                    final int lMax = entryNodeReference.getLayer();

                    final AtomicReference<NodeReferenceWithDistance> nodeReferenceAtomic =
                            new AtomicReference<>(new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    Vector.comparativeDistance(metric, entryNodeReference.getVector(), vector)));
                    MoreAsyncUtil.forLoop(lMax,
                            layer -> layer > l,
                            layer -> layer - 1,
                            layer -> greedySearchLayer(InliningNode.factory(), transaction,
                                    nodeReferenceAtomic.get(), layer, vector)
                                    .thenApply(nodeReference -> {
                                        nodeReferenceAtomic.set(nodeReference);
                                        return null;
                                    }), executor);

                    final NodeReferenceWithDistance nodeReference = nodeReferenceAtomic.get();



                })
    }

    public <N extends NodeReference> void writeLonelyNodes(@Nonnull final NodeFactory<N> nodeFactory,
                                                           @Nonnull final Transaction transaction,
                                                           @Nonnull final Tuple primaryKey,
                                                           @Nonnull final Vector<Half> vector,
                                                           final int highestLayerInclusive,
                                                           final int lowestLayerExclusive) {
        for (int layer = highestLayerInclusive; layer > lowestLayerExclusive; layer --) {
            storageAdapter.writeNode(transaction,
                    nodeFactory.create(nodeFactory.getNodeKind(), primaryKey, vector, ImmutableList.of()), layer);
        }
    }

    private int insertionLayer(@Nonnull final Random random) {
        double lambda = 1.0 / Math.log(getConfig().getM());
        double u = 1.0 - random.nextDouble();  // Avoid log(0)
        return (int) Math.floor(-Math.log(u) * lambda);
    }
}
