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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HNSW {
    private static final Logger logger = LoggerFactory.getLogger(HNSW.class);

    public static final int MAX_CONCURRENT_NODE_READS = 16;
    public static final int MAX_CONCURRENT_NEIGHBOR_FETCHES = 3;
    @Nonnull public static final Random DEFAULT_RANDOM = new Random(0L);
    @Nonnull public static final Metric DEFAULT_METRIC = new Metric.EuclideanMetric();
    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_M_MAX = DEFAULT_M;
    public static final int DEFAULT_M_MAX_0 = 2 * DEFAULT_M;
    public static final int DEFAULT_EF_SEARCH = 64;
    public static final int DEFAULT_EF_CONSTRUCTION = 200;
    public static final boolean DEFAULT_EXTEND_CANDIDATES = false;
    public static final boolean DEFAULT_KEEP_PRUNED_CONNECTIONS = false;

    @Nonnull
    public static final Config DEFAULT_CONFIG = new Config();

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
     * Configuration settings for a {@link HNSW}.
     */
    @SuppressWarnings("checkstyle:MemberName")
    public static class Config {
        @Nonnull
        private final Random random;
        @Nonnull
        private final Metric metric;
        private final int m;
        private final int mMax;
        private final int mMax0;
        private final int efSearch;
        private final int efConstruction;
        private final boolean extendCandidates;
        private final boolean keepPrunedConnections;

        protected Config() {
            this.random = DEFAULT_RANDOM;
            this.metric = DEFAULT_METRIC;
            this.m = DEFAULT_M;
            this.mMax = DEFAULT_M_MAX;
            this.mMax0 = DEFAULT_M_MAX_0;
            this.efSearch = DEFAULT_EF_SEARCH;
            this.efConstruction = DEFAULT_EF_CONSTRUCTION;
            this.extendCandidates = DEFAULT_EXTEND_CANDIDATES;
            this.keepPrunedConnections = DEFAULT_KEEP_PRUNED_CONNECTIONS;
        }

        protected Config(@Nonnull final Random random, @Nonnull final Metric metric, final int m, final int mMax,
                         final int mMax0, final int efSearch, final int efConstruction, final boolean extendCandidates,
                         final boolean keepPrunedConnections) {
            this.random = random;
            this.metric = metric;
            this.m = m;
            this.mMax = mMax;
            this.mMax0 = mMax0;
            this.efSearch = efSearch;
            this.efConstruction = efConstruction;
            this.extendCandidates = extendCandidates;
            this.keepPrunedConnections = keepPrunedConnections;
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

        public int getMMax() {
            return mMax;
        }

        public int getMMax0() {
            return mMax0;
        }

        public int getEfSearch() {
            return efSearch;
        }

        public int getEfConstruction() {
            return efConstruction;
        }

        public boolean isExtendCandidates() {
            return extendCandidates;
        }

        public boolean isKeepPrunedConnections() {
            return keepPrunedConnections;
        }

        @Nonnull
        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(getRandom(), getMetric(), getM(), getMMax(), getMMax0(), getEfSearch(),
                    getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections());
        }

        @Override
        @Nonnull
        public String toString() {
            return "Config[metric=" + getMetric() + "M=" + getM() + " , MMax=" + getMMax() + " , MMax0=" + getMMax0() +
                    ", efSearch=" + getEfSearch() + ", efConstruction=" + getEfConstruction() +
                    ", isExtendCandidates=" + isExtendCandidates() +
                    ", isKeepPrunedConnections=" + isKeepPrunedConnections() + "]";
        }
    }

    /**
     * Builder for {@link Config}.
     *
     * @see #newConfigBuilder
     */
    @CanIgnoreReturnValue
    @SuppressWarnings("checkstyle:MemberName")
    public static class ConfigBuilder {
        @Nonnull
        private Random random = DEFAULT_RANDOM;
        @Nonnull
        private Metric metric = DEFAULT_METRIC;
        private int m = DEFAULT_M;
        private int mMax;
        private int mMax0;
        private int efSearch = DEFAULT_EF_SEARCH;
        private int efConstruction = DEFAULT_EF_CONSTRUCTION;
        private boolean extendCandidates = DEFAULT_EXTEND_CANDIDATES;
        private boolean keepPrunedConnections = DEFAULT_KEEP_PRUNED_CONNECTIONS;

        public ConfigBuilder() {
        }

        public ConfigBuilder(@Nonnull Random random, @Nonnull final Metric metric, final int m, final int mMax,
                             final int mMax0, final int efSearch, final int efConstruction,
                             final boolean extendCandidates, final boolean keepPrunedConnections) {
            this.random = random;
            this.metric = metric;
            this.m = m;
            this.mMax = mMax;
            this.mMax0 = mMax0;
            this.efSearch = efSearch;
            this.efConstruction = efConstruction;
            this.extendCandidates = extendCandidates;
            this.keepPrunedConnections = keepPrunedConnections;
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

        public int getMMax() {
            return mMax;
        }

        @Nonnull
        public ConfigBuilder setMMax(final int mMax) {
            this.mMax = mMax;
            return this;
        }

        public int getMMax0() {
            return mMax0;
        }

        @Nonnull
        public ConfigBuilder setMMax0(final int mMax0) {
            this.mMax0 = mMax0;
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

        public boolean isExtendCandidates() {
            return extendCandidates;
        }

        public ConfigBuilder setExtendCandidates(final boolean extendCandidates) {
            this.extendCandidates = extendCandidates;
            return this;
        }

        public boolean isKeepPrunedConnections() {
            return keepPrunedConnections;
        }

        public ConfigBuilder setKeepPrunedConnections(final boolean keepPrunedConnections) {
            this.keepPrunedConnections = keepPrunedConnections;
            return this;
        }

        public Config build() {
            return new Config(getRandom(), getMetric(), getM(), getMMax(), getMMax0(), getEfSearch(),
                    getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections());
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
     * TODO.
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Executor executor) {
        this(subspace, executor, DEFAULT_CONFIG, OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * TODO.
     */
    public HNSW(@Nonnull final Subspace subspace,
                @Nonnull final Executor executor, @Nonnull final Config config,
                @Nonnull final OnWriteListener onWriteListener,
                @Nonnull final OnReadListener onReadListener) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
    }


    @Nonnull
    public Subspace getSubspace() {
        return subspace;
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
     * TODO.
     */
    @SuppressWarnings("checkstyle:MethodName") // method name introduced by paper
    @Nonnull
    public CompletableFuture<? extends List<? extends NodeReferenceAndNode<? extends NodeReference>>> kNearestNeighborsSearch(@Nonnull final ReadTransaction readTransaction,
                                                                                                                              @Nonnull final Vector<Half> queryVector) {
        return StorageAdapter.fetchEntryNodeReference(readTransaction, getSubspace(), getOnReadListener())
                .thenCompose(entryPointAndLayer -> {
                    if (entryPointAndLayer == null) {
                        return CompletableFuture.completedFuture(null); // not a single node in the index
                    }

                    final Metric metric = getConfig().getMetric();

                    final NodeReferenceWithDistance entryState =
                            new NodeReferenceWithDistance(entryPointAndLayer.getPrimaryKey(),
                                    entryPointAndLayer.getVector(),
                                    Vector.comparativeDistance(metric, entryPointAndLayer.getVector(), queryVector));

                    final var entryLayer = entryPointAndLayer.getLayer();
                    if (entryLayer == 0) {
                        // entry data points to a node in layer 0 directly
                        return CompletableFuture.completedFuture(entryState);
                    }


                    final AtomicReference<NodeReferenceWithDistance> nodeReferenceAtomic =
                            new AtomicReference<>(entryState);

                    return MoreAsyncUtil.forLoop(entryLayer,
                            layer -> layer > 0,
                            layer -> layer - 1,
                            layer -> {
                                final var storageAdapter = getStorageAdapterForLayer(layer);
                                final var greedyIn = nodeReferenceAtomic.get();
                                return greedySearchLayer(storageAdapter, readTransaction, greedyIn, layer,
                                        queryVector)
                                        .thenApply(greedyState -> {
                                            nodeReferenceAtomic.set(greedyState);
                                            return null;
                                        });
                            }, executor)
                            .thenApply(ignored -> nodeReferenceAtomic.get());
                }).thenCompose(nodeReference -> {
                    if (nodeReference == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final var storageAdapter = getStorageAdapterForLayer(0);

                    return searchLayer(storageAdapter, readTransaction,
                                    ImmutableList.of(nodeReference), 0, config.getEfSearch(),
                                    Maps.newConcurrentMap(), queryVector);
                });
    }

    @Nonnull
    private <N extends NodeReference> CompletableFuture<NodeReferenceWithDistance> greedySearchLayer(@Nonnull StorageAdapter<N> storageAdapter,
                                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                                     @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                                                                                     final int layer,
                                                                                                     @Nonnull final Vector<Half> queryVector) {
        if (storageAdapter.getNodeKind() == NodeKind.INLINING) {
            return greedySearchInliningLayer(storageAdapter.asInliningStorageAdapter(), readTransaction, entryNeighbor, layer, queryVector);
        } else {
            return searchLayer(storageAdapter, readTransaction, ImmutableList.of(entryNeighbor), layer, 1, Maps.newConcurrentMap(), queryVector)
                    .thenApply(searchResult -> Iterables.getOnlyElement(searchResult).getNodeReferenceWithDistance());
        }
    }

    /**
     * TODO.
     */
    @Nonnull
    private CompletableFuture<NodeReferenceWithDistance> greedySearchInliningLayer(@Nonnull final StorageAdapter<NodeReferenceWithVector> storageAdapter,
                                                                                   @Nonnull final ReadTransaction readTransaction,
                                                                                   @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                                                                   final int layer,
                                                                                   @Nonnull final Vector<Half> queryVector) {
        Verify.verify(layer > 0);
        final Metric metric = getConfig().getMetric();
        final AtomicReference<NodeReferenceWithDistance> currentNodeReferenceAtomic =
                new AtomicReference<>(entryNeighbor);

        return AsyncUtil.whileTrue(() -> onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(readTransaction, layer, currentNodeReferenceAtomic.get().getPrimaryKey()))
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
                        return false;
                    }

                    currentNodeReferenceAtomic.set(
                            new NodeReferenceWithDistance(nearestNeighbor.getPrimaryKey(), nearestNeighbor.getVector(),
                                    minDistance));
                    return true;
                }), executor).thenApply(ignored -> currentNodeReferenceAtomic.get());
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>> searchLayer(@Nonnull StorageAdapter<N> storageAdapter,
                                                                                                   @Nonnull final ReadTransaction readTransaction,
                                                                                                   @Nonnull final Collection<NodeReferenceWithDistance> entryNeighbors,
                                                                                                   final int layer,
                                                                                                   final int efSearch,
                                                                                                   @Nonnull final Map<Tuple, Node<N>> nodeCache,
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

            return fetchNodeIfNotCached(storageAdapter, readTransaction, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(neighbor.getPrimaryKey())))
                    .thenCompose(neighborReferences -> fetchNeighborhood(storageAdapter, readTransaction,
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
        }).thenCompose(ignored ->
                fetchSomeNodesIfNotCached(storageAdapter, readTransaction, layer, nearestNeighbors, nodeCache));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<Node<N>> fetchNodeIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                      @Nonnull final ReadTransaction readTransaction,
                                                                                      final int layer,
                                                                                      @Nonnull final NodeReference nodeReference,
                                                                                      @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction, layer, nodeReference,
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
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<U> fetchNodeIfNecessaryAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
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
                        storageAdapter.fetchNode(readTransaction, layer, nodeReference.getPrimaryKey()))
                .thenApply(node -> biMapFunction.apply(nodeReference, node));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>> fetchNeighborhood(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                         @Nonnull final ReadTransaction readTransaction,
                                                                                                         final int layer,
                                                                                                         @Nonnull final Iterable<? extends NodeReference> neighborReferences,
                                                                                                         @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, layer, neighborReferences,
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
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>> fetchSomeNodesIfNotCached(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                                 @Nonnull final ReadTransaction readTransaction,
                                                                                                                 final int layer,
                                                                                                                 @Nonnull final Iterable<NodeReferenceWithDistance> nodeReferences,
                                                                                                                 @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(storageAdapter, readTransaction, layer, nodeReferences,
                nodeReference -> {
                    final Node<N> node = nodeCache.get(nodeReference.getPrimaryKey());
                    if (node == null) {
                        return null;
                    }
                    return new NodeReferenceAndNode<>(nodeReference, node);
                },
                (nodeReferenceWithDistance, node) -> {
                    nodeCache.put(nodeReferenceWithDistance.getPrimaryKey(), node);
                    return new NodeReferenceAndNode<>(nodeReferenceWithDistance, node);
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<List<U>> fetchSomeNodesAndApply(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                                    @Nonnull final ReadTransaction readTransaction,
                                                                                                                    final int layer,
                                                                                                                    @Nonnull final Iterable<R> nodeReferences,
                                                                                                                    @Nonnull final Function<R, U> fetchBypassFunction,
                                                                                                                    @Nonnull final BiFunction<R, Node<N>, U> biMapFunction) {
        return MoreAsyncUtil.forEach(nodeReferences,
                currentNeighborReference -> fetchNodeIfNecessaryAndApply(storageAdapter, readTransaction, layer,
                        currentNeighborReference, fetchBypassFunction, biMapFunction), MAX_CONCURRENT_NODE_READS,
                getExecutor());
    }

    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final Vector<Half> newVector) {
        final Metric metric = getConfig().getMetric();

        final int l = insertionLayer(getConfig().getRandom());

        return StorageAdapter.fetchEntryNodeReference(transaction, getSubspace(), getOnReadListener())
                .thenApply(entryNodeReference -> {
                    if (entryNodeReference == null) {
                        // this is the first node
                        writeLonelyNodes(transaction, newPrimaryKey, newVector, l, -1);
                        StorageAdapter.writeEntryNodeReference(transaction, getSubspace(),
                                new EntryNodeReference(newPrimaryKey, newVector, l), getOnWriteListener());
                    } else {
                        final int entryNodeLayer = entryNodeReference.getLayer();
                        if (l > entryNodeLayer) {
                            writeLonelyNodes(transaction, newPrimaryKey, newVector, l, entryNodeLayer);
                            StorageAdapter.writeEntryNodeReference(transaction, getSubspace(),
                                    new EntryNodeReference(newPrimaryKey, newVector, l), getOnWriteListener());
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
                                    entryNodeReference.getVector(),
                                    Vector.comparativeDistance(metric, entryNodeReference.getVector(), newVector)));
                    MoreAsyncUtil.forLoop(lMax,
                            layer -> layer > l,
                            layer -> layer - 1,
                            layer -> {
                                final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
                                return greedySearchLayer(storageAdapter, transaction,
                                        nodeReferenceAtomic.get(), layer, newVector)
                                        .thenApply(nodeReference -> {
                                            nodeReferenceAtomic.set(nodeReference);
                                            return null;
                                        });
                            }, executor);

                    final AtomicReference<List<NodeReferenceWithDistance>> nearestNeighborsAtomic =
                            new AtomicReference<>(ImmutableList.of(nodeReferenceAtomic.get()));

                    return MoreAsyncUtil.forLoop(Math.min(lMax, l),
                            layer -> layer >= 0,
                            layer -> layer - 1,
                            layer -> {
                                final StorageAdapter<? extends NodeReference> storageAdapter = getStorageAdapterForLayer(layer);
                                return insertIntoLayer(storageAdapter, transaction,
                                        nearestNeighborsAtomic.get(), layer, newPrimaryKey, newVector)
                                        .thenCompose(nearestNeighbors -> {
                                            nearestNeighborsAtomic.set(nearestNeighbors);
                                            return AsyncUtil.DONE;
                                        });
                            }, executor);
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>> insertIntoLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                         @Nonnull final Transaction transaction,
                                                                                                         @Nonnull final List<NodeReferenceWithDistance> nearestNeighbors,
                                                                                                         int layer,
                                                                                                         @Nonnull final Tuple newPrimaryKey,
                                                                                                         @Nonnull final Vector<Half> newVector) {
        final Map<Tuple, Node<N>> nodeCache = Maps.newConcurrentMap();

        return searchLayer(storageAdapter, transaction,
                nearestNeighbors, layer, config.getEfConstruction(), nodeCache, newVector)
                .thenCompose(searchResult -> {
                    final List<NodeReferenceWithDistance> references = NodeReferenceAndNode.getReferences(searchResult);

                    return selectNeighbors(storageAdapter, transaction, searchResult,
                            layer, getConfig().getM(), getConfig().isExtendCandidates(), nodeCache, newVector)
                            .thenCompose(selectedNeighbors -> {
                                final NodeFactory<N> nodeFactory = storageAdapter.getNodeFactory();

                                final Node<N> newNode =
                                        nodeFactory.create(newPrimaryKey, newVector,
                                                NodeReferenceAndNode.getReferences(selectedNeighbors));

                                final NeighborsChangeSet<N> newNodeChangeSet =
                                        new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                                                newNode.getNeighbors());

                                // create change sets for each selected neighbor and insert new node into them
                                final Map<Tuple /* primaryKey */, NeighborsChangeSet<N>> neighborChangeSetMap =
                                        Maps.newLinkedHashMap();
                                for (final NodeReferenceAndNode<N> selectedNeighbor : selectedNeighbors) {
                                    final NeighborsChangeSet<N> baseSet =
                                            new BaseNeighborsChangeSet<>(selectedNeighbor.getNode().getNeighbors());
                                    final NeighborsChangeSet<N> insertSet =
                                            new InsertNeighborsChangeSet<>(baseSet, ImmutableList.of(newNode.getSelfReference(newVector)));
                                    neighborChangeSetMap.put(selectedNeighbor.getNode().getPrimaryKey(),
                                            insertSet);
                                }

                                final int currentMMax = layer == 0 ? getConfig().getMMax0() : getConfig().getMMax();
                                return MoreAsyncUtil.forEach(selectedNeighbors,
                                                selectedNeighbor -> {
                                                    final Node<N> selectedNeighborNode = selectedNeighbor.getNode();
                                                    final NeighborsChangeSet<N> changeSet =
                                                            Objects.requireNonNull(neighborChangeSetMap.get(selectedNeighborNode.getPrimaryKey()));
                                                    return pruneNeighborsIfNecessary(storageAdapter, transaction,
                                                            selectedNeighbor, layer, currentMMax, changeSet, nodeCache)
                                                            .thenApply(nodeReferencesAndNodes -> {
                                                                if (nodeReferencesAndNodes == null) {
                                                                    return changeSet;
                                                                }
                                                                return resolveChangeSetFromNewNeighbors(changeSet, nodeReferencesAndNodes);
                                                            });
                                                }, MAX_CONCURRENT_NEIGHBOR_FETCHES, getExecutor())
                                        .thenApply(changeSets -> {
                                            for (int i = 0; i < selectedNeighbors.size(); i++) {
                                                final NodeReferenceAndNode<N> selectedNeighbor = selectedNeighbors.get(i);
                                                final NeighborsChangeSet<N> changeSet = changeSets.get(i);
                                                neighborChangeSetMap.put(
                                                        selectedNeighbor.getNodeReferenceWithDistance().getPrimaryKey(),
                                                        changeSet);
                                            }

                                            storageAdapter.writeNode(transaction, newNode, layer, newNodeChangeSet);

                                            return ImmutableList.copyOf(references);
                                        });
                            });
                });
    }

    private <N extends NodeReference> NeighborsChangeSet<N> resolveChangeSetFromNewNeighbors(@Nonnull final NeighborsChangeSet<N> beforeChangeSet,
                                                                                             @Nonnull final Iterable<NodeReferenceAndNode<N>> afterNeighbors) {
        final Map<Tuple, N> beforeNeighborsMap = Maps.newLinkedHashMap();
        for (final N n : beforeChangeSet.merge()) {
            beforeNeighborsMap.put(n.getPrimaryKey(), n);
        }

        final Map<Tuple, N> afterNeighborsMap = Maps.newLinkedHashMap();
        for (final NodeReferenceAndNode<N> nodeReferenceAndNode : afterNeighbors) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();

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

    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>> pruneNeighborsIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                                 @Nonnull final Transaction transaction,
                                                                                                                 @Nonnull final NodeReferenceAndNode<N> selectedNeighbor,
                                                                                                                 int layer,
                                                                                                                 int mMax,
                                                                                                                 @Nonnull final NeighborsChangeSet<N> neighborChangeSet,
                                                                                                                 @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        final Metric metric = getConfig().getMetric();
        final Node<N> selectedNeighborNode = selectedNeighbor.getNode();
        if (selectedNeighborNode.getNeighbors().size() < mMax) {
            return CompletableFuture.completedFuture(null);
        } else {
            return fetchNeighborhood(storageAdapter, transaction, layer, neighborChangeSet.merge(), nodeCache)
                    .thenCompose(nodeReferenceWithVectors -> {
                        final ImmutableList.Builder<NodeReferenceWithDistance> nodeReferencesWithDistancesBuilder =
                                ImmutableList.builder();
                        for (final NodeReferenceWithVector nodeReferenceWithVector : nodeReferenceWithVectors) {
                            final var vector = nodeReferenceWithVector.getVector();
                            final double distance =
                                    Vector.comparativeDistance(metric, vector,
                                            selectedNeighbor.getNodeReferenceWithDistance().getVector());
                            nodeReferencesWithDistancesBuilder.add(
                                    new NodeReferenceWithDistance(nodeReferenceWithVector.getPrimaryKey(),
                                            vector, distance));
                        }
                        return fetchSomeNodesIfNotCached(storageAdapter, transaction, layer,
                                nodeReferencesWithDistancesBuilder.build(), nodeCache);
                    })
                    .thenCompose(nodeReferencesAndNodes ->
                            selectNeighbors(storageAdapter, transaction,
                                    nodeReferencesAndNodes, layer,
                                    mMax, false, nodeCache,
                                    selectedNeighbor.getNodeReferenceWithDistance().getVector()));
        }
    }

    private <N extends NodeReference> CompletableFuture<List<NodeReferenceAndNode<N>>> selectNeighbors(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                       @Nonnull final ReadTransaction readTransaction,
                                                                                                       @Nonnull final Iterable<NodeReferenceAndNode<N>> nearestNeighbors,
                                                                                                       final int layer,
                                                                                                       final int m,
                                                                                                       final boolean isExtendCandidates,
                                                                                                       @Nonnull final Map<Tuple, Node<N>> nodeCache,
                                                                                                       @Nonnull final Vector<Half> vector) {
        return extendCandidatesIfNecessary(storageAdapter, readTransaction, nearestNeighbors, layer, isExtendCandidates, nodeCache, vector)
                .thenApply(extendedCandidates -> {
                    final List<NodeReferenceWithDistance> selected = Lists.newArrayListWithExpectedSize(m);
                    final Queue<NodeReferenceWithDistance> candidates =
                            new PriorityBlockingQueue<>(config.getM(),
                                    Comparator.comparing(NodeReferenceWithDistance::getDistance));
                    candidates.addAll(extendedCandidates);
                    final Queue<NodeReferenceWithDistance> discardedCandidates =
                            getConfig().isKeepPrunedConnections()
                            ? new PriorityBlockingQueue<>(config.getM(),
                                    Comparator.comparing(NodeReferenceWithDistance::getDistance))
                            : null;

                    final Metric metric = getConfig().getMetric();

                    while (!candidates.isEmpty() && selected.size() < m) {
                        final NodeReferenceWithDistance nearestCandidate = candidates.poll();
                        boolean shouldSelect = true;
                        for (final NodeReferenceWithDistance alreadySelected : selected) {
                            if (Vector.comparativeDistance(metric, nearestCandidate.getVector(),
                                    alreadySelected.getVector()) < nearestCandidate.getDistance()) {
                                shouldSelect = false;
                                break;
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

                    return ImmutableList.copyOf(selected);
                }).thenCompose(selectedNeighbors ->
                        fetchSomeNodesIfNotCached(storageAdapter, readTransaction, layer, selectedNeighbors, nodeCache));
    }

    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithDistance>> extendCandidatesIfNecessary(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                                                     @Nonnull final Iterable<NodeReferenceAndNode<N>> candidates,
                                                                                                                     int layer,
                                                                                                                     boolean isExtendCandidates,
                                                                                                                     @Nonnull final Map<Tuple, Node<N>> nodeCache,
                                                                                                                     @Nonnull final Vector<Half> vector) {
        if (isExtendCandidates) {
            final Metric metric = getConfig().getMetric();

            final Set<Tuple> candidatesSeen = Sets.newConcurrentHashSet();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                candidatesSeen.add(candidate.getNode().getPrimaryKey());
            }

            final ImmutableList.Builder<N> neighborsOfCandidatesBuilder = ImmutableList.builder();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                for (final N neighbor : candidate.getNode().getNeighbors()) {
                    final Tuple neighborPrimaryKey = neighbor.getPrimaryKey();
                    if (!candidatesSeen.contains(neighborPrimaryKey)) {
                        candidatesSeen.add(neighborPrimaryKey);
                        neighborsOfCandidatesBuilder.add(neighbor);
                    }
                }
            }

            final Iterable<N> neighborsOfCandidates = neighborsOfCandidatesBuilder.build();

            return fetchNeighborhood(storageAdapter, readTransaction, layer, neighborsOfCandidates, nodeCache)
                    .thenApply(withVectors -> {
                        final ImmutableList.Builder<NodeReferenceWithDistance> extendedCandidatesBuilder = ImmutableList.builder();
                        for (final NodeReferenceAndNode<N> candidate : candidates) {
                            extendedCandidatesBuilder.add(candidate.getNodeReferenceWithDistance());
                        }

                        for (final NodeReferenceWithVector withVector : withVectors) {
                            final double distance = Vector.comparativeDistance(metric, withVector.getVector(), vector);
                            extendedCandidatesBuilder.add(new NodeReferenceWithDistance(withVector.getPrimaryKey(),
                                    withVector.getVector(), distance));
                        }
                        return extendedCandidatesBuilder.build();
                    });
        } else {
            final ImmutableList.Builder<NodeReferenceWithDistance> resultBuilder = ImmutableList.builder();
            for (final NodeReferenceAndNode<N> candidate : candidates) {
                resultBuilder.add(candidate.getNodeReferenceWithDistance());
            }

            return CompletableFuture.completedFuture(resultBuilder.build());
        }
    }

    private void writeLonelyNodes(@Nonnull final Transaction transaction,
                                  @Nonnull final Tuple primaryKey,
                                  @Nonnull final Vector<Half> vector,
                                  final int highestLayerInclusive,
                                  final int lowestLayerExclusive) {
        for (int layer = highestLayerInclusive; layer > lowestLayerExclusive; layer --) {
            final StorageAdapter<?> storageAdapter = getStorageAdapterForLayer(layer);
            writeLonelyNodeOnLayer(storageAdapter, transaction, layer, primaryKey, vector);
        }
    }

    private <N extends NodeReference> void writeLonelyNodeOnLayer(@Nonnull final StorageAdapter<N> storageAdapter,
                                                                  @Nonnull final Transaction transaction,
                                                                  final int layer,
                                                                  @Nonnull final Tuple primaryKey,
                                                                  @Nonnull final Vector<Half> vector) {
        storageAdapter.writeNode(transaction,
                storageAdapter.getNodeFactory()
                        .create(primaryKey, vector, ImmutableList.of()), layer,
                new BaseNeighborsChangeSet<>(ImmutableList.of()));
    }

    @Nonnull
    private StorageAdapter<? extends NodeReference> getStorageAdapterForLayer(final int layer) {
        return layer > 0
               ? new InliningStorageAdapter(getConfig(), InliningNode.factory(), getSubspace(), getOnWriteListener(), getOnReadListener())
               : new CompactStorageAdapter(getConfig(), CompactNode.factory(), getSubspace(), getOnWriteListener(), getOnReadListener());
    }

    private int insertionLayer(@Nonnull final Random random) {
        double lambda = 1.0 / Math.log(getConfig().getM());
        double u = 1.0 - random.nextDouble();  // Avoid log(0)
        return (int) Math.floor(-Math.log(u) * lambda);
    }
}
