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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
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
import java.util.function.Supplier;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class HNSW {
    private static final Logger logger = LoggerFactory.getLogger(HNSW.class);

    /**
     * root id. The root id is always only zeros.
     */
    static final byte[] rootId = new byte[16];

    public static final int MAX_CONCURRENT_READS = 16;

    /**
     * Indicator if we should maintain a secondary node index consisting of hilbet value and key to speed up
     * update/deletes.
     */
    public static final boolean DEFAULT_USE_NODE_SLOT_INDEX = false;

    /**
     * The minimum number of slots a node has (if not the root node). {@code M} should be chosen in a way that the
     * minimum is half of the maximum. That in turn guarantees that overflow/underflow handling can be performed without
     * causing further underflow/overflow.
     */
    public static final int DEFAULT_MIN_M = 16;
    /**
     * The maximum number of slots a node has. This value is derived from {@link #DEFAULT_MIN_M}.
     */
    public static final int DEFAULT_MAX_M = 2 * DEFAULT_MIN_M;

    /**
     * The magic split number. We split {@code S} to {@code S + 1} nodes while inserting data and fuse
     * {@code S + 1} to {@code S} nodes while deleting data. Academically, 2-to-3 splits and 3-to-2 fuses
     * seem to yield the best results. Please be aware of the following constraints:
     * <ol>
     *     <li>When splitting {@code S} to {@code S + 1} nodes, we re-distribute the children of {@code S} nodes
     *     into {@code S + 1} nodes which may cause an underflow if {@code S} and {@code M} are not set carefully with
     *     respect to each other. Example: {@code MIN_M = 25}, {@code MAX_M = 32}, {@code S = 2}, two nodes at
     *     already at maximum capacity containing a combined total of 64 children when a new child is inserted.
     *     We split the two nodes into three as indicated by {@code S = 2}. We have 65 children but there is no way
     *     of distributing them among three nodes such that none of them underflows. This constraint can be
     *     formulated as {@code S * MAX_M / (S + 1) >= MIN_M}.</li>
     *     <li>When fusing {@code S + 1} to {@code S} nodes, we re-distribute the children of {@code S + 1} nodes
     *     into {@code S + 1} nodes which may cause an overflow if {@code S} and {@code M} are not set carefully with
     *     respect to each other. Example: {@code MIN_M = 25}, {@code MAX_M = 32}, {@code S = 2}, three nodes at
     *     already at minimum capacity containing a combined total of 75 children when a child is deleted.
     *     We fuse the three nodes into two as indicated by {@code S = 2}. We have 75 children but there is no way
     *     of distributing them among two nodes such that none of them overflows. This constraint can be formulated as
     *     {@code (S + 1) * MIN_M / S <= MAX_M}.</li>
     * </ol>
     * Both constraints are in fact the same constraint and can be written as {@code MAX_M / MIN_M >= (S + 1) / S}.
     */
    public static final int DEFAULT_S = 2;

    /**
     * Default storage layout. Can be either {@code BY_SLOT} or {@code BY_NODE}. {@code BY_SLOT} encodes all information
     * pertaining to a {@link NodeSlot} as one key/value pair in the database; {@code BY_NODE} encodes all information
     * pertaining to a {@link Node} as one key/value pair in the database. While {@code BY_SLOT} avoids conflicts as
     * most inserts/updates only need to update one slot, it is by far less compact as some information is stored
     * in a normalized fashion and therefore repeated multiple times (i.e. node identifiers, etc.). {@code BY_NODE}
     * inlines slot information into the node leading to a more size-efficient layout of the data. That advantage is
     * offset by a higher likelihood of conflicts.
     */
    @Nonnull
    public static final Storage DEFAULT_STORAGE = Storage.BY_NODE;

    /**
     * Indicator if Hilbert values should be stored or not with the data (in leaf nodes). A Hilbert value can always
     * be recomputed from the point.
     */
    public static final boolean DEFAULT_STORE_HILBERT_VALUES = true;

    @Nonnull
    public static final Config DEFAULT_CONFIG = new Config();

    @Nonnull
    private final StorageAdapter storageAdapter;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Config config;
    @Nonnull
    private final Supplier<byte[]> nodeIdSupplier;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    /**
     * Different kinds of storage layouts.
     */
    public enum Storage {
        /**
         * Every node slot is serialized as a key/value pair in FDB.
         */
        BY_SLOT(BySlotStorageAdapter::new),
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
                                                 @Nonnull final Function<NodeReferenceWithDistance, BigInteger> hilbertValueFunction,
                                                 @Nonnull final OnWriteListener onWriteListener,
                                                 @Nonnull final OnReadListener onReadListener) {
            return storageAdapterCreator.create(config, subspace, nodeSlotIndexSubspace,
                    hilbertValueFunction, onWriteListener, onReadListener);
        }
    }

    /**
     * Functional interface to create a {@link StorageAdapter}.
     */
    private interface StorageAdapterCreator {
        StorageAdapter create(@Nonnull Config config, @Nonnull Subspace subspace, @Nonnull Subspace nodeSlotIndexSubspace,
                              @Nonnull Function<NodeReferenceWithDistance, BigInteger> hilbertValueFunction,
                              @Nonnull OnWriteListener onWriteListener,
                              @Nonnull OnReadListener onReadListener);
    }

    /**
     * Configuration settings for a {@link HNSW}.
     */
    public static class Config {
        private final boolean useNodeSlotIndex;
        private final int minM;
        private final int maxM;
        private final int splitS;
        @Nonnull
        private final Storage storage;

        private final boolean storeHilbertValues;

        protected Config() {
            this.useNodeSlotIndex = DEFAULT_USE_NODE_SLOT_INDEX;
            this.minM = DEFAULT_MIN_M;
            this.maxM = DEFAULT_MAX_M;
            this.splitS = DEFAULT_S;
            this.storage = DEFAULT_STORAGE;
            this.storeHilbertValues = DEFAULT_STORE_HILBERT_VALUES;
        }

        protected Config(final boolean useNodeSlotIndex, final int minM, final int maxM, final int splitS,
                         @Nonnull final Storage storage, final boolean storeHilbertValues) {
            this.useNodeSlotIndex = useNodeSlotIndex;
            this.minM = minM;
            this.maxM = maxM;
            this.splitS = splitS;
            this.storage = storage;
            this.storeHilbertValues = storeHilbertValues;
        }

        public boolean isUseNodeSlotIndex() {
            return useNodeSlotIndex;
        }

        public int getMinM() {
            return minM;
        }

        public int getMaxM() {
            return maxM;
        }

        public int getSplitS() {
            return splitS;
        }

        @Nonnull
        public Storage getStorage() {
            return storage;
        }

        public boolean isStoreHilbertValues() {
            return storeHilbertValues;
        }

        public Metric getMetric() {
            return Metric.euclideanMetric();
        }

        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(useNodeSlotIndex, minM, maxM, splitS, storage, storeHilbertValues);
        }

        @Override
        public String toString() {
            return storage + ", M=" + minM + "-" + maxM + ", S=" + splitS +
                   (useNodeSlotIndex ? ", slotIndex" : "") +
                   (storeHilbertValues ? ", storeHV" : "");
        }
    }

    /**
     * Builder for {@link Config}.
     *
     * @see #newConfigBuilder
     */
    @CanIgnoreReturnValue
    public static class ConfigBuilder {
        private boolean useNodeSlotIndex = DEFAULT_USE_NODE_SLOT_INDEX;
        private int minM = DEFAULT_MIN_M;
        private int maxM = DEFAULT_MAX_M;
        private int splitS = DEFAULT_S;
        @Nonnull
        private Storage storage = DEFAULT_STORAGE;
        private boolean storeHilbertValues = DEFAULT_STORE_HILBERT_VALUES;

        public ConfigBuilder() {
        }

        public ConfigBuilder(final boolean useNodeSlotIndex, final int minM, final int maxM, final int splitS,
                             @Nonnull final Storage storage, final boolean storeHilbertValues) {
            this.useNodeSlotIndex = useNodeSlotIndex;
            this.minM = minM;
            this.maxM = maxM;
            this.splitS = splitS;
            this.storage = storage;
            this.storeHilbertValues = storeHilbertValues;
        }

        public int getMinM() {
            return minM;
        }

        public ConfigBuilder setMinM(final int minM) {
            this.minM = minM;
            return this;
        }

        public int getMaxM() {
            return maxM;
        }

        public ConfigBuilder setMaxM(final int maxM) {
            this.maxM = maxM;
            return this;
        }

        public int getSplitS() {
            return splitS;
        }

        public ConfigBuilder setSplitS(final int splitS) {
            this.splitS = splitS;
            return this;
        }

        @Nonnull
        public Storage getStorage() {
            return storage;
        }

        public ConfigBuilder setStorage(@Nonnull final Storage storage) {
            this.storage = storage;
            return this;
        }

        public boolean isStoreHilbertValues() {
            return storeHilbertValues;
        }

        public ConfigBuilder setStoreHilbertValues(final boolean storeHilbertValues) {
            this.storeHilbertValues = storeHilbertValues;
            return this;
        }

        public boolean isUseNodeSlotIndex() {
            return useNodeSlotIndex;
        }

        public ConfigBuilder setUseNodeSlotIndex(final boolean useNodeSlotIndex) {
            this.useNodeSlotIndex = useNodeSlotIndex;
            return this;
        }

        public Config build() {
            return new Config(isUseNodeSlotIndex(), getMinM(), getMaxM(), getSplitS(), getStorage(), isStoreHilbertValues());
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
     * @param hilbertValueFunction function to compute the Hilbert value from a {@link NodeReferenceWithDistance}
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Subspace secondarySubspace,
                @Nonnull final Executor executor, @Nonnull final Function<NodeReferenceWithDistance, BigInteger> hilbertValueFunction) {
        this(subspace, secondarySubspace, executor, DEFAULT_CONFIG, hilbertValueFunction, NodeHelpers::newRandomNodeId,
                OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * Initialize a new R-tree.
     * @param subspace the subspace where the r-tree is stored
     * @param nodeSlotIndexSubspace the subspace where the node index (if used is stored)
     * @param executor an executor to use when running asynchronous tasks
     * @param config configuration to use
     * @param hilbertValueFunction function to compute the Hilbert value for a {@link NodeReferenceWithDistance}
     * @param nodeIdSupplier supplier to be invoked when new nodes are created
     * @param onWriteListener an on-write listener to be called after writes take place
     * @param onReadListener an on-read listener to be called after reads take place
     */
    public HNSW(@Nonnull final Subspace subspace, @Nonnull final Subspace nodeSlotIndexSubspace,
                @Nonnull final Executor executor, @Nonnull final Config config,
                @Nonnull final Function<NodeReferenceWithDistance, BigInteger> hilbertValueFunction,
                @Nonnull final Supplier<byte[]> nodeIdSupplier,
                @Nonnull final OnWriteListener onWriteListener,
                @Nonnull final OnReadListener onReadListener) {
        this.storageAdapter = config.getStorage()
                .newStorageAdapter(config, subspace, nodeSlotIndexSubspace, hilbertValueFunction, onWriteListener,
                        onReadListener);
        this.executor = executor;
        this.config = config;
        this.hilbertValueFunction = hilbertValueFunction;
        this.nodeIdSupplier = nodeIdSupplier;
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
                                                                                   final int efSearch,
                                                                                   @Nonnull final Vector<Half> queryVector) {
        return storageAdapter.fetchEntryNodeKey(readTransaction)
                .thenCompose(entryPointAndLayer -> {
                    if (entryPointAndLayer == null) {
                        return CompletableFuture.completedFuture(null); // not a single node in the index
                    }

                    final Metric metric = getConfig().getMetric();

                    final var entryState = new GreedyState(entryPointAndLayer.getLayer(),
                            entryPointAndLayer.getPrimaryKey(),
                            Vector.comparativeDistance(metric, entryPointAndLayer.getVector(), queryVector));
                    final AtomicReference<GreedyState> greedyStateReference =
                            new AtomicReference<>(entryState);

                    if (entryPointAndLayer.getLayer() == 0) {
                        // entry data points to a node in layer 0 directly
                        return CompletableFuture.completedFuture(entryState);
                    }

                    return AsyncUtil.whileTrue(() -> {
                        final var greedyIn = greedyStateReference.get();
                        return greedySearchLayer(readTransaction, greedyIn.toNodeReferenceWithDistance(),
                                greedyIn.getLayer(), queryVector)
                                .thenApply(greedyState -> {
                                    greedyStateReference.set(greedyState);
                                    return greedyState.getLayer() > 0;
                                });
                    }, executor).thenApply(ignored -> greedyStateReference.get());
                }).thenCompose(greedyState -> {
                    if (greedyState == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return searchLayer(CompactNode::creator, readTransaction,
                            ImmutableList.of(greedyState.toNodeReferenceWithDistance()), 0, efSearch,
                            queryVector);
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    private CompletableFuture<GreedyState> greedySearchLayer(@Nonnull final ReadTransaction readTransaction,
                                                             @Nonnull final NodeReferenceWithDistance entryNeighbor,
                                                             final int layer,
                                                             @Nonnull final Vector<Half> queryVector) {
        Verify.verify(layer > 0);
        final Metric metric = getConfig().getMetric();
        final AtomicReference<GreedyState> greedyStateReference =
                new AtomicReference<>(new GreedyState(layer, entryNeighbor.getPrimaryKey(), entryNeighbor.getDistance()));

        return AsyncUtil.whileTrue(() -> onReadListener.onAsyncRead(
                        storageAdapter.fetchNode(InliningNode::creator, readTransaction,
                                layer, greedyStateReference.get().getPrimaryKey()))
                .thenApply(node -> {
                    if (node == null) {
                        throw new IllegalStateException("unable to fetch node");
                    }
                    final InliningNode inliningNode = node.asInliningNode();
                    final List<NodeReferenceWithVector> neighbors = inliningNode.getNeighbors();

                    final GreedyState currentNodeKey = greedyStateReference.get();
                    double minDistance = currentNodeKey.getDistance();

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
                        greedyStateReference.set(
                                new GreedyState(layer - 1, currentNodeKey.getPrimaryKey(), minDistance));
                        return false;
                    }

                    greedyStateReference.set(
                            new GreedyState(layer, nearestNeighbor.getPrimaryKey(),
                                    minDistance));
                    return true;
                }), executor).thenApply(ignored -> greedyStateReference.get());
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<SearchResult<N>> searchLayer(@Nonnull Node.NodeCreator<N> creator,
                                                                                     @Nonnull final ReadTransaction readTransaction,
                                                                                     @Nonnull final List<NodeReferenceWithDistance> entryNeighbors,
                                                                                     final int layer,
                                                                                     final int efSearch,
                                                                                     @Nonnull final Vector<Half> queryVector) {
        final Set<Tuple> visited = Sets.newConcurrentHashSet(NodeReference.primaryKeys(entryNeighbors));
        final PriorityBlockingQueue<NodeReferenceWithDistance> candidates =
                new PriorityBlockingQueue<>(entryNeighbors.size(),
                        Comparator.comparing(NodeReferenceWithDistance::getDistance));
        candidates.addAll(entryNeighbors);
        final PriorityBlockingQueue<NodeReferenceWithDistance> nearestNeighbors =
                new PriorityBlockingQueue<>(entryNeighbors.size(),
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

            return fetchNodeIfNotCached(creator, readTransaction, layer, candidate, nodeCache)
                    .thenApply(candidateNode ->
                            Iterables.filter(candidateNode.getNeighbors(),
                                    neighbor -> !visited.contains(neighbor.getPrimaryKey())))
                    .thenCompose(neighborReferences -> neighborhood(creator, readTransaction,
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
        }).thenCompose(ignored -> fetchResultsIfNecessary(creator, readTransaction, layer, nearestNeighbors,
                nodeCache));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<Node<N>> fetchNodeIfNotCached(@Nonnull final Node.NodeCreator<N> creator,
                                                                                      @Nonnull final ReadTransaction readTransaction,
                                                                                      final int layer,
                                                                                      @Nonnull final NodeReference nodeReference,
                                                                                      @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchNodeIfNecessaryAndApply(creator, readTransaction, layer, nodeReference,
                nR -> nodeCache.get(nR.getPrimaryKey()),
                (ignored, node) -> node);
    }

    /**
     * TODO.
     */
    @Nonnull
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<U> fetchNodeIfNecessaryAndApply(@Nonnull final Node.NodeCreator<N> creator,
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
                        storageAdapter.fetchNode(creator, readTransaction, layer, nodeReference.getPrimaryKey()))
                .thenApply(node -> biMapFunction.apply(nodeReference, node));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<List<NodeReferenceWithVector>> neighborhood(@Nonnull final Node.NodeCreator<N> creator,
                                                                                                    @Nonnull final ReadTransaction readTransaction,
                                                                                                    final int layer,
                                                                                                    @Nonnull final Iterable<? extends NodeReference> neighborReferences,
                                                                                                    @Nonnull final Map<Tuple, Node<N>> nodeCache) {
        return fetchSomeNodesAndApply(creator, readTransaction, layer, neighborReferences,
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
                (neighborReference, neighborNode) ->
                        new NodeReferenceWithVector(neighborReference.getPrimaryKey(), neighborNode.asCompactNode().getVector()));
    }

    /**
     * TODO.
     */
    @Nonnull
    private <N extends NodeReference> CompletableFuture<SearchResult<N>> fetchResultsIfNecessary(@Nonnull final Node.NodeCreator<N> creator,
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
                SearchResult.NodeReferenceWithNode::new)
                .thenApply(nodeReferencesWithNodes -> {
                    final ImmutableMap.Builder<NodeReferenceWithDistance, Node<N>> nodeMapBuilder =
                            ImmutableMap.builder();
                    for (final SearchResult.NodeReferenceWithNode<N> nodeReferenceWithNode : nodeReferencesWithNodes) {
                        nodeMapBuilder.put(nodeReferenceWithNode.getNodeReferenceWithDistance(), nodeReferenceWithNode.getNode());
                    }
                    return new SearchResult<>(layer, nodeMapBuilder.build());
                });
    }

    /**
     * TODO.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    private <R extends NodeReference, N extends NodeReference, U> CompletableFuture<List<U>> fetchSomeNodesAndApply(@Nonnull final Node.NodeCreator<N> creator,
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
}
