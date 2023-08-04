/*
 * RTree.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of an R-tree. See <a href="https://en.wikipedia.org/wiki/R-tree">this</a>} link for a general
 * introduction to R-trees.
 * <br>
 * The main use-case for R-trees, while they are normally used for spatial querying of actual objects in N-dimensional
 * space, is to function as a natural extension of regular B+-tree-based indexes in FDB, but spanning into multiple
 * dimensions. That allows to answer queries using multiple inequalities which is not possible with 1-D indexes.
 * <br>
 * Here is a short introduction copied from the explanation at the linked wikipedia page. The key idea of the data
 * structure is to group nearby objects and represent them with their minimum bounding rectangle in the next higher
 * level of the tree; the "R" in R-tree is for rectangle. Since all objects lie within this bounding rectangle, a query
 * that does not intersect the bounding rectangle also cannot intersect any of the contained objects. At the leaf level,
 * each rectangle describes a single object; at higher levels the aggregation includes an increasing number of objects.
 * This can also be seen as an increasingly coarse approximation of the data set.
 * <br>
 * Similar to the B-tree, the R-tree is also a balanced search tree (so all leaf nodes are at the same depth),
 * organizes the data in pages/nodes, and is designed for storage on disk. Each node can contain a maximum number of
 * entries, often denoted as {@code M}. It also guarantees a minimum fill (except for the root node).
 * <br>
 * One of the key properties of an R-tree is that the minimum bounding rectangles (MBR) of the children of a node in the
 * tree may overlap which may cause multiple children to intersect with a query even if that query's mbr is just a
 * single point. An object is only stored in exactly one leaf node of the tree, however, during a search of the
 * tree multiple paths may have to exhaustively followed in order go find all matching objects of that query.
 * <br>
 * The search performance of the tree is strongly linked to the size of the area being covered by a child (as indicated
 * by a child's mbr) as well as the overlap among children's mbrs at each level of the tree. The key difficulty in
 * constructing a search-efficient tree is to minimize both covered area and the overlap while keeping the tree balanced.
 * Variants of the R-tree such as R+-trees and R*-trees employ different techniques to improve on the basic R-tree ideas
 * and are provably superior with respect to packing of the data structure as well as search performance. These
 * improvements are accomplished by a more complex write path; R+-trees strive to eliminate overlap altogether which
 * becomes more problematic with higher dimensionality while R*-trees attempt to minimize both the covered area by a node
 * and the sibling overlap by approximations as well as re-insertions in order to avoid node-splits. For more information
 * about R+-trees see
 * <a href="http://www.cs.cmu.edu/~christos/PUBLICATIONS.OLDER/vldb87.pdf">R+-tree</a>,
 * for more information about R*-trees see
 * <a href="https://infolab.usc.edu/csci599/Fall2001/paper/rstar-tree.pdf">R*-tree</a>.
 * <br>
 * All variants of R-tree mentioned so far have a fatal flaw when considered in context with FDB and specifically the
 * record layer. None of the R-tree variants can return their elements in a stable order that is not sensitive to
 * the physical layout of the tree at query time. That proves to be problematic for queries that are continued at a
 * later time as the physical structure of the tree may have changed due to re-balancing. Thus, it would become necessary
 * to encode all already returned items into the continuation which is simply not feasible.
 * <br>
 * Another variant (the one we implement here) is a Hilbert R-tree. See
 * <a href="https://www.vldb.org/conf/1994/P500.PDF">Hilbert R-tree</a> for details. In short, the Hilbert R-tree,
 * in addition to being a regular R-tree, also utilizes the Hilbert value
 * (<a href="https://github.com/davidmoten/hilbert-curve">Hilbert value</a>) of the center of an mbr of an object
 * (or the point itself if the object is a point) to establish an ordering among objects and nodes stored in the tree.
 * All traversals of the tree return objects in Hilbert Value order. The Hilbert value usually is a {@link BigInteger}
 * that can be encoded into the continuation of a query thus overcoming the fundamental problems plaguing other
 * variants of the R-trees as mentioned above. In addition to a stable and logical traversal order, the Hilbert
 * value is used to naturally cluster the tree as similar values in Hilbert space map to nearby points in N-dimensional
 * Euclidean space. Lastly, the Hilbert value is also used to avoid eager node-splitting during insertions as well as
 * eager node-fusing during deletions as it defines a natural order between siblings. A node can <em>transfer</em> empty
 * slots from their siblings (for insertions) or children (for deletions). In this way the tree is packed more tightly
 * and costly re-balancing can be avoided while we still do not have to resort to re-insertions of overflowing children.
 * <br>
 * Clustering based on the Hilbert value has been proven to be superior compared to R-trees, R+-trees, and R*-trees.
 * A disadvantage of a Hilbert R-tree is the definition of the canvas the Hilbert curve is defined over. While there
 * are ways to define a Hilbert curve for floating point coordinates, we cannot support variable length dimensions such
 * as strings. In fact, we only support INT32 and INT64 dimensions.
 */
@API(API.Status.EXPERIMENTAL)
public class RTree {
    private static final Logger logger = LoggerFactory.getLogger(RTree.class);

    /**
     * root id. The root id is always only zeros.
     */
    private static final byte[] rootId = new byte[16];

    public static final int NODE_ID_LENGTH = 16;
    public static final int MAX_CONCURRENT_READS = 16;

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

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
     * pertaining to a {@link NodeSlot} as one key/value pair in the database, {@code BY_NODE} encodes all information
     * pertaining to a {@link Node} as one key/value pair in the database. While {@code BY_SLOT} avoids conflicts as
     * most inserts/updates only need to update one slot, it is by far less compact as some information is stored
     * in a normalized fashion and therefore repeated multiple times (i.e. node identifiers, etc.). {@code BY_NODE}
     * inlines slot information into the node leading to a more size-efficient layout of the data. That advantage is
     * offset by a higher likelihood of conflicts.
     */
    @Nonnull
    public static final Storage DEFAULT_STORAGE = Storage.BY_SLOT;

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
    private final Function<Point, BigInteger> hilbertValueFunction;
    @Nonnull
    private final Supplier<byte[]> nodeIdSupplier;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    /**
     * Only used for debugging to keep node ids readable.
     */
    private static final AtomicLong nodeIdState = new AtomicLong(1); // skip the root which is always 0

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
        public StorageAdapter newStorageAdapter(@Nonnull final Subspace subspace,
                                                final boolean storeHilbertValues,
                                                @Nonnull final Function<Point, BigInteger> hilbertValueFunction,
                                                @Nonnull final OnWriteListener onWriteListener,
                                                @Nonnull final OnReadListener onReadListener) {
            return storageAdapterCreator.create(subspace, storeHilbertValues, hilbertValueFunction,
                    onWriteListener, onReadListener);
        }
    }

    /**
     * Functional interface to create a {@link StorageAdapter}.
     */
    public interface StorageAdapterCreator {
        StorageAdapter create(@Nonnull Subspace subspace,
                              boolean storeHilbertValues,
                              @Nonnull Function<Point, BigInteger> hilbertValueFunction,
                              @Nonnull OnWriteListener onWriteListener,
                              @Nonnull OnReadListener onReadListener);
    }

    /**
     * Configuration settings for a {@link RTree}.
     */
    public static class Config {
        private final int minM;
        private final int maxM;
        private final int splitS;
        @Nonnull
        private final Storage storage;

        private final boolean storeHilbertValues;

        protected Config() {
            this.minM = DEFAULT_MIN_M;
            this.maxM = DEFAULT_MAX_M;
            this.splitS = DEFAULT_S;
            this.storage = DEFAULT_STORAGE;
            this.storeHilbertValues = DEFAULT_STORE_HILBERT_VALUES;
        }

        protected Config(final int minM, final int maxM, final int splitS, @Nonnull final Storage storage,
                         final boolean storeHilbertValues) {
            this.minM = minM;
            this.maxM = maxM;
            this.splitS = splitS;
            this.storage = storage;
            this.storeHilbertValues = storeHilbertValues;
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

        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(minM, maxM, splitS, storage, storeHilbertValues);
        }
    }

    /**
     * Builder for {@link Config}.
     *
     * @see #newConfigBuilder
     */
    public static class ConfigBuilder {
        private int minM = DEFAULT_MIN_M;
        private int maxM = DEFAULT_MAX_M;
        private int splitS = DEFAULT_S;
        @Nonnull
        private Storage storage = DEFAULT_STORAGE;
        private boolean storeHilbertValues = DEFAULT_STORE_HILBERT_VALUES;

        protected ConfigBuilder() {
        }

        protected ConfigBuilder(final int minM, final int maxM, final int splitS, @Nonnull final Storage storage,
                                final boolean storeHilbertValues) {
            this.minM = minM;
            this.maxM = maxM;
            this.splitS = splitS;
            this.storage = storage;
            this.storeHilbertValues = storeHilbertValues;
        }

        public int getMinM() {
            return minM;
        }

        public void setMinM(final int minM) {
            this.minM = minM;
        }

        public int getMaxM() {
            return maxM;
        }

        public void setMaxM(final int maxM) {
            this.maxM = maxM;
        }

        public int getSplitS() {
            return splitS;
        }

        public void setSplitS(final int splitS) {
            this.splitS = splitS;
        }

        @Nonnull
        public Storage getStorage() {
            return storage;
        }

        public void setStorage(@Nonnull final Storage storage) {
            this.storage = storage;
        }

        public boolean isStoreHilbertValues() {
            return storeHilbertValues;
        }

        public void setStoreHilbertValues(final boolean storeHilbertValues) {
            this.storeHilbertValues = storeHilbertValues;
        }

        public Config build() {
            return new Config(getMinM(), getMaxM(), getSplitS(), getStorage(), isStoreHilbertValues());
        }
    }

    /**
     * Start building a {@link Config}.
     * @return a new {@code Config} that can be altered and then built for use with a {@link RTree}
     * @see ConfigBuilder#build
     */
    public static ConfigBuilder newConfigBuilder() {
        return new ConfigBuilder();
    }

    /**
     * Initialize a new R-tree with the default configuration.
     * @param subspace the subspace where the r-tree is stored
     * @param executor an executor to use when running asynchronous tasks
     * @param hilbertValueFunction function to compute the Hilbert value from a {@link Point}
     */
    public RTree(@Nonnull final Subspace subspace, @Nonnull final Executor executor,
                 @Nonnull final Function<Point, BigInteger> hilbertValueFunction) {
        this(subspace, executor, DEFAULT_CONFIG, hilbertValueFunction, RTree::newRandomNodeId,
                OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * Initialize a new R-tree.
     * @param subspace the subspace where the r-tree is stored
     * @param executor an executor to use when running asynchronous tasks
     * @param config configuration to use
     * @param hilbertValueFunction function to compute the Hilbert value for a {@link Point}
     * @param nodeIdSupplier supplier to be invoked when new nodes are created
     * @param onWriteListener an on-write listener to be called after writes take place
     * @param onReadListener an on-read listener to be called after reads take place
     */
    public RTree(@Nonnull final Subspace subspace, @Nonnull final Executor executor, @Nonnull final Config config,
                 @Nonnull final Function<Point, BigInteger> hilbertValueFunction,
                 @Nonnull final Supplier<byte[]> nodeIdSupplier,
                 @Nonnull final OnWriteListener onWriteListener,
                 @Nonnull final OnReadListener onReadListener) {
        this.storageAdapter = config.getStorage()
                .newStorageAdapter(subspace, config.isStoreHilbertValues(), hilbertValueFunction, onWriteListener,
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
    public StorageAdapter getStorageAdapter() {
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
     * @return an {@link AsyncIterator} of {@link ItemSlot}s.
     */
    @Nonnull
    public AsyncIterator<ItemSlot> scan(@Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final Predicate<Rectangle> mbrPredicate) {
        return scan(readTransaction, null, null, mbrPredicate);
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
     * @return an {@link AsyncIterator} of {@link ItemSlot}s.
     */
    @Nonnull
    public AsyncIterator<ItemSlot> scan(@Nonnull final ReadTransaction readTransaction,
                                        @Nullable final BigInteger lastHilbertValue,
                                        @Nullable final Tuple lastKey,
                                        @Nonnull final Predicate<Rectangle> mbrPredicate) {
        Preconditions.checkArgument((lastHilbertValue == null && lastKey == null) ||
                                    (lastHilbertValue != null && lastKey != null));
        AsyncIterator<LeafNode> leafIterator =
                new LeafIterator(readTransaction, rootId, lastHilbertValue, lastKey, mbrPredicate);
        return new ItemSlotIterator(leafIterator);
    }

    /**
     * Returns the left-most path from a given node id to a leaf node containing items as a {@link TraversalState}.
     * The term <em>left-most</em> used here is defined by comparing {@code (largestHilbertValue, largestKey)} when
     * comparing nodes (the left one being the smaller, the right one being the greater).
     * @param readTransaction the transaction to use 
     * @param nodeId node id to start from. This may be the actual root of the tree or some other node within the tree.
     * @param mbrPredicate a predicate on an mbr {@link Rectangle}. This predicate is evaluated on the way down to the
     *        leaf node.
     * @return a {@link TraversalState} of the left-most path from {@code nodeId} to a {@link LeafNode} whose
     *         {@link Node}s all pass the mbr predicate test.
     */
    @Nonnull
    private CompletableFuture<TraversalState> fetchLeftmostPathToLeaf(@Nonnull final ReadTransaction readTransaction,
                                                                      @Nonnull final byte[] nodeId,
                                                                      @Nullable final BigInteger lastHilbertValue,
                                                                      @Nullable final Tuple lastKey,
                                                                      @Nonnull final Predicate<Rectangle> mbrPredicate) {
        final AtomicReference<byte[]> currentId = new AtomicReference<>(nodeId);
        final List<Deque<ChildSlot>> toBeProcessed = Lists.newArrayList();
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> onReadListener.onAsyncRead(fetchNode(readTransaction, currentId.get()))
                .thenApply(node -> {
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final List<ChildSlot> childSlots = ((IntermediateNode)node).getSlots();
                        Deque<ChildSlot> toBeProcessedThisLevel = new ArrayDeque<>();
                        for (Iterator<ChildSlot> iterator = childSlots.iterator(); iterator.hasNext(); ) {
                            final ChildSlot childSlot = iterator.next();
                            if (lastHilbertValue != null &&
                                    lastKey != null) {
                                final int hilbertValueAndKeyCompare = childSlot.compareHilbertValueAndKey(lastHilbertValue, lastKey);
                                if (hilbertValueAndKeyCompare < 0) {
                                    //
                                    // The (lastHilbertValue, lastKey) pair is larger than the
                                    // (largestHilbertValue, largestKey) pair of the current child. Advance to the next
                                    // child.
                                    //
                                    continue;
                                }
                            }

                            if (mbrPredicate.test(childSlot.getMbr())) {
                                toBeProcessedThisLevel.addLast(childSlot);
                                iterator.forEachRemaining(toBeProcessedThisLevel::addLast);
                            }
                        }
                        toBeProcessed.add(toBeProcessedThisLevel);

                        final ChildSlot nextChildSlot = resolveNextIdForFetch(toBeProcessed, mbrPredicate);
                        if (nextChildSlot == null) {
                            return false;
                        }

                        currentId.set(Objects.requireNonNull(nextChildSlot.getChildId()));
                        return true;
                    } else {
                        leafNode.set((LeafNode)node);
                        return false;
                    }
                }), executor).thenApply(vignore -> leafNode.get() == null
                                                   ? TraversalState.end()
                                                   : TraversalState.of(toBeProcessed, leafNode.get()));
    }

    /**
     * Returns the next left-most path from a given {@link TraversalState} to a leaf node containing items as
     * a {@link TraversalState}. The term <em>left-most</em> used here is defined by comparing
     * {@code (largestHilbertValue, largestKey)} when comparing nodes (the left one being the smaller, the right one
     * being the greater).
     * @param readTransaction the transaction to use
     * @param traversalState traversal state to start from. The initial traversal state is always obtained by initially
     *        calling {@link #fetchLeftmostPathToLeaf(ReadTransaction, byte[], BigInteger, Tuple, Predicate)}.
     * @param mbrPredicate a predicate on an mbr {@link Rectangle}. This predicate is evaluated for each node that
     *        is processed.
     * @return a {@link TraversalState} of the left-most path from {@code nodeId} to a {@link LeafNode} whose
     *         {@link Node}s all pass the mbr predicate test.
     */
    @Nonnull
    private CompletableFuture<TraversalState> fetchNextPathToLeaf(@Nonnull final ReadTransaction readTransaction,
                                                                  @Nonnull final TraversalState traversalState,
                                                                  @Nullable final BigInteger lastHilbertValue,
                                                                  @Nullable final Tuple lastKey,
                                                                  @Nonnull final Predicate<Rectangle> mbrPredicate) {

        final List<Deque<ChildSlot>> toBeProcessed = traversalState.getToBeProcessed();
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);

        return AsyncUtil.whileTrue(() -> {
            final ChildSlot nextChildSlot = resolveNextIdForFetch(toBeProcessed, mbrPredicate);
            if (nextChildSlot == null) {
                return AsyncUtil.READY_FALSE;
            }

            // fetch the left-most path rooted at the current child to its left-most leaf and concatenate the paths
            return fetchLeftmostPathToLeaf(readTransaction, nextChildSlot.getChildId(), lastHilbertValue,
                    lastKey, mbrPredicate)
                    .thenApply(nestedTraversalState -> {
                        if (nestedTraversalState.isEnd()) {
                            // no more data in this subtree
                            return true;
                        }
                        // combine the traversal states
                        leafNode.set(nestedTraversalState.getCurrentLeafNode());
                        toBeProcessed.addAll(nestedTraversalState.getToBeProcessed());
                        return false;
                    });
        }, executor).thenApply(v -> leafNode.get() == null
                                    ? TraversalState.end()
                                    : TraversalState.of(toBeProcessed, leafNode.get()));
    }

    /**
     * Return the next {@link ChildSlot} that needs to be processed given a list of deques that need to be processed
     * as part of the current scan.
     * @param toBeProcessed list of deques
     * @param mbrPredicate a predicate on an mbr {@link Rectangle}
     * @return The next child slot that needs to be processed or {@code null} if there is no next child slot.
     *         As a side effect of calling this method the child slot is removed from {@code toBeProcessed}.
     */
    @Nullable
    private static ChildSlot resolveNextIdForFetch(@Nonnull final List<Deque<ChildSlot>> toBeProcessed,
                                                   @Nonnull final Predicate<Rectangle> mbrPredicate) {
        for (int level = toBeProcessed.size() - 1; level >= 0; level--) {
            final Deque<ChildSlot> toBeProcessedThisLevel = toBeProcessed.get(level);

            while (!toBeProcessedThisLevel.isEmpty()) {
                final ChildSlot nextChild = toBeProcessedThisLevel.pollFirst();
                if (mbrPredicate.test(nextChild.getMbr())) {
                    toBeProcessed.subList(level + 1, toBeProcessed.size()).clear();
                    return nextChild;
                }
            }
        }
        return null;
    }

    //
    // Insert/Update path
    //


    /**
     * Method to insert an object/item into the R-tree. The item is treated unique per its point in space as well as its
     * additional key that is also passed in. The Hilbert value of the point is passed in as to allow the caller to
     * compute Hilbert values themselves. Note that there is a bijective mapping between point and Hilbert
     * value which allows us to recompute point from Hilbert value as well as Hilbert value from point. We currently
     * treat point and Hilbert value independent, however, they are redundant and not independent at all. The implication
     * is that we do not have to store both point and Hilbert value (but we currently do).
     * @param tc transaction context
     * @param point the point to be used in space
     * @param keySuffix the additional key to be stored with the item
     * @param value the additional value to be stored with the item
     * @return a completable future that completes when the insert is completed
     */
    @Nonnull
    public CompletableFuture<Void> insertOrUpdate(@Nonnull final TransactionContext tc,
                                                  @Nonnull final Point point,
                                                  @Nonnull final Tuple keySuffix,
                                                  @Nonnull final Tuple value) {
        final BigInteger hilbertValue = hilbertValueFunction.apply(point);
        final Tuple itemKey = Tuple.from(point.getCoordinates(), keySuffix);

        //
        // Get to the leaf node we need to start the insert from and then call the appropriate method to perform
        // the actual insert/update.
        //
        return tc.runAsync(transaction -> fetchUpdatePathToLeaf(transaction, hilbertValue, itemKey)
                .thenCompose(leafNode -> insertOrUpdateSlot(transaction, leafNode, point, hilbertValue, itemKey, value)));
    }

    /**
     * Inserts a new slot into the {@link LeafNode} passed in or updates an existing slot of the {@link LeafNode} passed
     * in.
     * @param transaction transaction
     * @param targetNode leaf node that is the target of this insert or update
     * @param point the point to be used in space
     * @param hilbertValue the hilbert value of the point
     * @param key the additional key to be stored with the item
     * @param value the additional value to be stored with the item
     * @return a completable future that completes when the insert/update is completed
     */
    @Nonnull
    private CompletableFuture<Void> insertOrUpdateSlot(@Nonnull final Transaction transaction,
                                                       @Nonnull final LeafNode targetNode,
                                                       @Nonnull final Point point,
                                                       @Nonnull final BigInteger hilbertValue,
                                                       @Nonnull final Tuple key,
                                                       @Nonnull final Tuple value) {
        Verify.verify(targetNode.getSlots().size() <= config.getMaxM());

        final NodeSlot newSlot = new ItemSlot(hilbertValue, point, key, value);
        final AtomicInteger insertSlotIndex = new AtomicInteger(findInsertUpdateItemSlotIndex(targetNode, hilbertValue, key));
        if (insertSlotIndex.get() < 0) {
            // just update the slot with the potentially new value
            storageAdapter.writeNodeSlot(transaction, targetNode, newSlot);
            return AsyncUtil.DONE;
        }

        //
        // This is an insert.
        //

        final AtomicReference<Node> currentNode = new AtomicReference<>(targetNode);
        final AtomicReference<NodeSlot> parentSlot = new AtomicReference<>(newSlot);

        //
        // Inch our way upwards in the tree to perform the necessary adjustments. What needs to be done next
        // is informed by the result of the current operation:
        // 1. A split happened; we need to insert a new slot into the parent node -- prime current node and
        //    current slot and continue.
        // 2. The slot was inserted but mbrs, largest Hilbert Values and largest Keys need to be adjusted upwards.
        // 3. We are done as no further adjustments are necessary.
        //
        return AsyncUtil.whileTrue(() -> {
            final NodeSlot currentNewSlot = parentSlot.get();

            if (currentNewSlot != null) {
                return insertSlotIntoTargetNode(transaction, currentNode.get(), currentNewSlot, insertSlotIndex.get())
                        .thenApply(nodeOrAdjust -> {
                            if (currentNode.get().isRoot()) {
                                return false;
                            }
                            currentNode.set(currentNode.get().getParentNode());
                            parentSlot.set(nodeOrAdjust.getSlotInParent());
                            insertSlotIndex.set(nodeOrAdjust.getSplitNode() == null ? -1 : nodeOrAdjust.getSplitNode().getSlotIndexInParent());
                            return nodeOrAdjust.getSplitNode() != null || nodeOrAdjust.parentNeedsAdjustment();
                        });
            } else {
                // adjustment only
                final NodeOrAdjust splitNodeSlotOrAdjust = updateSlotsAndAdjustNode(transaction, currentNode.get());
                Verify.verify(splitNodeSlotOrAdjust.getSlotInParent() == null);
                if (currentNode.get().isRoot()) {
                    return AsyncUtil.READY_FALSE;
                }
                currentNode.set(currentNode.get().getParentNode());
                return splitNodeSlotOrAdjust.parentNeedsAdjustment()
                       ? AsyncUtil.READY_TRUE
                       : AsyncUtil.READY_FALSE;
            }
        }, executor);
    }

    /**
     * Insert a new slot into the target node passed in.
     * @param transaction transaction
     * @param targetNode target node
     * @param newSlot new slot
     * @param slotIndexInTargetNode The index of the new slot that we should use when inserting the new slot. While
     *        this information can be computed from the other arguments passed in, the caller already knows this
     *        information; we can avoid searching for the proper spot on our own.
     * @return a completable future that when completed indicates what needs to be done next (see {@link NodeOrAdjust}).
     */
    @Nonnull
    private CompletableFuture<NodeOrAdjust> insertSlotIntoTargetNode(@Nonnull final Transaction transaction,
                                                                     @Nonnull final Node targetNode,
                                                                     @Nonnull final NodeSlot newSlot,
                                                                     final int slotIndexInTargetNode) {
        if (targetNode.getSlots().size() < config.getMaxM()) {
            // enough space let in target

            if (logger.isTraceEnabled()) {
                logger.trace("regular insert without splitting; node={}; size={}", bytesToHex(targetNode.getId()), targetNode.size());
            }
            targetNode.insertSlot(slotIndexInTargetNode, newSlot);

            if (targetNode.getKind() == Kind.INTERMEDIATE) {
                //
                // If this is an insert for an intermediate node, the child node referred to by newSlot
                // is a split node from a lower level meaning a split has happened on a lower level and the
                // participating siblings of that split have potentially changed. We resort to re-persisting all slots
                // of the target node. TODO This can be optimized in the future.
                //
                storageAdapter.writeNodes(transaction, Collections.singletonList(targetNode));
            } else {
                // if this is an insert for a leaf node we can just write the slot
                Verify.verify(targetNode.getKind() == Kind.LEAF);
                storageAdapter.writeNodeSlot(transaction, targetNode, newSlot);
            }

            // node has left some space -- indicate that we are done splitting at the current node
            if (!targetNode.isRoot()) {
                return CompletableFuture.completedFuture(adjustSlotInParent(targetNode)
                                                         ? NodeOrAdjust.ADJUST
                                                         : NodeOrAdjust.NONE);
            }

            // no split and no adjustment
            return CompletableFuture.completedFuture(NodeOrAdjust.NONE);
        } else {
            //
            // If this is the root we need to grow the tree taller by splitting the root to get a new root
            // with two children each containing half of the slots previously contained by the old root node.
            //
            if (targetNode.isRoot()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("splitting root node; size={}", targetNode.size());
                }
                // temporarily overfill the old root node
                targetNode.insertSlot(slotIndexInTargetNode, newSlot);
                splitRootNode(transaction, targetNode);
                return CompletableFuture.completedFuture(NodeOrAdjust.NONE);
            }

            //
            // Node is full -- borrow some space from the siblings if possible. The paper does overflow handling and
            // node splitting separately -- we do it in one path.
            //
            final CompletableFuture<List<Node>> siblings =
                    fetchSiblings(transaction, targetNode);

            return siblings.thenApply(siblingNodes -> {
                int numSlots =
                        Math.toIntExact(siblingNodes
                                .stream()
                                .mapToLong(siblingNode -> siblingNode.getSlots().size())
                                .sum());

                // First determine if we actually need to split; create the split node if we do; for the remainder of
                // this method splitNode != null <=> we are splitting; otherwise we handle overflow.
                final Node splitNode;
                final List<Node> newSiblingNodes;
                if (numSlots == siblingNodes.size() * config.getMaxM()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("splitting node; node={}, siblings={}",
                                bytesToHex(targetNode.getId()),
                                siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    }
                    splitNode = targetNode.newOfSameKind(nodeIdSupplier.get());
                    // link this split node to become the last node of the siblings
                    splitNode.linkToParent(Objects.requireNonNull(targetNode.getParentNode()),
                            siblingNodes.get(siblingNodes.size() - 1).getSlotIndexInParent() + 1);
                    newSiblingNodes = Lists.newArrayList(siblingNodes);
                    newSiblingNodes.add(splitNode);
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("handling overflow; node={}, numSlots={}, siblings={}",
                                bytesToHex(targetNode.getId()),
                                numSlots,
                                siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    }
                    splitNode = null;
                    newSiblingNodes = siblingNodes;
                }

                // temporarily overfill targetNode
                numSlots++;
                targetNode.insertSlot(slotIndexInTargetNode, newSlot);

                // sibling nodes are in hilbert value order
                final Iterator<? extends NodeSlot> slotIterator =
                        siblingNodes
                                .stream()
                                .flatMap(siblingNode -> siblingNode.getSlots().stream())
                                .iterator();

                //
                // Distribute all slots (including the new one which is now at its correct position among its brethren)
                // across all siblings (which includes the targetNode and (if we are splitting) the splitNode).
                // At the end of this modification all siblings have and (almost) equal count of slots that is
                // guaranteed to be between minM and maxM.
                //

                final int base = numSlots / newSiblingNodes.size();
                int rest = numSlots % newSiblingNodes.size();

                List<List<NodeSlot>> newNodeSlotLists = Lists.newArrayList();
                List<NodeSlot> currentNodeSlots = Lists.newArrayList();
                while (slotIterator.hasNext()) {
                    final NodeSlot slot = slotIterator.next();
                    currentNodeSlots.add(slot);
                    if (currentNodeSlots.size() == base + (rest > 0 ? 1 : 0)) {
                        if (rest > 0) {
                            // one fewer to distribute
                            rest--;
                        }

                        newNodeSlotLists.add(currentNodeSlots);
                        currentNodeSlots = Lists.newArrayList();
                    }
                }

                Verify.verify(newSiblingNodes.size() == newNodeSlotLists.size());

                final Iterator<Node> newSiblingNodesIterator = newSiblingNodes.iterator();
                final Iterator<List<NodeSlot>> newNodeSlotsIterator = newNodeSlotLists.iterator();

                // assign slots to nodes
                while (newSiblingNodesIterator.hasNext()) {
                    final Node newSiblingNode = newSiblingNodesIterator.next();
                    Verify.verify(newNodeSlotsIterator.hasNext());
                    final List<NodeSlot> newNodeSlots = newNodeSlotsIterator.next();
                    newSiblingNode.setSlots(newNodeSlots);
                }

                // update nodes
                storageAdapter.writeNodes(transaction, newSiblingNodes);

                //
                // Adjust the parent's slot information in memory only; we'll write it in the next iteration when
                // we go one level up.
                //
                for (final Node siblingNode : siblingNodes) {
                    adjustSlotInParent(siblingNode);
                }

                if (splitNode == null) {
                    // didn't split -- just continue adjusting
                    return NodeOrAdjust.ADJUST;
                }

                //
                // Manufacture a new slot for the splitNode;the caller will then use that slot to insert it into the
                // parent.
                //
                final var lastSlotOfSplitNode = splitNode.getSlots().get(splitNode.size() - 1);
                return new NodeOrAdjust(new ChildSlot(lastSlotOfSplitNode.getHilbertValue(),
                        lastSlotOfSplitNode.getKey(), splitNode.getId(), computeMbr(splitNode.getSlots())), splitNode,
                        true);
            });
        }
    }

    /**
     * Split the root node. This method first creates two nodes {@code left} and {@code right}. The root node,
     * whose ID is always a string of {@code 0x00}, contains some number {@code n} of slots. {@code n / 2} slots of those
     * {@code n} slots are moved to {@code left}, the rest to {@code right}. The root node is then updated to have two
     * children: {@code left} and {@code right}. All three nodes are then updated in the database.
     * @param transaction transaction to use
     * @param oldRootNode the old root node
     */
    private void splitRootNode(@Nonnull final Transaction transaction,
                               @Nonnull final Node oldRootNode) {
        final Node leftNode = oldRootNode.newOfSameKind(nodeIdSupplier.get());
        final Node rightNode = oldRootNode.newOfSameKind(nodeIdSupplier.get());
        final int leftSize = oldRootNode.size() / 2;
        final ArrayList<? extends NodeSlot> leftSlots = Lists.newArrayList(oldRootNode.getSlots().subList(0, leftSize));
        leftNode.setSlots(leftSlots);
        final int rightSize = oldRootNode.size() - leftSize;
        final ArrayList<? extends NodeSlot> rightSlots = Lists.newArrayList(oldRootNode.getSlots().subList(leftSize, leftSize + rightSize));
        rightNode.setSlots(rightSlots);

        final NodeSlot lastSlotOfLeftNode = leftSlots.get(leftSlots.size() - 1);
        final NodeSlot lastSlotOfRightNode = rightSlots.get(rightSlots.size() - 1);

        final ArrayList<ChildSlot> rootNodeSlots =
                Lists.newArrayList(
                        new ChildSlot(lastSlotOfLeftNode.getHilbertValue(), lastSlotOfLeftNode.getKey(), leftNode.getId(),
                                computeMbr(leftNode.getSlots())),
                        new ChildSlot(lastSlotOfRightNode.getHilbertValue(), lastSlotOfRightNode.getKey(), rightNode.getId(),
                                computeMbr(rightNode.getSlots())));
        final IntermediateNode newRootNode = new IntermediateNode(rootId, rootNodeSlots);

        storageAdapter.writeNodes(transaction, Lists.newArrayList(leftNode, rightNode, newRootNode));
    }

    // Delete Path

    /**
     * Method to delete from the R-tree. The item is treated unique per its point in space as well as its
     * additional key that is passed in.
     * @param tc transaction context
     * @param point the point
     * @param keySuffix the additional key to be stored with the item
     * @return a completable future that completes when the insert is completed
     */
    @Nonnull
    public CompletableFuture<Void> delete(@Nonnull final TransactionContext tc,
                                          @Nonnull final Point point,
                                          @Nonnull final Tuple keySuffix) {
        final BigInteger hilbertValue = hilbertValueFunction.apply(point);
        final Tuple itemKey = Tuple.from(point.getCoordinates(), keySuffix);

        //
        // Get to the leaf node we need to start the delete operation from and then call the appropriate method to
        // perform the actual delete.
        //
        return tc.runAsync(transaction -> fetchUpdatePathToLeaf(transaction, hilbertValue, itemKey)
                .thenCompose(leafNode -> deleteSlotIfExists(transaction, leafNode, hilbertValue, itemKey)));
    }

    /**
     * Deletes a slot from the {@link LeafNode} passed or exits if the slot could not be found in the target node.
     * in.
     * @param transaction transaction
     * @param targetNode leaf node that is the target of this insert or update
     * @param hilbertValue the hilbert value of the point
     * @param key the additional key to be stored with the item
     * @return a completable future that completes when the delete is completed
     */
    @Nonnull
    private CompletableFuture<Void> deleteSlotIfExists(@Nonnull final Transaction transaction,
                                                       @Nonnull final LeafNode targetNode,
                                                       @Nonnull final BigInteger hilbertValue,
                                                       @Nonnull final Tuple key) {
        Verify.verify(targetNode.getSlots().size() <= config.getMaxM());

        final AtomicInteger deleteSlotIndex = new AtomicInteger(findDeleteItemSlotIndex(targetNode, hilbertValue, key));
        if (deleteSlotIndex.get() < 0) {
            //
            // The slot was not found meaning that the item was not found and that means we don't have to do anything
            // here.
            //
            return AsyncUtil.DONE;
        }

        //
        // We found the slot and therefore the item.
        //

        final NodeSlot deleteSlot = targetNode.getSlots().get(deleteSlotIndex.get());
        final AtomicReference<Node> currentNode = new AtomicReference<>(targetNode);
        final AtomicReference<NodeSlot> parentSlot = new AtomicReference<>(deleteSlot);

        //
        // Inch our way upwards in the tree to perform the necessary adjustments. What needs to be done next
        // is informed by the result of the current operation:
        // 1. A fuse happened; we need to delete an existing slot from the parent node -- prime current node and
        //    current slot and continue.
        // 2. The slot was deleted but mbrs, largest Hilbert Values and largest Keys need to be adjusted upwards.
        // 3. We are done as no further adjustments are necessary.
        //
        return AsyncUtil.whileTrue(() -> {
            final NodeSlot currentDeleteSlot = parentSlot.get();

            if (currentDeleteSlot != null) {
                return deleteSlotFromTargetNode(transaction, currentNode.get(), currentDeleteSlot, deleteSlotIndex.get())
                        .thenApply(nodeOrAdjust -> {
                            if (currentNode.get().isRoot()) {
                                return false;
                            }
                            currentNode.set(currentNode.get().getParentNode());
                            parentSlot.set(nodeOrAdjust.getSlotInParent());
                            deleteSlotIndex.set(nodeOrAdjust.getTombstoneNode() == null ? -1 : nodeOrAdjust.getTombstoneNode().getSlotIndexInParent());
                            return nodeOrAdjust.getTombstoneNode() != null || nodeOrAdjust.parentNeedsAdjustment();
                        });
            } else {
                // adjustment only
                final NodeOrAdjust splitNodeSlotOrAdjust = updateSlotsAndAdjustNode(transaction, currentNode.get());
                Verify.verify(splitNodeSlotOrAdjust.getSlotInParent() == null);
                if (currentNode.get().isRoot()) {
                    return AsyncUtil.READY_FALSE;
                }
                currentNode.set(currentNode.get().getParentNode());
                return splitNodeSlotOrAdjust.parentNeedsAdjustment()
                       ? AsyncUtil.READY_TRUE
                       : AsyncUtil.READY_FALSE;
            }
        }, executor);
    }

    /**
     * Delete and existing slot from the target node passed in.
     * @param transaction transaction
     * @param targetNode target node
     * @param deleteSlot existing slot that is to be deleted
     * @param slotIndexInTargetNode The index of the new slot that we should use when inserting the new slot. While
     *        this information can be computed from the other arguments passed in, the caller already knows this
     *        information; we can avoid searching for the proper spot on our own.
     * @return a completable future that when completed indicates what needs to be done next (see {@link NodeOrAdjust}).
     */
    @Nonnull
    private CompletableFuture<NodeOrAdjust> deleteSlotFromTargetNode(@Nonnull final Transaction transaction,
                                                                     @Nonnull final Node targetNode,
                                                                     @Nonnull final NodeSlot deleteSlot,
                                                                     final int slotIndexInTargetNode) {
        //
        // We need to keep the number of slots per node between minM <= size() <= maxM unless this is the root node.
        //
        if (targetNode.isRoot() || targetNode.getSlots().size() > config.getMinM()) {
            if (logger.isTraceEnabled()) {
                logger.trace("regular delete; node={}; size={}", bytesToHex(targetNode.getId()), targetNode.size());
            }
            targetNode.deleteSlot(slotIndexInTargetNode);

            if (targetNode.getKind() == Kind.INTERMEDIATE) {
                //
                // If this node is the root and the root node is an intermediate node, then it should at least have two
                // children.
                //
                Verify.verify(!targetNode.isRoot() || targetNode.getSlots().size() >= 2);
                //
                // If this is a delete within an intermediate node, the slot being deleted results from a fuse operation
                // meaning a split has occurred on a lower level and the participating siblings of that split have
                // potentially changed. We resort to re-persisting all slots of the target node.
                // TODO This can be optimized in the future.
                //
                storageAdapter.writeNodes(transaction, Collections.singletonList(targetNode));
            } else {
                Verify.verify(targetNode.getKind() == Kind.LEAF);
                storageAdapter.clearNodeSlot(transaction, targetNode, deleteSlot);
            }

            // node is not under-flowing -- indicate that we are done fusing at the current node
            if (!targetNode.isRoot()) {
                return CompletableFuture.completedFuture(adjustSlotInParent(targetNode)
                                                         ? NodeOrAdjust.ADJUST
                                                         : NodeOrAdjust.NONE);
            }

            // no fuse and no adjustment
            return CompletableFuture.completedFuture(NodeOrAdjust.NONE); // no fuse and no adjustment
        } else {
            //
            // Node is under min-capacity -- borrow some children/items from the siblings if possible.
            //
            final CompletableFuture<List<Node>> siblings =
                    fetchSiblings(transaction, targetNode);

            return siblings.thenApply(siblingNodes -> {
                int numSlots =
                        Math.toIntExact(siblingNodes
                                .stream()
                                .mapToLong(siblingNode -> siblingNode.getSlots().size())
                                .sum());

                final Node tombstoneNode;
                final List<Node> newSiblingNodes;
                if (numSlots == siblingNodes.size() * config.getMinM()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("fusing nodes; node={}, siblings={}",
                                bytesToHex(targetNode.getId()),
                                siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    }
                    tombstoneNode = siblingNodes.get(siblingNodes.size() - 1);
                    newSiblingNodes = siblingNodes.subList(0, siblingNodes.size() - 1);
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("handling underflow; node={}, numSlots={}, siblings={}",
                                bytesToHex(targetNode.getId()),
                                numSlots,
                                siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    }
                    tombstoneNode = null;
                    newSiblingNodes = siblingNodes;
                }

                // temporarily underfill targetNode
                numSlots--;
                targetNode.deleteSlot(slotIndexInTargetNode);

                // sibling nodes are in hilbert value order
                final Iterator<? extends NodeSlot> slotIterator =
                        siblingNodes
                                .stream()
                                .flatMap(siblingNode -> siblingNode.getSlots().stream())
                                .iterator();

                //
                // Distribute all slots (excluding the one we want to delete) across all siblings (which also excludes
                // the targetNode and (if we are fusing) the tombstoneNode).
                // At the end of this modification all siblings have and (almost) equal count of slots that is
                // guaranteed to be between minM and maxM.
                //

                final int base = numSlots / newSiblingNodes.size();
                int rest = numSlots % newSiblingNodes.size();

                List<List<NodeSlot>> newNodeSlotLists = Lists.newArrayList();
                List<NodeSlot> currentNodeSlots = Lists.newArrayList();
                while (slotIterator.hasNext()) {
                    final NodeSlot slot = slotIterator.next();
                    currentNodeSlots.add(slot);
                    if (currentNodeSlots.size() == base + (rest > 0 ? 1 : 0)) {
                        if (rest > 0) {
                            // one fewer to distribute
                            rest--;
                        }

                        newNodeSlotLists.add(currentNodeSlots);
                        currentNodeSlots = Lists.newArrayList();
                    }
                }

                Verify.verify(newSiblingNodes.size() == newNodeSlotLists.size());

                if (tombstoneNode != null) {
                    // remove the slots for the tombstone node and update
                    tombstoneNode.setSlots(Lists.newArrayList());
                    storageAdapter.writeNodes(transaction, Collections.singletonList(tombstoneNode));
                }

                final Iterator<Node> newSiblingNodesIterator = newSiblingNodes.iterator();
                final Iterator<List<NodeSlot>> newNodeSlotsIterator = newNodeSlotLists.iterator();

                // assign the slots to the appropriate nodes
                while (newSiblingNodesIterator.hasNext()) {
                    final Node newSiblingNode = newSiblingNodesIterator.next();
                    Verify.verify(newNodeSlotsIterator.hasNext());
                    final List<NodeSlot> newNodeSlots = newNodeSlotsIterator.next();
                    newSiblingNode.setSlots(newNodeSlots);
                }

                final IntermediateNode parentNode = Objects.requireNonNull(targetNode.getParentNode());
                if (parentNode.isRoot() && parentNode.size() == 2 && tombstoneNode != null) {
                    //
                    // The parent node (root) would only have one child after this delete.
                    // We shrink the tree by removing the root and making the last remaining sibling the root.
                    //
                    promoteNodeToRoot(transaction, Iterables.getOnlyElement(newSiblingNodes));
                    return NodeOrAdjust.NONE;
                }

                storageAdapter.writeNodes(transaction, newSiblingNodes);

                for (final Node newSiblingNode : newSiblingNodes) {
                    adjustSlotInParent(newSiblingNode);
                }

                if (tombstoneNode == null) {
                    //
                    // We only handled underfill (and didn't need to fuse) but still need to continue adjusting
                    // mbrs, largest Hilbert values, and largest keys upward the tree.
                    //
                    return NodeOrAdjust.ADJUST;
                }

                //
                // We need to signal that the current operation ended ina fuse, and we need to delete the slot for
                // the tombstoneNode one level higher.
                //
                return new NodeOrAdjust(parentNode.getSlots().get(tombstoneNode.getSlotIndexInParent()),
                        tombstoneNode, true);
            });
        }
    }

    /**
     * Promote the given node to become the new root node. The node that is passed only changes its node id but retains
     * all of it slots. This operation is the opposite of {@link #splitRootNode(Transaction, Node)} which can be invoked
     * by the insert code path.
     * @param transaction transaction
     * @param node node to be promoted.
     */
    private void promoteNodeToRoot(final @Nonnull Transaction transaction, final Node node) {
        final List<? extends NodeSlot> newRootSlots = node.getSlots();
        node.setSlots(Lists.newArrayList());
        final Node newRootNode = node.newOfSameKind(rootId);
        newRootNode.setSlots(newRootSlots);
        // We need to update the node and the new root node in order to clear out the existing slots of the pre-promoted
        // node.
        storageAdapter.writeNodes(transaction, ImmutableList.of(newRootNode, node));
    }

    //
    // Helper methods that may be called from more than one code path.
    //

    /**
     * Updates (persists) the slots for a target node and then computes the necessary adjustments in its parent
     * node (without persisting those).
     * @param transaction the transaction to use
     * @param targetNode the target node
     * @return Either {@link NodeOrAdjust#NONE} if no further adjustments need to be persisted or
     *         {@link NodeOrAdjust#ADJUST} if the slots of the parent node of the target node need to be adjusted as
     *         well.
     */
    @Nonnull
    private NodeOrAdjust updateSlotsAndAdjustNode(@Nonnull final Transaction transaction,
                                                  @Nonnull final Node targetNode) {
        storageAdapter.writeNodes(transaction, Collections.singletonList(targetNode));
        if (targetNode.isRoot()) {
            return NodeOrAdjust.NONE;
        }

        return adjustSlotInParent(targetNode)
               ? NodeOrAdjust.ADJUST
               : NodeOrAdjust.NONE;
    }

    /**
     * Updates the target node's mbr, largest Hilbert value as well its largest key in the target node's parent slot.
     * @param targetNode target node
     * @return {@code true} if any attributes of the target slot were modified, {@code false} otherwise. This will
     *         inform the caller if modifications need to be persisted and/or if the parent node itseld=f needs to be
     *         adjusted as well.
     */
    private static boolean adjustSlotInParent(final @Nonnull Node targetNode) {
        Preconditions.checkArgument(!targetNode.isRoot());
        boolean slotHasChanged;
        final IntermediateNode parentNode = Objects.requireNonNull(targetNode.getParentNode());
        final ChildSlot childSlot = Objects.requireNonNull(parentNode.getSlots()).get(targetNode.getSlotIndexInParent());
        final Rectangle newMbr = computeMbr(targetNode.getSlots());
        slotHasChanged = !childSlot.getMbr().equals(newMbr);
        childSlot.setMbr(newMbr);
        final NodeSlot lastSlotOfTargetNode = targetNode.getSlots().get(targetNode.size() - 1);
        slotHasChanged |= !childSlot.getLargestHilbertValue().equals(lastSlotOfTargetNode.getHilbertValue());
        childSlot.setLargestHilbertValue(lastSlotOfTargetNode.getHilbertValue());
        slotHasChanged |= !childSlot.getKey().equals(lastSlotOfTargetNode.getKey());
        childSlot.setLargestKey(lastSlotOfTargetNode.getKey());
        return slotHasChanged;
    }

    /**
     * Method to fetch the update path of a given {@code (hilbertValue, key)} pair. The update path is a {@link LeafNode}
     * and all its parent nodes to the root node. The caller can invoke {@link Node#getParentNode()} to navigate to
     * all nodes in the update path starting from the {@link LeafNode} that is returned. The {@link LeafNode} that is
     * returned may or may not already contain a slot for the {@code (hilbertValue, key)} pair passed in. This logic is
     * invoked for insert, updates, as well as delete operations. If it is used for insert and the item is not yet
     * part of the leaf node, the leaf node that is returned can be understood as the correct place to insert the item
     * in question.
     * @param transaction the transaction to use
     * @param hilbertValue the Hilbert value to look for
     * @param key the key to look for
     * @return A completable future containing a {@link LeafNode} and by extension (through {@link Node#getParentNode()})
     *         all intermediate nodes up to the root node that may get affected by an insert, update, or delete
     *         of the specified item.
     */
    @Nonnull
    private CompletableFuture<LeafNode> fetchUpdatePathToLeaf(@Nonnull final Transaction transaction,
                                                              @Nonnull final BigInteger hilbertValue,
                                                              @Nonnull final Tuple key) {
        final AtomicReference<IntermediateNode> parentNode = new AtomicReference<>(null);
        final AtomicInteger slotInParent = new AtomicInteger(-1);
        final AtomicReference<byte[]> currentId = new AtomicReference<>(rootId);
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> onWriteListener.onAsyncReadForWrite(fetchNode(transaction, currentId.get()))
                        .thenApply(node -> {
                            if (parentNode.get() != null) {
                                node.linkToParent(parentNode.get(), slotInParent.get());
                            }
                            if (node.getKind() == Kind.INTERMEDIATE) {
                                final IntermediateNode intermediateNode = (IntermediateNode)node;
                                final int slotIndex = findChildSlotIndex(intermediateNode, hilbertValue, key);
                                final List<ChildSlot> children = intermediateNode.getSlots();
                                parentNode.set(intermediateNode);
                                slotInParent.set(slotIndex);
                                final ChildSlot childSlot = children.get(slotIndex);
                                currentId.set(childSlot.getChildId());
                                return true;
                            } else {
                                leafNode.set((LeafNode)node);
                                return false;
                            }
                        }), executor)
                .thenApply(ignored -> {
                    final LeafNode node = leafNode.get();
                    if (logger.isTraceEnabled()) {
                        logger.trace("update path; path={}", nodeIdPath(node));
                    }
                    return node;
                });
    }

    /**
     * Method to fetch the siblings of a given node. The node passed in must not be the root node and must be linked up
     * to its parent. The parent already has information obout the children ids. This method (through the slot
     * information of the node passed in) can then determine adjacent nodes.
     * @param transaction the transaction to use
     * @param node the node to fetch siblings for
     * @return a completable future containing a list of {@link Node}s that contain the {@link Config#getSplitS()}
     *         number of siblings (where the node passed in is counted as a sibling) if that many siblings exist. In
     *         the case (i.e. for a small root node) where there are not enough siblings we return the maximum possible
     *         number of siblings. The returned sibling nodes are returned in Hilbert value order and contain the node
     *         passed in at the correct position in the returned list. The siblings will also attempt to hug the nodes
     *         passed in as good as possible meaning that we attempt to return the node passed in as middle-most element
     *         of the returned list.
     */
    @Nonnull
    private CompletableFuture<List<Node>> fetchSiblings(@Nonnull final Transaction transaction,
                                                        @Nonnull final Node node) {
        // this deque is only modified by once upon creation
        final ArrayDeque<byte[]> toBeProcessed = new ArrayDeque<>();
        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final int numSiblings = config.getSplitS();
        final Node[] siblings = new Node[numSiblings];

        //
        // Do some acrobatics to find the best start/end positions for the siblings. Take into account how many
        // are warranted, if the node that was passed occupies a slot in its parent node that is touching the end or the
        // beginning of the parent's slots, and the total number of slots in the parent of the node that was
        // passed in.
        //
        final IntermediateNode parentNode = Objects.requireNonNull(node.getParentNode());
        final List<ChildSlot> children = parentNode.getSlots();
        int slotIndexInParent = node.getSlotIndexInParent();
        int start = slotIndexInParent - numSiblings / 2;
        int end = start + numSiblings;
        if (start < 0) {
            start = 0;
            end = numSiblings;
        } else if (end > children.size()) {
            end = children.size();
            start = end - numSiblings;
        }

        // because lambdas
        final int minSibling = start;

        for (int i = start; i < end; i++) {
            toBeProcessed.addLast(children.get(i).getChildId());
        }

        // Fetch all sibling nodes (in parallel if possible).
        return AsyncUtil.whileTrue(() -> {
            working.removeIf(CompletableFuture::isDone);

            while (working.size() <= MAX_CONCURRENT_READS) {
                final int index = numSiblings - toBeProcessed.size();
                final byte[] currentId = toBeProcessed.pollFirst();
                if (currentId == null) {
                    break;
                }

                final int slotIndex = minSibling + index;
                if (slotIndex != slotIndexInParent) {
                    working.add(onWriteListener.onAsyncReadForWrite(fetchNode(transaction, currentId))
                            .thenAccept(siblingNode -> {
                                siblingNode.linkToParent(parentNode, slotIndex);
                                siblings[index] = siblingNode;
                            }));
                } else {
                    // put node in the list of siblings -- even though node is strictly speaking not a sibling of itself
                    siblings[index] = node;
                }
            }

            if (working.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(working).thenApply(v -> true);
        }, executor).thenApply(vignore -> Lists.newArrayList(siblings));
    }

    /**
     * Method to validate the Hilbert R-tree.
     * @param transaction the transaction to use
     * @return a completable future that completes successfully if the tree is valid, completes with failure otherwise
     */
    @Nonnull
    public CompletableFuture<Void> validate(@Nonnull final Transaction transaction) {
        final ArrayDeque<ParentNodeAndChildId> toBeProcessed = new ArrayDeque<>();
        final List<CompletableFuture<List<ParentNodeAndChildId>>> working = Lists.newArrayList();

        toBeProcessed.addLast(new ParentNodeAndChildId(null, rootId));

        // Fetch the entire tree.
        return AsyncUtil.whileTrue(() -> {
            final Iterator<CompletableFuture<List<ParentNodeAndChildId>>> workingIterator = working.iterator();
            while (workingIterator.hasNext()) {
                final CompletableFuture<List<ParentNodeAndChildId>> nextFuture = workingIterator.next();
                if (nextFuture.isDone()) {
                    toBeProcessed.addAll(nextFuture.join());
                    workingIterator.remove();
                }
            }
            
            while (working.size() <= MAX_CONCURRENT_READS) {
                final ParentNodeAndChildId currentParentNodeAndChildId = toBeProcessed.pollFirst();
                if (currentParentNodeAndChildId == null) {
                    break;
                }

                working.add(onReadListener.onAsyncRead(fetchNode(transaction, currentParentNodeAndChildId.getChildId())).thenApply(childNode -> {
                    BigInteger lastHilbertValue = null;
                    Tuple lastKey = null;

                    // check that all (hilbert values;key pairs) are monotonically increasing
                    for (final NodeSlot nodeSlot : childNode.getSlots()) {
                        if (lastHilbertValue != null) {
                            final int hilbertValueCompare = nodeSlot.getHilbertValue().compareTo(lastHilbertValue);
                            Verify.verify(hilbertValueCompare >= 0,
                                    "(hilbertValue, key) pairs are not monotonically increasing (hilbertValueCheck)");
                            if (hilbertValueCompare == 0) {
                                Verify.verify(TupleHelpers.compare(nodeSlot.getKey(), lastKey) >= 0,
                                        "(hilbertValue, key) pairs are not monotonically increasing (keyCheck)");
                            }
                        }
                        lastHilbertValue = nodeSlot.getHilbertValue();
                        lastKey = nodeSlot.getKey();
                    }

                    final IntermediateNode parentNode = currentParentNodeAndChildId.getParentNode();
                    if (parentNode == null) {
                        // child is root
                        Verify.verify(childNode.isRoot());
                    } else {
                        final ChildSlot childSlotInParentNode =
                                parentNode.getSlots()
                                        .stream()
                                        .filter(childSlot -> Arrays.equals(childSlot.getChildId(), childNode.getId()))
                                        .findFirst()
                                        .orElseThrow(() -> new IllegalStateException("child slot not found in parent for child node"));

                        // Recompute the mbr of the child and compare it to the mbr the parent has.
                        final Rectangle computedMbr = computeMbr(childNode.getSlots());
                        Verify.verify(childSlotInParentNode.getMbr().equals(computedMbr),
                                "computed mbr does not match mbr from node");

                        // Verify that the largest hilbert value in the parent node is indeed the hilbert value of
                        // the right-most child in childNode.
                        Verify.verify(childSlotInParentNode.getLargestHilbertValue().equals(childNode.getSlots().get(childNode.size() - 1).getHilbertValue()),
                                "expected largest hilbert does not match the actual hilbert value of the last child in childNode");

                        // Verify that the largest hilbert value in the parent node is indeed the hilbert value of
                        // the right-most child in childNode.
                        Verify.verify(TupleHelpers.equals(childSlotInParentNode.getLargestKey(), childNode.getSlots().get(childNode.size() - 1).getKey()),
                                "expected largest key does not match the actual key of the last child in childNode");
                    }

                    // add all children to the to be processed queue
                    if (childNode.getKind() == Kind.INTERMEDIATE) {
                        return ((IntermediateNode)childNode).getSlots()
                                .stream()
                                .map(childSlot -> new ParentNodeAndChildId((IntermediateNode)childNode, childSlot.getChildId()))
                                .collect(ImmutableList.toImmutableList());
                    } else {
                        return ImmutableList.of();
                    }
                }));
            }

            if (working.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(working).thenApply(v -> true);
        }, executor).thenApply(vignore -> null);
    }

    @Nonnull
    public CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction, @Nonnull final byte[] nodeId) {
        return storageAdapter.fetchNode(transaction, nodeId).thenApply(this::checkNode);
    }

    /**
     * Method to perform basic invariant check(s) on a newly fetched node.
     * @param node the node to check
     * @param <N> the type param for the node in order for this method to not be lossy on the type of the node that
     *            was passed in
     * @return the node that was passed in
     */
    @Nonnull
    private <N extends Node> N checkNode(@Nonnull final N node) {
        if (node.size() < config.getMinM() || node.size() > config.getMaxM()) {
            if (!node.isRoot()) {
                throw new IllegalStateException("packing of non-root packing is out of valid range");
            }
        }
        return node;
    }

    /**
     * Method to find the appropriate child slot index for a given Hilbert value and key. This method is used
     * to find the proper slot indexes for the insert/update path. Note that if
     * {@code (largestHilbertValue, largestKey)} of the last child is less than {@code (hilbertValue, key)}, we insert
     * through the last child as we treat the (non-existing) next item as {@code (infinity, infinity)}.
     * @param intermediateNode the intermediate node to search
     * @param hilbertValue hilbert value
     * @param key key
     * @return the 0-based slot index that corresponds to the given {@code (hilbertValue, key)} pair
     */
    private static int findChildSlotIndex(@Nonnull final IntermediateNode intermediateNode,
                                          @Nonnull final BigInteger hilbertValue,
                                          @Nonnull final Tuple key) {
        Verify.verify(!intermediateNode.isEmpty());
        final List<ChildSlot> childSlots = intermediateNode.getSlots();
        for (int slotIndex = 0; slotIndex < childSlots.size(); slotIndex++) {
            final ChildSlot childSlot = childSlots.get(slotIndex);

            //
            // Choose subtree with the minimum Hilbert value that is greater than the target
            // Hilbert value. If there is no such subtree, i.e. the target Hilbert value is the
            // largest Hilbert value, we choose the largest one in the current node.
            //
            final int hilbertValueCompare = childSlot.getLargestHilbertValue().compareTo(hilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    // child == target
                    final var tupleCompare = TupleHelpers.compare(childSlot.getLargestKey(), key);
                    if (tupleCompare >= 0) {
                        return slotIndex;
                    }
                } else {
                    return slotIndex;
                }
            }
        }

        // this is an intermediate node; we insert through the last child
        return childSlots.size() - 1;
    }

    /**
     * Method to find the appropriate item slot index for a given Hilbert value and key. This method is used
     * to find the proper item slot index for the insert/update path.
     * @param leafNode the leaf node to search
     * @param hilbertValue hilbert value
     * @param key key
     * @return {@code -1} if the item specified by {@code (hilbertValue, key)} already exists in {@code leafNode};
     *         the 0-based slot index that represents the insertion point index of the given {@code (hilbertValue, key)}
     *         pair, otherwise
     */
    private static int findInsertUpdateItemSlotIndex(@Nonnull final LeafNode leafNode,
                                                     @Nonnull final BigInteger hilbertValue,
                                                     @Nonnull final Tuple key) {
        final List<ItemSlot> items = leafNode.getSlots();
        for (int slotIndex = 0; slotIndex < items.size(); slotIndex++) {
            final ItemSlot slot = items.get(slotIndex);
            final int hilbertValueCompare = slot.getHilbertValue().compareTo(hilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    // child == target
                    final var tupleCompare = TupleHelpers.compare(slot.getKey(), key);
                    if (tupleCompare >= 0) {
                        if (tupleCompare == 0) {
                            return -1;
                        }
                        return slotIndex;
                    }
                } else {
                    return slotIndex;
                }
            }
        }

        return leafNode.size();
    }

    /**
     * Method to find the appropriate item slot index for a given Hilbert value and key. This method is used
     * to find the proper item slot index for the delete path.
     * @param leafNode the leaf node to search
     * @param hilbertValue hilbert value
     * @param key key
     * @return {@code -1} if the item specified by {@code (hilbertValue, key)} does not exist in {@code leafNode};
     *         the 0-based slot index that corresponds to the slot for the given {@code (hilbertValue, key)}
     *         pair, otherwise
     */
    private static int findDeleteItemSlotIndex(@Nonnull final LeafNode leafNode,
                                               @Nonnull final BigInteger hilbertValue,
                                               @Nonnull final Tuple key) {
        final List<ItemSlot> items = leafNode.getSlots();
        for (int slotIndex = 0; slotIndex < items.size(); slotIndex++) {
            final ItemSlot slot = items.get(slotIndex);
            final int hilbertValueCompare = slot.getHilbertValue().compareTo(hilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    final var tupleCompare = TupleHelpers.compare(slot.getKey(), key);
                    if (tupleCompare >= 0) {
                        if (tupleCompare == 0) {
                            return slotIndex;
                        }
                        return -1;
                    }
                } else {
                    return -1;
                }
            }
        }

        return -1;
    }

    /**
     * Compute the minimum bounding rectangle (mbr) of a list of slots. This method is used when a node's secondary
     * attributes need to be recomputed.
     * @param slots a list of slots
     * @return a {@link Rectangle} representing the mbr of the {@link Point}s of the given slots.
     */
    @Nonnull
    private static Rectangle computeMbr(@Nonnull final List<? extends NodeSlot> slots) {
        Verify.verify(!slots.isEmpty());
        Rectangle mbr = null;
        for (final NodeSlot slot : slots) {
            if (slot instanceof ItemSlot) {
                final Point position = ((ItemSlot)slot).getPosition();
                if (mbr == null) {
                    mbr = Rectangle.fromPoint(position);
                } else {
                    mbr = mbr.unionWith(position);
                }
            }  else if (slot instanceof ChildSlot) {
                final Rectangle mbrForSlot = ((ChildSlot)slot).getMbr();
                if (mbr == null) {
                    mbr = mbrForSlot;
                } else {
                    mbr = mbr.unionWith(mbrForSlot);
                }
            } else {
                throw new IllegalStateException("slot of unknown kind");
            }
        }
        return mbr;
    }

    /**
     * Method to create a new node identifier. This method uses {@link UUID#randomUUID()} and should be used in
     * production to avoid conflicts.
     * @return a new 16-byte byte array containing a new unique node identifier
     */
    @Nonnull
    public static byte[] newRandomNodeId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = new byte[NODE_ID_LENGTH];
        ByteBuffer.wrap(uuidBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits());
        return uuidBytes;
    }

    /**
     * Method to create a new node identifier. This method uses an internal static {@link AtomicLong} that is
     * incremented. This method creates monotonically increasing node identifiers which can be shortened when printed
     * or logged. This way of creating node identifiers should only be used for testing and debugging purposes.
     * @return a new 16-byte byte array containing a new unique node identifier
     */
    @Nonnull
    public static byte[] newSequentialNodeId() {
        final long nodeIdAsLong = nodeIdState.getAndIncrement();
        final byte[] uuidBytes = new byte[NODE_ID_LENGTH];
        ByteBuffer.wrap(uuidBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(0L)
                .putLong(nodeIdAsLong);
        return uuidBytes;
    }

    /**
     * Helper method to format bytes as hex strings for logging and debugging.
     * @param bytes an array of bytes
     * @return a {@link String} containing the hexadecimal representation of the byte array passed in
     */
    @Nonnull
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return "0x" + new String(hexChars).replaceFirst("^0+(?!$)", "");
    }

    /**
     * Helper method to format the node ids of an insert/update path as a string.
     * @param node a node that is usually linked up to its parents to form an insert/update path
     * @return a {@link String} containing the string presentation of the insert/update path starting at {@code node}
     */
    @Nonnull
    private static String nodeIdPath(@Nonnull Node node) {
        final List<String> nodeIds = Lists.newArrayList();
        do {
            nodeIds.add(bytesToHex(node.getId()));
            node = node.getParentNode();
        } while (node != null);
        Collections.reverse(nodeIds);
        return String.join(", ", nodeIds);
    }

    /**
     * Enum to capture the kind of node.
     */
    public enum Kind {
        LEAF((byte)0x00),
        INTERMEDIATE((byte)0x01);

        private final byte serialized;

        Kind(final byte serialized) {
            this.serialized = serialized;
        }

        public byte getSerialized() {
            return serialized;
        }

        @Nonnull
        private static Kind fromSerializedNodeKind(byte serializedNodeKind) {
            final Kind nodeKind;
            switch (serializedNodeKind) {
                case 0x00:
                    nodeKind = Kind.LEAF;
                    break;
                case 0x01:
                    nodeKind = Kind.INTERMEDIATE;
                    break;
                default:
                    throw new IllegalArgumentException("unknown node kind");
            }
            Verify.verify(nodeKind.getSerialized() == serializedNodeKind);
            return nodeKind;
        }
    }

    /**
     * Abstract base class to capture the common aspects of {@link LeafNode} and {@link IntermediateNode}. All nodes
     * have a node id and slots and can be linked up to a parent. Note that while the root node mostly is an
     * intermediate node it can also be a leaf node if the tree is nearly empty.
     */
    public abstract static class Node {
        @Nonnull
        private final byte[] id;

        @Nullable
        private IntermediateNode parentNode;
        int slotIndexInParent;

        @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
        public Node(@Nonnull final byte[] id, @Nullable final IntermediateNode parentNode, final int slotIndexInParent) {
            this.id = id;
            this.parentNode = parentNode;
            this.slotIndexInParent = slotIndexInParent;
        }

        @Nonnull
        @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
        public byte[] getId() {
            return id;
        }

        public int size() {
            return getSlots().size();
        }

        public boolean isEmpty() {
            return getSlots().isEmpty();
        }

        public abstract void setSlots(@Nonnull List<? extends NodeSlot> newSlots);

        @Nonnull
        public abstract List<? extends NodeSlot> getSlots();

        public abstract void insertSlot(int slotIndex, @Nonnull NodeSlot slot);

        public void deleteSlot(int slotIndex) {
            getSlots().remove(slotIndex);
        }

        public boolean isRoot() {
            return Arrays.equals(rootId, id);
        }

        @Nonnull
        public abstract Kind getKind();

        @Nullable
        public IntermediateNode getParentNode() {
            return parentNode;
        }

        public int getSlotIndexInParent() {
            return slotIndexInParent;
        }

        public void linkToParent(@Nonnull final IntermediateNode parentNode, final int slotInParent) {
            this.parentNode = parentNode;
            this.slotIndexInParent = slotInParent;
        }

        @Nonnull
        public abstract Node newOfSameKind(@Nonnull byte[] nodeId);
    }

    /**
     * A leaf node of the tree. A leaf node holds the actual data in {@link ItemSlot}s.
     */
    public static class LeafNode extends Node {
        @Nonnull
        private List<ItemSlot> items;

        public LeafNode(@Nonnull final byte[] id,
                        @Nonnull final List<ItemSlot> items) {
            this(id, items, null, -1);
        }

        public LeafNode(@Nonnull final byte[] id,
                        @Nonnull final List<ItemSlot> items,
                        @Nullable final IntermediateNode parentNode,
                        final int slotIndexInParent) {
            super(id, parentNode, slotIndexInParent);
            this.items = items;
        }

        @Nonnull
        @Override
        public Kind getKind() {
            return Kind.LEAF;
        }

        @Nonnull
        @Override
        public List<ItemSlot> getSlots() {
            return items;
        }

        @Override
        public void setSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
            this.items =
                    newSlots.stream()
                            .map(slot -> (ItemSlot)slot)
                            .collect(Collectors.toList());
        }

        @Override
        public void insertSlot(final int slotIndex, @Nonnull final NodeSlot slot) {
            Preconditions.checkArgument(slot instanceof ItemSlot);
            items.add(slotIndex, (ItemSlot)slot);
        }

        @Nonnull
        @Override
        public LeafNode newOfSameKind(@Nonnull final byte[] nodeId) {
            return new LeafNode(nodeId, Lists.newArrayList());
        }
    }

    /**
     * An intermediate node of the tree. An intermediate node holds the holds information about its children nodes that
     * be intermediate nodes or leaf nodes. The secondary attributes such as {@code largestHilbertValue},
     * {@code largestKey} can be derived (and recomputed) if the children of this node are available to be introspected.
     */
    public static class IntermediateNode extends Node {
        @Nonnull
        private List<ChildSlot> children;

        public IntermediateNode(@Nonnull final byte[] id,
                                @Nonnull final List<ChildSlot> children) {
            this(id, children, null, -1);
        }

        public IntermediateNode(@Nonnull final byte[] id,
                                @Nonnull final List<ChildSlot> children,
                                @Nullable final IntermediateNode parentNode,
                                final int slotIndexInParent) {
            super(id, parentNode, slotIndexInParent);
            this.children = children;
        }

        @Nonnull
        @Override
        public Kind getKind() {
            return Kind.INTERMEDIATE;
        }

        @Nonnull
        @Override
        public List<ChildSlot> getSlots() {
            return children;
        }

        @Override
        public void setSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
            this.children =
                    newSlots.stream()
                            .map(slot -> (ChildSlot)slot)
                            .collect(Collectors.toList());
        }

        @Override
        public void insertSlot(final int slotIndex, @Nonnull final NodeSlot slot) {
            Preconditions.checkArgument(slot instanceof ChildSlot);
            children.add(slotIndex, (ChildSlot)slot);
        }

        @Nonnull
        @Override
        public IntermediateNode newOfSameKind(@Nonnull final byte[] nodeId) {
            return new IntermediateNode(nodeId, Lists.newArrayList());
        }

        @Nonnull
        public String getPlotMbrs() {
            return getSlots().stream()
                    .map(child -> child.getMbr().toPlotString())
                    .collect(Collectors.joining("\n"));
        }
    }

    /**
     * Abstract base class for all node slots. Holds a Hilbert value and a key. The semantics of these attributes
     * is refined in the subclasses {@link ItemSlot} and {@link ChildSlot}.
     */
    public abstract static class NodeSlot {
        @Nonnull
        protected BigInteger hilbertValue;

        @Nonnull
        protected Tuple key;

        protected NodeSlot(@Nonnull final BigInteger hilbertValue, @Nonnull final Tuple key) {
            this.hilbertValue = hilbertValue;
            this.key = key;
        }

        @Nonnull
        public BigInteger getHilbertValue() {
            return hilbertValue;
        }

        @Nonnull
        public Tuple getKey() {
            return key;
        }

        @Nonnull
        public Tuple getKeySuffix() {
            return key.getNestedTuple(1);
        }

        /**
         * Create a tuple for the key part of this slot. This tuple is used when the slot is persisted in the database.
         * Note that the serialization format is not yet finalized.
         * @param storeHilbertValues indicator if the hilbert value should be encoded into the slot key or null-ed out
         * @return a new tuple
         */
        @Nonnull
        protected abstract Tuple getSlotKey(boolean storeHilbertValues);

        /**
         * Create a tuple for the value part of this slot. This tuple is used when the slot is persisted in the database.
         * Note that the serialization format is not yet finalized.
         * @return a new tuple
         */
        @Nonnull
        protected abstract Tuple getSlotValue();

        /**
         * Compare this node slot's {@code (hilbertValue, key)} pair with another {@code (hilbertValue, key)} pair.
         * We do not use a proper {@link java.util.Comparator} as we don't want to wrap the pair in another object.
         * @param hilbertValue Hilbert value
         * @param key first key
         * @return {@code -1, 0, 1} if this node slot's pair is less/equal/greater than the pair passed in
         */
        public int compareHilbertValueAndKey(@Nonnull final BigInteger hilbertValue,
                                             @Nonnull final Tuple key) {
            final int hilbertValueCompare = getHilbertValue().compareTo(hilbertValue);
            if (hilbertValueCompare != 0) {
                return hilbertValueCompare;
            }
            return TupleHelpers.compare(getKey(), key);
        }
    }

    /**
     * An item slot that is used by {@link LeafNode}s. Holds the actual data of the item as well as the items Hilbert
     * value and its key.
     */
    public static class ItemSlot extends NodeSlot {
        public static final int SLOT_KEY_TUPLE_SIZE = 2;
        public static final int SLOT_VALUE_TUPLE_SIZE = 1;

        @Nonnull
        private final Tuple value;
        @Nonnull
        private final Point position;

        public ItemSlot(@Nonnull final BigInteger hilbertValue, @Nonnull final Point position, @Nonnull final Tuple key,
                        @Nonnull final Tuple value) {
            super(hilbertValue, key);
            this.value = value;
            this.position = position;
        }

        @Nonnull
        public Tuple getValue() {
            return value;
        }

        @Nonnull
        public Point getPosition() {
            return position;
        }

        @Nonnull
        @Override
        protected Tuple getSlotKey(final boolean storeHilbertValues) {
            return Tuple.from(storeHilbertValues ? getHilbertValue() : null, getKey());
        }

        @Nonnull
        @Override
        protected Tuple getSlotValue() {
            return Tuple.from(getValue());
        }

        @Override
        public String toString() {
            return "[" + getPosition() + ";" + getHilbertValue() + "; " + getKey() + "]";
        }

        @Nonnull
        private static ItemSlot fromKeyAndValue(@Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple,
                                                @Nonnull final Function<Point, BigInteger> hilbertValueFunction) {
            Verify.verify(keyTuple.size() == SLOT_KEY_TUPLE_SIZE);
            Verify.verify(valueTuple.size() == SLOT_VALUE_TUPLE_SIZE);
            final Tuple itemKey = keyTuple.getNestedTuple(1);
            final Point point = new Point(itemKey.getNestedTuple(0));
            final BigInteger hilbertValue;
            if (keyTuple.get(0) == null) {
                hilbertValue = hilbertValueFunction.apply(point);
            } else {
                hilbertValue = keyTuple.getBigInteger(0);
            }
            return new ItemSlot(hilbertValue, point, itemKey, valueTuple.getNestedTuple(0));
        }
    }

    /**
     * A child slot that is used by {@link IntermediateNode}s. Holds the id of a child node, as well as the largest
     * hilbert value of its child, the largest key of its child and an mbr that encompasses all points in the subtree
     * rooted at the child.
     */
    public static class ChildSlot extends NodeSlot {
        public static final int SLOT_KEY_TUPLE_SIZE = 2;
        public static final int SLOT_VALUE_TUPLE_SIZE = 2;

        @Nonnull
        private final byte[] childId;
        @Nonnull
        private Rectangle mbr;

        @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
        public ChildSlot(@Nonnull final BigInteger largestHilbertValue, @Nonnull final Tuple largestKey,
                         @Nonnull final byte[] childId, @Nonnull final Rectangle mbr) {
            super(largestHilbertValue, largestKey);
            this.childId = childId;
            this.mbr = mbr;
        }

        @Nonnull
        @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
        public byte[] getChildId() {
            return childId;
        }

        public void setLargestHilbertValue(@Nonnull final BigInteger largestHilbertValue) {
            this.hilbertValue = largestHilbertValue;
        }

        @Nonnull
        public BigInteger getLargestHilbertValue() {
            return hilbertValue;
        }

        public void setLargestKey(@Nonnull final Tuple largestKey) {
            this.key = largestKey;
        }

        @Nonnull
        public Tuple getLargestKey() {
            return key;
        }

        public void setMbr(@Nonnull final Rectangle mbr) {
            this.mbr = mbr;
        }

        @Nonnull
        public Rectangle getMbr() {
            return mbr;
        }

        @Nonnull
        @Override
        protected Tuple getSlotKey(final boolean storeHilbertValue) {
            return Tuple.from(getLargestHilbertValue(), getLargestKey());
        }

        @Nonnull
        @Override
        protected Tuple getSlotValue() {
            return Tuple.from(getChildId(), getMbr().getRanges());
        }

        @Nonnull
        @Override
        public String toString() {
            //return "[" + getMbr() + ";" + getLargestHilbertValue() + "]";
            return getMbr().toString();
        }

        @Nonnull
        private static ChildSlot fromKeyAndValue(@Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
            Verify.verify(keyTuple.size() == SLOT_KEY_TUPLE_SIZE);
            Verify.verify(valueTuple.size() == SLOT_VALUE_TUPLE_SIZE);
            return new ChildSlot(keyTuple.getBigInteger(0), keyTuple.getNestedTuple(1),
                    valueTuple.getBytes(0), new Rectangle(valueTuple.getNestedTuple(1)));
        }
    }

    /**
     * Storage adapter used for serialization and deserialization of nodes.
     */
    public interface StorageAdapter {

        /**
         * Get the subspace used to store this r-tree.
         * @return r-tree subspace
         */
        @Nonnull
        Subspace getSubspace();

        /**
         * Get the on-write listener.
         * @return the on-write listener.
         */
        @Nonnull
        OnWriteListener getOnWriteListener();

        /**
         * Get the on-read listener.
         * @return the on-read listener.
         */
        @Nonnull
        OnReadListener getOnReadListener();

        /**
         * Persist a node slot.
         * @param transaction the transaction to use
         * @param node node whose slot to persist
         * @param nodeSlot the node slot to persist
         */
        void writeNodeSlot(@Nonnull Transaction transaction, @Nonnull Node node, @Nonnull NodeSlot nodeSlot);

        /**
         * Clear out a node slot.
         * @param transaction the transaction to use
         * @param node node whose slot is cleared out
         * @param nodeSlot the node slot to clear out
         */
        void clearNodeSlot(@Nonnull Transaction transaction, @Nonnull Node node, @Nonnull NodeSlot nodeSlot);

        /**
         * Method to (re-)persist all slots for a list of nodes passed in. The method will first clear the range starting with
         * the node ids of the nodes passed in and then persist all the nodes slots.
         * @param transaction the transaction to use
         * @param nodes a list of nodes to be (re-persisted)
         */
        void writeNodes(@Nonnull Transaction transaction, @Nonnull List<? extends Node> nodes);

        /**
         * Method to fetch the data needed to construct a {@link Node}. Note that a node on disk is represented by its
         * slots. Each slot is represented by a key/value pair in FDB. Each key (common for both leaf and intermediate
         * nodes) starts with an 8-byte node id (which is usually a serialized {@link UUID}) followed by one byte which
         * indicates the {@link RTree.Kind} of node the slot belongs to.
         * @param transaction the transaction to use
         * @param nodeId the node id we should use
         * @return A completable future containing the {@link Node} that was fetched from the database once completed.
         *         The node may be an object of {@link LeafNode} or of {@link IntermediateNode}.
         */
        @Nonnull
        CompletableFuture<Node> fetchNode(@Nonnull ReadTransaction transaction, @Nonnull byte[] nodeId);

        @Nonnull
        default byte[] packWithSubspace(final byte[] key) {
            return getSubspace().pack(key);
        }
    }

    /**
     * Storage adapter that normalizes internal nodes such that each node slot is a key/value pair in the database.
     */
    public static class BySlotStorageAdapter implements StorageAdapter {
        private static final Comparator<ItemSlot> comparator =
                Comparator.<RTree.ItemSlot, BigInteger>comparing(NodeSlot::getHilbertValue)
                        .thenComparing(NodeSlot::getKey);

        @Nonnull
        private final Subspace subspace;
        private final boolean storeHilbertValues;
        @Nonnull
        private final Function<Point, BigInteger> hilbertValueFunction;
        @Nonnull
        private final OnWriteListener onWriteListener;
        @Nonnull
        private final OnReadListener onReadListener;

        public BySlotStorageAdapter(@Nonnull final Subspace subspace, final boolean storeHilbertValues,
                                    @Nonnull final Function<Point, BigInteger> hilbertValueFunction,
                                    @Nonnull final OnWriteListener onWriteListener,
                                    @Nonnull final OnReadListener onReadListener) {
            this.subspace = subspace;
            this.storeHilbertValues = storeHilbertValues;
            this.hilbertValueFunction = hilbertValueFunction;
            this.onWriteListener = onWriteListener;
            this.onReadListener = onReadListener;
        }

        @Override
        @Nonnull
        public Subspace getSubspace() {
            return subspace;
        }

        @Nonnull
        @Override
        public OnWriteListener getOnWriteListener() {
            return onWriteListener;
        }

        @Nonnull
        @Override
        public OnReadListener getOnReadListener() {
            return onReadListener;
        }

        @Override
        public void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
            Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
            keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(storeHilbertValues));
            final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
            final byte[] packedValue = nodeSlot.getSlotValue().pack();
            transaction.set(packedKey, packedValue);
            onWriteListener.onKeyValueWritten(node, packedKey, packedValue);
        }

        @Override
        public void clearNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
            Tuple keyTuple = Tuple.from(node.getKind().getSerialized());
            keyTuple = keyTuple.addAll(nodeSlot.getSlotKey(storeHilbertValues));
            final byte[] packedKey = keyTuple.pack(packWithSubspace(node.getId()));
            transaction.clear(packedKey);
            onWriteListener.onNodeCleared(node);
        }

        @Override
        public void writeNodes(@Nonnull final Transaction transaction, @Nonnull final List<? extends Node> nodes) {
            // TODO For performance reasons we should attempt to not clear and rewrite slots that remained identical.
            for (final Node node : nodes) {
                transaction.clear(Range.startsWith(packWithSubspace(node.getId())));
                for (final NodeSlot nodeSlot : node.getSlots()) {
                    writeNodeSlot(transaction, node, nodeSlot);
                }
                onWriteListener.onNodeWritten(node);
            }
        }

        @Nonnull
        @Override
        public CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction,
                                                 @Nonnull final byte[] nodeId) {
            return AsyncUtil.collect(transaction.getRange(Range.startsWith(packWithSubspace(nodeId)),
                            ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL))
                    .thenApply(keyValues -> {
                        final Node node = fromKeyValues(nodeId, keyValues);
                        onReadListener.onNodeRead(node);
                        keyValues.forEach(keyValue -> onReadListener.onKeyValueRead(node, keyValue.getKey(), keyValue.getValue()));
                        return node;
                    });
        }

        /**
         * Interpret the returned key/value pairs from a fetch of slots from the database and reconstruct an in-memory
         * {@link Node}.
         * @param nodeId node id
         * @param keyValues a list of key values as returned by the call to {@link Transaction#getRange(Range)}.
         * @return an instance of a subclass of {@link Node} specific to the node kind as read from the database.
         */
        @SuppressWarnings("ConstantValue")
        @Nonnull
        private Node fromKeyValues(@Nonnull final byte[] nodeId, final List<KeyValue> keyValues) {
            List<ItemSlot> itemSlots = null;
            List<ChildSlot> childSlots = null;
            Kind nodeKind = null;

            // one key/value pair corresponds to one slot
            for (final KeyValue keyValue : keyValues) {
                final Tuple keyTuple = getSubspace().unpack(keyValue.getKey()).popFront();
                final Tuple valueTuple = Tuple.fromBytes(keyValue.getValue());

                final Kind currentNodeKind = Kind.fromSerializedNodeKind((byte)keyTuple.getLong(0));
                if (nodeKind == null) {
                    nodeKind = currentNodeKind;
                } else if (nodeKind != currentNodeKind) {
                    throw new IllegalArgumentException("same node id uses different node kinds");
                }

                final Tuple slotKeyTuple = keyTuple.popFront();
                if (nodeKind == Kind.LEAF) {
                    if (itemSlots == null) {
                        itemSlots = Lists.newArrayList();
                    }
                    itemSlots.add(ItemSlot.fromKeyAndValue(slotKeyTuple, valueTuple, hilbertValueFunction));
                } else {
                    Verify.verify(nodeKind == Kind.INTERMEDIATE);
                    if (childSlots == null) {
                        childSlots = Lists.newArrayList();
                    }
                    childSlots.add(ChildSlot.fromKeyAndValue(slotKeyTuple, valueTuple));
                }
            }

            if (nodeKind == null && Arrays.equals(rootId, nodeId)) {
                // root node but nothing read -- root node is the only node that can be empty --
                // this only happens when the R-Tree is completely empty.
                nodeKind = Kind.LEAF;
                itemSlots = Lists.newArrayList();
            }

            Verify.verify((nodeKind == Kind.LEAF && itemSlots != null && childSlots == null) ||
                          (nodeKind == Kind.INTERMEDIATE && itemSlots == null && childSlots != null));

            if (nodeKind == Kind.LEAF &&
                    !storeHilbertValues) {
                //
                // We need to sort the slots by the computed Hilbert value/key. This is not necessary when we store
                // the Hilbert value as fdb does the sorting for us.
                //
                itemSlots.sort(comparator);
            }

            return nodeKind == Kind.LEAF
                   ? new LeafNode(nodeId, itemSlots)
                   : new IntermediateNode(nodeId, childSlots);
        }
    }

    /**
     * Storage adapter that normalizes internal nodes such that each node slot is a key/value pair in the database.
     */
    public static class ByNodeStorageAdapter implements StorageAdapter {
        @Nonnull
        private final Subspace subspace;
        private final boolean storeHilbertValues;
        @Nonnull
        private final Function<Point, BigInteger> hilbertValueFunction;
        @Nonnull
        private final OnWriteListener onWriteListener;
        @Nonnull
        private final OnReadListener onReadListener;

        public ByNodeStorageAdapter(@Nonnull final Subspace subspace, final boolean storeHilbertValues,
                                    @Nonnull final Function<Point, BigInteger> hilbertValueFunction,
                                    @Nonnull final OnWriteListener onWriteListener,
                                    @Nonnull final OnReadListener onReadListener) {
            this.subspace = subspace;
            this.storeHilbertValues = storeHilbertValues;
            this.hilbertValueFunction = hilbertValueFunction;
            this.onWriteListener = onWriteListener;
            this.onReadListener = onReadListener;
        }

        @Override
        @Nonnull
        public Subspace getSubspace() {
            return subspace;
        }

        @Nonnull
        @Override
        public OnWriteListener getOnWriteListener() {
            return onWriteListener;
        }

        @Nonnull
        @Override
        public OnReadListener getOnReadListener() {
            return onReadListener;
        }

        @Override
        public void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
            writeNode(transaction, node);
        }

        @Override
        public void clearNodeSlot(@Nonnull final Transaction transaction, @Nonnull final Node node, @Nonnull final NodeSlot nodeSlot) {
            writeNode(transaction, node);
        }

        private void writeNode(@Nonnull final Transaction transaction, @Nonnull final Node node) {
            final byte[] packedKey = packWithSubspace(node.getId());
            if (node.isEmpty()) {
                // if this node slot was the last node slot, delete the entire node
                transaction.clear(packedKey);
            } else {
                final byte[] packedValue = toTuple(node).pack();
                transaction.set(packedKey, packedValue);
            }
        }

        @Override
        public void writeNodes(@Nonnull final Transaction transaction, @Nonnull final List<? extends Node> nodes) {
            for (final Node node : nodes) {
                writeNode(transaction, node);
            }
        }

        @Nonnull
        private Tuple toTuple(@Nonnull final Node node) {
            final List<Tuple> slotTuples = Lists.newArrayListWithExpectedSize(node.size());
            for (final NodeSlot nodeSlot : node.getSlots()) {
                final Tuple slotTuple = Tuple.fromStream(
                        Streams.concat(nodeSlot.getSlotKey(storeHilbertValues).getItems().stream(),
                                nodeSlot.getSlotValue().getItems().stream()));
                slotTuples.add(slotTuple);
            }
            return Tuple.from(node.getKind().getSerialized(), slotTuples);
        }

        @Nonnull
        @Override
        public CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction,
                                                 @Nonnull final byte[] nodeId) {
            return transaction.get(packWithSubspace(nodeId))
                    .thenApply(valueBytes -> {
                        final Node node = fromTuple(nodeId, valueBytes == null ? null : Tuple.fromBytes(valueBytes));
                        onReadListener.onNodeRead(node);
                        onReadListener.onKeyValueRead(node, null, valueBytes);
                        return node;
                    });
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        private Node fromTuple(@Nonnull final byte[] nodeId, @Nullable final Tuple tuple) {
            if (tuple == null) {
                if (Arrays.equals(rootId, nodeId)) {
                    return new LeafNode(nodeId, Lists.newArrayList());
                }
                throw new IllegalStateException("unable to find node for given node id");
            }

            final Kind nodeKind = Kind.fromSerializedNodeKind((byte)tuple.getLong(0));
            final List<Object> nodeSlotObjects = tuple.getNestedList(1);

            List<ItemSlot> itemSlots = null;
            List<ChildSlot> childSlots = null;

            for (final Object nodeSlotObject : nodeSlotObjects) {
                final List<Object> nodeSlotItems = (List<Object>)nodeSlotObject;

                switch (nodeKind) {
                    case LEAF:
                        final Tuple itemSlotKeyTuple = Tuple.fromList(nodeSlotItems.subList(0, ItemSlot.SLOT_KEY_TUPLE_SIZE));
                        final Tuple itemSlotValueTuple = Tuple.fromList(nodeSlotItems.subList(ItemSlot.SLOT_KEY_TUPLE_SIZE, nodeSlotItems.size()));

                        if (itemSlots == null) {
                            itemSlots = Lists.newArrayListWithExpectedSize(nodeSlotObjects.size());
                        }
                        itemSlots.add(ItemSlot.fromKeyAndValue(itemSlotKeyTuple, itemSlotValueTuple, hilbertValueFunction));
                        break;

                    case INTERMEDIATE:
                        final Tuple childSlotKeyTuple = Tuple.fromList(nodeSlotItems.subList(0, ChildSlot.SLOT_KEY_TUPLE_SIZE));
                        final Tuple childSlotValueTuple = Tuple.fromList(nodeSlotItems.subList(ChildSlot.SLOT_KEY_TUPLE_SIZE, nodeSlotItems.size()));

                        if (childSlots == null) {
                            childSlots = Lists.newArrayListWithExpectedSize(nodeSlotObjects.size());
                        }
                        childSlots.add(ChildSlot.fromKeyAndValue(childSlotKeyTuple, childSlotValueTuple));
                        break;
                    default:
                        throw new IllegalStateException("unknown node kind");
                }
            }

            Verify.verify((nodeKind == Kind.LEAF && itemSlots != null) ||
                          (nodeKind == Kind.INTERMEDIATE && childSlots != null));

            return nodeKind == Kind.LEAF
                   ? new LeafNode(nodeId, itemSlots)
                   : new IntermediateNode(nodeId, childSlots);
        }
    }

    /**
     * Traversal state of a scan over the tree. A scan consists of an initial walk to the left-most applicable leaf node
     * potentially containing items relevant to the scan. The caller then consumes that leaf node and advances to the
     * next leaf node that is relevant to the scan. The notion of <em>next</em> emerges using the order defined by the
     * composite {@code (hilbertValue, key)} for items in leaf nodes and {@code (largestHilbertValue, largestKey)} in
     * intermediate nodes. The traversal state captures the node ids that still have to be processed on each discovered
     * level in order to fulfill the requirements of the scan operation.
     */
    private static class TraversalState {
        @Nullable
        private final List<Deque<ChildSlot>> toBeProcessed;

        @Nullable
        private final LeafNode currentLeafNode;

        private TraversalState(@Nullable final List<Deque<ChildSlot>> toBeProcessed, @Nullable final LeafNode currentLeafNode) {
            this.toBeProcessed = toBeProcessed;
            this.currentLeafNode = currentLeafNode;
        }

        @Nonnull
        public List<Deque<ChildSlot>> getToBeProcessed() {
            return Objects.requireNonNull(toBeProcessed);
        }

        @Nonnull
        public LeafNode getCurrentLeafNode() {
            return Objects.requireNonNull(currentLeafNode);
        }

        public boolean isEnd() {
            return currentLeafNode == null;
        }

        public static TraversalState of(@Nonnull final List<Deque<ChildSlot>> toBeProcessed, @Nonnull final LeafNode currentLeafNode) {
            return new TraversalState(toBeProcessed, currentLeafNode);
        }

        public static TraversalState end() {
            return new TraversalState(null, null);
        }
    }

    /**
     * An {@link AsyncIterator} over the leaf nodes that represent the result of a scan over the tree. This iterator
     * interfaces with the scan logic
     * (see {@link #fetchLeftmostPathToLeaf(ReadTransaction, byte[], BigInteger, Tuple, Predicate)} and
     * {@link #fetchNextPathToLeaf(ReadTransaction, TraversalState, BigInteger, Tuple, Predicate)}) and wraps
     * intermediate {@link TraversalState}s created by these methods.
     */
    public class LeafIterator implements AsyncIterator<LeafNode> {
        @Nonnull
        private final ReadTransaction readTransaction;
        @Nonnull
        private final byte[] rootId;
        @Nullable
        private final BigInteger lastHilbertValue;
        @Nullable
        private final Tuple lastKey;
        @Nonnull
        private final Predicate<Rectangle> mbrPredicate;

        @Nullable
        private TraversalState currentState;
        @Nullable
        private CompletableFuture<TraversalState> nextStateFuture;

        @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
        public LeafIterator(@Nonnull final ReadTransaction readTransaction, @Nonnull final byte[] rootId,
                            @Nullable final BigInteger lastHilbertValue, @Nullable final Tuple lastKey,
                            @Nonnull final Predicate<Rectangle> mbrPredicate) {
            Preconditions.checkArgument((lastHilbertValue == null && lastKey == null) ||
                                        (lastHilbertValue != null && lastKey != null));
            this.readTransaction = readTransaction;
            this.rootId = rootId;
            this.lastHilbertValue = lastHilbertValue;
            this.lastKey = lastKey;
            this.mbrPredicate = mbrPredicate;
            this.currentState = null;
            this.nextStateFuture = null;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (nextStateFuture == null) {
                if (currentState == null) {
                    nextStateFuture = fetchLeftmostPathToLeaf(readTransaction, rootId, lastHilbertValue, lastKey, mbrPredicate);
                } else {
                    nextStateFuture = fetchNextPathToLeaf(readTransaction, currentState, lastHilbertValue, lastKey, mbrPredicate);
                }
            }
            return nextStateFuture.thenApply(traversalState -> !traversalState.isEnd());
        }

        @Override
        public boolean hasNext() {
            return onHasNext().join();
        }

        @Override
        public LeafNode next() {
            if (hasNext()) {
                // underlying has already completed
                currentState = Objects.requireNonNull(nextStateFuture).join();
                nextStateFuture = null;
                return currentState.getCurrentLeafNode();
            }
            throw new NoSuchElementException("called next() on exhausted iterator");
        }

        @Override
        public void cancel() {
            if (nextStateFuture != null) {
                nextStateFuture.cancel(false);
            }
        }
    }

    /**
     * Iterator for iterating the items contained in the leaf nodes produced by an underlying {@link LeafIterator}.
     * This iterator is the async equivalent of
     * {@code Streams.stream(leafIterator).flatMap(leafNode -> leafNode.getItems().stream()).toIterator()}.
     */
    public static class ItemSlotIterator implements AsyncIterator<ItemSlot> {
        @Nonnull
        private final AsyncIterator<LeafNode> leafIterator;
        @Nullable
        private LeafNode currentLeafNode;
        @Nullable
        private Iterator<ItemSlot> currenLeafItemsIterator;

        public ItemSlotIterator(@Nonnull final AsyncIterator<LeafNode> leafIterator) {
            this.leafIterator = leafIterator;
            this.currentLeafNode = null;
            this.currenLeafItemsIterator = null;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (currenLeafItemsIterator != null && currenLeafItemsIterator.hasNext()) {
                return CompletableFuture.completedFuture(true);
            }
            // we know that each leaf has items (or if it doesn't it is the root; we are done if there are no items
            return leafIterator.onHasNext()
                    .thenApply(hasNext -> {
                        if (hasNext) {
                            this.currentLeafNode = leafIterator.next();
                            this.currenLeafItemsIterator = currentLeafNode.getSlots().iterator();
                            return currenLeafItemsIterator.hasNext();
                        }
                        return false;
                    });
        }

        @Override
        public boolean hasNext() {
            return onHasNext().join();
        }

        @Override
        public ItemSlot next() {
            if (hasNext()) {
                return Objects.requireNonNull(currenLeafItemsIterator).next();
            }
            throw new NoSuchElementException("called next() on exhausted iterator");
        }

        @Override
        public void cancel() {
            leafIterator.cancel();
        }
    }

    /**
     * Class to signal the caller of insert/update/delete code paths what the next action in that path should be.
     * The indicated action is either another insert/delete on a higher level in the tree, further adjustments of
     * secondary attributes on a higher level in the tree, or an indication that the insert/update/delete path is done
     * with all necessary modifications.
     */
    private static class NodeOrAdjust {
        public static final NodeOrAdjust NONE = new NodeOrAdjust(null, null, false);
        public static final NodeOrAdjust ADJUST = new NodeOrAdjust(null, null, true);

        @Nullable
        private final ChildSlot slotInParent;
        @Nullable
        private final Node node;

        private final boolean parentNeedsAdjustment;

        private NodeOrAdjust(@Nullable final ChildSlot slotInParent, @Nullable final Node node, final boolean parentNeedsAdjustment) {
            Verify.verify((slotInParent == null && node == null) ||
                          (slotInParent != null && node != null));
            this.slotInParent = slotInParent;
            this.node = node;
            this.parentNeedsAdjustment = parentNeedsAdjustment;
        }

        @Nullable
        public ChildSlot getSlotInParent() {
            return slotInParent;
        }

        @Nullable
        public Node getSplitNode() {
            return node;
        }

        @Nullable
        public Node getTombstoneNode() {
            return node;
        }

        public boolean parentNeedsAdjustment() {
            return parentNeedsAdjustment;
        }
    }

    /**
     * Helper class for the traversal of nodes during tree validation.
     */
    private static class ParentNodeAndChildId {
        @Nullable
        private final IntermediateNode parentNode;
        @Nonnull
        private final byte[] childId;

        public ParentNodeAndChildId(@Nullable final IntermediateNode parentNode, @Nonnull final byte[] childId) {
            this.parentNode = parentNode;
            this.childId = childId;
        }

        @Nullable
        public IntermediateNode getParentNode() {
            return parentNode;
        }

        @Nonnull
        public byte[] getChildId() {
            return childId;
        }
    }

    /**
     * Class to capture an N-dimensional point. It wraps a {@link Tuple} mostly due to proximity with its serialization
     * format and provides helpers for Euclidean operations. Note that the coordinates used here do not need to be
     * numbers.
     */
    public static class Point {
        @Nonnull
        private final Tuple coordinates;

        public Point(@Nonnull final Tuple coordinates) {
            Preconditions.checkArgument(!coordinates.isEmpty());
            this.coordinates = coordinates;
        }

        @Nonnull
        public Tuple getCoordinates() {
            return coordinates;
        }

        public int getNumDimensions() {
            return coordinates.size();
        }

        @Nullable
        public Object getCoordinate(final int dimension) {
            return coordinates.get(dimension);
        }

        @Nullable
        public Number getCoordinateAsNumber(final int dimension) {
            return (Number)getCoordinate(dimension);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Point)) {
                return false;
            }
            final Point point = (Point)o;
            return TupleHelpers.equals(coordinates, point.coordinates);
        }

        @Override
        public int hashCode() {
            return coordinates.hashCode();
        }

        @Nonnull
        @Override
        public String toString() {
            return coordinates.toString();
        }
    }

    /**
     * Class to capture an N-dimensional rectangle/cube/hypercube. It wraps a {@link Tuple} mostly due to proximity
     * with its serialization format and provides helpers for Euclidean operations. Note that the coordinates used here
     * do not need to be numbers.
     */
    public static class Rectangle {
        /**
         * A tuple that holds the coordinates of this N-dimensional rectangle. The layout is defined as
         * {@code (low1, low2, ..., lowN, high1, high2, ..., highN}. Note that we don't use nested {@link Tuple}s for
         * space-saving reasons (when the tuple is serialized).
         */
        @Nonnull
        private final Tuple ranges;

        public Rectangle(final Tuple ranges) {
            Preconditions.checkArgument(!ranges.isEmpty() && ranges.size() % 2 == 0);
            this.ranges = ranges;
        }

        public int getNumDimensions() {
            return ranges.size() >> 1;
        }

        @Nonnull
        public Tuple getRanges() {
            return ranges;
        }

        @Nonnull
        public Object getLow(final int dimension) {
            return ranges.get(dimension);
        }

        @Nonnull
        public Object getHigh(final int dimension) {
            return ranges.get((ranges.size() >> 1) + dimension);
        }

        @Nonnull
        public BigInteger area() {
            BigInteger currentArea = BigInteger.ONE;
            for (int d = 0; d < getNumDimensions(); d++) {
                currentArea = currentArea.multiply(BigInteger.valueOf(((Number)getHigh(d)).longValue() - ((Number)getLow(d)).longValue()));
            }
            return currentArea;
        }

        @Nonnull
        public Rectangle unionWith(@Nonnull final Point point) {
            Preconditions.checkArgument(getNumDimensions() == point.getNumDimensions());
            boolean isModified = false;
            Object[] ranges = new Object[getNumDimensions() << 1];

            for (int d = 0; d < getNumDimensions(); d++) {
                final Object coordinate = point.getCoordinate(d);
                final Tuple coordinateTuple = Tuple.from(coordinate);
                final Object low = getLow(d);
                final Tuple lowTuple = Tuple.from(low);
                if (TupleHelpers.compare(coordinateTuple, lowTuple) < 0) {
                    ranges[d] = coordinate;
                    isModified = true;
                } else {
                    ranges[d] = low;
                }

                final Object high = getHigh(d);
                final Tuple highTuple = Tuple.from(high);
                if (TupleHelpers.compare(coordinateTuple, highTuple) > 0) {
                    ranges[getNumDimensions() + d] = coordinate;
                    isModified = true;
                } else {
                    ranges[getNumDimensions() + d] = high;
                }
            }

            if (!isModified) {
                return this;
            }

            return new Rectangle(Tuple.from(ranges));
        }

        @Nonnull
        public Rectangle unionWith(@Nonnull final Rectangle other) {
            Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
            boolean isModified = false;
            Object[] ranges = new Object[getNumDimensions() << 1];

            for (int d = 0; d < getNumDimensions(); d++) {
                final Object otherLow = other.getLow(d);
                final Tuple otherLowTuple = Tuple.from(otherLow);
                final Object otherHigh = other.getHigh(d);
                final Tuple otherHighTuple = Tuple.from(otherHigh);

                final Object low = getLow(d);
                final Tuple lowTuple = Tuple.from(low);
                if (TupleHelpers.compare(otherLowTuple, lowTuple) < 0) {
                    ranges[d] = otherLow;
                    isModified = true;
                } else {
                    ranges[d] = low;
                }
                final Object high = getHigh(d);
                final Tuple highTuple = Tuple.from(high);
                if (TupleHelpers.compare(otherHighTuple, highTuple) > 0) {
                    ranges[getNumDimensions() + d] = otherHigh;
                    isModified = true;
                } else {
                    ranges[getNumDimensions() + d] = high;
                }
            }

            if (!isModified) {
                return this;
            }

            return new Rectangle(Tuple.from(ranges));
        }

        public boolean isOverlapping(@Nonnull final Rectangle other) {
            Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());

            for (int d = 0; d < getNumDimensions(); d++) {
                final Tuple otherLowTuple = Tuple.from(other.getLow(d));
                final Tuple otherHighTuple = Tuple.from(other.getHigh(d));

                final Tuple lowTuple = Tuple.from(getLow(d));
                final Tuple highTuple = Tuple.from(getHigh(d));

                if (TupleHelpers.compare(highTuple, otherLowTuple) < 0 ||
                        TupleHelpers.compare(lowTuple, otherHighTuple) > 0) {
                    return false;
                }
            }
            return true;
        }

        public boolean contains(@Nonnull final Point point) {
            Preconditions.checkArgument(getNumDimensions() == point.getNumDimensions());

            for (int d = 0; d < getNumDimensions(); d++) {
                final Tuple otherTuple = Tuple.from(point.getCoordinate(d));

                final Tuple lowTuple = Tuple.from(getLow(d));
                final Tuple highTuple = Tuple.from(getHigh(d));

                if (TupleHelpers.compare(highTuple, otherTuple) < 0 ||
                        TupleHelpers.compare(lowTuple, otherTuple) > 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Rectangle)) {
                return false;
            }
            final Rectangle rectangle = (Rectangle)o;
            return TupleHelpers.equals(ranges, rectangle.ranges);
        }

        @Override
        public int hashCode() {
            return ranges.hashCode();
        }

        @Nonnull
        public String toPlotString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("rectangle(");
            for (int d = 0; d < getNumDimensions(); d++) {
                builder.append(((Number)getLow(d)).longValue());
                if (d + 1 < getNumDimensions()) {
                    builder.append("|");
                }
            }

            builder.append(" ");

            for (int d = 0; d < getNumDimensions(); d++) {
                builder.append(((Number)getHigh(d)).longValue() - ((Number)getLow(d)).longValue());
                if (d + 1 < getNumDimensions()) {
                    builder.append(" ");
                }
            }
            builder.append(")#");
            return builder.toString();
        }

        @Nonnull
        @Override
        public String toString() {
            return ranges.toString();
        }
        
        @Nonnull
        public static Rectangle fromPoint(@Nonnull final Point point) {
            final Object[] mbrRanges = new Object[point.getNumDimensions() * 2];
            for (int d = 0; d < point.getNumDimensions(); d++) {
                final Object coordinate = point.getCoordinate(d);
                mbrRanges[d] = coordinate;
                mbrRanges[point.getNumDimensions() + d] = coordinate;
            }
            return new Rectangle(Tuple.from(mbrRanges));
        }
    }

    /**
     * Function interface for a call back whenever we read the slots for a node.
     */
    public interface OnWriteListener {
        OnWriteListener NOOP = new OnWriteListener() {
            @Override
            public <T> CompletableFuture<T> onAsyncReadForWrite(@Nonnull final CompletableFuture<T> future) {
                return future;
            }

            @Override
            public void onNodeWritten(@Nonnull final Node node) {
                // nothing
            }

            @Override
            public void onKeyValueWritten(@Nonnull final Node node, @Nullable final byte[] key, @Nullable final byte[] value) {
                // nothing
            }

            @Override
            public void onNodeCleared(@Nonnull final Node node) {
                // nothing
            }
        };

        <T> CompletableFuture<T> onAsyncReadForWrite(@Nonnull CompletableFuture<T> future);

        void onNodeWritten(@Nonnull Node node);

        void onKeyValueWritten(@Nonnull Node node,
                               @Nullable byte[] key,
                               @Nullable byte[] value);

        void onNodeCleared(@Nonnull Node node);
    }

    /**
     * Function interface for a call back whenever we read the slots for a node.
     */
    public interface OnReadListener {
        OnReadListener NOOP = new OnReadListener() {
            @Override
            public <T> CompletableFuture<T> onAsyncRead(@Nonnull final CompletableFuture<T> future) {
                return future;
            }

            @Override
            public void onNodeRead(@Nonnull final Node node) {
                // nothing
            }

            @Override
            public void onKeyValueRead(@Nonnull final Node node, @Nullable final byte[] key, @Nullable final byte[] value) {
                // nothing
            }
        };

        <T> CompletableFuture<T> onAsyncRead(@Nonnull CompletableFuture<T> future);

        void onNodeRead(@Nonnull Node node);

        void onKeyValueRead(@Nonnull Node node,
                            @Nullable byte[] key,
                            @Nullable byte[] value);
    }
}
