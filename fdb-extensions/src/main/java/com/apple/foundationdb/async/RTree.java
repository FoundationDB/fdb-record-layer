/*
 * RankedSet.java
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.checkerframework.checker.units.qual.A;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import static com.apple.foundationdb.async.AsyncUtil.DONE;
import static com.apple.foundationdb.async.AsyncUtil.READY_FALSE;
import static com.apple.foundationdb.async.AsyncUtil.READY_TRUE;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class RTree {
    private static final int MAX_CONCURRENT_FETCHES = 16;

    public static final Config DEFAULT_CONFIG = new Config();

    public static final int DEFAULT_MIN_M = 16;
    public static final int DEFAULT_MAX_M = 32;
    public static final int DEFAULT_MIN_S = 2;
    public static final int DEFAULT_MAX_S = 3;

    protected final Subspace subspace;
    protected final Executor executor;
    protected final Config config;

    private static final byte[] EMPTY_ARRAY = { };
    private static final byte[] ZERO_ARRAY = { 0 };

    private static byte[] encodeLong(long count) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(count).array();
    }

    private static long decodeLong(byte[] v) {
        return ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Configuration settings for a {@link RTree}.
     */
    public static class Config {
        private final int minM;
        private final int maxM;
        private final int minS;
        private final int maxS;

        protected Config() {
            this.minM = DEFAULT_MIN_M;
            this.maxM = DEFAULT_MAX_M;
            this.minS = DEFAULT_MIN_S;
            this.maxS = DEFAULT_MAX_S;
        }

        protected Config(final int minM, final int maxM, final int minS, final int maxS) {
            this.minM = minM;
            this.maxM = maxM;
            this.minS = minS;
            this.maxS = maxS;
        }

        public int getMinM() {
            return minM;
        }

        public int getMaxM() {
            return maxM;
        }

        public int getMinS() {
            return minS;
        }

        public int getMaxS() {
            return maxS;
        }

        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(minM, maxM);
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
        private int minS = DEFAULT_MIN_S;
        private int maxS = DEFAULT_MAX_S;

        protected ConfigBuilder() {
        }

        protected ConfigBuilder(final int minM, final int maxM, final int minS, final int maxS) {
            this.minM = minM;
            this.maxM = maxM;
            this.minS = minS;
            this.maxS = maxS;
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

        public int getMinS() {
            return minS;
        }

        public void setMinS(final int minS) {
            this.minS = minS;
        }

        public int getMaxS() {
            return maxS;
        }

        public Config build() {
            return new Config(getMinM(), getMaxM(), getMinS(), getMaxS());
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
     * Initialize a new R-tree.
     * @param subspace the subspace where the ranked set is stored
     * @param executor an executor to use when running asynchronous tasks
     * @param config configuration to use
     */
    public RTree(Subspace subspace, Executor executor, Config config) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
    }

    /**
     * Initialize a new R-tree with the default configuration.
     * @param subspace the subspace where the ranked set is stored
     * @param executor an executor to use when running asynchronous tasks
     */
    public RTree(Subspace subspace, Executor executor) {
        this(subspace, executor, DEFAULT_CONFIG);
    }

    public CompletableFuture<Void> init(TransactionContext tc) {
        return initLevels(tc);
    }

    /**
     * Determine whether {@link #init} needs to be called.
     * @param tc the transaction to use to access the database
     * @return {@code true} if this ranked set needs to be initialized
     */
    public CompletableFuture<Boolean> initNeeded(ReadTransactionContext tc) {
        return countCheckedKey(tc, EMPTY_ARRAY).thenApply(Objects::isNull);
    }

    /**
     * Get the subspace used to store this ranked set.
     * @return ranked set subspace
     */
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get executed used by this ranked set.
     * @return executor used when running asynchronous tasks
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this ranked set's configuration.
     * @return ranked set configuration
     */
    public Config getConfig() {
        return config;
    }

//    public CompletableFuture<List<Node>> fetchPartialTree2(@Nonnull final TransactionContext tc,
//                                                           @Nonnull final byte[] rootId,
//                                                           @Nonnull final Predicate<Hypercube> mbrPredicate) {
//        checkKey(rootId);
//        final List<Node> nodesAtLowestLevel = Lists.newArrayList();
//        final ArrayDeque<Slot> toBeProcessed = new ArrayDeque<>();
//        final List<CompletableFuture<Node>> working = Lists.newArrayList();
//        working.add(fetchNode(tc, rootId));
//        return AsyncUtil.whileTrue(() -> {
//            final Iterator<CompletableFuture<Node>> workIterator = working.iterator();
//            while (workIterator.hasNext()) {
//                final CompletableFuture<Node> workItem = workIterator.next();
//                if (workItem.isDone()) {
//                    workIterator.remove();
//                    final Node node = workItem.join();
//                    if (node.getKind() == Kind.INTERMEDIATE) {
//                        final List<byte[]> childrenIds = node.getChildrenIds();
//                        final List<Hypercube> childrenMbrs = node.getChildrenMbrs();
//                        for (int i = 0; i < childrenIds.size(); i++) {
//                            final Hypercube childMbr = childrenMbrs.get(i);
//                            if (mbrPredicate.test(childMbr)) {
//                                final byte[] childId = childrenIds.get(i);
//                                toBeProcessed.addLast(new Slot(node, childId));
//                            }
//                        }
//                    } else {
//                        nodesAtLowestLevel.add(node);
//                    }
//                }
//            }
//
//            while (working.size() <= MAX_CONCURRENT_FETCHES) {
//                final Slot currentSlot = toBeProcessed.pollFirst();
//                if (currentSlot == null) {
//                    break;
//                }
//
//                working.add(fetchNode(tc, currentSlot.getChildId()));
//            }
//
//            if (working.isEmpty()) {
//                return AsyncUtil.READY_FALSE;
//            }
//            return AsyncUtil.whenAny(working).thenApply(v -> true);
//        }, executor).thenApply(vignore -> nodesAtLowestLevel);
//    }

    public CompletableFuture<TraversalState> fetchNextPathToLeaf(@Nonnull final TransactionContext tc,
                                                                 @Nonnull final TraversalState traversalState,
                                                                 @Nonnull final Predicate<Hypercube> mbrPredicate) {
        final List<Deque<byte[]>> toBeProcessed = traversalState.getToBeProcessed();

        int level;
        byte[] currentId = null;
        for (level = toBeProcessed.size() - 1; level >= 0; level --) {
            final var toBeProcessedThisLevel = toBeProcessed.get(level);
            if (!toBeProcessedThisLevel.isEmpty()) {
                currentId = toBeProcessedThisLevel.pollFirst();
                break;
            }
        }

        if (currentId == null) {
            return CompletableFuture.completedFuture(TraversalState.end());
        }

        final int branchLevel = level;
        return fetchLeftmostPathToLeaf(tc, currentId, mbrPredicate).thenApply(nestedTraversalState -> {
            traversalState.setCurrentLeafNode(nestedTraversalState.getCurrentLeafNode());
            toBeProcessed.subList(branchLevel, toBeProcessed.size()).clear();
            toBeProcessed.addAll(nestedTraversalState.getToBeProcessed());
            return traversalState;
        });
    }

    public CompletableFuture<TraversalState> fetchLeftmostPathToLeaf(@Nonnull final TransactionContext tc,
                                                                     @Nonnull final byte[] rootId,
                                                                     @Nonnull final Predicate<Hypercube> mbrPredicate) {
        checkKey(rootId);
        final AtomicReference<byte[]> currentId = new AtomicReference<>(rootId);
        final List<Deque<byte[]>> toBeProcessed = Lists.newArrayList();
        final AtomicReference<Node> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(tc, currentId.get())
                .thenApply(node -> {
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final List<byte[]> childrenIds = node.getChildrenIds();
                        final List<Hypercube> childrenMbrs = node.getChildrenMbrs();
                        final Deque<byte[]> toBeProcessedThisLevel = new ArrayDeque<>();
                        for (int i = 0; i < childrenIds.size(); i++) {
                            final Hypercube childMbr = childrenMbrs.get(i);
                            if (mbrPredicate.test(childMbr)) {
                                final byte[] childId = childrenIds.get(i);
                                toBeProcessedThisLevel.addLast(childId);
                            }
                        }
                        currentId.set(Objects.requireNonNull(toBeProcessedThisLevel.pollFirst()));
                        return true;
                    } else {
                        leafNode.set(node);
                        return false;
                    }
                }), executor).thenApply(vignore -> TraversalState.of(toBeProcessed, leafNode.get()));
    }

    public CompletableFuture<Node> fetchUpdatePathToLeaf(@Nonnull final TransactionContext tc,
                                                         @Nonnull final BigInteger targetHilbertValue) {
        final AtomicReference<Node> parentNode = new AtomicReference<>(null);
        final AtomicInteger slotInParent = new AtomicInteger(-1);
        final AtomicReference<byte[]> currentId = new AtomicReference<>(Node.ROOT_ID);
        final AtomicReference<Node> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(tc, currentId.get())
                .thenApply(node -> {
                    if (parentNode.get() != null) {
                        node.linkToParent(parentNode.get(), slotInParent.get());
                    }
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        byte[] childId = null;
                        final List<byte[]> childrenIds = node.getChildrenIds();
                        final List<BigInteger> childrenLhvs = node.getChildrenLhvs();
                        int slot;
                        for (slot = 0; slot < childrenIds.size(); slot++) {
                            final BigInteger childLhv = childrenLhvs.get(slot);
                            //
                            // Choose subtree with the minimum Hilbert value that is greater than the target
                            // Hilbert value. If there is no such subtree, i.e. the target Hilbert value is the
                            // largest Hilbert value, we choose the largest one in the current node.
                            //
                            if (childLhv.compareTo(targetHilbertValue) > 0 || slot + 1 == childrenIds.size()) {
                                childId = childrenIds.get(slot);
                                break;
                            }
                        }
                        parentNode.set(node);
                        slotInParent.set(slot);
                        currentId.set(Objects.requireNonNull(childId));
                        return true;
                    } else {
                        leafNode.set(node);
                        return false;
                    }
                }), executor).thenApply(vignore -> leafNode.get());
    }

    public CompletableFuture<List<Node>> fetchSiblings(@Nonnull final TransactionContext tc,
                                                       @Nonnull Config config,
                                                       @Nonnull final Node node) {
        Preconditions.checkArgument(!node.isRoot());
        final Node parentNode = Preconditions.checkNotNull(node.getParentNode());
        final ArrayDeque<byte[]> toBeProcessed = new ArrayDeque<>();
        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final int minS = config.getMinS();
        final Node[] siblings = new Node[minS];

        final List<byte[]> childrenIds = parentNode.getChildrenIds();
        int minSibling = node.slotInParent - minS / 2;
        int maxSibling = minSibling + minS;
        if (minSibling < 0) {
            minSibling = 0;
            maxSibling = minS;
        } else if (maxSibling > childrenIds.size()) {
            maxSibling = childrenIds.size();
            minSibling = maxSibling - minS;
        }

        for (int i = minSibling; i < maxSibling; i ++) {
            toBeProcessed.addLast(childrenIds.get(i));
        }

        return AsyncUtil.whileTrue(() -> {
            working.removeIf(CompletableFuture::isDone);

            while (working.size() <= MAX_CONCURRENT_FETCHES) {
                final int index = minS - toBeProcessed.size();
                final byte[] currentId = toBeProcessed.pollFirst();
                if (currentId == null) {
                    break;
                }

                working.add(fetchNode(tc, currentId).thenAccept(siblingNode -> siblings[index] = siblingNode));
            }

            if (working.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(working).thenApply(v -> true);
        }, executor).thenApply(vignore -> Lists.newArrayList(siblings));
    }


    public CompletableFuture<Node> fetchNode(@Nonnull TransactionContext tc, byte[] key) {

    }

    private enum Kind {
        INTERMEDIATE,
        LEAF;
    }

    private static class Node {
        public static byte[] ROOT_ID = new byte[16]; // all zeros for the root
        private final byte[] id;

        @Nullable
        private final List<byte[]> childrenIds;
        @Nullable
        private final List<Hypercube> childrenMbrs;
        @Nullable
        private final List<BigInteger> childrenLhvs;

        @Nullable
        private Node parentNode;
        int slotInParent;

        public Node(@Nonnull final byte[] id,
                    @Nullable final List<byte[]> childrenIds,
                    @Nullable final List<Hypercube> childrenMbrs,
                    @Nullable final List<BigInteger> childrenLhvs) {
            this.id = id;
            this.childrenIds = childrenIds;
            this.childrenMbrs = childrenMbrs;
            this.childrenLhvs = childrenLhvs;
            this.parentNode = null;
            this.slotInParent = -1;
        }

        @Nonnull
        public List<byte[]> getChildrenIds() {
            return Objects.requireNonNull(childrenIds);
        }

        @Nonnull
        public List<Hypercube> getChildrenMbrs() {
            return Objects.requireNonNull(childrenMbrs);
        }

        @Nonnull
        public List<BigInteger> getChildrenLhvs() {
            return Objects.requireNonNull(childrenLhvs);
        }

        public boolean isRoot() {
            return Arrays.equals(ROOT_ID, id);
        }

        @Nonnull
        public Kind getKind() {
            return childrenIds == null ? Kind.LEAF : Kind.INTERMEDIATE;
        }

        @Nullable
        public Node getParentNode() {
            return parentNode;
        }

        public int getSlotInParent() {
            return slotInParent;
        }

        public void linkToParent(@Nonnull final Node parentNode, final int slotInParent) {
            this.parentNode = parentNode;
            this.slotInParent = slotInParent;
        }
    }

    private static class TraversalState {
        @Nullable
        private final List<Deque<byte[]>> toBeProcessed;

        @Nullable
        private Node currentLeafNode;

        private TraversalState(@Nullable final List<Deque<byte[]>> toBeProcessed, @Nullable final Node currentLeafNode) {
            this.toBeProcessed = toBeProcessed;
            this.currentLeafNode = currentLeafNode;
        }

        @Nonnull
        public List<Deque<byte[]>> getToBeProcessed() {
            return Objects.requireNonNull(toBeProcessed);
        }

        @Nonnull
        public Node getCurrentLeafNode() {
            return Objects.requireNonNull(currentLeafNode);
        }

        public void setCurrentLeafNode(@Nullable final Node currentLeafNode) {
            this.currentLeafNode = currentLeafNode;
        }

        public boolean isEnd() {
            return currentLeafNode == null;
        }

        public static TraversalState of(@Nonnull final List<Deque<byte[]>> toBeProcessed, @Nonnull final Node currentLeafNode) {
            return new TraversalState(toBeProcessed, currentLeafNode);
        }

        public static TraversalState end() {
            return new TraversalState(null, null);
        }
    }

    private static class NodeCache {
        @Nonnull
        private final Map<ByteBuffer, Node> cachedNodeMap;

        public NodeCache() {
            this.cachedNodeMap = Maps.newHashMap();
        }

        boolean add(@Nonnull Node node) {
            cachedNodeMap.put(ByteBuffer.wrap(node.id), node);
        }

        boolean contains(@Nonnull final byte[] nodeId) {
            return cachedNodeMap.containsKey(ByteBuffer.wrap(nodeId));
        }
    }

    private static class Hypercube {
        @Nonnull
        private final Object[] ranges;

        public Hypercube(final Object[] ranges) {
            Preconditions.checkArgument(ranges.length > 0 && ranges.length % 2 == 0);
            this.ranges = ranges;
        }

        public int getNumDimensions() {
            return ranges.length >> 1;
        }

        public Object getLow(final int dimension) {
            return ranges[dimension];
        }

        public Object getHigh(final int dimension) {
            return ranges[(ranges.length >> 1) + dimension];
        }
    }

    private static Kind checkAndGetKindOnSameLevel(@Nonnull final Collection<Node> nodesOnSameLevel) {
        Kind kind = null;
        for (final Node node : nodesOnSameLevel) {
            if (kind == null) {
                kind = node.getKind();
            } else {
                if (node.getKind() != kind) {
                    throw new IllegalStateException("all nodes on the same level must be of the same kind");
                }
            }
        }
        return kind;
    }

    /**
     * Add a key to the set.
     *
     * If {@link Config#isCountDuplicates} is {@code false} and {@code key} is already present, the return value is {@code false}.
     * If {@link Config#isCountDuplicates} is {@code true}, the return value is never {@code false} and a duplicate will
     * cause all {@link #rank}s below it to increase by one.
     * @param tc the transaction to use to access the database
     * @param key the key to add
     * @return a future that completes to {@code true} if the ranked set was modified
     */
    public CompletableFuture<Boolean> add(TransactionContext tc, byte[] key) {
        checkKey(key);
        final int keyHash = getKeyHash(key);
        return tc.runAsync(tr ->
            countCheckedKey(tr, key)
                .thenCompose(count -> {
                    final boolean duplicate = count != null && count > 0;   // Is this key already present in the set?
                    if (duplicate && !config.isCountDuplicates()) {
                        return READY_FALSE;
                    }
                    final int nlevels = config.getNLevels();
                    List<CompletableFuture<Void>> futures = new ArrayList<>(nlevels);
                    for (int li = 0; li < nlevels; ++li) {
                        final int level = li;
                        CompletableFuture<Void> future;
                        if (level == 0) {
                            future = addLevelZeroKey(tr, key, level, duplicate);
                        } else if (duplicate || (keyHash & LEVEL_FAN_VALUES[level]) != 0) {
                            // If key is already present (duplicate), then whatever splitting it causes should have
                            // already been done when first added. So, no more now. It is therefore possible, though,
                            // that the count to increment matches the key, rather than being one that precedes it.
                            future = addIncrementLevelKey(tr, key, level, duplicate);
                        } else {
                            // Insert into this level by looking at the count of the previous key in the level
                            // and recounting the next lower level to correct the counts.
                            // Must complete lower levels first for count to be accurate.
                            future = AsyncUtil.whenAll(futures);
                            futures = new ArrayList<>(nlevels - li);
                            future = future.thenCompose(vignore -> addInsertLevelKey(tr, key, level));
                        }
                        futures.add(future);
                    }
                    return AsyncUtil.whenAll(futures).thenApply(vignore -> true);
                }));
    }

    // Use the hash of the key, instead a p value and randomLevel. The key is likely Tuple-encoded.
    protected int getKeyHash(final byte[] key) {
        return config.getHashFunction().hash(key);
    }

    protected CompletableFuture<Void> addLevelZeroKey(Transaction tr, byte[] key, int level, boolean increment) {
        final byte[] k = subspace.pack(Tuple.from(level, key));
        final byte[] v = encodeLong(1);
        if (increment) {
            tr.mutate(MutationType.ADD, k, v);
        } else {
            tr.set(k, v);
        }
        return DONE;
    }

    protected CompletableFuture<Void> addIncrementLevelKey(Transaction tr, byte[] key, int level, boolean orEqual) {
        return getPreviousKey(tr, level, key, orEqual)
                .thenAccept(prevKey -> tr.mutate(MutationType.ADD, subspace.pack(Tuple.from(level, prevKey)), encodeLong(1)));
    }

    protected CompletableFuture<Void> addInsertLevelKey(Transaction tr, byte[] key, int level) {
        return getPreviousKey(tr, level, key, false).thenCompose(prevKey -> {
            CompletableFuture<Long> prevCount = tr.get(subspace.pack(Tuple.from(level, prevKey))).thenApply(RTree::decodeLong);
            CompletableFuture<Long> newPrevCount = countRange(tr, level - 1, prevKey, key);
            return prevCount.thenAcceptBoth(newPrevCount, (prev, newPrev) -> {
                long count = prev - newPrev + 1;
                tr.set(subspace.pack(Tuple.from(level, prevKey)), encodeLong(newPrev));
                tr.set(subspace.pack(Tuple.from(level, key)), encodeLong(count));
            });
        });
    }

    /**
     * Removes a key from the set.
     * @param tc the transaction to use to access the database
     * @param key the key to remove
     * @return a future that completes to {@code true} if the set was modified, that is, if the key was present before this operation
     */
    public CompletableFuture<Boolean> remove(TransactionContext tc, byte[] key) {
        checkKey(key);
        return tc.runAsync(tr ->
                countCheckedKey(tr, key)
                        .thenCompose(count -> {
                            if (count == null || count <= 0) {
                                return READY_FALSE;
                            }
                            // This works even if the current set does not track duplicates but duplicates were added
                            // earlier by one that did.
                            final boolean duplicate = count > 1;
                            final int nlevels = config.getNLevels();
                            final List<CompletableFuture<Void>> futures = new ArrayList<>(nlevels);
                            for (int li = 0; li < nlevels; ++li) {
                                final int level = li;

                                final CompletableFuture<Void> future;

                                if (duplicate) {
                                    // Always subtract one, never clearing a level count key.
                                    // Concurrent requests both subtracting one when the count is two will conflict
                                    // on the level zero key. So it should not be possible for a count to go to zero.
                                    if (level == 0) {
                                        // There is already a read conflict on the level 0 key, so no benefit to atomic op.
                                        tr.set(subspace.pack(Tuple.from(level, key)), encodeLong(count - 1));
                                        future = DONE;
                                    } else {
                                        future = getPreviousKey(tr, level, key, true)
                                                .thenAccept(k -> tr.mutate(MutationType.ADD, subspace.pack(Tuple.from(level, k)), encodeLong(-1)));
                                    }
                                } else {
                                    // This could be optimized to check the hash for which levels should have this key.
                                    // That would require that the hash function never changes, though.
                                    // This allows for it to change, with the distribution perhaps getting a little uneven
                                    // as a result. It even allows for the hash function to return a random number.
                                    // It also further guarantees that counts never go to zero.
                                    final byte[] k = subspace.pack(Tuple.from(level, key));
                                    if (level == 0) {
                                        tr.clear(k);
                                        future = DONE;
                                    } else {
                                        final CompletableFuture<byte[]> cf = tr.get(k);
                                        final CompletableFuture<byte[]> prevKeyF = getPreviousKey(tr, level, key, false);
                                        future = cf.thenAcceptBoth(prevKeyF, (c, prevKey) -> {
                                            long countChange = -1;
                                            if (c != null) {
                                                // Give back additional count from the key we are erasing to the neighbor.
                                                countChange += decodeLong(c);
                                                tr.clear(k);
                                            }
                                            tr.mutate(MutationType.ADD, subspace.pack(Tuple.from(level, prevKey)), encodeLong(countChange));
                                        });
                                    }
                                }
                                futures.add(future);
                            }
                            return AsyncUtil.whenAll(futures).thenApply(vignore -> true);
                        }));
    }

    /**
     * Clears the entire set.
     * @param tc the transaction to use to access the database
     * @return a future that completes when the ranked set has been cleared
     */
    public CompletableFuture<Void> clear(TransactionContext tc) {
        Range range = subspace.range();
        return tc.runAsync(tr -> {
            tr.clear(range);
            return initLevels(tr);
        });
    }

    /**
     * Checks for the presence of a key in the set.
     * @param tc the transaction to use to access the database
     * @param key the key to check for
     * @return a future that completes to {@code true} if the key is present in the ranked set
     */
    public CompletableFuture<Boolean> contains(ReadTransactionContext tc, byte[] key) {
        checkKey(key);
        return countCheckedKey(tc, key).thenApply(c -> c != null && c > 0);
    }

    /**
     * Count the number of occurrences of a key in the set.
     * @param tc the transaction to use to access the database
     * @param key the key to check for
     * @return a future that completes to {@code 0} if the key is not present in the ranked set or
     * {@code 1} if the key is present in the ranked set and duplicates are not counted or
     * the number of occurrences if duplicated are counted separately
     */
    public CompletableFuture<Long> count(ReadTransactionContext tc, byte[] key) {
        checkKey(key);
        return countCheckedKey(tc, key).thenApply(c -> c == null ? Long.valueOf(0) : c);
    }

    private CompletableFuture<Long> countCheckedKey(ReadTransactionContext tc, byte[] key) {
        return tc.readAsync(tr -> tr.get(subspace.pack(Tuple.from(0, key))).thenApply(b -> b == null ? null : decodeLong(b)));
    }

    class NthLookup implements Lookup {
        private long rank;
        private byte[] key = EMPTY_ARRAY;
        private int level = config.getNLevels();
        private Subspace levelSubspace;
        private AsyncIterator<KeyValue> asyncIterator = null;

        public NthLookup(long rank) {
            this.rank = rank;
        }

        public byte[] getKey() {
            return key;
        }

        @Override
        public CompletableFuture<Boolean> next(ReadTransaction tr) {
            final boolean newIterator = asyncIterator == null;
            if (newIterator) {
                level--;
                if (level < 0) {
                    // Down to finest level without finding enough.
                    if (!config.isCountDuplicates()) {
                        key = null;
                    }
                    return READY_FALSE;
                }
                levelSubspace = subspace.get(level);
                asyncIterator = lookupIterator(tr.getRange(levelSubspace.pack(key), levelSubspace.range().end,
                        ReadTransaction.ROW_LIMIT_UNLIMITED,
                        false,
                        StreamingMode.WANT_ALL));
            }
            final long startTime = System.nanoTime();
            final CompletableFuture<Boolean> onHasNext = asyncIterator.onHasNext();
            final boolean wasDone = onHasNext.isDone();
            return onHasNext.thenApply(hasNext -> {
                if (!wasDone) {
                    nextLookupKey(System.nanoTime() - startTime, newIterator, hasNext, level, false);
                }
                if (!hasNext) {
                    // Not enough on this level.
                    key = null;
                    return false;
                }
                KeyValue kv = asyncIterator.next();
                key = levelSubspace.unpack(kv.getKey()).getBytes(0);
                if (rank == 0 && key.length > 0) {
                    // Moved along correct rank, this is the key.
                    return false;
                }
                long count = decodeLong(kv.getValue());
                if (count > rank) {
                    // Narrow search in next finer level.
                    asyncIterator = null;
                    return true;
                }
                rank -= count;
                return true;
            });
        }
    }

    /**
     * Return the Nth item in the set.
     * This operation is also referred to as <i>select</i>.
     * @param tc the transaction to use to access the database
     * @param rank the rank index to find
     * @return a future that completes to the key for the {@code rank}th item or {@code null} if that index is out of bounds
     * @see #rank
     */
    public CompletableFuture<byte[]> getNth(ReadTransactionContext tc, long rank) {
        if (rank < 0) {
            return CompletableFuture.completedFuture((byte[])null);
        }
        return tc.readAsync(tr -> {
            NthLookup nth = new NthLookup(rank);
            return AsyncUtil.whileTrue(() -> nextLookup(nth, tr), executor).thenApply(vignore -> nth.getKey());
        });
    }

    /**
     * Returns the ordered set of keys in a given range.
     * @param tc the transaction to use to access the database
     * @param beginKey the (inclusive) lower bound for the range
     * @param endKey the (exclusive) upper bound for the range
     * @return a list of keys in the ranked set within the given range
     */
    public List<byte[]> getRangeList(ReadTransactionContext tc, byte[] beginKey, byte[] endKey) {
        return tc.read(tr -> getRange(tr, beginKey, endKey).asList().join());
    }

    public AsyncIterable<byte[]> getRange(ReadTransaction tr, byte[] beginKey, byte[] endKey) {
        checkKey(beginKey);
        return AsyncUtil.mapIterable(tr.getRange(subspace.pack(Tuple.from(0, beginKey)),
                subspace.pack(Tuple.from(0, endKey))),
                keyValue -> {
                    Tuple t = subspace.unpack(keyValue.getKey());
                    return t.getBytes(1);
                });
    }

    /**
     * Read the deeper, likely empty, levels to get them into the RYW cache, since individual lookups may only
     * add pieces, requiring additional requests as keys increase.
     * @param tr the transaction to use to access the database
     * @return a future that is complete when the deeper levels have been loaded
     */
    public CompletableFuture<Void> preloadForLookup(ReadTransaction tr) {
        return tr.getRange(subspace.range(), config.getNLevels(), true).asList().thenApply(l -> null);
    }

    protected CompletableFuture<Boolean> nextLookup(Lookup lookup, ReadTransaction tr) {
        return lookup.next(tr);
    }

    protected <T> AsyncIterator<T> lookupIterator(AsyncIterable<T> iterable) {
        return iterable.iterator();
    }

    protected void nextLookupKey(long duration, boolean newIter, boolean hasNext, int level, boolean rankLookup) {
    }

    protected interface Lookup {
        CompletableFuture<Boolean> next(ReadTransaction tr);
    }

    class RankLookup implements Lookup {
        private final byte[] key;
        private final boolean keyShouldBePresent;
        private byte[] rankKey = EMPTY_ARRAY;
        private long rank = 0;
        private Subspace levelSubspace;
        private int level = config.getNLevels();
        private AsyncIterator<KeyValue> asyncIterator = null;
        private long lastCount;

        public RankLookup(byte[] key, boolean keyShouldBePresent) {
            this.key = key;
            this.keyShouldBePresent = keyShouldBePresent;
        }

        public long getRank() {
            return rank;
        }

        @Override
        public CompletableFuture<Boolean> next(ReadTransaction tr) {
            final boolean newIterator = asyncIterator == null;
            if (newIterator) {
                level--;
                if (level < 0) {
                    // Finest level: rank is accurate.
                    return READY_FALSE;
                }
                levelSubspace = subspace.get(level);
                asyncIterator = lookupIterator(tr.getRange(
                        KeySelector.firstGreaterOrEqual(levelSubspace.pack(rankKey)),
                        KeySelector.firstGreaterThan(levelSubspace.pack(key)),
                        ReadTransaction.ROW_LIMIT_UNLIMITED,
                        false,
                        StreamingMode.WANT_ALL));
                lastCount = 0;
            }
            final long startTime = System.nanoTime();
            final CompletableFuture<Boolean> onHasNext = asyncIterator.onHasNext();
            final boolean wasDone = onHasNext.isDone();
            return onHasNext.thenApply(hasNext -> {
                if (!wasDone) {
                    nextLookupKey(System.nanoTime() - startTime, newIterator, hasNext, level, true);
                }
                if (!hasNext) {
                    // Totalled this level: move to next.
                    asyncIterator = null;
                    rank -= lastCount;
                    if (Arrays.equals(rankKey, key)) {
                        // Exact match on this level: no need for finer.
                        return false;
                    }
                    if (!keyShouldBePresent && level == 0 && lastCount > 0) {
                        // If the key need not be present and we are on the finest level, then if it wasn't an exact
                        // match, key would have the next rank after the last one. Except in the case where key is less
                        // than the lowest key in the set, in which case it takes rank 0. This is recognizable because
                        // at level 0, only the leftmost empty array has a count of zero; every other key has a count of one
                        // (or the number of duplicates if those are counted separately).
                        rank++;
                    }
                    return true;
                }
                KeyValue kv = asyncIterator.next();
                rankKey = levelSubspace.unpack(kv.getKey()).getBytes(0);
                lastCount = decodeLong(kv.getValue());
                rank += lastCount;
                return true;
            });

        }
    }

    /**
     * Return the index of a key within the set.
     * @param tc the transaction to use to access the database
     * @param key the key to find
     * @return a future that completes to the index of {@code key} in the ranked set or {@code null} if it is not present
     * @see #getNth
     */
    public CompletableFuture<Long> rank(ReadTransactionContext tc, byte[] key) {
        return rank(tc, key, true);
    }

    /**
     * Return the index of a key within the set.
     * @param tc the transaction to use to access the database
     * @param key the key to find
     * @param nullIfMissing whether to return {@code null} when {@code key} is not present in the set
     * @return a future that completes to the index of {@code key} in the ranked set, or {@code null} if it is not present and {@code nullIfMissing} is {@code true}, or the index {@code key} would have in the ranked set
     * @see #getNth
     */
    public CompletableFuture<Long> rank(ReadTransactionContext tc, byte[] key, boolean nullIfMissing) {
        checkKey(key);
        return tc.readAsync(tr -> {
            if (nullIfMissing) {
                return countCheckedKey(tr, key).thenCompose(count -> {
                    if (count == null || count <= 0) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return rankLookup(tr, key, true);
                });
            } else {
                return rankLookup(tr, key, false);
            }
        });
    }

    private CompletableFuture<Long> rankLookup(ReadTransaction tr, byte[] key, boolean keyShouldBePresent) {
        RankLookup rank = new RankLookup(key, keyShouldBePresent);
        return AsyncUtil.whileTrue(() -> nextLookup(rank, tr), executor).thenApply(vignore -> rank.getRank());
    }

    /**
     * Count the items in the set.
     * @param tc the transaction to use to access the database
     * @return a future that completes to the number of items in the set
     */
    public CompletableFuture<Long> size(ReadTransactionContext tc) {
        Range r = subspace.get(config.getNLevels() - 1).range();
        return tc.readAsync(tr -> AsyncUtil.mapIterable(tr.getRange(r), keyValue -> decodeLong(keyValue.getValue()))
                .asList()
                .thenApply(longs -> longs.stream().reduce(0L, Long::sum)));
    }

    protected Consistency checkConsistency(ReadTransactionContext tc) {
        return tc.read(tr -> {
            final int nlevels = config.getNLevels();
            for (int level = 1; level < nlevels; ++level) {
                byte[] prevKey = null;
                long prevCount = 0;
                AsyncIterator<KeyValue> it = tr.getRange(subspace.range(Tuple.from(level))).iterator();
                while (true) {
                    boolean more = it.hasNext();

                    KeyValue kv = more ? it.next() : null;
                    byte[] nextKey = kv == null ? null : subspace.unpack(kv.getKey()).getBytes(1);
                    if (prevKey != null) {
                        long count = countRange(tr, level - 1, prevKey, nextKey).join();
                        if (prevCount != count) {
                            return new Consistency(level, prevCount, count, toDebugString(tc));
                        }
                    }
                    if (!more) {
                        break;
                    }
                    prevKey = nextKey;
                    prevCount = decodeLong(kv.getValue());
                }
            }
            return new Consistency();
        });
    }

    protected String toDebugString(ReadTransactionContext tc) {
        return tc.read(tr -> {
            final StringBuilder str = new StringBuilder();
            final int nlevels = config.getNLevels();
            for (int level = 0; level < nlevels; ++level) {
                if (level > 0) {
                    str.setLength(str.length() - 2);
                    str.append("\n");
                }
                str.append("L").append(level).append(": ");
                for (KeyValue kv : tr.getRange(subspace.range(Tuple.from(level)))) {
                    byte[] key = subspace.unpack(kv.getKey()).getBytes(1);
                    long count = decodeLong(kv.getValue());
                    str.append("'").append(ByteArrayUtil2.loggable(key)).append("': ").append(count).append(", ");
                }
            }
            return str.toString();
        });
    }

    //
    // Internal
    //
    private static int KEY_LENGTH = 16;

    private static void checkKey(byte[] key) {
        if (key.length != KEY_LENGTH) {
            throw new IllegalArgumentException("key of unsupported format");
        }
    }

    private CompletableFuture<Long> countRange(ReadTransactionContext tc, int level, byte[] beginKey, byte[] endKey) {
        return tc.readAsync(tr ->
                AsyncUtil.mapIterable(tr.getRange(beginKey == null ?
                                subspace.range(Tuple.from(level)).begin :
                                subspace.pack(Tuple.from(level, beginKey)),
                        endKey == null ?
                                subspace.range(Tuple.from(level)).end :
                                subspace.pack(Tuple.from(level, endKey))),
                        keyValue -> decodeLong(keyValue.getValue()))
                        .asList()
                        .thenApply(longs -> longs.stream().reduce(0L, Long::sum)));
    }

    // Get the key before this one at the given level.
    // If orEqual is given, then an exactly matching key is also considered. This is only used when the key is known
    // to be a duplicate or an existing key and so should do whatever it did.
    private CompletableFuture<byte[]> getPreviousKey(TransactionContext tc, int level, byte[] key, boolean orEqual) {
        byte[] k = subspace.pack(Tuple.from(level, key));
        CompletableFuture<byte[]> kf = tc.run(tr ->
                tr.snapshot()
                        .getRange(subspace.pack(Tuple.from(level, EMPTY_ARRAY)),
                                  orEqual ? ByteArrayUtil.join(k, ZERO_ARRAY) : k,
                                  1, true)
                        .asList()
                        .thenApply(kvs -> {
                            if (kvs.isEmpty()) {
                                throw new IllegalStateException("no key found on level");
                            }
                            byte[] prevk = kvs.get(0).getKey();
                            if (!orEqual || !Arrays.equals(prevk, k)) {
                                // If another key were inserted after between this and the target key,
                                // it wouldn't be the one we should increment any more.
                                // But do not conflict when key itself is incremented.
                                byte[] exclusiveBegin = ByteArrayUtil.join(prevk, ZERO_ARRAY);
                                tr.addReadConflictRange(exclusiveBegin, k);
                            }
                            // Do conflict if key is removed entirely.
                            tr.addReadConflictKey(subspace.pack(Tuple.from(0, subspace.unpack(prevk).getBytes(1))));
                            return prevk;
                        }));
        return kf.thenApply(prevk -> subspace.unpack(prevk).getBytes(1));
    }

    private CompletableFuture<Void> initLevels(TransactionContext tc) {
        return tc.runAsync(tr -> {
            final int nlevels = config.getNLevels();
            final List<CompletableFuture<Void>> futures = new ArrayList<>(nlevels);
            // TODO: Add a way to change the number of levels in a ranked set that already exists (https://github.com/FoundationDB/fdb-record-layer/issues/141)
            for (int level = 0; level < nlevels; ++level) {
                byte[] k = subspace.pack(Tuple.from(level, EMPTY_ARRAY));
                byte[] v = encodeLong(0);
                futures.add(tr.get(k).thenAccept(value -> {
                    if (value == null) {
                        tr.set(k, v);
                    }
                }));
            }
            return AsyncUtil.whenAll(futures);
        });
    }

    protected static class Consistency {

        private final boolean consistent;
        private final int level;
        private final long prevCount;
        private final long count;
        private String structure;

        public Consistency(int level, long prevCount, long count, String structure) {
            this.level = level;
            this.prevCount = prevCount;
            this.count = count;
            this.structure = structure;
            consistent = false;
        }

        public Consistency() {
            consistent = true;
            level = 0;
            prevCount = 0;
            count = 0;
            structure = null;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(67);
            sb.append("Consistency{")
                    .append("consistent:").append(isConsistent())
                    .append(", level:").append(level)
                    .append(", prevCount:").append(prevCount)
                    .append(", count:").append(count)
                    .append(", structure:'").append(structure).append('\'')
                    .append('}');
            return sb.toString();
        }

        public boolean isConsistent() {
            return consistent;
        }
    }
}
