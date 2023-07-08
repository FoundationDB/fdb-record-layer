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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class RTree {
    private static final Logger logger = LoggerFactory.getLogger(RTree.class);

    private static final byte[] rootId = new byte[16]; // all zeros for the root

    public static final int NODE_ID_LENGTH = 16;
    public static final int MAX_CONCURRENT_READS = 16;

    public static final Config DEFAULT_CONFIG = new Config();

    public static final int DEFAULT_MIN_M = 16;
    public static final int DEFAULT_MAX_M = 32;
    public static final int DEFAULT_MIN_S = 2;

    @Nonnull
    protected final byte[] subspacePrefix;
    @Nonnull
    protected final Executor executor;
    @Nonnull
    protected final Config config;
    @Nonnull
    protected final Supplier<byte[]> nodeIdSupplier;

    private static final AtomicLong nodeIdState = new AtomicLong(1); // skip the root which is always 0

    /**
     * Configuration settings for a {@link RTree}.
     */
    public static class Config {
        private final int minM;
        private final int maxM;
        private final int minS;

        protected Config() {
            this.minM = DEFAULT_MIN_M;
            this.maxM = DEFAULT_MAX_M;
            this.minS = DEFAULT_MIN_S;
        }

        protected Config(final int minM, final int maxM, final int minS) {
            this.minM = minM;
            this.maxM = maxM;
            this.minS = minS;
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

        public ConfigBuilder toBuilder() {
            return new ConfigBuilder(minM, maxM, minS);
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

        protected ConfigBuilder() {
        }

        protected ConfigBuilder(final int minM, final int maxM, final int minS) {
            this.minM = minM;
            this.maxM = maxM;
            this.minS = minS;
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

        public Config build() {
            return new Config(getMinM(), getMaxM(), getMinS());
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
     * @param subspace the subspace where the r-tree is stored
     * @param executor an executor to use when running asynchronous tasks
     * @param config configuration to use
     * @param nodeIdSupplier supplier to be invoked when new nodes are created
     */
    public RTree(@Nonnull final Subspace subspace, @Nonnull final Executor executor, @Nonnull final Config config, @Nonnull final Supplier<byte[]> nodeIdSupplier) {
        this.subspacePrefix = subspace.getKey();
        this.executor = executor;
        this.config = config;
        this.nodeIdSupplier = nodeIdSupplier;
    }

    /**
     * Initialize a new R-tree with the default configuration.
     * @param subspace the subspace where the r-tree is stored
     * @param executor an executor to use when running asynchronous tasks
     */
    public RTree(Subspace subspace, Executor executor) {
        this(subspace, executor, DEFAULT_CONFIG, RTree::newRandomNodeId);
    }

    /**
     * Get the subspace prefix used to store this r-tree.
     * @return r-tree subspace
     */
    @Nonnull
    public byte[] getSubspacePrefix() {
        return subspacePrefix;
    }

    @Nonnull
    private byte[] packWithPrefix(final byte[] key) {
        return Bytes.concat(getSubspacePrefix(), key);
    }

    /**
     * Get executer used by this r-tree.
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

    @Nonnull
    public AsyncIterator<ItemSlot> scan(@Nonnull final ReadTransaction readTransaction,
                                        @Nonnull final Predicate<Rectangle> mbrPredicate) {
        final LeafIterator leafIterator = new LeafIterator(readTransaction, rootId, mbrPredicate);
        return new ItemIterator(leafIterator);
    }

    @Nonnull
    private CompletableFuture<TraversalState> fetchLeftmostPathToLeaf(@Nonnull final ReadTransaction transaction,
                                                                      @Nonnull final byte[] rootId,
                                                                      @Nonnull final Predicate<Rectangle> mbrPredicate) {
        final AtomicReference<byte[]> currentId = new AtomicReference<>(rootId);
        final List<Deque<ChildSlot>> toBeProcessed = Lists.newArrayList();
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(transaction, currentId.get())
                .thenApply(node -> {
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final List<ChildSlot> children = node.getChildren();
                        Deque<ChildSlot> toBeProcessedThisLevel = new ArrayDeque<>();
                        for (Iterator<ChildSlot> iterator = children.iterator(); iterator.hasNext(); ) {
                            final ChildSlot child = iterator.next();
                            if (mbrPredicate.test(child.getMbr())) {
                                toBeProcessedThisLevel.addLast(child);
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

    @Nonnull
    private CompletableFuture<TraversalState> fetchNextPathToLeaf(@Nonnull final ReadTransaction transaction,
                                                                  @Nonnull final TraversalState traversalState,
                                                                  @Nonnull final Predicate<Rectangle> mbrPredicate) {

        final List<Deque<ChildSlot>> toBeProcessed = traversalState.getToBeProcessed();
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);

        return AsyncUtil.whileTrue(() -> {
            final ChildSlot nextChildSlot = resolveNextIdForFetch(toBeProcessed, mbrPredicate);
            if (nextChildSlot == null) {
                return AsyncUtil.READY_FALSE;
            }

            return fetchLeftmostPathToLeaf(transaction, nextChildSlot.getChildId(), mbrPredicate).thenApply(nestedTraversalState -> {
                if (nestedTraversalState.isEnd()) {
                    return true;
                }
                leafNode.set(nestedTraversalState.getCurrentLeafNode());
                toBeProcessed.addAll(nestedTraversalState.getToBeProcessed());
                return false;
            });
        }, executor).thenApply(v -> leafNode.get() == null
                                    ? TraversalState.end()
                                    : TraversalState.of(toBeProcessed, leafNode.get()));
    }

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

    // INSERT/UPDATE

    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final TransactionContext tc,
                                          @Nonnull final Point point,
                                          @Nonnull final BigInteger hilbertValue,
                                          @Nonnull final Tuple key,
                                          @Nonnull final Tuple value) {
        return tc.runAsync(transaction -> fetchUpdatePathToLeaf(transaction, hilbertValue, key)
                .thenCompose(leafNode -> insertOrUpdateSlot(transaction, leafNode, point, hilbertValue, key, value)));
    }

    @Nonnull
    private CompletableFuture<Void> insertOrUpdateSlot(@Nonnull final Transaction transaction,
                                                       @Nonnull final LeafNode targetNode,
                                                       @Nonnull final Point point,
                                                       @Nonnull final BigInteger hilbertValue,
                                                       @Nonnull final Tuple key,
                                                       @Nonnull final Tuple value) {
        Verify.verify(targetNode.getSlots().size() <= config.getMaxM());

        final NodeSlot newSlot = new ItemSlot(hilbertValue, key, value, point);
        final AtomicInteger insertSlotIndex = new AtomicInteger(findInsertItemSlotIndex(targetNode, hilbertValue, key));
        if (insertSlotIndex.get() < 0) {
            // just update the slot with the potentially new value
            writeNodeSlot(transaction, targetNode.getId(), newSlot);
            return AsyncUtil.DONE;
        }

        final AtomicReference<Node> currentNode = new AtomicReference<>(targetNode);
        final AtomicReference<NodeSlot> parentSlot = new AtomicReference<>(newSlot);

        return AsyncUtil.whileTrue(() -> {
            final NodeSlot currentNewSlot = parentSlot.get();

            if (currentNewSlot != null) {
                return insertSlotIntoTargetNode(transaction, currentNode.get(), currentNewSlot, insertSlotIndex.get())
                        .thenApply(splitNodeOrAdjust -> {
                            if (currentNode.get().isRoot()) {
                                return false;
                            }
                            currentNode.set(currentNode.get().getParentNode());
                            parentSlot.set(splitNodeOrAdjust.getSlotInParent());
                            insertSlotIndex.set(splitNodeOrAdjust.getSplitNode() == null ? -1 : splitNodeOrAdjust.getSplitNode().getSlotIndexInParent());
                            return splitNodeOrAdjust.getSplitNode() != null || splitNodeOrAdjust.parentNeedsAdjustment();
                        });
            } else {
                // adjustment only
                final SplitNodeOrAdjust splitNodeSlotOrAdjust = updateSlotsAndAdjustNode(transaction, currentNode.get());
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

    @Nonnull
    private CompletableFuture<SplitNodeOrAdjust> insertSlotIntoTargetNode(@Nonnull final Transaction transaction,
                                                                          @Nonnull final Node targetNode,
                                                                          @Nonnull final NodeSlot newSlot,
                                                                          final int slotIndexInTargetNode) {
        if (targetNode.getSlots().size() < config.getMaxM()) {
            logger.trace("regular insert without splitting; node={}; size={}", bytesToHex(targetNode.getId()), targetNode.size());
            targetNode.insertSlot(slotIndexInTargetNode, newSlot);

            if (targetNode.getKind() == Kind.INTERMEDIATE) {
                // if this is an insert for an intermediate node, the child node referred to by newSlot
                // is a split node from a lower level and all slots need to be updated
                updateNodeSlotsForNodes(transaction, Collections.singletonList(targetNode));
            } else {
                Verify.verify(targetNode.getKind() == Kind.LEAF);
                writeNodeSlot(transaction, targetNode.getId(), newSlot);
            }

            // node has left some space -- indicate that we are done splitting at the current node
            if (!targetNode.isRoot()) {
                return CompletableFuture.completedFuture(adjustSlotInParent(targetNode)
                                                         ? SplitNodeOrAdjust.ADJUST
                                                         : SplitNodeOrAdjust.NONE);
            }
            return CompletableFuture.completedFuture(SplitNodeOrAdjust.NONE); // no split and no adjustment
        } else {
            //
            // if this is the root we need to grow the tree taller
            //
            if (targetNode.isRoot()) {
                logger.trace("splitting root node; size={}", targetNode.size());
                // temporarily overfill the old root node
                targetNode.insertSlot(slotIndexInTargetNode, newSlot);
                splitRootNode(transaction, targetNode);
                return CompletableFuture.completedFuture(SplitNodeOrAdjust.NONE);
            }

            //
            // Node is full -- borrow some space from the siblings if possible
            //
            final CompletableFuture<List<Node>> siblings =
                    fetchSiblings(transaction, targetNode);

            return siblings.thenApply(siblingNodes -> {
                int numSlots =
                        Math.toIntExact(siblingNodes
                                .stream()
                                .mapToLong(siblingNode -> siblingNode.getSlots().size())
                                .sum());

                final Node splitNode;
                if (numSlots == siblingNodes.size() * config.getMaxM()) {
                    logger.trace("splitting node; node={}, siblings={}",
                            bytesToHex(targetNode.getId()),
                            siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    splitNode = targetNode.newOfSameKind();
                    // link this split node to become the last node of the siblings
                    splitNode.linkToParent(Objects.requireNonNull(targetNode.getParentNode()),
                            siblingNodes.get(siblingNodes.size() - 1).getSlotIndexInParent() + 1);
                    siblingNodes.add(splitNode);
                } else {
                    logger.trace("handling overflow; node={}, numSlots={}, siblings={}",
                            bytesToHex(targetNode.getId()),
                            numSlots,
                            siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    splitNode = null;
                }

                // temporarily overfill targetNode
                numSlots ++;
                targetNode.insertSlot(slotIndexInTargetNode, newSlot);

                // sibling nodes are in hilbert value order
                final Iterator<? extends NodeSlot> slotIterator =
                        siblingNodes
                                .stream()
                                .flatMap(siblingNode -> siblingNode.getSlots().stream())
                                .iterator();

                final int base = numSlots / siblingNodes.size();
                int rest = numSlots % siblingNodes.size();

                List<List<NodeSlot>> newNodeSlotLists = Lists.newArrayList();
                List<NodeSlot> currentNodeSlots = Lists.newArrayList();
                while (slotIterator.hasNext()) {
                    final NodeSlot slot = slotIterator.next();
                    currentNodeSlots.add(slot);
                    if (currentNodeSlots.size() == base + (rest > 0 ? 1 : 0)) {
                        if (rest > 0) {
                            // one fewer to smear
                            rest--;
                        }

                        newNodeSlotLists.add(currentNodeSlots);
                        currentNodeSlots = Lists.newArrayList();
                    }
                }

                Verify.verify(siblingNodes.size() == newNodeSlotLists.size());

                final Iterator<Node> siblingNodesIterator = siblingNodes.iterator();
                final Iterator<List<NodeSlot>> newNodeSlotsIterator = newNodeSlotLists.iterator();

                while (siblingNodesIterator.hasNext()) {
                    final Node siblingNode = siblingNodesIterator.next();
                    Verify.verify(newNodeSlotsIterator.hasNext());
                    final List<NodeSlot> newNodeSlots = newNodeSlotsIterator.next();
                    siblingNode.setSlots(newNodeSlots);
                }

                updateNodeSlotsForNodes(transaction, siblingNodes);

                for (final Node siblingNode : siblingNodes) {
                    if (siblingNode != splitNode) {
                        adjustSlotInParent(siblingNode);
                    }
                }

                if (splitNode == null) {
                    return SplitNodeOrAdjust.ADJUST;
                }

                final var lastSlotOfSplitNode = splitNode.getSlots().get(splitNode.size() - 1);
                return new SplitNodeOrAdjust(new ChildSlot(lastSlotOfSplitNode.getHilbertValue(),
                        lastSlotOfSplitNode.getKey(), splitNode.getId(), computeMbr(splitNode.getSlots())), splitNode,
                        true);
            });
        }
    }

    private void splitRootNode(@Nonnull final Transaction transaction,
                               @Nonnull final Node targetNode) {
        final Node leftNode = targetNode.newOfSameKind();
        final Node rightNode = targetNode.newOfSameKind();
        final int leftSize = targetNode.size() / 2;
        final ArrayList<? extends NodeSlot> leftSlots = Lists.newArrayList(targetNode.getSlots().subList(0, leftSize));
        leftNode.setSlots(leftSlots);
        final int rightSize = targetNode.size() - leftSize;
        final ArrayList<? extends NodeSlot> rightSlots = Lists.newArrayList(targetNode.getSlots().subList(leftSize, leftSize + rightSize));
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

        updateNodeSlotsForNodes(transaction, Lists.newArrayList(leftNode, rightNode, newRootNode));
    }

    // DELETE

    @Nonnull
    public CompletableFuture<Void> delete(@Nonnull final TransactionContext tc,
                                          @Nonnull final BigInteger hilbertValue,
                                          @Nonnull final Tuple key) {
        return tc.runAsync(transaction -> fetchUpdatePathToLeaf(transaction, hilbertValue, key)
                .thenCompose(leafNode -> deleteSlotIfExists(transaction, leafNode, hilbertValue, key)));
    }

    @Nonnull
    public CompletableFuture<Void> deleteSlotIfExists(@Nonnull final Transaction transaction,
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
        final NodeSlot deleteSlot = targetNode.getItems().get(deleteSlotIndex.get());
        final AtomicReference<Node> currentNode = new AtomicReference<>(targetNode);
        final AtomicReference<NodeSlot> parentSlot = new AtomicReference<>(deleteSlot);

        return AsyncUtil.whileTrue(() -> {
            final NodeSlot currentDeleteSlot = parentSlot.get();

            if (currentDeleteSlot != null) {
                return deleteSlotFromTargetNode(transaction, currentNode.get(), currentDeleteSlot, deleteSlotIndex.get())
                        .thenApply(splitNodeOrAdjust -> {
                            if (currentNode.get().isRoot()) {
                                return false;
                            }
                            currentNode.set(currentNode.get().getParentNode());
                            parentSlot.set(splitNodeOrAdjust.getSlotInParent());
                            deleteSlotIndex.set(splitNodeOrAdjust.getSplitNode() == null ? -1 : splitNodeOrAdjust.getSplitNode().getSlotIndexInParent());
                            return splitNodeOrAdjust.getSplitNode() != null || splitNodeOrAdjust.parentNeedsAdjustment();
                        });
            } else {
                // adjustment only
                final SplitNodeOrAdjust splitNodeSlotOrAdjust = updateSlotsAndAdjustNode(transaction, currentNode.get());
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

    @Nonnull
    public CompletableFuture<SplitNodeOrAdjust> deleteSlotFromTargetNode(@Nonnull final Transaction transaction,
                                                                         @Nonnull final Node targetNode,
                                                                         @Nonnull final NodeSlot deleteSlot,
                                                                         final int slotIndexInTargetNode) {
        //
        // We need to keep the number of slots per node between minM <= size() <= maxM unless this is the root node.
        //
        if (targetNode.isRoot() || targetNode.getSlots().size() > config.getMinM()) {
            logger.trace("regular delete; node={}; size={}", bytesToHex(targetNode.getId()), targetNode.size());
            targetNode.deleteSlot(slotIndexInTargetNode);

            if (targetNode.getKind() == Kind.INTERMEDIATE) {
                // If this node is the root and the root node is an intermediate node, then it should at least have two
                // children.
                Verify.verify(!targetNode.isRoot() || targetNode.getSlots().size() > 2);
                // if this is a delete of an intermediate node, that node is a split node and all
                // slots need to be updated (i.e. cleared)
                updateNodeSlotsForNodes(transaction, Collections.singletonList(targetNode));
            } else {
                Verify.verify(targetNode.getKind() == Kind.LEAF);
                clearNodeSlot(transaction, targetNode.getId(), deleteSlot);
            }

            // node is not under-flowing -- indicate that we are done fusing at the current node
            if (!targetNode.isRoot()) {
                return CompletableFuture.completedFuture(adjustSlotInParent(targetNode)
                                                         ? SplitNodeOrAdjust.ADJUST
                                                         : SplitNodeOrAdjust.NONE);
            }
            return CompletableFuture.completedFuture(SplitNodeOrAdjust.NONE); // no fuse and no adjustment
        } else {
            //
            // Node is under min-capacity -- borrow some children/items from the siblings if possible
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
                if (numSlots == siblingNodes.size() * config.getMinM()) {
                    logger.trace("fusing nodes; node={}, siblings={}",
                            bytesToHex(targetNode.getId()),
                            siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    tombstoneNode = siblingNodes.get(siblingNodes.size() - 1);
                    siblingNodes.remove(siblingNodes.size() - 1);
                } else {
                    logger.trace("handling underflow; node={}, numSlots={}, siblings={}",
                            bytesToHex(targetNode.getId()),
                            numSlots,
                            siblingNodes.stream().map(node -> bytesToHex(node.getId())).collect(Collectors.joining(",")));
                    tombstoneNode = null;
                }

                // temporarily underfill targetNode
                numSlots --;
                targetNode.deleteSlot(slotIndexInTargetNode);

                // sibling nodes are in hilbert value order
                final Iterator<? extends NodeSlot> slotIterator =
                        siblingNodes
                                .stream()
                                .flatMap(siblingNode -> siblingNode.getSlots().stream())
                                .iterator();

                final int base = numSlots / siblingNodes.size();
                int rest = numSlots % siblingNodes.size();

                List<List<NodeSlot>> newNodeSlotLists = Lists.newArrayList();
                List<NodeSlot> currentNodeSlots = Lists.newArrayList();
                while (slotIterator.hasNext()) {
                    final NodeSlot slot = slotIterator.next();
                    currentNodeSlots.add(slot);
                    if (currentNodeSlots.size() == base + (rest > 0 ? 1 : 0)) {
                        if (rest > 0) {
                            // one fewer to smear
                            rest--;
                        }

                        newNodeSlotLists.add(currentNodeSlots);
                        currentNodeSlots = Lists.newArrayList();
                    }
                }

                Verify.verify(siblingNodes.size() == newNodeSlotLists.size());

                if (tombstoneNode != null) {
                    // remove the slots for the tombstone node and update
                    tombstoneNode.setSlots(Lists.newArrayList());
                    updateNodeSlotsForNodes(transaction, Collections.singletonList(tombstoneNode));
                }

                final Iterator<Node> siblingNodesIterator = siblingNodes.iterator();
                final Iterator<List<NodeSlot>> newNodeSlotsIterator = newNodeSlotLists.iterator();

                // assign the slots to the appropriate nodes
                while (siblingNodesIterator.hasNext()) {
                    final Node siblingNode = siblingNodesIterator.next();
                    Verify.verify(newNodeSlotsIterator.hasNext());
                    final List<NodeSlot> newNodeSlots = newNodeSlotsIterator.next();
                    siblingNode.setSlots(newNodeSlots);
                }

                final IntermediateNode parentNode = Objects.requireNonNull(targetNode.getParentNode());
                if (parentNode.isRoot() && parentNode.size() == 2 && tombstoneNode != null) {
                    //
                    // The parent node (root) would only have one child after this delete.
                    // We shrink the tree by removing the root and making the last remaining sibling the root.
                    //
                    promoteNodeToRoot(transaction, Iterables.getOnlyElement(siblingNodes));
                    return SplitNodeOrAdjust.NONE;
                }

                updateNodeSlotsForNodes(transaction, siblingNodes);

                for (final Node siblingNode : siblingNodes) {
                    adjustSlotInParent(siblingNode);
                }

                if (tombstoneNode == null) {
                    return SplitNodeOrAdjust.ADJUST;
                }

                return new SplitNodeOrAdjust(parentNode.getChildren().get(tombstoneNode.getSlotIndexInParent()),
                        tombstoneNode, true);
            });
        }
    }

    private void promoteNodeToRoot(final @Nonnull Transaction transaction, final Node node) {
        final List<? extends NodeSlot> newRootSlots = node.getSlots();
        node.setSlots(Lists.newArrayList());
        final Node newRootNode = node.newOfSameKind(rootId);
        newRootNode.setSlots(newRootSlots);
        updateNodeSlotsForNodes(transaction, ImmutableList.of(newRootNode, node));
    }

    // GENERAL HELPER METHODS

    @Nonnull
    public SplitNodeOrAdjust updateSlotsAndAdjustNode(@Nonnull final Transaction transaction,
                                                      @Nonnull final Node targetNode) {
        updateNodeSlotsForNodes(transaction, Collections.singletonList(targetNode));
        if (targetNode.isRoot()) {
            return SplitNodeOrAdjust.NONE;
        }

        return adjustSlotInParent(targetNode)
               ? SplitNodeOrAdjust.ADJUST
               : SplitNodeOrAdjust.NONE;
    }

    private static boolean adjustSlotInParent(final @Nonnull Node targetNode) {
        Preconditions.checkArgument(!targetNode.isRoot());
        boolean slotHasChanged;
        final IntermediateNode parentNode = Objects.requireNonNull(targetNode.getParentNode());
        final ChildSlot childSlot = Objects.requireNonNull(parentNode.getChildren()).get(targetNode.getSlotIndexInParent());
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

    @Nonnull
    public CompletableFuture<LeafNode> fetchUpdatePathToLeaf(@Nonnull final Transaction transaction,
                                                             @Nonnull final BigInteger hilbertValue,
                                                             @Nonnull final Tuple key) {
        final AtomicReference<IntermediateNode> parentNode = new AtomicReference<>(null);
        final AtomicInteger slotInParent = new AtomicInteger(-1);
        final AtomicReference<byte[]> currentId = new AtomicReference<>(rootId);
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(transaction, currentId.get())
                .thenApply(node -> {
                    if (parentNode.get() != null) {
                        node.linkToParent(parentNode.get(), slotInParent.get());
                    }
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final int slotIndex = findChildSlotIndex((IntermediateNode)node, hilbertValue, key);
                        final List<ChildSlot> children = node.getChildren();
                        parentNode.set((IntermediateNode)node);
                        slotInParent.set(slotIndex);
                        final ChildSlot childSlot = children.get(slotIndex);
                        currentId.set(childSlot.getChildId());
                        return true;
                    } else {
                        //logger.info("path to leaf; path={}", nodeIdPath(node));
                        leafNode.set((LeafNode)node);
                        return false;
                    }
                }), executor)
                .thenApply(vignore -> {
                    final LeafNode node = leafNode.get();
                    logger.trace("update path; path={}", nodeIdPath(node));
                    return node;
                });
    }

    /**
     * We assume that {@code parentNode} has at least {@link Config#getMinM()} entries.
     */
    @Nonnull
    public CompletableFuture<List<Node>> fetchSiblings(@Nonnull final Transaction transaction,
                                                       @Nonnull final Node node) {
        final ArrayDeque<byte[]> toBeProcessed = new ArrayDeque<>();
        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final int numSiblings = config.getMinS();
        final Node[] siblings = new Node[numSiblings];

        final IntermediateNode parentNode = Objects.requireNonNull(node.getParentNode());
        final List<ChildSlot> children = parentNode.getChildren();
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

        for (int i = start; i < end; i ++) {
            toBeProcessed.addLast(children.get(i).getChildId());
        }

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
                    working.add(fetchNode(transaction, currentId).thenAccept(siblingNode -> {
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

    @Nonnull
    private CompletableFuture<Node> fetchNode(@Nonnull final ReadTransaction transaction, byte[] nodeId) {
        return AsyncUtil.collect(transaction.getRange(Range.startsWith(packWithPrefix(nodeId))))
                .thenApply(keyValues -> nodeFromKeyValues(nodeId, keyValues));
    }

    @SuppressWarnings("ConstantValue")
    @Nonnull
    protected Node nodeFromKeyValues(final byte[] nodeId, final List<KeyValue> keyValues) {
        List<ChildSlot> childSlots = null;
        List<ItemSlot> itemSlots = null;
        Kind nodeKind = null;
        for (final KeyValue keyValue : keyValues) {
            final byte[] rawKey = keyValue.getKey();
            final int tupleOffset = getSubspacePrefix().length + NODE_ID_LENGTH;
            final Tuple keyTuple = Tuple.fromBytes(rawKey, tupleOffset, rawKey.length - tupleOffset);
            Verify.verify(keyTuple.size() == 3);
            final Kind currentNodeKind;
            switch ((byte)(long)keyTuple.get(0)) {
                case 0x00:
                    currentNodeKind = Kind.LEAF;
                    break;
                case 0x01:
                    currentNodeKind = Kind.INTERMEDIATE;
                    break;
                default:
                    throw new IllegalArgumentException("unknown node kind");
            }
            if (nodeKind == null) {
                nodeKind = currentNodeKind;
            } else if (nodeKind != currentNodeKind) {
                throw new IllegalArgumentException("same node id uses different node kinds");
            }

            final Tuple valueTuple = Tuple.fromBytes(keyValue.getValue());

            if (nodeKind == Kind.LEAF) {
                if (itemSlots == null) {
                    itemSlots = Lists.newArrayList();
                }
                itemSlots.add(new ItemSlot(keyTuple.getBigInteger(1), keyTuple.getNestedTuple(2),
                        valueTuple.getNestedTuple(0), new Point(valueTuple.getNestedTuple(1))));
            } else {
                Verify.verify(nodeKind == Kind.INTERMEDIATE);
                if (childSlots == null) {
                    childSlots = Lists.newArrayList();
                }
                childSlots.add(new ChildSlot(keyTuple.getBigInteger(1),
                        keyTuple.getNestedTuple(2), valueTuple.getBytes(0),
                        new Rectangle(valueTuple.getNestedTuple(1))));
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
        return nodeKind == Kind.LEAF
               ? new LeafNode(nodeId, itemSlots)
               : new IntermediateNode(nodeId, childSlots);
    }

    private void updateNodeSlotsForNodes(@Nonnull final Transaction transaction, @Nonnull List<? extends Node> nodes) {
        for (final Node node : nodes) {
            transaction.clear(Range.startsWith(packWithPrefix(node.getId())));
            for (final NodeSlot nodeSlot : node.getSlots()) {
                writeNodeSlot(transaction, node.getId(), nodeSlot);
            }
        }
    }

    private void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final byte[] nodeId, @Nonnull NodeSlot nodeSlot) {
        final byte[] packedKey;
        final byte[] packedValue;
        if (nodeSlot instanceof ItemSlot) {
            final ItemSlot itemSlot = (ItemSlot)nodeSlot;
            packedKey = Tuple.from((byte)0x00, itemSlot.getHilbertValue(), itemSlot.getKey()).pack(packWithPrefix(nodeId));
            packedValue = Tuple.from(itemSlot.getValue(), itemSlot.getPosition().getCoordinates()).pack();
        } else {
            Verify.verify(nodeSlot instanceof ChildSlot);
            final ChildSlot childSlot = (ChildSlot)nodeSlot;
            packedKey = Tuple.from((byte)0x01, childSlot.getLargestHilbertValue(), childSlot.getLargestKey()).pack(packWithPrefix(nodeId));
            packedValue = Tuple.from(childSlot.getChildId(), childSlot.getMbr().getRanges()).pack();
        }
        transaction.set(packedKey, packedValue);
    }

    private void clearNodeSlot(@Nonnull final Transaction transaction, @Nonnull final byte[] nodeId, @Nonnull NodeSlot nodeSlot) {
        final byte[] packedKey;
        if (nodeSlot instanceof ItemSlot) {
            final ItemSlot itemSlot = (ItemSlot)nodeSlot;
            packedKey = Tuple.from((byte)0x00, itemSlot.getHilbertValue(), itemSlot.getKey()).pack(packWithPrefix(nodeId));
        } else {
            Verify.verify(nodeSlot instanceof ChildSlot);
            final ChildSlot childSlot = (ChildSlot)nodeSlot;
            packedKey = Tuple.from((byte)0x01, childSlot.getLargestHilbertValue(), childSlot.getLargestKey()).pack(packWithPrefix(nodeId));
        }
        transaction.clear(packedKey);
    }

    private static int findChildSlotIndex(@Nonnull final IntermediateNode node,
                                          @Nonnull final BigInteger hilbertValue,
                                          @Nonnull final Tuple key) {
        Verify.verify(!node.isEmpty());
        final List<? extends NodeSlot> slots = node.getSlots();
        for (int slotIndex = 0; slotIndex < slots.size(); slotIndex++) {
            final NodeSlot slot = slots.get(slotIndex);
            //
            // Choose subtree with the minimum Hilbert value that is greater than the target
            // Hilbert value. If there is no such subtree, i.e. the target Hilbert value is the
            // largest Hilbert value, we choose the largest one in the current node.
            //
            final int hilbertValueCompare = slot.getHilbertValue().compareTo(hilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    // child == target
                    final var tupleCompare = TupleHelpers.compare(slot.getKey(), key);
                    if (tupleCompare >= 0) {
                        return slotIndex;
                    }
                } else {
                    return slotIndex;
                }
            }
        }

        // this is an intermediate node; we insert INTO the last child
        return slots.size() - 1;
    }

    private static int findInsertItemSlotIndex(@Nonnull final LeafNode leafNode,
                                               @Nonnull final BigInteger hilbertValue,
                                               @Nonnull final Tuple key) {
        final List<ItemSlot> items = leafNode.getItems();
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

    private static int findDeleteItemSlotIndex(@Nonnull final LeafNode leafNode,
                                               @Nonnull final BigInteger hilbertValue,
                                               @Nonnull final Tuple key) {
        final List<ItemSlot> items = leafNode.getItems();
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

    @Nonnull
    private static Rectangle computeMbr(@Nonnull final List<? extends NodeSlot> slots) {
        Verify.verify(!slots.isEmpty());
        Rectangle mbr = null;
        for (final NodeSlot slot : slots) {
            if (slot instanceof ItemSlot) {
                final var position = ((ItemSlot)slot).getPosition();
                if (mbr == null) {
                    mbr = Rectangle.fromPoint(position);
                } else {
                    mbr = mbr.unionWith(position);
                }
            }  else if (slot instanceof ChildSlot) {
                final var mbrForSlot = ((ChildSlot)slot).getMbr();
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

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    @Nonnull
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return "0x" + new String(hexChars).replaceFirst("^0+(?!$)", "");
    }

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

    enum Kind {
        INTERMEDIATE,
        LEAF
    }

    abstract class Node {
        private final byte[] id;

        @Nullable
        private IntermediateNode parentNode;
        int slotIndexInParent;

        public Node(@Nonnull final byte[] id, @Nullable final IntermediateNode parentNode, final int slotIndexInParent) {
            this.id = id;
            this.parentNode = parentNode;
            this.slotIndexInParent = slotIndexInParent;
        }

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
        public List<? extends NodeSlot> getSlots() {
            if (getKind() == Kind.LEAF) {
                return getItems();
            } else if (getKind() == Kind.INTERMEDIATE) {
                return getChildren();
            }
            throw new IllegalStateException("unable to return slots");
        }

        public abstract void insertSlot(int slotIndex, @Nonnull NodeSlot slot);

        public void deleteSlot(int slotIndex) {
            getSlots().remove(slotIndex);
        }

        @Nonnull
        public abstract List<ChildSlot> getChildren();

        @Nonnull
        public abstract List<ItemSlot> getItems();

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

        public Node newOfSameKind() {
            return newOfSameKind(nodeIdSupplier.get());
        }

        @Nonnull
        public abstract Node newOfSameKind(@Nonnull final byte[] nodeId);
    }

    private class LeafNode extends Node {
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
        public List<ChildSlot> getChildren() {
            throw new IllegalStateException("this method should not be called for leaf nodes");
        }

        @Nonnull
        public List<ItemSlot> getItems() {
            return items;
        }

        @Nonnull
        public Kind getKind() {
            return Kind.LEAF;
        }

        @Override
        public void setSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
            this.items =
                    newSlots.stream()
                            .map(slot -> (ItemSlot)slot)
                            .collect(Collectors.toList());
        }

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

    private class IntermediateNode extends Node {
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
        public List<ChildSlot> getChildren() {
            return children;
        }

        @Nonnull
        public List<ItemSlot> getItems() {
            throw new IllegalStateException("this method should not be called for intermediate nodes");
        }

        @Nonnull
        public Kind getKind() {
            return Kind.INTERMEDIATE;
        }

        @Override
        public void setSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
            this.children =
                    newSlots.stream()
                            .map(slot -> (ChildSlot)slot)
                            .collect(Collectors.toList());
        }

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
            return getChildren().stream()
                    .map(child -> child.getMbr().toPlotString())
                    .collect(Collectors.joining("\n"));
        }
    }

    private abstract static class NodeSlot {
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
    }

    /**
     * Rectangle.
     */
    static class ItemSlot extends NodeSlot {
        @Nonnull
        private final Tuple value;
        @Nonnull
        private final Point position;

        public ItemSlot(@Nonnull final BigInteger hilbertValue, @Nonnull final Tuple key, @Nonnull final Tuple value, @Nonnull final Point position) {
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

        public String toString() {
            return "[" + getPosition() + ";" + getHilbertValue() + "; " + getKey() + "]";
        }
    }

    private static class ChildSlot extends NodeSlot {
        @Nonnull
        private final byte[] childId;
        @Nonnull
        private Rectangle mbr;

        public ChildSlot(@Nonnull final BigInteger largestHilbertValue, @Nonnull final Tuple largestKey,
                         @Nonnull final byte[] childId, @Nonnull final Rectangle mbr) {
            super(largestHilbertValue, largestKey);
            this.childId = childId;
            this.mbr = mbr;
        }

        @Nonnull
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
        public String toString() {
            //return "[" + getMbr() + ";" + getLargestHilbertValue() + "]";
            return getMbr().toString();
        }
    }

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
     * TODO.
     */
    public class LeafIterator implements AsyncIterator<LeafNode> {

        @Nonnull
        private final ReadTransaction readTransaction;
        @Nonnull
        private final byte[] rootId;
        @Nonnull
        private final Predicate<Rectangle> mbrPredicate;

        @Nullable
        private TraversalState currentState;
        @Nullable
        private CompletableFuture<TraversalState> nextStateFuture;

        public LeafIterator(@Nonnull final ReadTransaction readTransaction, @Nonnull final byte[] rootId) {
            this(readTransaction, rootId, r -> true);
        }

        public LeafIterator(@Nonnull final ReadTransaction readTransaction, @Nonnull final byte[] rootId, @Nonnull final Predicate<Rectangle> mbrPredicate) {
            this.readTransaction = readTransaction;
            this.rootId = rootId;
            this.mbrPredicate = mbrPredicate;
            this.currentState = null;
            this.nextStateFuture = null;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (nextStateFuture == null) {
                if (currentState == null) {
                    nextStateFuture = fetchLeftmostPathToLeaf(readTransaction, rootId, mbrPredicate);
                } else {
                    nextStateFuture = fetchNextPathToLeaf(readTransaction, currentState, mbrPredicate);
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
     * TODO.
     */
    public class ItemIterator implements AsyncIterator<ItemSlot> {
        @Nonnull
        private final LeafIterator leafIterator;
        @Nullable
        private LeafNode currentLeafNode;
        @Nullable
        private Iterator<ItemSlot> currenLeafItemsIterator;

        public ItemIterator(@Nonnull final LeafIterator leafIterator) {
            this.leafIterator = leafIterator;
            this.currentLeafNode = null;
            this.currenLeafItemsIterator = null;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (currenLeafItemsIterator != null && currenLeafItemsIterator.hasNext()) {
                return CompletableFuture.completedFuture(true);
            }
            // we know that each leaf has items (or if it doesn't it is the root and we are done when there are no items
            return leafIterator.onHasNext()
                    .thenApply(hasNext -> {
                        if (hasNext) {
                            this.currentLeafNode = leafIterator.next();
                            this.currenLeafItemsIterator = currentLeafNode.getItems().iterator();
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

    private static class SplitNodeOrAdjust {
        public static SplitNodeOrAdjust NONE = new SplitNodeOrAdjust(null, null, false);
        public static SplitNodeOrAdjust ADJUST = new SplitNodeOrAdjust(null, null, true);

        @Nullable
        private final ChildSlot slotInParent;
        @Nullable
        private final Node splitNode;

        private final boolean parentNeedsAdjustment;

        private SplitNodeOrAdjust(@Nullable final ChildSlot slotInParent, @Nullable final Node splitNode, final boolean parentNeedsAdjustment) {
            Verify.verify((slotInParent == null && splitNode == null) ||
                          (slotInParent != null && splitNode != null));
            this.slotInParent = slotInParent;
            this.splitNode = splitNode;
            this.parentNeedsAdjustment = parentNeedsAdjustment;
        }

        @Nullable
        public ChildSlot getSlotInParent() {
            return slotInParent;
        }

        @Nullable
        public Node getSplitNode() {
            return splitNode;
        }

        public boolean parentNeedsAdjustment() {
            return parentNeedsAdjustment;
        }
    }

    static class Point {
        @Nonnull
        private final Tuple coordinates;

        public Point(@Nonnull final Tuple coordinates) {
            Preconditions.checkArgument(coordinates.size() > 0);
            this.coordinates = coordinates;
        }

        @Nonnull
        public Tuple getCoordinates() {
            return coordinates;
        }

        public int getNumDimensions() {
            return coordinates.size();
        }

        public Object getCoordinate(final int dimension) {
            return coordinates.get(dimension);
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
            return Objects.hash(coordinates);
        }

        @Nonnull
        @Override
        public String toString() {
            return coordinates.toString();
        }
    }

    /**
     * Rectangle.
     */
    static class Rectangle {
        @Nonnull
        private final Tuple ranges;

        public Rectangle(final Tuple ranges) {
            Preconditions.checkArgument(ranges.size() > 0 && ranges.size() % 2 == 0);
            this.ranges = ranges;
        }

        public int getNumDimensions() {
            return ranges.size() >> 1;
        }

        @Nonnull
        public Tuple getRanges() {
            return ranges;
        }

        public Object getLow(final int dimension) {
            return ranges.get(dimension);
        }

        public Object getHigh(final int dimension) {
            return ranges.get((ranges.size() >> 1) + dimension);
        }

        public BigInteger area() {
            BigInteger currentArea = BigInteger.ONE;
            for (int d = 0; d < getNumDimensions(); d ++) {
                currentArea = currentArea.multiply(BigInteger.valueOf(((Number)getHigh(d)).longValue() - ((Number)getLow(d)).longValue()));
            }
            return currentArea;
        }

        public Rectangle unionWith(@Nonnull final Point point) {
            Preconditions.checkArgument(getNumDimensions() == point.getNumDimensions());
            boolean isModified = false;
            Object[] ranges = new Object[getNumDimensions() << 1];

            for (int d = 0; d < getNumDimensions(); d ++) {
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

            Verify.verify(Arrays.stream(ranges).allMatch(Objects::nonNull));

            return new Rectangle(Tuple.from(ranges));
        }

        public Rectangle unionWith(@Nonnull final Rectangle other) {
            Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
            boolean isModified = false;
            Object[] ranges = new Object[getNumDimensions() << 1];

            for (int d = 0; d < getNumDimensions(); d ++) {
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

            Verify.verify(Arrays.stream(ranges).allMatch(Objects::nonNull));
            
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
            return Objects.hash(ranges);
        }

        @Nonnull
        public String toPlotString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("rectangle(");
            for (int d = 0; d < getNumDimensions(); d ++) {
                builder.append(((Number)getLow(d)).longValue());
                if (d + 1 < getNumDimensions()) {
                    builder.append("|");
                }
            }

            builder.append(" ");

            for (int d = 0; d < getNumDimensions(); d ++) {
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
            for (int d = 0; d < point.getNumDimensions(); d ++) {
                final Object coordinate = point.getCoordinate(d);
                mbrRanges[d] = coordinate;
                mbrRanges[point.getNumDimensions() + d] = coordinate;
            }
            return new Rectangle(Tuple.from(mbrRanges));
        }
    }
}
