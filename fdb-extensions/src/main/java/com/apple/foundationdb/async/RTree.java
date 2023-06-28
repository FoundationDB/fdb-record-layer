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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class RTree {
    private static final int MAX_CONCURRENT_READS = 16;

    public static final Config DEFAULT_CONFIG = new Config();

    public static final int DEFAULT_MIN_M = 16;
    public static final int DEFAULT_MAX_M = 32;
    public static final int DEFAULT_MIN_S = 2;

    protected final Subspace subspace;
    protected final Executor executor;
    protected final Config config;

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
     */
    public RTree(Subspace subspace, Executor executor, Config config) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
    }

    /**
     * Initialize a new R-tree with the default configuration.
     * @param subspace the subspace where the r-tree is stored
     * @param executor an executor to use when running asynchronous tasks
     */
    public RTree(Subspace subspace, Executor executor) {
        this(subspace, executor, DEFAULT_CONFIG);
    }

    /**
     * Get the subspace used to store this r-tree.
     * @return r-tree subspace
     */
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get executer used by this r-tree.
     * @return executor used when running asynchronous tasks
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this r-tree's configuration.
     * @return r-tree configuration
     */
    public Config getConfig() {
        return config;
    }

    public CompletableFuture<TraversalState> fetchLeftmostPathToLeaf(@Nonnull final Transaction transaction,
                                                                     @Nonnull final byte[] rootId,
                                                                     @Nonnull final Predicate<Rectangle> mbrPredicate) {
        final AtomicReference<byte[]> currentId = new AtomicReference<>(rootId);
        final List<Deque<byte[]>> toBeProcessed = Lists.newArrayList();
        final AtomicReference<Node> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(transaction, currentId.get())
                .thenApply(node -> {
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final List<ChildSlot> children = node.getChildren();
                        final Deque<byte[]> toBeProcessedThisLevel = new ArrayDeque<>();
                        for (final ChildSlot child : children) {
                            if (mbrPredicate.test(child.getMbr())) {
                                toBeProcessedThisLevel.addLast(child.getChildId());
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

    public CompletableFuture<TraversalState> fetchNextPathToLeaf(@Nonnull final Transaction transaction,
                                                                 @Nonnull final TraversalState traversalState,
                                                                 @Nonnull final Predicate<Rectangle> mbrPredicate) {
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
        return fetchLeftmostPathToLeaf(transaction, currentId, mbrPredicate).thenApply(nestedTraversalState -> {
            traversalState.setCurrentLeafNode(nestedTraversalState.getCurrentLeafNode());
            toBeProcessed.subList(branchLevel, toBeProcessed.size()).clear();
            toBeProcessed.addAll(nestedTraversalState.getToBeProcessed());
            return traversalState;
        });
    }

    public CompletableFuture<Void> insert(@Nonnull final TransactionContext tc,
                                          @Nonnull final BigInteger hilbertValue,
                                          @Nonnull final Tuple key,
                                          @Nonnull final Tuple value,
                                          @Nonnull final Point point) {
        return tc.runAsync(transaction -> fetchUpdatePathToLeaf(transaction, hilbertValue, key)
                .thenCompose(leafNode -> insertOrUpdateSlot(transaction, leafNode, new ItemSlot(hilbertValue, key, value, point))));
    }

    public CompletableFuture<Void> insertOrUpdateSlot(@Nonnull final Transaction transaction,
                                                      @Nonnull final Node targetNode,
                                                      @Nonnull final NodeSlot newSlot) {
        Verify.verify(targetNode.getSlots().size() <= config.getMaxM());

        final int insertSlotIndex = findInsertSlotIndex(targetNode, newSlot.getHilbertValue(), newSlot.getKey());
        if (targetNode.getKind() == Kind.LEAF && insertSlotIndex < 0) {
            // just update the slot with the potentially new value
            writeNodeSlot(transaction, targetNode.getId(), newSlot);
            return AsyncUtil.DONE;
        }

        final AtomicReference<Node> currentNode = new AtomicReference<>(targetNode);
        final AtomicReference<SplitNodeSlotOrAdjust> currentSplitNodeSlotOrAdjust = new AtomicReference<>(null);

        return AsyncUtil.whileTrue(() -> {
            final NodeSlot currentNewSlot;
            final SplitNodeSlotOrAdjust lastSplitNodeOrAdjust = currentSplitNodeSlotOrAdjust.get();
            if (lastSplitNodeOrAdjust == null) {
                currentNewSlot = newSlot;
            } else if (lastSplitNodeOrAdjust.getSplitNodeSlot() != null) {
                currentNewSlot = Objects.requireNonNull(lastSplitNodeOrAdjust.getSplitNodeSlot());
            } else {
                currentNewSlot = null;
            }

            if (currentNewSlot != null) {
                return insertSlotIntoTargetNode(transaction, currentNode.get(), currentNewSlot, insertSlotIndex)
                        .thenApply(splitNodeSlotOrAdjust -> {
                            if (currentNode.get().isRoot()) {
                                return false;
                            }
                            currentNode.set(currentNode.get().getParentNode());
                            currentSplitNodeSlotOrAdjust.set(splitNodeSlotOrAdjust);
                            return splitNodeSlotOrAdjust.getSplitNodeSlot() != null || splitNodeSlotOrAdjust.parentNeedsAdjustment();
                        });
            } else {
                // adjustment only
                final SplitNodeSlotOrAdjust splitNodeSlotOrAdjust = adjustUpdateNode(transaction, currentNode.get());
                Verify.verify(splitNodeSlotOrAdjust.getSplitNodeSlot() == null);
                if (currentNode.get().isRoot()) {
                    return AsyncUtil.READY_FALSE;
                }
                currentNode.set(currentNode.get().getParentNode());
                currentSplitNodeSlotOrAdjust.set(splitNodeSlotOrAdjust);
                return splitNodeSlotOrAdjust.parentNeedsAdjustment()
                       ? AsyncUtil.READY_TRUE
                       : AsyncUtil.READY_FALSE;
            }
        }, executor);
    }

    public CompletableFuture<SplitNodeSlotOrAdjust> insertSlotIntoTargetNode(@Nonnull final Transaction transaction,
                                                                             @Nonnull final Node targetNode,
                                                                             @Nonnull final NodeSlot newSlot,
                                                                             final int slotIndexInTargetNode) {
        if (targetNode.getSlots().size() < config.getMaxM()) {
            targetNode.insertSlot(slotIndexInTargetNode, newSlot);

            writeNodeSlot(transaction, targetNode.getId(), newSlot);

            // node has left some space -- indicate that we are done splitting at the current node
            if (!targetNode.isRoot()) {
                return CompletableFuture.completedFuture(adjustSlotInParent(targetNode)
                                                         ? SplitNodeSlotOrAdjust.ADJUST
                                                         : SplitNodeSlotOrAdjust.NONE);
            }
            return CompletableFuture.completedFuture(SplitNodeSlotOrAdjust.NONE); // no split and no adjustment
        } else {
            //
            // if this is the root we need to grow the tree taller
            //
            if (targetNode.isRoot()) {
                splitRootNode(transaction, targetNode);
                return CompletableFuture.completedFuture(SplitNodeSlotOrAdjust.NONE);
            }

            //
            // Node is full -- borrow some space from the siblings if possible
            //
            final CompletableFuture<List<Node>> siblings =
                    fetchSiblings(transaction, Objects.requireNonNull(targetNode));

            return siblings.thenApply(siblingNodes -> {
                final int numSlots =
                        Math.toIntExact(siblingNodes
                                .stream()
                                .mapToLong(siblingNode -> siblingNode.getSlots().size())
                                .sum());

                final Node splitNode;
                if (numSlots == siblingNodes.size() * config.getMaxM()) {
                    splitNode = targetNode.newOfSameKind();
                    // link this split node to become the last node of the siblings
                    splitNode.linkToParent(Objects.requireNonNull(targetNode.getParentNode()),
                            siblingNodes.get(siblingNodes.size() - 1).getSlotIndexInParent() + 1);
                    siblingNodes.add(splitNode);
                } else {
                    splitNode = null;
                }

                // temporarily overfill targetNode
                targetNode.insertSlot(slotIndexInTargetNode, newSlot);

                final Iterator<Node> siblingNodesIterator = siblingNodes.iterator();
                // sibling nodes are in hilbert value order
                final Iterator<? extends NodeSlot> slotIterator =
                        siblingNodes
                                .stream()
                                .flatMap(siblingNode -> siblingNode.getSlots().stream())
                                .iterator();

                final int base = numSlots / siblingNodes.size();
                int rest = numSlots % siblingNodes.size();

                final List<NodeSlot> currentNodeSlots = Lists.newArrayList();
                while (slotIterator.hasNext()) {
                    final NodeSlot slot = slotIterator.next();
                    currentNodeSlots.add(slot);
                    if (currentNodeSlots.size() == base + (rest > 0 ? 1 : 0)) {
                        if (rest > 0) {
                            // one fewer to smear
                            rest--;
                        }

                        Verify.verify(siblingNodesIterator.hasNext());
                        final Node currentSiblingNode = siblingNodesIterator.next();
                        currentSiblingNode.setSlots(currentNodeSlots);
                    }
                }

                updateNodeSlotsForNodes(transaction, siblingNodes);

                for (final Node siblingNode : siblingNodes) {
                    if (siblingNode != splitNode) {
                        adjustSlotInParent(siblingNode);
                    }
                }

                if (splitNode == null) {
                    return SplitNodeSlotOrAdjust.ADJUST;
                }

                final var lastSlotOfSplitNode = splitNode.getSlots().get(splitNode.size() - 1);
                return new SplitNodeSlotOrAdjust(new ChildSlot(lastSlotOfSplitNode.getHilbertValue(),
                        lastSlotOfSplitNode.getKey(), splitNode.getId(), computeMbr(splitNode.getChildren())),
                        true);
            });
        }
    }

    private void splitRootNode(final @Nonnull Transaction transaction,
                               final @Nonnull Node targetNode) {
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
                                computeMbr(leftNode.getChildren())),
                        new ChildSlot(lastSlotOfRightNode.getHilbertValue(), lastSlotOfRightNode.getKey(), leftNode.getId(),
                                computeMbr(leftNode.getChildren())));
        final IntermediateNode newRootNode = new IntermediateNode(Node.ROOT_ID, rootNodeSlots);

        updateNodeSlotsForNodes(transaction, Lists.newArrayList(leftNode, rightNode, newRootNode));
    }

    public SplitNodeSlotOrAdjust adjustUpdateNode(@Nonnull final Transaction transaction,
                                                  @Nonnull final Node targetNode) {
        updateNodeSlotsForNodes(transaction, Collections.singletonList(targetNode));
        return adjustSlotInParent(targetNode)
               ? SplitNodeSlotOrAdjust.ADJUST
               : SplitNodeSlotOrAdjust.NONE;
    }

    private static boolean adjustSlotInParent(final @Nonnull Node targetNode) {
        boolean slotHasChanged;
        final IntermediateNode parentNode = Objects.requireNonNull(targetNode.getParentNode());
        final ChildSlot childSlot = Objects.requireNonNull(parentNode.getChildren()).get(targetNode.getSlotIndexInParent());
        final Rectangle newMbr = computeMbr(targetNode.getChildren());
        slotHasChanged = !childSlot.getMbr().equals(newMbr);
        childSlot.setMbr(computeMbr(targetNode.getChildren()));
        final NodeSlot lastSlotOfTargetNode = targetNode.getSlots().get(targetNode.size() - 1);
        slotHasChanged |= !childSlot.getLargestHilbertValue().equals(lastSlotOfTargetNode.getHilbertValue());
        childSlot.setLargestHilbertValue(lastSlotOfTargetNode.getHilbertValue());
        slotHasChanged |= !childSlot.getKey().equals(lastSlotOfTargetNode.getKey());
        childSlot.setLargestKey(lastSlotOfTargetNode.getKey());
        return slotHasChanged;
    }

    public CompletableFuture<LeafNode> fetchUpdatePathToLeaf(@Nonnull final Transaction transaction,
                                                             @Nonnull final BigInteger targetHilbertValue,
                                                             @Nonnull final Tuple targetKey) {
        final AtomicReference<IntermediateNode> parentNode = new AtomicReference<>(null);
        final AtomicInteger slotInParent = new AtomicInteger(-1);
        final AtomicReference<byte[]> currentId = new AtomicReference<>(Node.ROOT_ID);
        final AtomicReference<LeafNode> leafNode = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> fetchNode(transaction, currentId.get())
                .thenApply(node -> {
                    if (parentNode.get() != null) {
                        node.linkToParent(parentNode.get(), slotInParent.get());
                    }
                    if (node.getKind() == Kind.INTERMEDIATE) {
                        final int slotIndex = chooseSlotIndexForUpdate((IntermediateNode)node, targetHilbertValue, targetKey);
                        final List<ChildSlot> children = node.getChildren();
                        parentNode.set((IntermediateNode)node);
                        slotInParent.set(slotIndex);
                        currentId.set(children.get(slotIndex).getChildId());
                        return true;
                    } else {
                        leafNode.set((LeafNode)node);
                        return false;
                    }
                }), executor).thenApply(vignore -> leafNode.get());
    }

    /**
     * We assume that {@code parentNode} has at least {@link Config#getMinM()} entries.
     */
    public CompletableFuture<List<Node>> fetchSiblings(@Nonnull final Transaction transaction,
                                                       @Nonnull final Node node) {
        final ArrayDeque<byte[]> toBeProcessed = new ArrayDeque<>();
        final List<CompletableFuture<Void>> working = Lists.newArrayList();
        final int numSiblings = config.getMinS() - 1;
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

    @SuppressWarnings("ConstantValue")
    public CompletableFuture<Node> fetchNode(@Nonnull final Transaction transaction, byte[] nodeId) {
        return AsyncUtil.collect(transaction.getRange(Range.startsWith(nodeId)))
                .thenApply(keyValues -> {
                    List<ChildSlot> childSlots = null;
                    List<ItemSlot> itemSlots = null;
                    Kind nodeKind = null;
                    for (final KeyValue keyValue : keyValues) {
                        final Tuple keyTuple = Tuple.fromBytes(keyValue.getKey());
                        Verify.verify(keyTuple.size() == 3);
                        final Kind currentNodeKind;
                        switch ((byte)keyTuple.get(0)) {
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

                    if (nodeKind == null && Arrays.equals(Node.ROOT_ID, nodeId)) {
                        // root node but nothing read -- root node is the only node that can be empty --
                        // this only happens when the R-Tree is completely empty.
                        itemSlots = Lists.newArrayList();
                    }

                    Verify.verify((nodeKind == Kind.LEAF && itemSlots != null && childSlots == null) ||
                                  (nodeKind == Kind.INTERMEDIATE && itemSlots == null && childSlots != null));
                    return nodeKind == Kind.LEAF
                           ? new LeafNode(nodeId, itemSlots)
                           : new IntermediateNode(nodeId, childSlots);
                });
    }

    public void updateNodeSlotsForNodes(@Nonnull final Transaction transaction, @Nonnull List<? extends Node> nodes) {
        for (final Node node : nodes) {
            transaction.clear(Range.startsWith(node.getId()));
            for (final NodeSlot nodeSlot : node.getSlots()) {
                writeNodeSlot(transaction, node.getId(), nodeSlot);
            }
        }
    }

    public void writeNodeSlot(@Nonnull final Transaction transaction, @Nonnull final byte[] nodeId, @Nonnull NodeSlot nodeSlot) {
        if (nodeSlot instanceof ItemSlot) {
            final ItemSlot itemSlot = (ItemSlot)nodeSlot;
            final byte[] packedKey = Tuple.from((byte)0x00, itemSlot.getHilbertValue(), itemSlot.getKey()).pack(nodeId);
            final byte[] packedValue = Tuple.from(itemSlot.getValue(), itemSlot.getPosition().getCoordinates()).pack();
            transaction.set(packedKey, packedValue);
        } else {
            Verify.verify(nodeSlot instanceof ChildSlot);
            final ChildSlot childSlot = (ChildSlot)nodeSlot;
            final byte[] packedKey = Tuple.from((byte)0x01, childSlot.getLargestHilbertValue(), childSlot.getLargestKey()).pack(nodeId);
            final byte[] packedValue = Tuple.from(childSlot.getChildId(), childSlot.getMbr().getRanges()).pack();
            transaction.set(packedKey, packedValue);
        }
    }

    private static int chooseSlotIndexForUpdate(@Nonnull final IntermediateNode node,
                                                @Nonnull final BigInteger targetHilbertValue,
                                                @Nonnull final Tuple targetKey) {
        Verify.verify(!node.isEmpty());
        final List<? extends NodeSlot> slots = node.getSlots();
        for (int slotIndex = 0; slotIndex < slots.size(); slotIndex++) {
            final NodeSlot slot = slots.get(slotIndex);
            //
            // Choose subtree with the minimum Hilbert value that is greater than the target
            // Hilbert value. If there is no such subtree, i.e. the target Hilbert value is the
            // largest Hilbert value, we choose the largest one in the current node.
            //
            final int hilbertValueCompare = slot.getHilbertValue().compareTo(targetHilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    // child == target
                    final var tupleCompare = TupleHelpers.compare(slot.getKey(), targetKey);
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

    private static int findInsertSlotIndex(@Nonnull final Node node,
                                           @Nonnull final BigInteger targetHilbertValue,
                                           @Nonnull final Tuple targetKey) {
        final List<? extends NodeSlot> slots = node.getSlots();
        for (int slotIndex = 0; slotIndex < slots.size(); slotIndex++) {
            final NodeSlot slot = slots.get(slotIndex);
            final int hilbertValueCompare = slot.getHilbertValue().compareTo(targetHilbertValue);
            if (hilbertValueCompare >= 0) {
                // child HV >= target HV
                if (hilbertValueCompare == 0) {
                    // child == target
                    final var tupleCompare = TupleHelpers.compare(slot.getKey(), targetKey);
                    if (tupleCompare >= 0) {
                        if (tupleCompare == 0 && node.getKind() == Kind.LEAF) {
                            return -1;
                        }
                        return slotIndex;
                    }
                } else {
                    return slotIndex;
                }
            }
        }

        return node.size();
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
            }
            if (slot instanceof ChildSlot) {
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

    private static byte[] newNodeId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = new byte[17];
        ByteBuffer.wrap(uuidBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits());
        return uuidBytes;
    }

    private enum Kind {
        INTERMEDIATE,
        LEAF
    }

    private abstract static class Node {
        public static byte[] ROOT_ID = new byte[16]; // all zeros for the root
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

        @Nonnull
        public abstract List<ChildSlot> getChildren();

        @Nonnull
        public abstract List<ItemSlot> getItems();

        public boolean isRoot() {
            return Arrays.equals(ROOT_ID, id);
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
        public abstract Node newOfSameKind();
    }

    private static class LeafNode extends Node {
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
        public LeafNode newOfSameKind() {
            return new LeafNode(newNodeId(), Lists.newArrayList());
        }
    }

    private static class IntermediateNode extends Node {
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
        public IntermediateNode newOfSameKind() {
            return new IntermediateNode(newNodeId(), Lists.newArrayList());
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

    private static class ItemSlot extends NodeSlot {
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

    private static class SplitNodeSlotOrAdjust {
        public static SplitNodeSlotOrAdjust NONE = new SplitNodeSlotOrAdjust(null, false);
        public static SplitNodeSlotOrAdjust ADJUST = new SplitNodeSlotOrAdjust(null, true);

        @Nullable
        private final ChildSlot childSlot;

        private final boolean parentNeedsAdjustment;

        private SplitNodeSlotOrAdjust(@Nullable final ChildSlot childSlot, final boolean parentNeedsAdjustment) {
            this.childSlot = childSlot;
            this.parentNeedsAdjustment = parentNeedsAdjustment;
        }

        @Nullable
        public ChildSlot getSplitNodeSlot() {
            return childSlot;
        }

        public boolean parentNeedsAdjustment() {
            return parentNeedsAdjustment;
        }
    }

    private static class Point {
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
    }

    private static class Rectangle {
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

        public Rectangle unionWith(@Nonnull final Point point) {
            Preconditions.checkArgument(getNumDimensions() == point.getNumDimensions());
            boolean isModified = false;
            Object[] ranges = new Object[getNumDimensions() << 1];

            for (int d = 0; d < getNumDimensions(); d ++) {
                final Object coordinate = point.getCoordinate(d);
                final Tuple coordinateTuple = Tuple.from(coordinate);
                final Object low = getLow(d);
                Tuple lowTuple = Tuple.from(low);
                if (TupleHelpers.compare(coordinateTuple, lowTuple) < 0) {
                    ranges[d] = coordinate;
                    isModified = true;
                } else {
                    ranges[d] = low;

                    final Object high = getHigh(d);
                    final Tuple highTuple = Tuple.from(high);
                    if (TupleHelpers.compare(coordinateTuple, highTuple) > 0) {
                        ranges[getNumDimensions() + d] = coordinate;
                        isModified = true;
                    } else {
                        ranges[getNumDimensions() + d] = high;
                    }
                }
            }

            if (!isModified) {
                return this;
            }

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
                Tuple lowTuple = Tuple.from(low);
                if (TupleHelpers.compare(otherLowTuple, lowTuple) < 0) {
                    ranges[d] = otherLow;
                    isModified = true;
                } else {
                    ranges[d] = low;

                    final Object high = getHigh(d);
                    final Tuple highTuple = Tuple.from(high);
                    if (TupleHelpers.compare(otherHighTuple, highTuple) > 0) {
                        ranges[getNumDimensions() + d] = otherHigh;
                        isModified = true;
                    } else {
                        ranges[getNumDimensions() + d] = high;
                    }
                }
            }

            if (!isModified) {
                return this;
            }

            return new Rectangle(Tuple.from(ranges));
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
