/*
 * Node.java
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Interface to capture the common aspects of nodes being either {@link LeafNode}s or {@link IntermediateNode}s.
 * All nodes have a node id and slots and can be linked up to a parent. Note that while the root node mostly is an
 * intermediate node it can also be a leaf node if the tree is nearly empty.
 * <br>
 * This interface is then implemented by {@link AbstractNode} in a stronger-typed way. A node for most practical
 * purposes interacts with e.f. {@link NodeSlot}s, only some specific use-cases including the internals of nodes and
 * node slots themselves need to be aware of the pairing of specific node type and specific belonging node slot type.
 * A {@link Node} hides these private typing details much like a private method is hidden. Other languages have
 * built-in features to do similar things. Scala uses an approach called type aliases to allow generic type variables
 * to become private to be only used by implementations of the inner classes.
 */
public interface Node {
    /**
     * Return the id of this node.
     * @return a byte array that represents the unique identifier of this node
     */
    @Nonnull
    byte[] getId();

    /**
     * Return the number of used slots of this node.
     * @return the number of used slots
     */
    int size();

    /**
     * Return if this node does not hold any slots. Note that a node can ony be temporarily empty, for instance when
     * slots are moved out of a node before other slots get moved in. Such a node must not be persisted as it violates
     * the invariants of ancR-tree.
     * @return {@code true} if the node currently does not hold any node slots.
     */
    boolean isEmpty();

    /**
     * Return an iterable of all node slots. Note that the slots are returned as an {@link Iterable} of
     * {@code ? extends NodeSlot} rather than the particular actual node slot type.
     * @return an {@link Iterable} of node slots.
     */
    @Nonnull
    Iterable<? extends NodeSlot> getSlots();

    /**
     * Return an iterable of a sub range of node slots. Note that the slots are returned as an {@link Iterable} of
     * {@code ? extends NodeSlot} rather than the particular actual node slot type.
     * @param startIndexInclusive start index (inclusive)
     * @param endIndexExclusive end index (exclusive)
     * @return an {@link Iterable} of node slots.
     */
    @Nonnull
    Iterable<? extends NodeSlot> getSlots(int startIndexInclusive, int endIndexExclusive);

    /**
     * Return the {@link NodeSlot} at the position indicated by {@code index}.
     * @param index the index
     * @return a {@link NodeSlot} at position {@code index}
     */
    @Nonnull
    NodeSlot getSlot(int index);

    /**
     * Return a stream of node slots. Note that the slots are returned as a {@link Stream} of
     * {@code ? extends NodeSlot} rather than the particular actual node slot type.
     * @return a {@link Stream} of node slots
     */
    @Nonnull
    Stream<? extends NodeSlot> slotsStream();

    /**
     * Returns the change set that need to be applied in order to correctly persist the node.
     * @return the change set, or {@code null} if the node has not been altered since it was read from the database
     */
    @Nullable
    ChangeSet getChangeSet();

    /**
     * Move slots into this node that were previously part of another node (of the same kind). This operation differs
     * from the semantics of {@link #insertSlot(StorageAdapter, int, int, NodeSlot)} as it does not update the node slot
     * index as the node slots were part of another node before and are assumed to have been persisted in that index
     * before. This method represents the counterpart of {@link #moveOutAllSlots(StorageAdapter)}.
     * @param storageAdapter the storage adapter in use
     * @param slots an iterable of slots to be moved in
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node moveInSlots(@Nonnull StorageAdapter storageAdapter, @Nonnull Iterable<? extends NodeSlot> slots);

    /**
     * Move all slots out of this node. This operation differs from the semantics of
     * {@link #deleteAllSlots(StorageAdapter, int)} as it does not update the node slot
     * index. It is the counterpart of {@link #moveInSlots(StorageAdapter, Iterable)}.
     * @param storageAdapter the storage adapter in use
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node moveOutAllSlots(@Nonnull StorageAdapter storageAdapter);

    /**
     * Insert a new slot into the node.
     * @param storageAdapter storage adapter in use
     * @param level level (for use in the node slot index)
     * @param slotIndex ordinal position where {@code slot} is inserted
     * @param slot the new slot
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node insertSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot slot);

    /**
     * Update an existing slot of this node.
     * @param storageAdapter storage adapter in use
     * @param level level (for use in the node slot index)
     * @param slotIndex ordinal position of the slot to be updated
     * @param updatedSlot the updated slot
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node updateSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot updatedSlot);

    /**
     * Delete a slot from the node.
     * @param storageAdapter storage adapter in use
     * @param level level (for use in the node slot index)
     * @param slotIndex ordinal position where {@code slot} is inserted
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node deleteSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex);

    /**
     * Delete all slots from the node.
     * @param storageAdapter storage adapter in use
     * @param level level (for use in the node slot index)
     * @return this node
     */
    @CanIgnoreReturnValue
    @Nonnull
    Node deleteAllSlots(@Nonnull StorageAdapter storageAdapter, int level);

    /**
     * Returns if this node is the root node. Note that a node is considered the root node if its node id is
     * equal to {@link RTree#rootId}
     * @return {@code true} if this node is the root node, {@code false} otherwise
     */
    default boolean isRoot() {
        return Arrays.equals(RTree.rootId, getId());
    }

    /**
     * Return the kind of the node, i.e. {@link NodeKind#LEAF} or {@link NodeKind#INTERMEDIATE}.
     * @return the kind of this node as a {@link NodeKind}
     */
    @Nonnull
    NodeKind getKind();

    /**
     * Return the parent of this node.
     * @return the parent of this node. Note that a result of {@code null} does not imply that this node is the
     *         rooot node. It simply means that the parent node linked up to this node yet. Usually that means
     *         that the actual parent node has not yet been fetched from the database.
     */
    @Nullable
    IntermediateNode getParentNode();

    int getSlotIndexInParent();

    /**
     * Return the {@link ChildSlot} of this node in this node's parent node. Note that this method returns {@code null}
     * if the parent node has not been linked up yet. Also, the invariant
     * {@code (parent node == null) <=> (slot in parent == null)} holds.
     * @return the {@link ChildSlot} corresponding to this node in the parent node or {@code null} if the parent node
     *         has not been linked up yet or this node is the root node
     */
    @Nullable
    default ChildSlot getSlotInParent() {
        final IntermediateNode parentNode = getParentNode();
        if (parentNode == null) {
            return null;
        }
        final int slotIndexInParent = getSlotIndexInParent();
        Verify.verify(slotIndexInParent >= 0);
        return parentNode.getSlot(slotIndexInParent);
    }

    /**
     * Link this node to its parent node. This sets upwards references allowing a bottom-up traversal of the levels of
     * the tree.
     * @param parentNode the parent node to link up to
     * @param slotInParent the slot index indicating the {@link ChildSlot} in the parent node that corresponds to this
     *        node
     */
    void linkToParent(@Nonnull IntermediateNode parentNode, int slotInParent);

    /**
     * Create a new node that is of the same {@link NodeKind} as this node.
     * @param nodeId node id for the new node
     * @return a new empty node using the unique node id passed in
     */
    @Nonnull
    Node newOfSameKind(@Nonnull byte[] nodeId);

    /**
     * Method to validate the invariants of this node.
     */
    default void validate() {
        BigInteger lastHilbertValue = null;
        Tuple lastKey = null;

        // check that all (hilbert values; key pairs) are monotonically increasing
        for (final NodeSlot nodeSlot : getSlots()) {
            if (lastHilbertValue != null) {
                final int hilbertValueCompare = nodeSlot.getSmallestHilbertValue().compareTo(lastHilbertValue);
                Verify.verify(hilbertValueCompare >= 0,
                        "smallest (hilbertValue, key) pairs are not monotonically increasing (hilbertValueCheck)");
                if (hilbertValueCompare == 0) {
                    Verify.verify(TupleHelpers.compare(nodeSlot.getSmallestKey(), lastKey) >= 0,
                            "smallest (hilbertValue, key) pairs are not monotonically increasing (keyCheck)");
                }
            }
            lastHilbertValue =  nodeSlot.getSmallestHilbertValue();
            lastKey = nodeSlot.getSmallestKey();
            final int hilbertValueCompare = nodeSlot.getLargestHilbertValue().compareTo(lastHilbertValue);
            Verify.verify(hilbertValueCompare >= 0,
                    "largest (hilbertValue, key) pairs are not monotonically increasing (hilbertValueCheck)");
            if (hilbertValueCompare == 0) {
                Verify.verify(TupleHelpers.compare(nodeSlot.getLargestKey(), lastKey) >= 0,
                        "largest (hilbertValue, key) pairs are not monotonically increasing (keyCheck)");
            }

            lastHilbertValue = nodeSlot.getLargestHilbertValue();
            lastKey = nodeSlot.getLargestKey();
        }
    }

    /**
     * Method to validate the invariants of the relationships of this node and this node's parent's {@link ChildSlot}
     * corresponding to this node.
     * @param parentNode a parent node if it exists
     * @param childSlotInParentNode the {@link ChildSlot} in the parent node handed in that corresponds to this node
     */
    default void validateParentNode(@Nullable final IntermediateNode parentNode,
                                    @Nullable final ChildSlot childSlotInParentNode) {
        if (parentNode == null) {
            // child is root
            Verify.verify(isRoot());
            Verify.verify(childSlotInParentNode == null);
        } else {
            Objects.requireNonNull(childSlotInParentNode);

            // Recompute the mbr of the child and compare it to the mbr the parent has.
            final RTree.Rectangle computedMbr = NodeHelpers.computeMbr(getSlots());
            Verify.verify(childSlotInParentNode.getMbr().equals(computedMbr),
                    "computed mbr does not match mbr from node");

            // Verify that the smallest hilbert value in the parent node is indeed the smallest hilbert value of
            // the left-most child in childNode.
            Verify.verify(childSlotInParentNode.getSmallestHilbertValue().equals(getSlot(0).getSmallestHilbertValue()),
                    "expected smallest hilbert value does not match the actual smallest hilbert value of the first child in childNode");

            // Verify that the smallest key in the parent node is indeed the smallest key of
            // the left-most child in childNode.
            Verify.verify(TupleHelpers.equals(childSlotInParentNode.getSmallestKey(), getSlot(0).getSmallestKey()),
                    "expected smallest key does not match the actual smallest key of the first child in childNode");

            // Verify that the largest hilbert value in the parent node is indeed the largest hilbert value of
            // the right-most child in childNode.
            Verify.verify(childSlotInParentNode.getLargestHilbertValue().equals(getSlot(size() - 1).getLargestHilbertValue()),
                    "expected largest hilbert value does not match the actual hilbert value of the last child in childNode");

            // Verify that the largest key in the parent node is indeed the largest key of the right-most\
            // child in childNode.
            Verify.verify(TupleHelpers.equals(childSlotInParentNode.getLargestKey(), getSlot(size() - 1).getLargestKey()),
                    "expected largest key does not match the actual largest key of the last child in childNode");
        }
    }

    /**
     * A change set for slots. Implemented within implementations of {@link StorageAdapter} to provide specific actions
     * when a node is written, mostly to avoid re-persisting all slots if not necessary.
     */
    interface ChangeSet {
        /**
         * Apply all mutations for the change set. These happen transactionally using the {@link Transaction}
         * provided.
         * @param transaction transaction to use when making all modifications
         */
        void apply(@Nonnull Transaction transaction);
    }
}
