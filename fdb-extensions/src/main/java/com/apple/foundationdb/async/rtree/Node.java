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
import java.util.List;
import java.util.Objects;

/**
 * Abstract base class to capture the common aspects of {@link LeafNode} and {@link IntermediateNode}. All
 * nodes
 * have a node id and slots and can be linked up to a parent. Note that while the root node mostly is an
 * intermediate node it can also be a leaf node if the tree is nearly empty.
 */
public interface Node {
    @Nonnull
    byte[] getId();

    default int size() {
        return getSlots().size();
    }

    default boolean isEmpty() {
        return getSlots().isEmpty();
    }

    @Nonnull
    List<? extends NodeSlot> getSlots();

    @Nonnull
    NodeSlot getSlot(int index);

    @Nullable
    ChangeSet getChangeSet();

    @CanIgnoreReturnValue
    @Nonnull
    Node moveInSlots(@Nonnull StorageAdapter storageAdapter, @Nonnull List<? extends NodeSlot> slots);

    @CanIgnoreReturnValue
    @Nonnull
    Node moveOutSlots(@Nonnull StorageAdapter storageAdapter);

    @CanIgnoreReturnValue
    @Nonnull
    Node insertSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot slot);

    @CanIgnoreReturnValue
    @Nonnull
    Node updateSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex, @Nonnull NodeSlot updatedSlot);

    @CanIgnoreReturnValue
    @Nonnull
    Node deleteSlot(@Nonnull StorageAdapter storageAdapter, int level, int slotIndex);

    @CanIgnoreReturnValue
    @Nonnull
    Node deleteAllSlots(@Nonnull StorageAdapter storageAdapter, int level);

    default boolean isRoot() {
        return Arrays.equals(RTree.rootId, getId());
    }

    @Nonnull
    NodeKind getKind();

    @Nullable
    IntermediateNode getParentNode();

    int getSlotIndexInParent();

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

    void linkToParent(@Nonnull final IntermediateNode parentNode, final int slotInParent);

    @Nonnull
    Node newOfSameKind(@Nonnull byte[] nodeId);

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

    default void validateParentNode(@Nullable final IntermediateNode parentNode, @Nullable final ChildSlot childSlotInParentNode) {
        if (parentNode == null) {
            // child is root
            Verify.verify(isRoot());
            Verify.verify(childSlotInParentNode == null);
        } else {
            Objects.requireNonNull(childSlotInParentNode);

            // Recompute the mbr of the child and compare it to the mbr the parent has.
            final RTree.Rectangle computedMbr = computeMbr(getSlots());
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
     * Compute the minimum bounding rectangle (mbr) of a list of slots. This method is used when a node's secondary
     * attributes need to be recomputed.
     * @param slots a list of slots
     * @return a {@link RTree.Rectangle} representing the mbr of the {@link RTree.Point}s of the given slots.
     */
    @Nonnull
    static RTree.Rectangle computeMbr(@Nonnull final List<? extends NodeSlot> slots) {
        Verify.verify(!slots.isEmpty());
        RTree.Rectangle mbr = null;
        for (final NodeSlot slot : slots) {
            if (slot instanceof ItemSlot) {
                final RTree.Point position = ((ItemSlot)slot).getPosition();
                if (mbr == null) {
                    mbr = RTree.Rectangle.fromPoint(position);
                } else {
                    mbr = mbr.unionWith(position);
                }
            }  else if (slot instanceof ChildSlot) {
                final RTree.Rectangle mbrForSlot = ((ChildSlot)slot).getMbr();
                if (mbr == null) {
                    mbr = mbrForSlot;
                } else {
                    mbr = mbr.unionWith(mbrForSlot);
                }
            } else {
                throw new IllegalStateException("slot of unknown kind");
            }
        }
        return Objects.requireNonNull(mbr); // cannot be null but compile cannot infer that
    }

    /**
     * A change set for slots. Implemented within implementations of {@link StorageAdapter} to provide specific actions
     * when a node is written, mostly to avoid re-persisting all slots if not necessary.
     */
    interface ChangeSet {
        void apply(@Nonnull final Transaction transaction);
    }
}
