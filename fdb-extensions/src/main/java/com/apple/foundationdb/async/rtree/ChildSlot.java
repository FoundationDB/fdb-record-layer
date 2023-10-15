/*
 * ChildSlot.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

/**
 * A child slot that is used by {@link IntermediateNode}s. Holds the id of a child node, as well as the largest
 * hilbert value of its child, the largest key of its child and an mbr that encompasses all points in the subtree
 * rooted at the child.
 */
public class ChildSlot implements NodeSlot {
    public static final int SLOT_KEY_TUPLE_SIZE = 4;
    public static final int SLOT_VALUE_TUPLE_SIZE = 2;

    @Nonnull
    private final byte[] childId;
    @Nonnull
    private RTree.Rectangle mbr;

    @Nonnull
    private BigInteger smallestHilbertValue;
    @Nonnull
    private Tuple smallestKey;
    @Nonnull
    private BigInteger largestHilbertValue;
    @Nonnull
    private Tuple largestKey;
    @Nullable
    private ChildSlot originalNodeSlot;
    private boolean isDirty;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public ChildSlot(@Nonnull final BigInteger smallestHilbertValue, @Nonnull final Tuple smallestKey,
                     @Nonnull final BigInteger largestHilbertValue, @Nonnull final Tuple largestKey,
                     @Nonnull final byte[] childId, @Nonnull final RTree.Rectangle mbr) {
        this.smallestHilbertValue = smallestHilbertValue;
        this.smallestKey = smallestKey;
        this.largestHilbertValue = largestHilbertValue;
        this.largestKey = largestKey;
        this.childId = childId;
        this.mbr = mbr;
        this.originalNodeSlot = null;
        this.isDirty = false;
    }

    @Nonnull
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getChildId() {
        return childId;
    }

    public void setSmallestHilbertValue(@Nonnull final BigInteger smallestHilbertValue) {
        this.smallestHilbertValue = smallestHilbertValue;
    }

    @Nonnull
    @Override
    public BigInteger getSmallestHilbertValue() {
        return smallestHilbertValue;
    }

    public void setSmallestKey(@Nonnull final Tuple smallestKey) {
        this.smallestKey = smallestKey;
    }

    @Nonnull
    @Override
    public Tuple getSmallestKey() {
        return smallestKey;
    }

    public void setLargestHilbertValue(@Nonnull final BigInteger largestHilbertValue) {
        this.largestHilbertValue = largestHilbertValue;
    }

    @Nonnull
    @Override
    public BigInteger getLargestHilbertValue() {
        return largestHilbertValue;
    }

    public void setLargestKey(@Nonnull final Tuple largestKey) {
        this.largestKey = largestKey;
    }

    @Nonnull
    @Override
    public Tuple getLargestKey() {
        return largestKey;
    }

    public void setMbr(@Nonnull final RTree.Rectangle mbr) {
        this.mbr = mbr;
    }

    @Nonnull
    public RTree.Rectangle getMbr() {
        return mbr;
    }

    @Nonnull
    @Override
    public Tuple getSlotKey(final boolean storeHilbertValue) {
        return Tuple.from(getSmallestHilbertValue(), getSmallestKey(), getLargestHilbertValue(), getLargestKey());
    }

    @Nonnull
    @Override
    public Tuple getSlotValue() {
        return Tuple.from(getChildId(), getMbr().getRanges());
    }

    public void setOriginalNodeSlotAndMarkDirty(@Nonnull final ChildSlot originalNodeSlot) {
        this.isDirty = true;
        Verify.verify(this.originalNodeSlot == null);
        this.originalNodeSlot = originalNodeSlot;
    }

    public void markClean() {
        this.isDirty = false;
        this.originalNodeSlot = null;
    }

    @Override
    public boolean isDirty() {
        return isDirty;
    }

    @Nonnull
    @Override
    public ChildSlot getOriginalNodeSlot() {
        if (isDirty) {
            return Objects.requireNonNull(originalNodeSlot);
        }
        throw new IllegalStateException("original slot requested for clean slot");
    }

    /**
     * Method to determine if (during a scan a suffix predicate can be applied). A suffix predicate can only
     * be applied, if the smallest and largest hilbert value as well as the non-suffix part of the key are the same.
     *
     * @return {@code true} if a suffix predicate can be applied on this child slot
     */
    public boolean suffixPredicateCanBeApplied() {
        final int hilbertValueCompare = getSmallestHilbertValue().compareTo(getLargestHilbertValue());
        Verify.verify(hilbertValueCompare <= 0);
        if (hilbertValueCompare != 0) {
            return false;
        }

        int positionTupleCompare =
                TupleHelpers.compare(getSmallestKey().getNestedTuple(0), getLargestKey().getNestedTuple(0));
        Verify.verify(positionTupleCompare <= 0);
        return positionTupleCompare == 0;
    }

    @Nonnull
    @Override
    public String toString() {
        return "[" + getMbr() + ";" + getSmallestHilbertValue() + "; " + getLargestHilbertValue() + "]";
    }

    @Nonnull
    static ChildSlot fromKeyAndValue(@Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(keyTuple.size() == SLOT_KEY_TUPLE_SIZE);
        Verify.verify(valueTuple.size() == SLOT_VALUE_TUPLE_SIZE);
        return new ChildSlot(keyTuple.getBigInteger(0), keyTuple.getNestedTuple(1),
                keyTuple.getBigInteger(2), keyTuple.getNestedTuple(3),
                valueTuple.getBytes(0), new RTree.Rectangle(valueTuple.getNestedTuple(1)));
    }
}
