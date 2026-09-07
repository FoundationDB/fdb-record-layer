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

import java.math.BigInteger;

/**
 * A child slot that is used by {@link IntermediateNode}s. Holds the id of a child node, as well as the largest
 * hilbert value of its child, the largest key of its child and an mbr that encompasses all points in the subtree
 * rooted at the child.
 */
public class ChildSlot implements NodeSlot {
    static final int SLOT_KEY_TUPLE_SIZE = 4;
    static final int SLOT_VALUE_TUPLE_SIZE = 2;

    private final byte[] childId;
    private final RTree.Rectangle mbr;

    private final BigInteger smallestHilbertValue;
    private final Tuple smallestKey;
    private final BigInteger largestHilbertValue;
    private final Tuple largestKey;

    ChildSlot(final BigInteger smallestHilbertValue, final Tuple smallestKey,
              final BigInteger largestHilbertValue, final Tuple largestKey,
              final byte[] childId, final RTree.Rectangle mbr) {
        this.smallestHilbertValue = smallestHilbertValue;
        this.smallestKey = smallestKey;
        this.largestHilbertValue = largestHilbertValue;
        this.largestKey = largestKey;
        this.childId = childId;
        this.mbr = mbr;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getChildId() {
        return childId;
    }

    @Override
    public BigInteger getSmallestHilbertValue() {
        return smallestHilbertValue;
    }

    @Override
    public Tuple getSmallestKey() {
        return smallestKey;
    }

    @Override
    public BigInteger getLargestHilbertValue() {
        return largestHilbertValue;
    }

    @Override
    public Tuple getLargestKey() {
        return largestKey;
    }

    public RTree.Rectangle getMbr() {
        return mbr;
    }

    @Override
    public Tuple getSlotKey(final boolean storeHilbertValue) {
        return Tuple.from(getSmallestHilbertValue(), getSmallestKey(), getLargestHilbertValue(), getLargestKey());
    }

    @Override
    public Tuple getSlotValue() {
        return Tuple.from(getChildId(), getMbr().getRanges());
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

    @Override
    public String toString() {
        return "[" + getMbr() + ";" + getSmallestHilbertValue() + "; " + getLargestHilbertValue() + "]";
    }

    static ChildSlot fromKeyAndValue(final Tuple keyTuple, final Tuple valueTuple) {
        Verify.verify(keyTuple.size() == SLOT_KEY_TUPLE_SIZE);
        Verify.verify(valueTuple.size() == SLOT_VALUE_TUPLE_SIZE);
        return new ChildSlot(keyTuple.getBigInteger(0), keyTuple.getNestedTuple(1),
                keyTuple.getBigInteger(2), keyTuple.getNestedTuple(3),
                valueTuple.getBytes(0), new RTree.Rectangle(valueTuple.getNestedTuple(1)));
    }
}
