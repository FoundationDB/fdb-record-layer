/*
 * ItemSlot.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.function.Function;

/**
 * An item slot that is used by {@link LeafNode}s. Holds the actual data of the item as well as the items Hilbert
 * value and its key.
 */
public class ItemSlot implements NodeSlot {
    static final Comparator<ItemSlot> comparator =
            Comparator.comparing(ItemSlot::getHilbertValue).thenComparing(ItemSlot::getKey);

    public static final int SLOT_KEY_TUPLE_SIZE = 2;
    public static final int SLOT_VALUE_TUPLE_SIZE = 1;

    @Nonnull
    private final BigInteger hilbertValue;
    @Nonnull
    private final Tuple key;

    @Nonnull
    private final Tuple value;
    @Nonnull
    private final RTree.Point position;

    public ItemSlot(@Nonnull final BigInteger hilbertValue, @Nonnull final RTree.Point position, @Nonnull final Tuple key,
                    @Nonnull final Tuple value) {
        this.hilbertValue = hilbertValue;
        this.key = key;
        this.value = value;
        this.position = position;
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

    @Nonnull
    public Tuple getValue() {
        return value;
    }

    @Nonnull
    public RTree.Point getPosition() {
        return position;
    }

    @Nonnull
    @Override
    public BigInteger getSmallestHilbertValue() {
        return hilbertValue;
    }

    @Nonnull
    @Override
    public BigInteger getLargestHilbertValue() {
        return hilbertValue;
    }

    @Nonnull
    @Override
    public Tuple getSmallestKey() {
        return key;
    }

    @Nonnull
    @Override
    public Tuple getLargestKey() {
        return key;
    }

    @Nonnull
    @Override
    public Tuple getSlotKey(final boolean storeHilbertValues) {
        return Tuple.from(storeHilbertValues ? getHilbertValue() : null, getKey());
    }

    @Nonnull
    @Override
    public Tuple getSlotValue() {
        return Tuple.from(getValue());
    }

    /**
     * Compare this node slot's {@code (hilbertValue, key)} pair with another {@code (hilbertValue, key)}
     * pair. We do not use a proper {@link Comparator} as we don't want to wrap the pair in another object.
     *
     * @param hilbertValue Hilbert value
     * @param key first key
     *
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

    @Override
    public String toString() {
        return "[" + getPosition() + ";" + getHilbertValue() + "; " + getKey() + "]";
    }

    @Nonnull
    static ItemSlot fromKeyAndValue(@Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple,
                                    @Nonnull final Function<RTree.Point, BigInteger> hilbertValueFunction) {
        Verify.verify(keyTuple.size() == SLOT_KEY_TUPLE_SIZE);
        Verify.verify(valueTuple.size() == SLOT_VALUE_TUPLE_SIZE);
        final Tuple itemKey = keyTuple.getNestedTuple(1);
        final RTree.Point point = new RTree.Point(itemKey.getNestedTuple(0));
        final BigInteger hilbertValue;
        if (keyTuple.get(0) == null) {
            hilbertValue = hilbertValueFunction.apply(point);
        } else {
            hilbertValue = keyTuple.getBigInteger(0);
        }
        return new ItemSlot(hilbertValue, point, itemKey, valueTuple.getNestedTuple(0));
    }
}
