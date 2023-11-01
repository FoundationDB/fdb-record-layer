/*
 * NodeSlot.java
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

import javax.annotation.Nonnull;
import java.math.BigInteger;

/**
 * Abstract base class for all node slots. Holds a Hilbert value and a key. The semantics of these attributes
 * is refined in the subclasses {@link ItemSlot} and {@link ChildSlot}.
 */
public interface NodeSlot {
    static int compareHilbertValueKeyPair(@Nonnull final BigInteger hilbertValue1, @Nonnull final Tuple key1,
                                          @Nonnull final BigInteger hilbertValue2, @Nonnull final Tuple key2) {
        final int hilbertValueCompare = hilbertValue1.compareTo(hilbertValue2);
        if (hilbertValueCompare != 0) {
            return hilbertValueCompare;
        }
        return TupleHelpers.compare(key1, key2);
    }

    @Nonnull
    BigInteger getSmallestHilbertValue();

    @Nonnull
    BigInteger getLargestHilbertValue();

    @Nonnull
    Tuple getSmallestKey();

    @Nonnull
    default Tuple getSmallestKeySuffix() {
        return getSmallestKey().getNestedTuple(1);
    }

    @Nonnull
    Tuple getLargestKey();

    @Nonnull
    default Tuple getLargestKeySuffix() {
        return getLargestKey().getNestedTuple(1);
    }

    /**
     * Create a tuple for the key part of this slot. This tuple is used when the slot is persisted in the database.
     * Note that the serialization format is not yet finalized.
     *
     * @param storeHilbertValues indicator if the hilbert value should be encoded into the slot key or null-ed out
     *
     * @return a new tuple
     */
    @Nonnull
    Tuple getSlotKey(boolean storeHilbertValues);

    /**
     * Create a tuple for the value part of this slot. This tuple is used when the slot is persisted in the database.
     * Note that the serialization format is not yet finalized.
     *
     * @return a new tuple
     */
    @Nonnull
    Tuple getSlotValue();

    /**
     * Compare this node slot's smallest {@code (hilbertValue, key)} pair with another {@code (hilbertValue, key)}
     * pair. We do not use a proper {@link java.util.Comparator} as we don't want to wrap the pair in another object.
     *
     * @param hilbertValue Hilbert value
     * @param key first key
     *
     * @return {@code -1, 0, 1} if this node slot's pair is less/equal/greater than the pair passed in
     */
    default int compareSmallestHilbertValueAndKey(@Nonnull final BigInteger hilbertValue,
                                                  @Nonnull final Tuple key) {
        return compareHilbertValueKeyPair(getSmallestHilbertValue(), getSmallestKey(), hilbertValue, key);
    }

    /**
     * Compare this node slot's largest {@code (hilbertValue, key)} pair with another {@code (hilbertValue, key)}
     * pair. We do not use a proper {@link java.util.Comparator} as we don't want to wrap the pair in another object.
     *
     * @param hilbertValue Hilbert value
     * @param key first key
     *
     * @return {@code -1, 0, 1} if this node slot's pair is less/equal/greater than the pair passed in
     */
    default int compareLargestHilbertValueAndKey(@Nonnull final BigInteger hilbertValue,
                                                 @Nonnull final Tuple key) {
        return compareHilbertValueKeyPair(getLargestHilbertValue(), getLargestKey(), hilbertValue, key);
    }
}
