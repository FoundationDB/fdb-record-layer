/*
 * TupleHelpers.java
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

package com.apple.foundationdb.tuple;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;

/**
 * Helper methods for working with {@link Tuple}s.
 */
@API(API.Status.UNSTABLE)
public class TupleHelpers {

    @Nonnull
    public static final Tuple EMPTY = Tuple.from();

    @Nonnull
    public static Tuple set(@Nonnull Tuple src, int index, Object value) {
        final List<Object> items = src.getItems();
        items.set(index, value);
        return Tuple.fromList(items);
    }

    @Nonnull
    public static Tuple subTuple(@Nonnull Tuple src, int start, int end) {
        final List<Object> items = src.getItems();
        return Tuple.fromList(items.subList(start, end));
    }

    /**
     * Compare two tuples lexicographically, that is, the same way they would sort when used as keys.
     *
     * Note that this is currently WAY more efficient than calling {@link Tuple#equals},
     * and slightly more efficient than calling {@link Tuple#compareTo}.
     * @param t1 the first {@link Tuple} to compare
     * @param t2 the second {@link Tuple} to compare
     * @return {@code 0} if {@code t1} and {@code t2} are equal
     *         a value less than {@code 0} if {@code t1} would sort before {@code t2}
     *         a value greater than {@code 0} if {@code t1} would sort after {@code t2}
     */
    @API(API.Status.UNSTABLE)
    public static int compare(@Nonnull Tuple t1, @Nonnull Tuple t2) {
        final int t1Len = t1.size();
        final int t2Len = t2.size();
        final int len = Math.min(t1Len, t2Len);

        for (int i = 0; i < len; i++) {
            int rc = TupleUtil.compareItems(t1.get(i), t2.get(i));
            if (rc != 0) {
                return rc;
            }
        }
        return t1Len - t2Len;
    }

    /**
     * Determine if two {@link Tuple}s have the same contents. Unfortunately, the implementation of
     * {@link Tuple#equals(Object) Tuple.equals()} serializes both {@code Tuple}s to byte arrays,
     * so it is fairly expensive. This method avoids serializing either {@code Tuple}, and it also
     * adds a short-circuit to return early if the two {@code Tuple}s are pointer-equal.
     *
     * @param t1 the first {@link Tuple} to compare
     * @param t2 the second {@link Tuple} to compare
     * @return {@code true} if the two {@link Tuple}s would serialize to the same array and {@code false} otherwise
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static boolean equals(@Nullable Tuple t1, @Nullable Tuple t2) {
        if (t1 == null) {
            return t2 == null;
        } else {
            return t2 != null && (t1 == t2 || compare(t1, t2) == 0);
        }
    }

    /**
     * Negate a number used as an element of a {@link Tuple}.
     * @param number the number to be negated
     * @return a negated number suitable for use in a new {@link Tuple}
     */
    public static Number negate(@Nonnull Number number) {
        if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
            long l = number.longValue();
            if (l == Long.MIN_VALUE) {  // The only long whose negation is not a long.
                return BigInteger.valueOf(l).negate();
            } else {
                return - l;
            }
        }
        if (number instanceof BigInteger) {
            BigInteger n = ((BigInteger)number).negate();
            if (n.equals(BigInteger.valueOf(Long.MIN_VALUE))) {
                return Long.MIN_VALUE;  // The normal form of this number.
            } else {
                return n;
            }
        }
        if (number instanceof Double || number instanceof Float) {
            return - number.doubleValue();
        }
        throw new IllegalArgumentException("Not an allowed number type from a Tuple: " + number.getClass());
    }

    /**
     * Get the number of bytes that an object would occupy as an element of an encoded {@link Tuple}.
     * Unlike {@link Tuple#getPackedSize()}, this does not include the extra space an incomplete {@link Versionstamp} would add to the end to record its position.
     * @param item an item of a tuple
     * @return the number of bytes in the encoded form of the item
     */
    public static int packedSizeAsTupleItem(Object item) {
        int size = Tuple.from(item).getPackedSize();
        if (item instanceof Versionstamp) {
            if (!((Versionstamp)item).isComplete()) {
                size -= FDB.instance().getAPIVersion() < 520 ? Short.BYTES : Integer.BYTES;
            }
        }
        return size;
    }

    /**
     * Get whether one tuple is a prefix of another.
     * @param potentialPrefix the potential prefix
     * @param wholeTuple the whole tuple
     * @return {@code true} if {@code potentialPrefix} is a prefix of {@code wholeTuple}
     */
    public static boolean isPrefix(@Nonnull Tuple potentialPrefix, @Nonnull Tuple wholeTuple) {
        final int len = potentialPrefix.size();
        if (wholeTuple.size() < len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int rc = TupleUtil.compareItems(potentialPrefix.get(i), wholeTuple.get(i));
            if (rc != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the index of a prefix of an encoded tuple that decodes as a sub-tuple of the given size.
     * @param bytes encoded tuple
     * @param size size of desired subtuple
     * @return the index into {@code bytes} that ends the sub-tuple or {@code -1} if {@code bytes} is too short or decoding fails
     */
    public static int prefixLengthOfSize(@Nonnull byte[] bytes, int size) {
        TupleUtil.DecodeState state = new TupleUtil.DecodeState();
        int pos = 0;
        int end = bytes.length;
        while (state.values.size() < size) {
            if (pos >= end) {
                return -1;      // Not long enough.
            }
            try {
                TupleUtil.decode(state, bytes, pos, end);
            } catch (Exception ex) {
                return -1;
            }
            pos = state.end;
        }
        return pos;
    }

    private TupleHelpers() {
    }
}
