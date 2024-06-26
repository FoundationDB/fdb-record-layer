/*
 * TupleOrdering.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Helper methods for encoding {@link Tuple}s in ways that implement SQL-like ordering.
 */
@API(API.Status.UNSTABLE)
public class TupleOrdering {
    /**
     * Extended tuple code used to represent a null for `NULLS LAST`.
     * Does not need to be a fully supported tuple code since ordered encoding is treated as a byte array.
     */
    static final byte NULL_LAST = (byte)0xFE;

    /**
     * Direction of ordering.
     * {@link #ASC_NULLS_FIRST} corresponds to the default {@code Tuple} packing and is included for completeness.
     */
    public enum Direction {
        ASC_NULLS_FIRST(false, false),
        ASC_NULLS_LAST(false, true),
        DESC_NULLS_FIRST(true, true),
        DESC_NULLS_LAST(true, false);

        private final boolean inverted;
        private final boolean counterflowNulls;

        Direction(boolean inverted, boolean counterflowNulls) {
            this.inverted = inverted;
            this.counterflowNulls = counterflowNulls;
        }

        /**
         * Get whether the byte ordering is inverted, that is, the opposite of the default unordered byte comparison of packed {@code Tuple}s.
         * @return {@code true} if byte order is inverted
         */
        public boolean isInverted() {
            return inverted;
        }

        /**
         * Get whether {@code null} values sort at the opposite end as in the default {@code Tuple} packing.
         * @return {@code true} if nulls come at the end when ascending
         */
        public boolean isCounterflowNulls() {
            return counterflowNulls;
        }

        /**
         * Get whether values are ordered ascending.
         * This is the opposite of {@link #isInverted()}, viewed from the name of the enum, as opposed to what encoding
         * would need to do.
         * @return {@code true} if greater values from first
         */
        public boolean isAscending() {
            return !inverted;
        }

        /**
         * Get whether values are ordered descending.
         * This is the same as {@link #isInverted()}, viewed from the name of the enum, as opposed to what encoding
         * would need to do.
         * @return {@code true} if greater values from first
         */
        public boolean isDescending() {
            return inverted;
        }

        /**
         * Get whether null values come earlier.
         * This corresponds to the second part of the enum name.
         * @return {@code true} if nulls values come before non-null values
         */
        public boolean isNullsFirst() {
            return inverted == counterflowNulls;
        }

        /**
         * Get whether null values come later.
         * This corresponds to the second part of the enum name.
         * @return {@code true} if nulls values come after non-null values
         */
        public boolean isNullsLast() {
            return inverted != counterflowNulls;
        }
    }

    private TupleOrdering() {
    }

    /**
     * Encode the given tuple in a way that unordered byte comparison respects the given direction.
     * @param tuple tuple to encode
     * @param direction direction of desired ordering
     * @return a byte string that compares properly
     */
    @Nonnull
    public static byte[] pack(@Nonnull Tuple tuple, @Nonnull Direction direction) {
        final byte[] packed = direction.isCounterflowNulls() ? packNullsLast(tuple.elements) : tuple.pack();
        return direction.isInverted() ? invert(packed) : packed;
    }

    /**
     * Decode the result of {@link #pack}, recovering the original tuple.
     * @param packed comparable byte string encoding
     * @param direction direction used by encoding
     * @return tuple that would encode that way
     */
    @Nonnull
    public static Tuple unpack(@Nonnull byte[] packed, @Nonnull Direction direction) {
        final byte[] bytes = direction.isInverted() ? uninvert(packed) : packed;
        return direction.isCounterflowNulls() ? Tuple.fromList(unpackNullsLast(bytes)) : Tuple.fromBytes(bytes);
    }

    @Nonnull
    static byte[] packNullsLast(@Nonnull List<Object> elements) {
        ByteBuffer dest = ByteBuffer.allocate(TupleUtil.getPackedSize(elements, false));
        ByteOrder origOrder = dest.order();
        TupleUtil.EncodeState state = new TupleUtil.EncodeState(dest);
        for (Object obj : elements) {
            encodeNullsLast(state, obj);
        }
        dest.order(origOrder);
        if (state.versionPos >= 0) {
            throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
        }
        return dest.array();
    }

    static void encodeNullsLast(@Nonnull TupleUtil.EncodeState state, @Nullable Object obj) {
        if (obj == null) {
            state.add(NULL_LAST);
        } else {
            TupleUtil.encode(state, obj, false);
        }
    }

    @Nonnull
    static List<Object> unpackNullsLast(@Nonnull byte[] bytes) {
        TupleUtil.DecodeState decodeState = new TupleUtil.DecodeState();
        int pos = 0;
        int end = bytes.length;
        while (pos < end) {
            decodeNullsLast(decodeState, bytes, pos, end);
            pos = decodeState.end;
        }
        return decodeState.values;
    }

    static void decodeNullsLast(@Nonnull TupleUtil.DecodeState state, @Nonnull byte[] bytes, int pos, int end) {
        if (bytes[pos] == NULL_LAST) {
            state.add(null, pos + 1);
        } else {
            TupleUtil.decode(state, bytes, pos, end);
        }
    }

    /**
     * Encode a byte array so that unsigned byte comparison is reversed.
     * Mostly this is inverting the individual bytes. Except that a prefix byte array needs to be greater than one
     * with additional bytes, which would mean appending an infinite number of {@code FF} bytes.
     * Instead, the inverted bytes are treated as a bit string and encoded 7 bits to a byte with a zero high bit, padded
     * with one bits, followed by a byte with a one high bit so that shorter is always greater.
     * In the scheme here, the final byte has the number of padding bits in the next three bits.
     * This allows for encoding arbitrary length bit strings properly, while keeping the invariant that shorter strings
     * are greater, since they have more padding.
     * Since this encoding is, in fact, only currently used to encode an even number of bytes, it would also work to always
     * have the final byte be the same, such as {@code 80} or {@code FF}.
     * @param bytes byte array to be inverted
     * @return a byte array that compared in the reverse direction
     */
    @Nonnull
    static byte[] invert(@Nonnull byte[] bytes) {
        final int originalLength = bytes.length;
        final int invertedLength = (originalLength * 8 + 6) / 7 + 1;
        final byte[] inverted = new byte[invertedLength];
        int bits = 0;
        int nbits = 0;
        int in = 0;
        int out = 0;
        while (in < originalLength) {
            bits = (bits << 8) | (bytes[in++] & 0xFF) ^ 0xFF;
            nbits += 8;
            while (nbits >= 7) {
                inverted[out++] = (byte)((bits >> (nbits - 7)) & 0x7F);
                nbits -= 7;
            }
        }
        if (nbits == 0) {
            inverted[out++] = (byte)0x80;
        } else {
            final int npad = 7 - nbits;
            inverted[out++] = (byte)(((bits << npad) | ((1 << npad) - 1)) & 0x7F);
            inverted[out++] = (byte)(0x80 | (npad << 4));
        }
        if (out != invertedLength) {
            throw new IllegalStateException("ordering invert did not encode to correct number of bytes");
        }
        return inverted;
    }

    @Nonnull
    static byte[] uninvert(@Nonnull byte[] inverted) {
        final int invertedLength = inverted.length;
        if (invertedLength == 0 || (inverted[invertedLength - 1] & 0x80) == 0) {
            throw new IllegalArgumentException("inverted bytes not in expected format");
        }
        final int uninvertedBitLength = ((invertedLength - 1) * 7) - ((inverted[invertedLength - 1] & 0x70) >> 4);
        if ((uninvertedBitLength % 8) != 0) {
            throw new IllegalStateException("inverted length not even number of bytes");
        }
        final int uninvertedLength = uninvertedBitLength / 8;
        final byte[] uninverted = new byte[uninvertedLength];
        int bits = 0;
        int nbits = 0;
        int in = 0;
        int out = 0;
        while (in < invertedLength - 1) {
            final int next = inverted[in++] ^ 0x7F;
            if ((next & 0x80) != 0) {
                throw new IllegalArgumentException("non final inverted byte has high bit");
            }
            bits = (bits << 7) | next;
            nbits += 7;
            while (nbits >= 8) {
                uninverted[out++] = (byte)(bits >> (nbits - 8));
                nbits -= 8;
            }
        }
        if (out != uninvertedLength) {
            throw new IllegalStateException("ordering uninvert did not encode to correct number of bytes");
        }
        return uninverted;
    }
}
