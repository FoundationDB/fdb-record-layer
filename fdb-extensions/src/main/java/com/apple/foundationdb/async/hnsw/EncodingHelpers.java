/*
 * EncodingHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import javax.annotation.Nonnull;

public class EncodingHelpers {
    private EncodingHelpers() {
        // nothing
    }

    /**
     * Constructs a short from two bytes in a byte array in big-endian order.
     * <p>
     * This method reads two consecutive bytes from the {@code bytes} array, starting at the given {@code offset}. The
     * byte at {@code offset} is treated as the most significant byte (MSB), and the byte at {@code offset + 1} is the
     * least significant byte (LSB).
     * @param bytes the source byte array from which to read the short.
     * @param offset the starting index in the byte array.
     * @return the short value constructed from the two bytes.
     */
    public static short shortFromBytes(final byte[] bytes, final int offset) {
        int high = bytes[offset] & 0xFF;   // Convert to unsigned int
        int low  = bytes[offset + 1] & 0xFF;

        return (short) ((high << 8) | low);
    }

    /**
     * Converts a {@code short} value into a 2-element byte array.
     * <p>
     * The conversion is performed in big-endian byte order, where the most significant byte (MSB) is placed at index 0
     * and the least significant byte (LSB) is at index 1.
     * @param value the {@code short} value to be converted.
     * @return a new 2-element byte array representing the short value in big-endian order.
     */
    public static byte[] bytesFromShort(final short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);  // high byte first
        result[1] = (byte) (value & 0xFF);         // low byte second
        return result;
    }

    /**
     * Constructs a long from eight bytes in a byte array in big-endian order.
     * <p>
     * This method reads two consecutive bytes from the {@code bytes} array, starting at the given {@code offset}. The
     * byte array is treated to be in big-endian order.
     * @param bytes the source byte array from which to read the short.
     * @param offset the starting index in the byte array.
     * @return the long value constructed from the two bytes.
     */
    public static long longFromBytes(final byte[] bytes, final int offset) {
        return ((bytes[offset    ] & 0xFFL) << 56) |
                ((bytes[offset + 1] & 0xFFL) << 48) |
                ((bytes[offset + 2] & 0xFFL) << 40) |
                ((bytes[offset + 3] & 0xFFL) << 32) |
                ((bytes[offset + 4] & 0xFFL) << 24) |
                ((bytes[offset + 5] & 0xFFL) << 16) |
                ((bytes[offset + 6] & 0xFFL) <<  8) |
                ((bytes[offset + 7] & 0xFFL));
    }

    /**
     * Converts a {@code short} value into a 2-element byte array.
     * <p>
     * The conversion is performed in big-endian byte order.
     * @param value the {@code long} value to be converted.
     * @return a new 8-element byte array representing the short value in big-endian order.
     */
    @Nonnull
    public static byte[] bytesFromLong(final long value) {
        byte[] result = new byte[8];
        fromLongIntoBytes(value, result, 0);
        return result;
    }

    /**
     * Converts a {@code short} value into a 2-element byte array.
     * <p>
     * The conversion is performed in big-endian byte order.
     * @param value the {@code long} value to be converted.
     */
    public static void fromLongIntoBytes(final long value, final byte[] bytes, final int offset) {
        bytes[offset] = (byte)(value >>> 56);
        bytes[offset + 1] = (byte)(value >>> 48);
        bytes[offset + 2] = (byte)(value >>> 40);
        bytes[offset + 3] = (byte)(value >>> 32);
        bytes[offset + 4] = (byte)(value >>> 24);
        bytes[offset + 5] = (byte)(value >>> 16);
        bytes[offset + 6] = (byte)(value >>>  8);
        bytes[offset + 7] = (byte)value;
    }
}
