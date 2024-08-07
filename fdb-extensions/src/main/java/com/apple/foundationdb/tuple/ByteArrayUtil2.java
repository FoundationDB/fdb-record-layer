/*
 * ByteArrayUtil2.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper methods in the spirit of {@link ByteArrayUtil}.
 */
@API(API.Status.UNSTABLE)
public class ByteArrayUtil2 {

    private static final byte EQUALS_CHARACTER = (byte)'=';
    private static final byte DOUBLE_QUOTE_CHARACTER = (byte)'"';
    private static final byte BACKSLASH_CHARACTER = (byte)'\\';
    private static final byte MINIMUM_PRINTABLE_CHARACTER = 32;
    private static final int MAXIMUM_PRINTABLE_CHARACTER = 127;

    private static final char[] HEX_CHARS =
            { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    private static final char[] LOWER_CASE_HEX_CHARS =
            { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * Create a base-16 representation of the give byte array. This uses upper case letters for
     * hex digits {@code A} through {@code F}, so the returned string will match regex
     * {@code ([0-9A-F])*}.
     *
     * @param bytes a {@code byte} array
     * @return a hex representation of {@code bytes}
     */
    @Nullable
    public static String toHexString(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        char[] hex = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hex[j * 2] = HEX_CHARS[v >>> 4];
            hex[j * 2 + 1] = HEX_CHARS[v & 0x0F];
        }
        return new String(hex);
    }

    /**
     * Creates a human-readable representation of {@code bytes} for logging purposes.
     * This differs from {@link ByteArrayUtil#printable(byte[])} in tha it excludes the
     * {@code =} and {@code "} characters, which can cause trouble when included as a
     * key or value in log messages with keys and values.
     *
     * @param bytes a {@code byte} array
     * @return a hex representation of {@code bytes}
     * @see ByteArrayUtil#printable(byte[])
     */
    @API(API.Status.MAINTAINED)
    @Nullable
    public static String loggable(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();

            for (byte b : bytes) {
                // remove '=' and '"' because they confuse parsing of key=value log messages
                if (b >= MINIMUM_PRINTABLE_CHARACTER && b < MAXIMUM_PRINTABLE_CHARACTER &&
                        b != BACKSLASH_CHARACTER && b != EQUALS_CHARACTER && b != DOUBLE_QUOTE_CHARACTER) {
                    sb.append((char)b);
                } else if (b == BACKSLASH_CHARACTER) {
                    sb.append("\\\\");
                } else {
                    sb.append("\\x").append(LOWER_CASE_HEX_CHARS[(b >>> 4) & 0x0F]).append(LOWER_CASE_HEX_CHARS[b & 0x0F]);
                }
            }

            return sb.toString();
        }
    }

    /**
     * Construct a byte array from a human-readable representation returned by
     * {@link ByteArrayUtil#printable(byte[])} or {@link #loggable(byte[])}. This
     * will only return {@code null} if the provided {@code loggableBytes} array
     * is {@code null}.
     *
     * @param loggedBytes a string representation of a byte array in the format
     *     returned by {@link ByteArrayUtil#printable(byte[])}
     * @return a byte array parsed from {@code loggedBytes}
     */
    @API(API.Status.MAINTAINED)
    @Nullable
    public static byte[] unprint(@Nullable String loggedBytes) {
        if (loggedBytes == null) {
            return null;
        }
        List<Byte> bytes = new ArrayList<>();
        int i = 0;
        while (i < loggedBytes.length()) {
            char c = loggedBytes.charAt(i);
            if (c == '\\') {
                i++;
                c = loggedBytes.charAt(i);
                if (c == '\\') {
                    bytes.add((byte)'\\');
                } else if (c == 'x') {
                    i++;
                    bytes.add((byte)Integer.parseInt(loggedBytes.substring(i, i + 2), 16));
                    i++;
                } else {
                    throw new IllegalArgumentException("unexpected char at " + i);
                }
            } else {
                bytes.add((byte)c);
            }
            i++;
        }
        byte[] bytesArray = new byte[bytes.size()];
        for (int j = 0; j < bytes.size(); j++) {
            bytesArray[j] = bytes.get(j);
        }
        return bytesArray;
    }

    /**
     * Return whether {@code bytes1} and {@code bytes2} each begin with a common
     * prefix with a size of at least {@code prefixSize}.
     *
     * @param bytes1 one byte array
     * @param bytes2 another byte array
     * @param prefixSize a number of bytes
     * @return whether the first {@code prefixSize} bytes from {@code bytes1} match
     *      the first {@code prefixSize} bytes from {@code bytes2}
     */
    public static boolean hasCommonPrefix(@Nonnull byte[] bytes1, @Nonnull byte[] bytes2, int prefixSize) {
        if (bytes1.length < prefixSize || bytes2.length < prefixSize) {
            return false;
        }
        for (int i = 0; i < prefixSize; i++) {
            if (bytes1[i] != bytes2[i]) {
                return false;
            }
        }
        return true;
    }

    private ByteArrayUtil2() {
    }
}
