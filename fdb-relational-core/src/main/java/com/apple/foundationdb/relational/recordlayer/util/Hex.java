/*
 * Hex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Converts hexadecimal Strings.
 *
 * This is forked from apache-commons-codec as it shouldn't ever change. This should go away once we move to JDK17 which
 * has native support for this.
 */
public class Hex {
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final char[] DIGITS_LOWER =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    private static final char[] DIGITS_UPPER =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private final Charset charset;

    /**
     * Converts a String representing hexadecimal values into an array of bytes of those same values. The
     * returned array will be half the length of the passed String, as it takes two characters to represent any given
     * byte. An exception is thrown if the passed String has an odd number of elements.
     *
     * @param data
     *            A String containing hexadecimal digits
     * @return A byte array containing binary data decoded from the supplied char array.
     * @throws RelationalException
     *             Thrown if an odd number or illegal of characters is supplied
     */
    public static byte[] decodeHex(final String data) throws RelationalException {
        return decodeHex(data.toCharArray());
    }

    public static byte[] decodeHexNullSafe(@Nullable final String data) throws RelationalException {
        if (data == null) {
            return new byte[0];
        }
        return decodeHex(data);
    }

    /**
     * Converts an array of characters representing hexadecimal values into an array of bytes of those same values. The
     * returned array will be half the length of the passed array, as it takes two characters to represent any given
     * byte. An exception is thrown if the passed char array has an odd number of elements.
     *
     * @param data
     *            An array of characters containing hexadecimal digits
     * @return A byte array containing binary data decoded from the supplied char array.
     * @throws RelationalException
     *             Thrown if an odd number or illegal of characters is supplied
     */
    @SuppressWarnings("PMD.OneDeclarationPerLine")
    public static byte[] decodeHex(final char[] data) throws RelationalException {
        final int len = data.length;

        if ((len & 0x01) != 0) {
            throw new RelationalException("Odd number of characters.", ErrorCode.INVALID_BINARY_REPRESENTATION);
        }

        final byte[] out = new byte[len >> 1];

        // two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;
            j++;
            f = f | toDigit(data[j], j);
            j++;
            out[i] = (byte) (f & 0xFF);
        }

        return out;
    }

    /**
     * Converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @return A char[] containing lower-case hexadecimal characters
     */
    public static char[] encodeHex(final byte[] data) {
        return encodeHex(data, true);
    }

    /**
     * Converts a byte buffer into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte buffer to convert to Hex characters
     * @return A char[] containing lower-case hexadecimal characters
     */
    public static char[] encodeHex(final ByteBuffer data) {
        return encodeHex(data, true);
    }

    /**
     * Converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @param toLowerCase
     *            <code>true</code> converts to lowercase, <code>false</code> to uppercase
     * @return A char[] containing hexadecimal characters in the selected case
     */
    public static char[] encodeHex(final byte[] data, final boolean toLowerCase) {
        return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    /**
     * Converts a byte buffer into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte buffer to convert to Hex characters
     * @param toLowerCase
     *            <code>true</code> converts to lowercase, <code>false</code> to uppercase
     * @return A char[] containing hexadecimal characters in the selected case
     * @since 1.11
     */
    public static char[] encodeHex(final ByteBuffer data, final boolean toLowerCase) {
        return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    /**
     * Converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @param toDigits
     *            the output alphabet (must contain at least 16 chars)
     * @return A char[] containing the appropriate characters from the alphabet
     *         For best results, this should be either upper- or lower-case hex.
     * @since 1.4
     */
    @SuppressWarnings("PMD.OneDeclarationPerLine")
    protected static char[] encodeHex(final byte[] data, final char[] toDigits) {
        final int l = data.length;
        final char[] out = new char[l << 1];
        // two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
            out[j++] = toDigits[0x0F & data[i]];
        }
        return out;
    }

    /**
     * Converts a byte buffer into an array of characters representing the hexadecimal values of each byte in order.
     * The returned array will be double the length of the passed array, as it takes two characters to represent any
     * given byte.
     *
     * @param data
     *            a byte buffer to convert to Hex characters
     * @param toDigits
     *            the output alphabet (must be at least 16 characters)
     * @return A char[] containing the appropriate characters from the alphabet
     *         For best results, this should be either upper- or lower-case hex.
     * @since 1.11
     */
    protected static char[] encodeHex(final ByteBuffer data, final char[] toDigits) {
        return encodeHex(data.array(), toDigits);
    }

    /**
     * Converts an array of bytes into a String representing the hexadecimal values of each byte in order. The returned
     * String will be double the length of the passed array, as it takes two characters to represent any given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @return A String containing lower-case hexadecimal characters
     * @since 1.4
     */
    public static String encodeHexString(final byte[] data) {
        return new String(encodeHex(data));
    }

    /**
     * Converts an array of bytes into a String representing the hexadecimal values of each byte in order. The returned
     * String will be double the length of the passed array, as it takes two characters to represent any given byte.
     *
     * @param data
     *            a byte[] to convert to Hex characters
     * @param toLowerCase
     *            <code>true</code> converts to lowercase, <code>false</code> to uppercase
     * @return A String containing lower-case hexadecimal characters
     * @since 1.11
     */
    public static String encodeHexString(final byte[] data, final boolean toLowerCase) {
        return new String(encodeHex(data, toLowerCase));
    }

    /**
     * Converts a byte buffer into a String representing the hexadecimal values of each byte in order. The returned
     * String will be double the length of the passed array, as it takes two characters to represent any given byte.
     *
     * @param data
     *            a byte buffer to convert to Hex characters
     * @return A String containing lower-case hexadecimal characters
     * @since 1.11
     */
    public static String encodeHexString(final ByteBuffer data) {
        return new String(encodeHex(data));
    }

    /**
     * Converts a byte buffer into a String representing the hexadecimal values of each byte in order. The returned
     * String will be double the length of the passed array, as it takes two characters to represent any given byte.
     *
     * @param data
     *            a byte buffer to convert to Hex characters
     * @param toLowerCase
     *            <code>true</code> converts to lowercase, <code>false</code> to uppercase
     * @return A String containing lower-case hexadecimal characters
     */
    public static String encodeHexString(final ByteBuffer data, final boolean toLowerCase) {
        return new String(encodeHex(data, toLowerCase));
    }

    /**
     * Converts a hexadecimal character to an integer.
     *
     * @param ch
     *            A character to convert to an integer digit
     * @param index
     *            The index of the character in the source
     * @return An integer
     * @throws RelationalException
     *             Thrown if ch is an illegal hex character
     */
    protected static int toDigit(final char ch, final int index) throws RelationalException {
        final int digit = Character.digit(ch, 16);
        if (digit == -1) {
            throw new RelationalException("Illegal hexadecimal character " + ch + " at index " + index, ErrorCode.INVALID_BINARY_REPRESENTATION);
        }
        return digit;
    }

    /**
     * Creates a new codec with the default charset name {@link #DEFAULT_CHARSET}.
     */
    public Hex() {
        // use default encoding
        this.charset = DEFAULT_CHARSET;
    }

    /**
     * Creates a new codec with the given Charset.
     *
     * @param charset
     *            the charset.
     * @since 1.7
     */
    public Hex(final Charset charset) {
        this.charset = charset;
    }

    /**
     * Creates a new codec with the given charset name.
     *
     * @param charsetName
     *            the charset name.
     * @throws java.nio.charset.UnsupportedCharsetException
     *             If the named charset is unavailable
     */
    public Hex(final String charsetName) {
        this(Charset.forName(charsetName));
    }

    /**
     * Converts an array of character bytes representing hexadecimal values into an array of bytes of those same values.
     * The returned array will be half the length of the passed array, as it takes two characters to represent any given
     * byte. An exception is thrown if the passed char array has an odd number of elements.
     *
     * @param array
     *            An array of character bytes containing hexadecimal digits
     * @return A byte array containing binary data decoded from the supplied byte array (representing characters).
     * @throws RelationalException
     *             Thrown if an odd number of characters is supplied to this function
     */
    public byte[] decode(final byte[] array) throws RelationalException {
        return decodeHex(new String(array, getCharset()).toCharArray());
    }

    /**
     * Converts a buffer of character bytes representing hexadecimal values into an array of bytes of those same values.
     * The returned array will be half the length of the passed array, as it takes two characters to represent any given
     * byte. An exception is thrown if the passed char array has an odd number of elements.
     *
     * @param buffer
     *            An array of character bytes containing hexadecimal digits
     * @return A byte array containing binary data decoded from the supplied byte array (representing characters).
     * @throws RelationalException
     *             Thrown if an odd number of characters is supplied to this function
     * @see #decodeHex(char[])
     */
    public byte[] decode(final ByteBuffer buffer) throws RelationalException {
        return decodeHex(new String(buffer.array(), getCharset()).toCharArray());
    }

    /**
     * Converts a String or an array of character bytes representing hexadecimal values into an array of bytes of those
     * same values. The returned array will be half the length of the passed String or array, as it takes two characters
     * to represent any given byte. An exception is thrown if the passed char array has an odd number of elements.
     *
     * @param object
     *            A String, ByteBuffer, byte[], or an array of character bytes containing hexadecimal digits
     * @return A byte array containing binary data decoded from the supplied byte array (representing characters).
     * @throws RelationalException
     *             Thrown if an odd number of characters is supplied to this function or the object is not a String or
     *             char[]
     */
    public Object decode(final Object object) throws RelationalException {
        if (object instanceof String) {
            return decode(((String) object).toCharArray());
        } else if (object instanceof byte[]) {
            return decode((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            return decode((ByteBuffer) object);
        } else {
            try {
                return decodeHex((char[]) object);
            } catch (final ClassCastException e) {
                throw new RelationalException(e.getMessage(), ErrorCode.INVALID_BINARY_REPRESENTATION, e);
            }
        }
    }

    /**
     * Converts an array of bytes into an array of bytes for the characters representing the hexadecimal values of each
     * byte in order. The returned array will be double the length of the passed array, as it takes two characters to
     * represent any given byte.
     * <p>
     * The conversion from hexadecimal characters to the returned bytes is performed with the charset named by
     * {@link #getCharset()}.
     * </p>
     *
     * @param array
     *            a byte[] to convert to Hex characters
     * @return A byte[] containing the bytes of the lower-case hexadecimal characters
     * @see #encodeHex(byte[])
     */
    public byte[] encode(final byte[] array) {
        return encodeHexString(array).getBytes(this.getCharset());
    }

    /**
     * Converts byte buffer into an array of bytes for the characters representing the hexadecimal values of each
     * byte in order. The returned array will be double the length of the passed array, as it takes two characters to
     * represent any given byte.
     * <p>
     * The conversion from hexadecimal characters to the returned bytes is performed with the charset named by
     * {@link #getCharset()}.
     * </p>
     *
     * @param array
     *            a byte buffer to convert to Hex characters
     * @return A byte[] containing the bytes of the lower-case hexadecimal characters
     * @see #encodeHex(byte[])
     */
    public byte[] encode(final ByteBuffer array) {
        return encodeHexString(array).getBytes(this.getCharset());
    }

    /**
     * Gets the charset.
     *
     * @return the charset.
     */
    public Charset getCharset() {
        return this.charset;
    }

    /**
     * Returns a string representation of the object, which includes the charset name.
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return super.toString() + "[charsetName=" + this.charset + "]";
    }
}
