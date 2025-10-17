/*
 * Copyright 2023 Christian Heina
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
 *
 * Modifications Copyright 2015-2025 Apple Inc. and the FoundationDB project authors.
 * This source file is part of the FoundationDB open source project
 */

package com.apple.foundationdb.half;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link Half}.
 *
 * @author Christian Heina (developer@christianheina.com)
 */
public class HalfTest {
    private static final short POSITIVE_INFINITY_SHORT_VALUE = (short) 0x7c00;
    private static final short NEGATIVE_INFINITY_SHORT_VALUE = (short) 0xfc00;
    private static final short NaN_SHORT_VALUE = (short) 0x7e00;
    private static final short MAX_VALUE_SHORT_VALUE = (short) 0x7bff;
    private static final short MIN_NORMAL_SHORT_VALUE = (short) 0x0400;
    private static final short MIN_VALUE_SHORT_VALUE = (short) 0x1;
    private static final int MAX_EXPONENT = 15;
    private static final int MIN_EXPONENT = -14;
    private static final int SIZE = 16;
    private static final int BYTES = 2;
    private static final short POSITIVE_ZERO_SHORT_VALUE = (short) 0x0;
    private static final short NEGATIVE_ZERO_SHORT_VALUE = (short) 0x8000;

    private static final short LOWEST_ABOVE_ONE_SHORT_VALUE = (short) 0x3c01;
    private static final Half LOWEST_ABOVE_ONE = Half.shortBitsToHalf(LOWEST_ABOVE_ONE_SHORT_VALUE);
    private static final short NEGATIVE_MAX_VALUE_SHORT_VALUE = (short) 0xfbff;
    private static final Half NEGATIVE_MAX_VALUE = Half.shortBitsToHalf(NEGATIVE_MAX_VALUE_SHORT_VALUE);

    @Test
    public void publicStaticClassVariableTest() {
        Assertions.assertEquals(POSITIVE_INFINITY_SHORT_VALUE, Half.halfToShortBits(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.shortBitsToHalf(POSITIVE_INFINITY_SHORT_VALUE));

        Assertions.assertEquals(NEGATIVE_INFINITY_SHORT_VALUE, Half.halfToShortBits(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.shortBitsToHalf(NEGATIVE_INFINITY_SHORT_VALUE));

        Assertions.assertEquals(NaN_SHORT_VALUE, Half.halfToShortBits(Half.NaN));
        Assertions.assertEquals(Half.NaN, Half.shortBitsToHalf(NaN_SHORT_VALUE));

        Assertions.assertEquals(MAX_VALUE_SHORT_VALUE, Half.halfToShortBits(Half.MAX_VALUE));
        Assertions.assertEquals(Half.MAX_VALUE, Half.shortBitsToHalf(MAX_VALUE_SHORT_VALUE));

        Assertions.assertEquals(NEGATIVE_MAX_VALUE_SHORT_VALUE, Half.halfToShortBits(Half.NEGATIVE_MAX_VALUE));
        Assertions.assertEquals(Half.NEGATIVE_MAX_VALUE, Half.shortBitsToHalf(NEGATIVE_MAX_VALUE_SHORT_VALUE));

        Assertions.assertEquals(MIN_NORMAL_SHORT_VALUE, Half.halfToShortBits(Half.MIN_NORMAL));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.shortBitsToHalf(MIN_NORMAL_SHORT_VALUE));
        Assertions.assertEquals(Half.MIN_NORMAL.doubleValue(), Math.pow(2, -14));

        Assertions.assertEquals(MIN_VALUE_SHORT_VALUE, Half.halfToShortBits(Half.MIN_VALUE));
        Assertions.assertEquals(Half.MIN_VALUE, Half.shortBitsToHalf(MIN_VALUE_SHORT_VALUE));
        Assertions.assertEquals(Half.MIN_VALUE.doubleValue(), Math.pow(2, -24));

        Assertions.assertEquals(Half.MAX_EXPONENT, MAX_EXPONENT);
        Assertions.assertEquals(Half.MAX_EXPONENT, Math.getExponent(Half.MAX_VALUE.floatValue()));

        Assertions.assertEquals(Half.MIN_EXPONENT, MIN_EXPONENT);
        Assertions.assertEquals(Half.MIN_EXPONENT, Math.getExponent(Half.MIN_NORMAL.floatValue()));

        Assertions.assertEquals(Half.SIZE, SIZE);
        Assertions.assertEquals(Half.BYTES, BYTES);

        Assertions.assertEquals(POSITIVE_ZERO_SHORT_VALUE, Half.halfToShortBits(Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.shortBitsToHalf(POSITIVE_ZERO_SHORT_VALUE));

        Assertions.assertEquals(NEGATIVE_ZERO_SHORT_VALUE, Half.halfToShortBits(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.shortBitsToHalf(NEGATIVE_ZERO_SHORT_VALUE));
    }

    @Test
    public void shortBitsToHalfTest() {
        Assertions.assertEquals(Float.POSITIVE_INFINITY, Half.shortBitsToHalf(POSITIVE_INFINITY_SHORT_VALUE).floatValue());
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, Half.shortBitsToHalf(NEGATIVE_INFINITY_SHORT_VALUE).floatValue());
        Assertions.assertEquals(Float.NaN, Half.shortBitsToHalf(NaN_SHORT_VALUE).floatValue());
        Assertions.assertEquals(65504f, Half.shortBitsToHalf(MAX_VALUE_SHORT_VALUE).floatValue());
        Assertions.assertEquals(6.103515625e-5f, Half.shortBitsToHalf(MIN_NORMAL_SHORT_VALUE).floatValue());
        Assertions.assertEquals(5.9604645e-8f, Half.shortBitsToHalf(MIN_VALUE_SHORT_VALUE).floatValue());
        Assertions.assertEquals(0f, Half.shortBitsToHalf(POSITIVE_ZERO_SHORT_VALUE).floatValue());
        Assertions.assertEquals(-0f, Half.shortBitsToHalf(NEGATIVE_ZERO_SHORT_VALUE).floatValue());

        Assertions.assertEquals(1.00097656f, Half.shortBitsToHalf(LOWEST_ABOVE_ONE_SHORT_VALUE).floatValue());
    }

    @Test
    public void halfToShortBitsTest() {
        Assertions.assertEquals(POSITIVE_INFINITY_SHORT_VALUE, Half.halfToShortBits(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(NEGATIVE_INFINITY_SHORT_VALUE, Half.halfToShortBits(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(NaN_SHORT_VALUE, Half.halfToShortBits(Half.NaN));
        Assertions.assertEquals(NaN_SHORT_VALUE, Half.halfToShortBits(Half.shortBitsToHalf((short) 0x7e04)));
        Assertions.assertEquals(NaN_SHORT_VALUE, Half.halfToShortBits(Half.shortBitsToHalf((short) 0x7fff)));
        Assertions.assertEquals(MAX_VALUE_SHORT_VALUE, Half.halfToShortBits(Half.MAX_VALUE));
        Assertions.assertEquals(MIN_NORMAL_SHORT_VALUE, Half.halfToShortBits(Half.MIN_NORMAL));
        Assertions.assertEquals(MIN_VALUE_SHORT_VALUE, Half.halfToShortBits(Half.MIN_VALUE));
        Assertions.assertEquals(POSITIVE_ZERO_SHORT_VALUE, Half.halfToShortBits(Half.POSITIVE_ZERO));
        Assertions.assertEquals(NEGATIVE_ZERO_SHORT_VALUE, Half.halfToShortBits(Half.NEGATIVE_ZERO));

        Assertions.assertEquals(LOWEST_ABOVE_ONE_SHORT_VALUE, Half.halfToShortBits(LOWEST_ABOVE_ONE));
    }

    @Test
    public void halfToRawShortBitsTest() {
        Assertions.assertEquals(POSITIVE_INFINITY_SHORT_VALUE, Half.halfToRawShortBits(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(NEGATIVE_INFINITY_SHORT_VALUE, Half.halfToRawShortBits(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(NaN_SHORT_VALUE, Half.halfToRawShortBits(Half.NaN));
        Assertions.assertEquals((short) 0x7e04, Half.halfToRawShortBits(Half.shortBitsToHalf((short) 0x7e04)));
        Assertions.assertEquals((short) 0x7fff, Half.halfToRawShortBits(Half.shortBitsToHalf((short) 0x7fff)));
        Assertions.assertEquals((short) 0x7e00, Half.halfToRawShortBits(Half.valueOf(Float.intBitsToFloat(0x7fe00000))));
        Assertions.assertEquals((short) 0x7e00, Half.halfToRawShortBits(Half.valueOf(Float.intBitsToFloat(0x7fe00001))));
        Assertions.assertEquals(MAX_VALUE_SHORT_VALUE, Half.halfToRawShortBits(Half.MAX_VALUE));
        Assertions.assertEquals(MIN_NORMAL_SHORT_VALUE, Half.halfToRawShortBits(Half.MIN_NORMAL));
        Assertions.assertEquals(MIN_VALUE_SHORT_VALUE, Half.halfToRawShortBits(Half.MIN_VALUE));
        Assertions.assertEquals(POSITIVE_ZERO_SHORT_VALUE, Half.halfToRawShortBits(Half.POSITIVE_ZERO));
        Assertions.assertEquals(NEGATIVE_ZERO_SHORT_VALUE, Half.halfToRawShortBits(Half.NEGATIVE_ZERO));

        Assertions.assertEquals(LOWEST_ABOVE_ONE_SHORT_VALUE, Half.halfToRawShortBits(LOWEST_ABOVE_ONE));
    }

    @Test
    public void shortValueTest() {
        Assertions.assertEquals(Short.MAX_VALUE, Half.POSITIVE_INFINITY.shortValue());
        Assertions.assertEquals(Short.MIN_VALUE, Half.NEGATIVE_INFINITY.shortValue());
        Assertions.assertEquals((short) 0, Half.NaN.shortValue());
        Assertions.assertEquals(Short.MAX_VALUE, Half.MAX_VALUE.shortValue());
        Assertions.assertEquals(Short.MIN_VALUE, NEGATIVE_MAX_VALUE.shortValue());
        Assertions.assertEquals((short) 0, Half.MIN_NORMAL.shortValue());
        Assertions.assertEquals((short) 0, Half.MIN_VALUE.shortValue());
        Assertions.assertEquals((short) 0, Half.POSITIVE_ZERO.shortValue());
        Assertions.assertEquals((short) 0, Half.NEGATIVE_ZERO.shortValue());

        Assertions.assertEquals((short) 1, LOWEST_ABOVE_ONE.shortValue());
    }

    @Test
    public void intValueTest() {
        Assertions.assertEquals(Integer.MAX_VALUE, Half.POSITIVE_INFINITY.intValue());
        Assertions.assertEquals(Integer.MIN_VALUE, Half.NEGATIVE_INFINITY.intValue());
        Assertions.assertEquals(0, Half.NaN.intValue());
        Assertions.assertEquals(65504, Half.MAX_VALUE.intValue());
        Assertions.assertEquals(0, Half.MIN_NORMAL.intValue());
        Assertions.assertEquals(0, Half.MIN_VALUE.intValue());
        Assertions.assertEquals(0, Half.POSITIVE_ZERO.intValue());
        Assertions.assertEquals(0, Half.NEGATIVE_ZERO.intValue());

        Assertions.assertEquals(1, LOWEST_ABOVE_ONE.intValue());
    }

    @Test
    public void longValueTest() {
        Assertions.assertEquals(Long.MAX_VALUE, Half.POSITIVE_INFINITY.longValue());
        Assertions.assertEquals(Long.MIN_VALUE, Half.NEGATIVE_INFINITY.longValue());
        Assertions.assertEquals(0, Half.NaN.longValue());
        Assertions.assertEquals(65504, Half.MAX_VALUE.longValue());
        Assertions.assertEquals(0, Half.MIN_NORMAL.longValue());
        Assertions.assertEquals(0, Half.MIN_VALUE.longValue());
        Assertions.assertEquals(0, Half.POSITIVE_ZERO.longValue());
        Assertions.assertEquals(0, Half.NEGATIVE_ZERO.longValue());

        Assertions.assertEquals(1, LOWEST_ABOVE_ONE.longValue());
    }

    @Test
    public void floatValueTest() {
        Assertions.assertEquals(Float.POSITIVE_INFINITY, Half.POSITIVE_INFINITY.floatValue());
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, Half.NEGATIVE_INFINITY.floatValue());
        Assertions.assertEquals(Float.NaN, Half.NaN.floatValue());
        Assertions.assertEquals(65504f, Half.MAX_VALUE.floatValue());
        Assertions.assertEquals(6.103515625e-5f, Half.MIN_NORMAL.floatValue());
        Assertions.assertEquals(5.9604645e-8f, Half.MIN_VALUE.floatValue());
        Assertions.assertEquals(0f, Half.POSITIVE_ZERO.floatValue());
        Assertions.assertEquals(-0f, Half.NEGATIVE_ZERO.floatValue());

        Assertions.assertEquals(1.00097656f, LOWEST_ABOVE_ONE.floatValue());
    }

    @Test
    public void doubleValueTest() {
        Assertions.assertEquals(Double.POSITIVE_INFINITY, Half.POSITIVE_INFINITY.doubleValue());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, Half.NEGATIVE_INFINITY.doubleValue());
        Assertions.assertEquals(Double.NaN, Half.NaN.doubleValue());
        Assertions.assertEquals(65504d, Half.MAX_VALUE.doubleValue());
        Assertions.assertEquals(6.103515625e-5d, Half.MIN_NORMAL.doubleValue());
        Assertions.assertEquals(5.9604644775390625E-8d, Half.MIN_VALUE.doubleValue());
        Assertions.assertEquals(0d, Half.POSITIVE_ZERO.doubleValue());
        Assertions.assertEquals(-0d, Half.NEGATIVE_ZERO.doubleValue());

        Assertions.assertEquals(1.0009765625d, LOWEST_ABOVE_ONE.doubleValue());
    }

    @Test
    public void byteValueTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY.byteValue(), Float.valueOf(Float.POSITIVE_INFINITY).byteValue());
        Assertions.assertEquals(Half.NEGATIVE_INFINITY.byteValue(), Float.valueOf(Float.NEGATIVE_INFINITY).byteValue());
        Assertions.assertEquals(Half.NaN.byteValue(), Float.valueOf(Float.NaN).byteValue());
        Assertions.assertEquals(Half.MAX_VALUE.byteValue(), Float.valueOf(Float.MAX_VALUE).byteValue());
        Assertions.assertEquals(Half.MIN_NORMAL.byteValue(), Float.valueOf(Float.MIN_NORMAL).byteValue());
        Assertions.assertEquals(Half.MIN_VALUE.byteValue(), Float.valueOf(Float.MIN_VALUE).byteValue());
        Assertions.assertEquals(Half.POSITIVE_ZERO.byteValue(), Float.valueOf(0.0f).byteValue());
        Assertions.assertEquals(Half.NEGATIVE_ZERO.byteValue(), Float.valueOf(-0.0f).byteValue());
    }

    @Test
    public void valueOfStringTest() {
        // Decmial values
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.valueOf("Infinity"));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.valueOf("-Infinity"));
        Assertions.assertEquals(Half.NaN, Half.valueOf("NaN"));
        Assertions.assertEquals(Half.MAX_VALUE, Half.valueOf("65504"));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.valueOf("6.103515625e-5"));
        Assertions.assertEquals(Half.MIN_VALUE, Half.valueOf("5.9604645e-8"));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.valueOf("0"));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.valueOf("-0"));

        Assertions.assertEquals(LOWEST_ABOVE_ONE, Half.valueOf("1.00097656f"));

        // Hex values
        Assertions.assertEquals(Half.valueOf("0x1.0p0"), Half.valueOf(1.0f));
        Assertions.assertEquals(Half.valueOf("-0x1.0p0"), Half.valueOf(-1.0f));
        Assertions.assertEquals(Half.valueOf("0x1.0p1"), Half.valueOf(2.0f));
        Assertions.assertEquals(Half.valueOf("0x1.8p1"), Half.valueOf(3.0f));
        Assertions.assertEquals(Half.valueOf("0x1.0p-1"), Half.valueOf(0.5f));
        Assertions.assertEquals(Half.valueOf("0x1.0p-2"), Half.valueOf(0.25f));
        Assertions.assertEquals(Half.valueOf("0x0.ffcp-14"), Half.shortBitsToHalf((short) 0x3ff));
    }

    @Test
    public void valueOfStringNumberFormatExceptionTest() {
        Assertions.assertThrows(NumberFormatException.class, () -> Half.valueOf("ABC"));
    }

    @Test
    public void valueOfStringNullPointerExceptionTest() {
        Assertions.assertThrows(NullPointerException.class, () -> Half.valueOf((String)null));
    }

    @Test
    public void valueOfDoubleTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.valueOf(Double.valueOf(Double.POSITIVE_INFINITY)));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.valueOf(Double.valueOf(Double.NEGATIVE_INFINITY)));
        Assertions.assertEquals(Half.NaN, Half.valueOf(Double.valueOf(Double.NaN)));
        Assertions.assertEquals(Half.MAX_VALUE, Half.valueOf(Double.valueOf(65504d)));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.valueOf(Double.valueOf(6.103515625e-5d)));
        Assertions.assertEquals(Half.MIN_VALUE, Half.valueOf(Double.valueOf(5.9604644775390625E-8d)));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.valueOf(Double.valueOf(0d)));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.valueOf(Double.valueOf(-0d)));

        Assertions.assertEquals(LOWEST_ABOVE_ONE, Half.valueOf(Double.valueOf(1.0009765625d)));
    }

    @Test
    public void valueOfFloatTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.valueOf(Float.valueOf(Float.POSITIVE_INFINITY)));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.valueOf(Float.valueOf(Float.NEGATIVE_INFINITY)));
        Assertions.assertEquals(Half.NaN, Half.valueOf(Float.valueOf(Float.NaN)));
        Assertions.assertEquals(Half.MAX_VALUE, Half.valueOf(Float.valueOf(65504f)));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.valueOf(Float.valueOf(6.103515625e-5f)));
        Assertions.assertEquals(Half.MIN_VALUE, Half.valueOf(Float.valueOf(5.9604645e-8f)));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.valueOf(Float.valueOf(0f)));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.valueOf(Float.valueOf(-0f)));

        Assertions.assertEquals(LOWEST_ABOVE_ONE, Half.valueOf(Float.valueOf(1.00097656f)));
    }

    @Test
    public void valueOfHalfTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.valueOf(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.valueOf(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.NaN, Half.valueOf(Half.NaN));
        Assertions.assertEquals(Half.MAX_VALUE, Half.valueOf(Half.MAX_VALUE));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.valueOf(Half.MIN_NORMAL));
        Assertions.assertEquals(Half.MIN_VALUE, Half.valueOf(Half.MIN_VALUE));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.valueOf(Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.valueOf(Half.NEGATIVE_ZERO));

        Assertions.assertEquals(LOWEST_ABOVE_ONE, Half.valueOf(LOWEST_ABOVE_ONE));
    }

    @Test
    public void isNaNTest() {
        Assertions.assertFalse(Half.POSITIVE_INFINITY.isNaN());
        Assertions.assertFalse(Half.NEGATIVE_INFINITY.isNaN());
        Assertions.assertTrue(Half.NaN.isNaN());
        Assertions.assertFalse(Half.MAX_VALUE.isNaN());
        Assertions.assertFalse(Half.MIN_NORMAL.isNaN());
        Assertions.assertFalse(Half.MIN_VALUE.isNaN());
        Assertions.assertFalse(Half.POSITIVE_ZERO.isNaN());
        Assertions.assertFalse(Half.NEGATIVE_ZERO.isNaN());

        Assertions.assertFalse(LOWEST_ABOVE_ONE.isNaN());
    }

    @Test
    public void isInfiniteTest() {
        Assertions.assertTrue(Half.POSITIVE_INFINITY.isInfinite());
        Assertions.assertTrue(Half.NEGATIVE_INFINITY.isInfinite());
        Assertions.assertFalse(Half.NaN.isInfinite());
        Assertions.assertFalse(Half.MAX_VALUE.isInfinite());
        Assertions.assertFalse(Half.MIN_NORMAL.isInfinite());
        Assertions.assertFalse(Half.MIN_VALUE.isInfinite());
        Assertions.assertFalse(Half.POSITIVE_ZERO.isInfinite());
        Assertions.assertFalse(Half.NEGATIVE_ZERO.isInfinite());

        Assertions.assertFalse(LOWEST_ABOVE_ONE.isInfinite());
    }

    @Test
    public void isFiniteTest() {
        Assertions.assertFalse(Half.POSITIVE_INFINITY.isFinite());
        Assertions.assertFalse(Half.NEGATIVE_INFINITY.isFinite());
        Assertions.assertFalse(Half.NaN.isFinite());
        Assertions.assertTrue(Half.MAX_VALUE.isFinite());
        Assertions.assertTrue(Half.MIN_NORMAL.isFinite());
        Assertions.assertTrue(Half.MIN_VALUE.isFinite());
        Assertions.assertTrue(Half.POSITIVE_ZERO.isFinite());
        Assertions.assertTrue(Half.NEGATIVE_ZERO.isFinite());

        Assertions.assertTrue(LOWEST_ABOVE_ONE.isFinite());
    }

    @Test
    public void toStringTest() {
        Assertions.assertEquals("Infinity", Half.POSITIVE_INFINITY.toString());
        Assertions.assertEquals("-Infinity", Half.NEGATIVE_INFINITY.toString());
        Assertions.assertEquals("NaN", Half.NaN.toString());
        Assertions.assertEquals("65504.0", Half.MAX_VALUE.toString());
        Assertions.assertEquals("6.1035156e-5", Half.MIN_NORMAL.toString().toLowerCase());
        Assertions.assertEquals("5.9604645e-8", Half.MIN_VALUE.toString().toLowerCase());
        Assertions.assertEquals("0.0", Half.POSITIVE_ZERO.toString());
        Assertions.assertEquals("-0.0", Half.NEGATIVE_ZERO.toString());

        Assertions.assertEquals("1.0009766", LOWEST_ABOVE_ONE.toString().toLowerCase());
    }

    @Test
    public void toHexStringTest() {
        Assertions.assertEquals("Infinity", Half.toHexString(Half.POSITIVE_INFINITY));
        Assertions.assertEquals("-Infinity", Half.toHexString(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals("NaN", Half.toHexString(Half.NaN));
        Assertions.assertEquals("0x1.ffcp15", Half.toHexString(Half.MAX_VALUE));
        Assertions.assertEquals("0x1.0p-14", Half.toHexString(Half.MIN_NORMAL).toLowerCase());
        Assertions.assertEquals("0x0.004p-14", Half.toHexString(Half.MIN_VALUE).toLowerCase());
        Assertions.assertEquals("0x0.0p0", Half.toHexString(Half.POSITIVE_ZERO));
        Assertions.assertEquals("-0x0.0p0", Half.toHexString(Half.NEGATIVE_ZERO));

        Assertions.assertEquals("0x1.004p0", Half.toHexString(LOWEST_ABOVE_ONE));

        Assertions.assertEquals("0x1.0p0", Half.toHexString(Half.valueOf(1.0f)));
        Assertions.assertEquals("-0x1.0p0", Half.toHexString(Half.valueOf(-1.0f)));
        Assertions.assertEquals("0x1.0p1", Half.toHexString(Half.valueOf(2.0f)));
        Assertions.assertEquals("0x1.8p1", Half.toHexString(Half.valueOf(3.0f)));
        Assertions.assertEquals("0x1.0p-1", Half.toHexString(Half.valueOf(0.5f)));
        Assertions.assertEquals("0x1.0p-2", Half.toHexString(Half.valueOf(0.25f)));
        Assertions.assertEquals("0x0.ffcp-14", Half.toHexString(Half.shortBitsToHalf((short) 0x3ff)));
    }

    @SuppressWarnings("EqualsWithItself")
    @Test
    public void equalsTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.POSITIVE_INFINITY);
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.NEGATIVE_INFINITY);
        Assertions.assertEquals(Half.NaN, Half.NaN);
        Assertions.assertEquals(Half.MAX_VALUE, Half.MAX_VALUE);
        Assertions.assertEquals(Half.MIN_NORMAL, Half.MIN_NORMAL);
        Assertions.assertEquals(Half.MIN_VALUE, Half.MIN_VALUE);
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.POSITIVE_ZERO);
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.NEGATIVE_ZERO);

        Assertions.assertEquals(LOWEST_ABOVE_ONE, LOWEST_ABOVE_ONE);

        Assertions.assertNotEquals(Half.POSITIVE_INFINITY, Half.NEGATIVE_INFINITY);
        Assertions.assertNotEquals(Half.NEGATIVE_INFINITY, Half.POSITIVE_INFINITY);
        Assertions.assertNotEquals(Half.NaN, Half.POSITIVE_INFINITY);
        Assertions.assertNotEquals(Half.MAX_VALUE, Half.NaN);
        Assertions.assertNotEquals(Half.MIN_NORMAL, Half.MIN_VALUE);
        Assertions.assertNotEquals(Half.MIN_VALUE, Half.POSITIVE_ZERO);
        Assertions.assertNotEquals(Half.POSITIVE_ZERO, Half.NEGATIVE_ZERO);
        Assertions.assertNotEquals(Half.NEGATIVE_ZERO, Half.POSITIVE_ZERO);

        Assertions.assertNotEquals(LOWEST_ABOVE_ONE, Half.MIN_NORMAL);

        Assertions.assertNotEquals(null, LOWEST_ABOVE_ONE);

        // Additional NaN tests
        Assertions.assertEquals(Half.NaN, Half.NaN);
        Assertions.assertEquals(Half.NaN, Half.shortBitsToHalf((short)0x7e04));
        Assertions.assertEquals(Half.NaN, Half.shortBitsToHalf((short)0x7fff));
    }

    @Test
    public void hashCodeTest() {
        Assertions.assertEquals(POSITIVE_INFINITY_SHORT_VALUE, Half.POSITIVE_INFINITY.hashCode());
        Assertions.assertEquals(NEGATIVE_INFINITY_SHORT_VALUE, Half.NEGATIVE_INFINITY.hashCode());
        Assertions.assertEquals(NaN_SHORT_VALUE, Half.NaN.hashCode());
        Assertions.assertEquals(MAX_VALUE_SHORT_VALUE, Half.MAX_VALUE.hashCode());
        Assertions.assertEquals(MIN_NORMAL_SHORT_VALUE, Half.MIN_NORMAL.hashCode());
        Assertions.assertEquals(MIN_VALUE_SHORT_VALUE, Half.MIN_VALUE.hashCode());
        Assertions.assertEquals(POSITIVE_ZERO_SHORT_VALUE, Half.POSITIVE_ZERO.hashCode());
        Assertions.assertEquals(NEGATIVE_ZERO_SHORT_VALUE, Half.NEGATIVE_ZERO.hashCode());

        Assertions.assertEquals(LOWEST_ABOVE_ONE_SHORT_VALUE, LOWEST_ABOVE_ONE.hashCode());
    }

    @SuppressWarnings("EqualsWithItself")
    @Test
    public void compareToTest() {
        // Left
        Assertions.assertEquals(1, Half.POSITIVE_INFINITY.compareTo(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(1, Half.MAX_VALUE.compareTo(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(1, Half.NaN.compareTo(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(1, Half.MAX_VALUE.compareTo(Half.MIN_NORMAL));
        Assertions.assertEquals(1, Half.MIN_NORMAL.compareTo(Half.MIN_VALUE));
        Assertions.assertEquals(1, Half.MIN_VALUE.compareTo(Half.POSITIVE_ZERO));
        Assertions.assertEquals(1, Half.POSITIVE_ZERO.compareTo(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(1, LOWEST_ABOVE_ONE.compareTo(Half.NEGATIVE_ZERO));

        // Right
        Assertions.assertEquals(-1, Half.NEGATIVE_INFINITY.compareTo(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(-1, Half.NEGATIVE_INFINITY.compareTo(Half.MAX_VALUE));
        Assertions.assertEquals(-1, Half.NEGATIVE_INFINITY.compareTo(Half.NaN));
        Assertions.assertEquals(-1, Half.MIN_NORMAL.compareTo(Half.MAX_VALUE));
        Assertions.assertEquals(-1, Half.MIN_VALUE.compareTo(Half.MIN_NORMAL));
        Assertions.assertEquals(-1, Half.POSITIVE_ZERO.compareTo(Half.MIN_VALUE));
        Assertions.assertEquals(-1, Half.NEGATIVE_ZERO.compareTo(Half.POSITIVE_ZERO));
        Assertions.assertEquals(-1, Half.NEGATIVE_ZERO.compareTo(LOWEST_ABOVE_ONE));

        // Equals
        Assertions.assertEquals(0, Half.POSITIVE_INFINITY.compareTo(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(0, Half.NEGATIVE_INFINITY.compareTo(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(0, Half.NaN.compareTo(Half.NaN));
        Assertions.assertEquals(0, Half.MAX_VALUE.compareTo(Half.MAX_VALUE));
        Assertions.assertEquals(0, Half.MIN_NORMAL.compareTo(Half.MIN_NORMAL));
        Assertions.assertEquals(0, Half.MIN_VALUE.compareTo(Half.MIN_VALUE));
        Assertions.assertEquals(0, Half.POSITIVE_ZERO.compareTo(Half.POSITIVE_ZERO));
        Assertions.assertEquals(0, Half.NEGATIVE_ZERO.compareTo(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(0, LOWEST_ABOVE_ONE.compareTo(LOWEST_ABOVE_ONE));
    }

    @Test
    public void sumTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.sum(Half.POSITIVE_INFINITY, Half.MAX_VALUE));
        Assertions.assertEquals(Half.NaN, Half.sum(Half.POSITIVE_INFINITY, Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.NaN, Half.sum(Half.NaN, Half.NaN));
        Assertions.assertEquals(Half.NaN, Half.sum(Half.NaN, Half.MAX_VALUE));
        Assertions.assertEquals(Half.sum(Half.MIN_NORMAL, Half.MIN_VALUE), Half.valueOf(6.109476E-5f));
        Assertions.assertEquals(Half.MIN_VALUE, Half.sum(Half.MIN_VALUE, Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.sum(Half.MAX_VALUE, LOWEST_ABOVE_ONE));
        Assertions.assertEquals(
                Half.NEGATIVE_INFINITY,
                Half.sum(Half.valueOf(-Half.MAX_VALUE.floatValue()), Half.valueOf(-LOWEST_ABOVE_ONE.floatValue())));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.sum(Half.POSITIVE_ZERO, Half.NEGATIVE_ZERO));

        Assertions.assertEquals(Half.NaN, Half.sum(Half.NaN, LOWEST_ABOVE_ONE));
    }

    @Test
    public void maxTest() {
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.max(Half.POSITIVE_INFINITY, Half.MAX_VALUE));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, Half.max(Half.POSITIVE_INFINITY, Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.NaN, Half.max(Half.NaN, Half.NaN));
        Assertions.assertEquals(Half.NaN, Half.max(Half.NaN, Half.MAX_VALUE));
        Assertions.assertEquals(Half.MIN_NORMAL, Half.max(Half.MIN_NORMAL, Half.MIN_VALUE));
        Assertions.assertEquals(Half.MIN_VALUE, Half.max(Half.MIN_VALUE, Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.MAX_VALUE, Half.max(Half.MAX_VALUE, LOWEST_ABOVE_ONE));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.max(Half.POSITIVE_ZERO, Half.NEGATIVE_ZERO));

        Assertions.assertEquals(Half.NaN, Half.max(Half.NaN, LOWEST_ABOVE_ONE));
    }

    @Test
    public void minTest() {
        Assertions.assertEquals(Half.MAX_VALUE, Half.min(Half.POSITIVE_INFINITY, Half.MAX_VALUE));
        Assertions.assertEquals(Half.NEGATIVE_INFINITY, Half.min(Half.POSITIVE_INFINITY, Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.NaN, Half.min(Half.NaN, Half.NaN));
        Assertions.assertEquals(Half.NaN, Half.min(Half.NaN, Half.MAX_VALUE));
        Assertions.assertEquals(Half.MIN_VALUE, Half.min(Half.MIN_NORMAL, Half.MIN_VALUE));
        Assertions.assertEquals(Half.POSITIVE_ZERO, Half.min(Half.MIN_VALUE, Half.POSITIVE_ZERO));
        Assertions.assertEquals(LOWEST_ABOVE_ONE, Half.min(Half.MAX_VALUE, LOWEST_ABOVE_ONE));
        Assertions.assertEquals(Half.NEGATIVE_ZERO, Half.min(Half.POSITIVE_ZERO, Half.NEGATIVE_ZERO));

        Assertions.assertEquals(Half.NaN, Half.min(Half.NaN, LOWEST_ABOVE_ONE));
    }
}
