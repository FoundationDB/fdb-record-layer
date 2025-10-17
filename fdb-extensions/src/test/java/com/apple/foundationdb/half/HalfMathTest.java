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
 * Unit test for {@link HalfMath}.
 *
 * @author Christian Heina (developer@christianheina.com)
 */
public class HalfMathTest {
    private static final Half LARGEST_SUBNORMAL = Half.shortBitsToHalf((short) 0x3ff);

    @Test
    public void ulpTest() {
        // Special cases
        Assertions.assertEquals(Half.NaN, HalfMath.ulp(Half.NaN));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, HalfMath.ulp(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, HalfMath.ulp(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(Half.POSITIVE_ZERO));
        Assertions.assertEquals(HalfMath.ulp(Half.MAX_VALUE), Half.valueOf(Math.pow(2, 5)));
        Assertions.assertEquals(HalfMath.ulp(Half.NEGATIVE_MAX_VALUE), Half.valueOf(Math.pow(2, 5)));

        // Regular cases
        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(Half.MIN_NORMAL));
        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(LARGEST_SUBNORMAL));

        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(Half.shortBitsToHalf((short) 0x7ff)));
        Assertions.assertEquals(Half.MIN_VALUE, HalfMath.ulp(Half.shortBitsToHalf((short) 0x7ff)));
    }

    @Test
    public void getExponentTest() {
        // Special cases
        Assertions.assertEquals(Half.MAX_EXPONENT + 1, HalfMath.getExponent(Half.NaN));
        Assertions.assertEquals(Half.MAX_EXPONENT + 1, HalfMath.getExponent(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(Half.MAX_EXPONENT + 1, HalfMath.getExponent(Half.NEGATIVE_INFINITY));
        Assertions.assertEquals(Half.MIN_EXPONENT - 1, HalfMath.getExponent(Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.MIN_EXPONENT - 1, HalfMath.getExponent(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(Half.MIN_EXPONENT - 1, HalfMath.getExponent(Half.MIN_VALUE));
        Assertions.assertEquals(Half.MIN_EXPONENT - 1, HalfMath.getExponent(LARGEST_SUBNORMAL));

        // Regular cases
        Assertions.assertEquals(-13, HalfMath.getExponent(Half.valueOf(0.0002f)));
        Assertions.assertEquals(-9, HalfMath.getExponent(Half.valueOf(0.002f)));
        Assertions.assertEquals(-6, HalfMath.getExponent(Half.valueOf(0.02f)));
        Assertions.assertEquals(-3, HalfMath.getExponent(Half.valueOf(0.2f)));
        Assertions.assertEquals(1, HalfMath.getExponent(Half.valueOf(2.0f)));
        Assertions.assertEquals(4, HalfMath.getExponent(Half.valueOf(20.0f)));
        Assertions.assertEquals(7, HalfMath.getExponent(Half.valueOf(200.0f)));
        Assertions.assertEquals(10, HalfMath.getExponent(Half.valueOf(2000.0f)));
        Assertions.assertEquals(14, HalfMath.getExponent(Half.valueOf(20000.0f)));
    }

    @Test
    public void absTest() {
        // Special cases
        Assertions.assertEquals(Half.POSITIVE_INFINITY, HalfMath.abs(Half.POSITIVE_INFINITY));
        Assertions.assertEquals(Half.POSITIVE_INFINITY, HalfMath.abs(Half.NEGATIVE_INFINITY));

        Assertions.assertEquals(Half.NaN, HalfMath.abs(Half.NaN));
        Assertions.assertEquals(HalfMath.abs(Half.shortBitsToHalf((short) 0x7e04)), Half.shortBitsToHalf((short) 0x7e04));
        Assertions.assertEquals(HalfMath.abs(Half.shortBitsToHalf((short) 0x7fff)), Half.shortBitsToHalf((short) 0x7fff));

        // Regular cases
        Assertions.assertEquals(Half.POSITIVE_ZERO, HalfMath.abs(Half.POSITIVE_ZERO));
        Assertions.assertEquals(Half.POSITIVE_ZERO, HalfMath.abs(Half.NEGATIVE_ZERO));
        Assertions.assertEquals(Half.MAX_VALUE, HalfMath.abs(Half.MAX_VALUE));
        Assertions.assertEquals(Half.MAX_VALUE, HalfMath.abs(Half.NEGATIVE_MAX_VALUE));
    }
}
