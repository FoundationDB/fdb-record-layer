/*
 * MoreHalfTest.java
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

package com.apple.foundationdb.half;

import com.apple.test.RandomSeedSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Random;

import static org.assertj.core.api.Assertions.within;

public class MoreHalfTest {
    private static final double HALF_MIN_NORMAL = Math.scalb(1.0, -14);
    private static final double REL_BOUND = Math.scalb(1.0, -10);      // 2^-10
    /** MAX_VALUE + 1/2 ulp; anything below this rounds to MAX_VALUE rather than overflowing to infinity */
    private static final float HALF_OVERFLOW_THRESHOLD = 65520.0f;

    @ParameterizedTest
    @RandomSeedSource({12345, 987654, 423, 18378195, -373519288781890703L})
    void roundTripTest(final long seed) {
        final Random rnd = new Random(seed);
        for (int i = 0; i < 10_000; i ++) {
            // uniform in [0, 1) or [-2 * HALF_MAX, 2 * HALF_MAX)
            double x = (i % 2 == 1)
                       ? rnd.nextDouble()
                       : ((rnd.nextDouble() * 2 - 1) * 2 * Half.MAX_VALUE.doubleValue());
            double y = Half.valueOf(x).doubleValue();

            Assertions.assertThat(sameSign(x, y)).isTrue();

            // Narrow to float before comparing; valueOf(double) narrows first, and the narrowing can round a double
            // just below the threshold (e.g. 65519.999) up onto it, overflowing to infinity, this mirrors the
            // conversion path in Half.valueOf(double) giving the same rounding/overflow behavior.
            if (Math.abs((float) x) >= HALF_OVERFLOW_THRESHOLD) {
                if (x > 0) {
                    Assertions.assertThat(y).isEqualTo(Double.POSITIVE_INFINITY);
                } else {
                    Assertions.assertThat(y).isEqualTo(Double.NEGATIVE_INFINITY);
                }
            } else if (Math.abs(x) >= HALF_MIN_NORMAL) {
                Assertions.assertThat((y - x) / x).isCloseTo(0.0d, within(REL_BOUND));
            } else {
                Assertions.assertThat(y - x).isCloseTo(0.0d, within(Math.scalb(1.0d, -(Math.getExponent(x) + 23))));
            }

            double z = Half.valueOf(y).doubleValue();
            Assertions.assertThat(z).isEqualTo(y);
        }
    }

    /**
     * Method to return if two doubles have the same sign including {@code Inf, -Inf, NaN, +-0}, etc.
     * @param a a double
     * @param b another double
     * @return {@code true} iff {@code a} and {@code b} have the same sign
     */
    private static boolean sameSign(final double a, final double b) {
        // extract raw bit representations
        long bitsA = Double.doubleToRawLongBits(a);
        long bitsB = Double.doubleToRawLongBits(b);
        // the sign bit is the most significant bit (bit 63)
        return ((bitsA ^ bitsB) & 0x8000_0000_0000_0000L) == 0;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void conversionRoundingTestMaxValue() {
        // The overflow threshold is MAX_VALUE + 1/2 ulp (65520), not MAX_VALUE (65504), so every float strictly
        // between the two rounds back down to MAX_VALUE.
        final float smallestFloatGreaterThanHalfMax = Math.nextUp(Half.MAX_VALUE.floatValue());
        Assertions.assertThat(Half.valueOf(smallestFloatGreaterThanHalfMax)).isEqualTo(Half.MAX_VALUE);
        Assertions.assertThat(Half.valueOf(Math.nextDown(HALF_OVERFLOW_THRESHOLD))).isEqualTo(Half.MAX_VALUE);
        Assertions.assertThat(Half.valueOf(HALF_OVERFLOW_THRESHOLD)).matches(h -> h.isInfinite());
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void conversionRoundingTestMinValue() {
        //
        // The conversion rounds to nearest, ties to even. The boundary we're probing is mid = 2^-25, exactly
        // half-way between 0 and MIN_VALUE (2^-24): the tie rounds to even, i.e. to zero, and anything strictly
        // above it -- even by a single float ULP -- rounds up to MIN_VALUE.
        //
        final float midF = Math.scalb(1.0f, -25);                      // 2^-25 exact in float

        Assertions.assertThat(Half.valueOf(Math.nextDown(midF)).doubleValue()).isEqualTo(0.0);
        Assertions.assertThat(Half.valueOf(midF).doubleValue()).isEqualTo(0.0);
        Assertions.assertThat(Half.valueOf(Math.nextUp(midF)).doubleValue()).isEqualTo(Math.scalb(1.0, -24));
        Assertions.assertThat(Half.valueOf(Math.scalb(1.0f, -24)).doubleValue()).isEqualTo(Math.scalb(1.0, -24));
        // 1.5 * 2^-24 is a tie between MIN_VALUE and 2 * MIN_VALUE; ties to even picks the latter.
        Assertions.assertThat(Half.valueOf(1.5f * Math.scalb(1.0f, -24)).doubleValue())
                .isEqualTo(Math.scalb(2.0, -24));
    }
}
