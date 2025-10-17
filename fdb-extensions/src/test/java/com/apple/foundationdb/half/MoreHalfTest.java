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

import com.apple.test.RandomizedTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.within;

public class MoreHalfTest {
    private static final double HALF_MIN_NORMAL = Math.scalb(1.0, -14);
    private static final double REL_BOUND = Math.scalb(1.0, -10);      // 2^-10

    @Nonnull
    private static Stream<Long> randomSeeds() {
        return RandomizedTestUtils.randomSeeds(12345, 987654, 423, 18378195);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void roundTripTest(final long seed) {
        final Random rnd = new Random(seed);
        for (int i = 0; i < 10_000; i ++) {
            // uniform in [0, 1) or [-2 * HALF_MAX, 2 * HALF_MAX)
            double x = (i % 2 == 1)
                       ? rnd.nextDouble()
                       : ((rnd.nextDouble() * 2 - 1) * 2 * Half.MAX_VALUE.doubleValue());
            double y = Half.valueOf(x).doubleValue();

            if (Math.abs(x) > Half.MAX_VALUE.doubleValue()) {
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

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void conversionRoundingTestMaxValue() {
        final float smallestFloatGreaterThanHalfMax = Math.nextUp(Half.MAX_VALUE.floatValue());
        Assertions.assertThat(Half.valueOf(smallestFloatGreaterThanHalfMax)).matches(h -> h.isInfinite());
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void conversionRoundingTestMinValue() {
        //
        // This conversion in Half implements round-to-nearest, ties-to-even for the half subnormal case by adding
        // 0x007FF000 (which is 0x00800000 - 0x00001000), then shifting and final rounding. A consequence of this
        // particular rounding scheme is:
        // At the boundary we’re probing (mid = 2^-25, float exponent = 102), the expression
        // ((0x007FF000 + significand) >> 23) stays 0 until significand >= 0x00001000 (4096 float-ULPs at that
        // exponent), which means Math.nextUp(2^-25f) (only 1 ULP above the midpoint) still rounds to 0 in this
        // implementation.
        //
        float midF = Math.scalb(1.0f, -25);               // 2^-25 exact in float
        int bits   = Float.floatToRawIntBits(midF);
        bits      += 0x00001000;                          // +4096 ULPs at this exponent
        float aboveF = Float.intBitsToFloat(bits);

        Half h0 = Half.valueOf(Math.nextDown(midF));      // -> 0.0
        Half h1 = Half.valueOf(aboveF);                   // -> 2^-24

        Assertions.assertThat(h0.doubleValue()).isEqualTo(0.0);
        Assertions.assertThat(h1.doubleValue()).isEqualTo(Math.scalb(1.0, -24));
    }
}
