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

/**
 * The class {@code HalfMath} contains methods for performing basic numeric operations on or using {@link Half} objects.
 *
 * @author Christian Heina (developer@christianheina.com)
 */
public class HalfMath {
    private HalfMath() {
        /* Hidden Constructor */
    }

    /**
     * Returns the size of an ulp of the argument. An ulp, unit in the last place, of a {@code half} value is the
     * positive distance between this floating-point value and the {@code half} value next larger in magnitude.<br>
     * Note that for non-NaN <i>x</i>, <code>ulp(-<i>x</i>) == ulp(<i>x</i>)</code>.
     *
     * <p>
     * Special Cases:
     * <ul>
     * <li>If the argument is NaN, then the result is NaN.
     * <li>If the argument is positive or negative infinity, then the result is positive infinity.
     * <li>If the argument is positive or negative zero, then the result is {@code Float.MIN_VALUE}.
     * <li>If the argument is &plusmn;{@code Half.MAX_VALUE}, then the result is equal to 2<sup>5</sup>.
     * </ul>
     *
     * @param half
     *            the floating-point value whose ulp is to be returned
     *
     * @return the size of an ulp of the argument
     */
    public static Half ulp(Half half) {
        int exp = getExponent(half);

        switch (exp) {
            case Half.MAX_EXPONENT + 1: // NaN or infinity values
                return abs(half);
            case Half.MIN_EXPONENT - 1: // zero or subnormal values
                return Half.MIN_VALUE;
            default: // Normal values
                exp = exp - (HalfConstants.SIGNIFICAND_WIDTH - 1);
                if (exp >= Half.MIN_EXPONENT) {
                    // Normal result
                    return powerOfTwoH(exp);
                } else {
                    // Subnormal result
                    return Half.shortBitsToHalf(
                            (short) (1 << (exp - (Half.MIN_EXPONENT - (HalfConstants.SIGNIFICAND_WIDTH - 1)))));
                }
        }
    }

    /**
     * Returns the unbiased exponent used in the representation of a {@code half}.
     *
     * <p>
     * Special cases:
     *
     * <ul>
     * <li>If the argument is NaN or infinite, then the result is {@link Half#MAX_EXPONENT} + 1.
     * <li>If the argument is zero or subnormal, then the result is {@link Half#MIN_EXPONENT} - 1.
     * </ul>
     *
     * @param half
     *            a {@code half} value
     *
     * @return the unbiased exponent of the argument
     */
    public static int getExponent(Half half) {
        return ((Half.halfToRawShortBits(half) & HalfConstants.EXP_BIT_MASK) >> (HalfConstants.SIGNIFICAND_WIDTH - 1))
                - HalfConstants.EXP_BIAS;
    }

    /**
     * Returns the absolute {@link Half} object of a {@code half} instance.
     *
     * <p>
     * Special cases:
     * <ul>
     * <li>If the argument is positive zero or negative zero, the result is positive zero.
     * <li>If the argument is infinite, the result is positive infinity.
     * <li>If the argument is NaN, the result is a "canonical" NaN (preserving Not-a-Number (NaN) signaling).
     * </ul>
     *
     * @param half
     *            the argument whose absolute value is to be determined
     *
     * @return the absolute value of the argument.
     */
    public static Half abs(Half half) {
        if (half.isNaN()) {
            return Half.valueOf(half);
        }
        return Half.shortBitsToHalf((short) (Half.halfToRawShortBits(half) & 0x7fff));
    }

    /**
     * Returns a floating-point power of two in the normal range.
     */
    private static Half powerOfTwoH(int n) {
        return Half.shortBitsToHalf(
                (short) (((n + HalfConstants.EXP_BIAS) << (HalfConstants.SIGNIFICAND_WIDTH - 1)) & HalfConstants.EXP_BIT_MASK));
    }
}
