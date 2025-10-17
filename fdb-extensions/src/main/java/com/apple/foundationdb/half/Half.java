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
 * The {@code Half} class implements half precision (FP16) float-point number according to IEEE 754 standard.
 * <p>
 * In addition, this class provides several methods for converting a {@code Half} to a {@code String} and a
 * {@code String} to a {@code Half}, as well as other constants and methods useful when dealing with a {@code Half}.
 * <p>
 * {@code Half} is implemented to provide, as much as possible, the same interface as {@link Float} and {@link Double}.
 *
 * @author Christian Heina (developer@christianheina.com)
 */
public class Half extends Number implements Comparable<Half> {

    /**
     * A constant holding the positive infinity of type {@code Half}.
     *
     * <p>
     * It is equal to the value returned by {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x7c00)}.
     */
    public static final Half POSITIVE_INFINITY = shortBitsToHalf((short) 0x7c00);

    /**
     * A constant holding the negative infinity of type {@code Half}.
     *
     * <p>
     * It is equal to the value returned by {@link #shortBitsToHalf(short) shortBitsToHalf((short)0xfc00)}.
     */
    public static final Half NEGATIVE_INFINITY = shortBitsToHalf((short) 0xfc00);

    /**
     * A constant holding a Not-a-Number (NaN) value of type {@code Half}.
     *
     * <p>
     * It is equivalent to the value returned by {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x7e00)}.
     */
    public static final Half NaN = shortBitsToHalf((short) 0x7e00);

    /**
     * A constant holding the largest positive finite value of type {@code Half},
     * (2-2<sup>-10</sup>)&middot;2<sup>15</sup>.
     *
     * <p>
     * It is equal to {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x7bff)}.
     */
    public static final Half MAX_VALUE = shortBitsToHalf((short) 0x7bff);

    /**
     * A constant holding the largest positive finite value of type {@code Half},
     * (2-2<sup>-10</sup>)&middot;2<sup>15</sup>.
     *
     * <p>
     * It is equal to {@link #shortBitsToHalf(short) shortBitsToHalf((short)0xfbff)}.
     */
    public static final Half NEGATIVE_MAX_VALUE = shortBitsToHalf((short) 0xfbff);

    /**
     * A constant holding the smallest positive normal value of type {@code Half}, 2<sup>-14</sup>.
     *
     * <p>
     * It is equal to {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x0400)}.
     */
    public static final Half MIN_NORMAL = shortBitsToHalf((short) 0x0400); // 6.103515625E-5

    /**
     * A constant holding the smallest positive nonzero value of type {@code Half}, 2<sup>-24</sup>.
     *
     * <p>
     * It is equal to {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x1)}.
     */
    public static final Half MIN_VALUE = shortBitsToHalf((short) 0x1); // 5.9604645E-8

    /**
     * Maximum exponent a finite {@code Half} variable may have.
     *
     * <p>
     * It is equal to the value returned by {@code HalfMath.getExponent(Half.MAX_VALUE)}.
     */
    public static final int MAX_EXPONENT = 15;

    /**
     * Minimum exponent a normalized {@code Half} variable may have.
     *
     * <p>
     * It is equal to the value returned by {@code HalfMath.getExponent(Half.MIN_NORMAL)}.
     */
    public static final int MIN_EXPONENT = -14;

    /**
     * The number of bits used to represent a {@code Half} value.
     */
    public static final int SIZE = 16;

    /**
     * The number of bytes used to represent a {@code Half} value.
     */
    public static final int BYTES = SIZE / Byte.SIZE;

    /**
     * A constant holding the positive zero of type {@code Half}.
     *
     * <p>
     * It is equal to the value returned by {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x0)}.
     */
    public static final Half POSITIVE_ZERO = shortBitsToHalf((short) 0x0);

    /**
     * A constant holding the negative zero of type {@code Half}.
     *
     * <p>
     * It is equal to the value returned by {@link #shortBitsToHalf(short) shortBitsToHalf((short)0x8000)}.
     */
    public static final Half NEGATIVE_ZERO = shortBitsToHalf((short) 0x8000);

    private static final long serialVersionUID = 1682650405628820816L;

    /**
     * The value of the half precision floating-point as a float.
     *
     * @serial
     */
    private final float floatRepresentation;

    private Half(float floatRepresentation) {
        /* Hidden Constructor */
        super();
        this.floatRepresentation = floatRepresentation;
    }

    /**
     * Returns the {@code Half} object corresponding to a given bit representation. The argument is considered to be a
     * representation of a floating-point value according to the IEEE 754 floating-point "half format" bit layout.
     *
     * <p>
     * If the argument is {@code 0x7c00}, the result is positive infinity.
     *
     * <p>
     * If the argument is {@code 0xfc00}, the result is negative infinity.
     *
     * <p>
     * If the argument is any value in the range {0x7c01} through {@code 0x7fff} or in the range {@code 0xfc01} through
     * {@code 0xffff}, the result is a NaN. No IEEE 754 floating-point operation provided by Java can distinguish
     * between two NaN values of the same type with different bit patterns. Distinct values of NaN are only
     * distinguishable by use of the {@code Half.halfToRawShortBits} method.
     *
     * <p>
     * In all other cases, let <i>s</i>, <i>e</i>, and <i>m</i> be three values that can be computed from the argument:
     *
     * <blockquote>
     *
     * <pre>
     * {
     *     &#64;code
     *     int s = ((bits &gt;&gt; 16) == 0) ? 1 : -1;
     *     int e = ((bits &gt;&gt; 10) &amp; 0x1f);
     *     int m = (e == 0) ? (bits &amp; 0x3ff) &lt;&lt; 1 : (bits &amp; 0x3ff) | 0x200;
     * }
     * </pre>
     *
     * </blockquote>
     *
     * Then the float-point result equals the value of the mathematical expression
     * <i>s</i>&middot;<i>m</i>&middot;2<sup><i>e</i>-25</sup>.
     *
     * <p>
     * Note that this method may not be able to return a {@code Half} NaN with exactly same bit pattern as the
     * {@code short} argument. IEEE 754 distinguishes between two kinds of NaNs, quiet NaNs and <i>signaling NaNs</i>.
     * The differences between the two kinds of NaN are generally not visible in Java. Arithmetic operations on
     * signaling NaNs turn them into quiet NaNs with a different, but often similar, bit pattern. However, on some
     * processors merely copying a signaling NaN also performs that conversion. In particular, copying a signaling NaN
     * to return it to the calling method may perform this conversion. So {@code shortBitsToHalf} may not be able to
     * return a {@code Half} with a signaling NaN bit pattern. Consequently, for some {@code short} values,
     * {@code halfToRawShortBits(shortBitsToHalf(start))} may <i>not</i> equal {@code start}. Moreover, which particular
     * bit patterns represent signaling NaNs is platform dependent; although all NaN bit patterns, quiet or signaling,
     * must be in the NaN range identified above.
     *
     * @param shortBits
     *            a short.
     *
     * @return the {@code Half} float-point object with the same bit pattern.
     */
    public static Half shortBitsToHalf(short shortBits) {
        return new Half(halfShortToFloat(shortBits));
    }

    public static float halfShortToFloat(short shortBits) {
        int intBits = (int) shortBits;
        int exponent = (intBits & HalfConstants.EXP_BIT_MASK) >> 10;
        int significand = (intBits & HalfConstants.SIGNIF_BIT_MASK) << 13;

        // Check infinities and NaN
        if (exponent == 31) {
            // sign | positive infinity integer value | significand
            return Float.intBitsToFloat((intBits & HalfConstants.SIGN_BIT_MASK) << 16 | 0x7f800000 | significand);
        }

        int v = Float.floatToIntBits((float) significand) >> 23;
        // sign | normal | subnormal
        return Float.intBitsToFloat(
                (intBits & 0x8000) << 16 | (exponent != 0 ? 1 : 0) * ((exponent + 112) << 23 | significand)
                        | ((exponent == 0 ? 1 : 0) & (significand != 0 ? 1 : 0))
                        * ((v - 37) << 23 | ((significand << (150 - v)) & 0x007FE000)));
    }

    /**
     * Returns a representation of the specified floating-point value according to the IEEE 754 floating-point "single
     * format" bit layout.
     *
     * <p>
     * Bit 15 (the bit that is selected by the mask {@code 0x8000}) represents the sign of the floating-point number.
     * Bits 14-10 (the bits that are selected by the mask {@code 0x7c00}) represent the exponent. Bits 9-0 (the bits
     * that are selected by the mask {@code 0x03ff}) represent the significand (sometimes called the mantissa) of the
     * floating-point number.
     *
     * <p>
     * If the argument is positive infinity, the result is {@code 0x7c00}.
     *
     * <p>
     * If the argument is negative infinity, the result is {@code 0xfc00}.
     *
     * <p>
     * If the argument is NaN, the result is {@code 0x7e00}.
     *
     * <p>
     * In all cases, the result is a short that, when given to the {@link #shortBitsToHalf(short)} method, will produce
     * a floating-point value the same as the argument to {@code halfToShortBits} (except all NaN values are collapsed
     * to a single "canonical" NaN value).
     *
     * @param half
     *            a Half object.
     *
     * @return the bits that represent the floating-point number.
     */
    public static short halfToShortBits(Half half) {
        if (!isNaN(half)) {
            return halfToRawShortBits(half);
        }
        return 0x7e00;
    }

    /**
     * Returns a representation of the specified floating-point value according to the IEEE 754 floating-point "single
     * format" bit layout.
     *
     * <p>
     * Bit 15 (the bit that is selected by the mask {@code 0x8000}) represents the sign of the floating-point number.
     * Bits 14-10 (the bits that are selected by the mask {@code 0x7c00}) represent the exponent. Bits 9-0 (the bits
     * that are selected by the mask {@code 0x03ff}) represent the significand (sometimes called the mantissa) of the
     * floating-point number.
     *
     * <p>
     * If the argument is positive infinity, the result is {@code 0x7c00}.
     *
     * <p>
     * If the argument is negative infinity, the result is {@code 0xfc00}.
     *
     * <p>
     * If the argument is NaN, the result is {@code 0x7e00}.
     *
     * <p>
     * In all cases, the result is a short that, when given to the {@link #shortBitsToHalf(short)} method, will produce
     * a floating-point value the same as the argument to {@code halfToShortBits} (except all NaN values are collapsed
     * to a single "canonical" NaN value).
     *
     * @param floatRepresentation
     *            a float representation as used within a {@code Half} object.
     *
     * @return the bits that represent the floating-point number.
     */
    public static short floatRepresentationToShortBits(final float floatRepresentation) {
        if (!Float.isNaN(floatRepresentation)) {
            return floatToHalfShortBits(floatRepresentation);
        }
        return 0x7e00;
    }

    /**
     * Returns a representation of the specified floating-point value according to the IEEE 754 floating-point "single
     * format" bit layout, preserving Not-a-Number (NaN) values.
     *
     * <p>
     * Bit 15 (the bit that is selected by the mask {@code 0x8000}) represents the sign of the floating-point number.
     * Bits 14-10 (the bits that are selected by the mask {@code 0x7c00}) represent the exponent. Bits 9-0 (the bits
     * that are selected by the mask {@code 0x03ff}) represent the significand (sometimes called the mantissa) of the
     * floating-point number.
     *
     * <p>
     * If the argument is positive infinity, the result is {@code 0x7c00}.
     *
     * <p>
     * If the argument is negative infinity, the result is {@code 0xfc00}.
     *
     * <p>
     * If the argument is NaN, the result is the integer representing the actual NaN value. Unlike the
     * {@code halfToShortBits} method, {@code halfToRawShortBits} does not collapse all the bit patterns encoding a NaN
     * to a single "canonical" NaN value.
     *
     * <p>
     * In all cases, the result is a short that, when given to the {@link #shortBitsToHalf(short)} method, will produce
     * a floating-point value the same as the argument to {@code halfToRawShortBits}.
     *
     * @param half
     *            a Half object.
     *
     * @return the bits that represent the half-point number.
     */
    public static short halfToRawShortBits(Half half) {
        return floatToHalfShortBits(half.floatRepresentation);
    }

    public static short floatToHalfShortBits(float floatValue) {
        int intBits = Float.floatToRawIntBits(floatValue);
        int exponent = (intBits & 0x7F800000) >> 23;
        int significand = intBits & 0x007FFFFF;

        // Check infinities and NaNs
        if (exponent > 142) {
            // sign | positive infinity short value
            return (short) ((intBits & 0x80000000) >> 16 | 0x7c00 | significand >> 13);
        }

        // sign | normal | subnormal
        return (short) ((intBits & 0x80000000) >> 16
                                | (exponent > 112 ? 1 : 0) * ((((exponent - 112) << 10) & 0x7C00) | significand >> 13)
                                | ((exponent < 113 ? 1 : 0) & (exponent > 101 ? 1 : 0))
                * ((((0x007FF000 + significand) >> (125 - exponent)) + 1) >> 1));
    }

    /**
     * Returns the value of the specified number as a {@code short}.
     *
     * @return the numeric value represented by this object after conversion to type {@code short}.
     */
    @Override
    public short shortValue() {
        if (isInfinite() || floatValue() > Short.MAX_VALUE || floatValue() < Short.MIN_VALUE) {
            return ((Float.floatToIntBits(floatValue()) & 0x80000000) >> 31) == 0 ? Short.MAX_VALUE : Short.MIN_VALUE;
        }
        return (short) floatValue();
    }

    @Override
    public int intValue() {
        return (int) floatValue();
    }

    @Override
    public long longValue() {
        return (long) floatValue();
    }

    @Override
    public float floatValue() {
        return floatRepresentation;
    }

    @Override
    public double doubleValue() {
        return floatValue();
    }

    /**
     * Returns the value of the specified number as a {@code byte}.
     *
     * @return the numeric value represented by this object after conversion to type {@code byte}.
     */
    @Override
    public byte byteValue() {
        return (byte) shortValue();
    }

    /**
     * Returns a {@code Half} object represented by the argument string {@code s}.
     *
     * <p>
     * If {@code s} is {@code null}, then a {@code NullPointerException} is thrown.
     *
     * <p>
     * Leading and trailing whitespace characters in {@code s} are ignored. Whitespace is removed as if by the
     * {@link String#trim} method; that is, both ASCII space and control characters are removed. The rest of {@code s}
     * should constitute a <i>FloatValue</i> as described by the lexical syntax rules:
     *
     * <blockquote>
     * <dl>
     * <dt><i>FloatValue:</i>
     * <dd><i>Sign<sub>opt</sub></i> {@code NaN}
     * <dd><i>Sign<sub>opt</sub></i> {@code Infinity}
     * <dd><i>Sign<sub>opt</sub> FloatingPointLiteral</i>
     * <dd><i>Sign<sub>opt</sub> HexFloatingPointLiteral</i>
     * <dd><i>SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexFloatingPointLiteral</i>:
     * <dd><i>HexSignificand BinaryExponent FloatTypeSuffix<sub>opt</sub></i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexSignificand:</i>
     * <dd><i>HexNumeral</i>
     * <dd><i>HexNumeral</i> {@code .}
     * <dd>{@code 0x} <i>HexDigits<sub>opt</sub> </i>{@code .}<i> HexDigits</i>
     * <dd>{@code 0X}<i> HexDigits<sub>opt</sub> </i>{@code .} <i>HexDigits</i>
     * </dl>
     *
     * <dl>
     * <dt><i>BinaryExponent:</i>
     * <dd><i>BinaryExponentIndicator SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>BinaryExponentIndicator:</i>
     * <dd>{@code p}
     * <dd>{@code P}
     * </dl>
     *
     * </blockquote>
     *
     * where <i>Sign</i>, <i>FloatingPointLiteral</i>, <i>HexNumeral</i>, <i>HexDigits</i>, <i>SignedInteger</i> and
     * <i>FloatTypeSuffix</i> are as defined in the lexical structure sections of <cite>The Java Language
     * Specification</cite>, except that underscores are not accepted between digits. If {@code s} does not have the
     * form of a <i>FloatValue</i>, then a {@code NumberFormatException} is thrown. Otherwise, {@code s} is regarded as
     * representing an exact decimal value in the usual "computerized scientific notation" or as an exact hexadecimal
     * value; this exact numerical value is then conceptually converted to an "infinitely precise" binary value that is
     * then rounded to type {@code half} by the usual round-to-nearest rule of IEEE 754 floating-point arithmetic, which
     * includes preserving the sign of a zero value.
     *
     * Note that the round-to-nearest rule also implies overflow and underflow behaviour; if the exact value of
     * {@code s} is large enough in magnitude (greater than or equal to ({@link #MAX_VALUE} +
     * {@link HalfMath#ulp(Half half) HalfMath.ulp(MAX_VALUE)}/2), rounding to {@code float} will result in an infinity
     * and if the exact value of {@code s} is small enough in magnitude (less than or equal to {@link #MIN_VALUE}/2),
     * rounding to float will result in a zero.
     *
     * Finally, after rounding a {@code Half} object is returned.
     *
     * <p>
     * To interpret localized string representations of a floating-point value, use subclasses of
     * {@link java.text.NumberFormat}.
     *
     * <p>
     * To avoid calling this method on an invalid string and having a {@code NumberFormatException} be thrown, the
     * documentation for {@link Double#valueOf Double.valueOf} lists a regular expression which can be used to screen
     * the input.
     *
     * @param s
     *            the string to be parsed.
     *
     * @return a {@code Half} object holding the value represented by the {@code String} argument.
     *
     * @throws NumberFormatException
     *             if the string does not contain a parsable number.
     */
    public static Half valueOf(String s) throws NumberFormatException {
        return valueOf(Float.valueOf(s));
    }

    /**
     * Returns a {@code Half} instance representing the specified {@code double} value.
     *
     * @param doubleValue
     *            a double value.
     *
     * @return a {@code Half} instance representing {@code doubleValue}.
     */
    public static Half valueOf(double doubleValue) {
        return valueOf((float) doubleValue);
    }

    /**
     * Returns a {@code Half} instance representing the specified {@code Double} value.
     *
     * @param doubleValue
     *            a double value.
     *
     * @return a {@code Half} instance representing {@code doubleValue}.
     */
    public static Half valueOf(Double doubleValue) {
        return valueOf(doubleValue.doubleValue());
    }

    /**
     * Returns a {@code Half} instance representing the specified {@code float} value.
     *
     * @param floatValue
     *            a float value.
     *
     * @return a {@code Half} instance representing {@code floatValue}.
     */
    public static Half valueOf(float floatValue) {
        return new Half(floatRepresentationOf(floatValue));
    }

    public static float floatRepresentationOf(final float floatValue) {
        // check for infinities
        if (floatValue > 65504.0f || floatValue < -65504.0f) {
            return Half.halfShortToFloat((short) ((Float.floatToIntBits(floatValue) & 0x80000000) >> 16 | 0x7c00));
        }
        //return floatValue;
        return Half.halfShortToFloat(floatRepresentationToShortBits(floatValue));
    }

    /**
     * Returns a {@code Half} instance representing the specified {@code Float} value.
     *
     * @param floatValue
     *            a float value.
     *
     * @return a {@code Half} instance representing {@code floatValue}.
     */
    public static Half valueOf(Float floatValue) {
        return valueOf(floatValue.floatValue());
    }

    /**
     * Returns a new {@code Half} instance identical to the specified {@code half}.
     *
     * @param half
     *            a half instance.
     *
     * @return a {@code Half} instance representing {@code doubleValue}.
     */
    public static Half valueOf(Half half) {
        return shortBitsToHalf(halfToRawShortBits(half));
    }

    /**
     * Returns {@code true} if the specified number is a Not-a-Number (NaN) value, {@code false} otherwise.
     *
     * @param half
     *            the {@code Half} to be tested.
     *
     * @return {@code true} if the argument is NaN; {@code false} otherwise.
     */
    public static boolean isNaN(Half half) {
        return Float.isNaN(half.floatRepresentation);
    }

    /**
     * Returns {@code true} if this {@code Half} value is a Not-a-Number (NaN), {@code false} otherwise.
     *
     * @return {@code true} if the value represented by this object is NaN; {@code false} otherwise.
     */
    public boolean isNaN() {
        return isNaN(this);
    }

    /**
     * Returns {@code true} if the specified {@code Half} is infinitely large in magnitude, {@code false} otherwise.
     *
     * @param half
     *            the {@code Half} to be tested.
     *
     * @return {@code true} if the argument is positive infinity or negative infinity; {@code false} otherwise.
     */
    public static boolean isInfinite(Half half) {
        return Float.isInfinite(half.floatRepresentation);
    }

    /**
     * Returns {@code true} if this {@code Half} value is infinitely large in magnitude, {@code false} otherwise.
     *
     * @return {@code true} if the value represented by this object is positive infinity or negative infinity;
     *         {@code false} otherwise.
     */
    public boolean isInfinite() {
        return isInfinite(this);
    }

    /**
     * Returns {@code true} if the argument is a finite floating-point value; returns {@code false} otherwise (for NaN
     * and infinity arguments).
     *
     * @param half
     *            the {@code Half} to be tested
     *
     * @return {@code true} if the argument is a finite floating-point value, {@code false} otherwise.
     */
    public static boolean isFinite(Half half) {
        return Float.isFinite(half.floatRepresentation);
    }

    /**
     * Returns {@code true} if the argument is a finite floating-point value; returns {@code false} otherwise (for NaN
     * and infinity arguments).
     *
     * @return {@code true} if the argument is a finite floating-point value, {@code false} otherwise.
     */
    public boolean isFinite() {
        return isFinite(this);
    }

    /**
     * Returns a string representation of the {@code half} argument. All characters mentioned below are ASCII
     * characters.
     * <ul>
     * <li>If the argument is NaN, the result is the string "{@code NaN}".
     * <li>Otherwise, the result is a string that represents the sign and magnitude (absolute value) of the argument. If
     * the sign is negative, the first character of the result is '{@code -}' ({@code '\u005Cu002D'}); if the sign is
     * positive, no sign character appears in the result. As for the magnitude <i>m</i>:
     * <ul>
     * <li>If <i>m</i> is infinity, it is represented by the characters {@code "Infinity"}; thus, positive infinity
     * produces the result {@code "Infinity"} and negative infinity produces the result {@code "-Infinity"}.
     * <li>If <i>m</i> is zero, it is represented by the characters {@code "0.0"}; thus, negative zero produces the
     * result {@code "-0.0"} and positive zero produces the result {@code "0.0"}.
     * <li>If <i>m</i> is greater than or equal to 10<sup>-3</sup> but less than 10<sup>7</sup>, then it is represented
     * as the integer part of <i>m</i>, in decimal form with no leading zeroes, followed by '{@code .}'
     * ({@code '\u005Cu002E'}), followed by one or more decimal digits representing the fractional part of <i>m</i>.
     * <li>If <i>m</i> is less than 10<sup>-3</sup> or greater than or equal to 10<sup>7</sup>, then it is represented
     * in so-called "computerized scientific notation." Let <i>n</i> be the unique integer such that 10<sup><i>n</i>
     * </sup>&le; <i>m</i> {@literal <} 10<sup><i>n</i>+1</sup>; then let <i>a</i> be the mathematically exact quotient
     * of <i>m</i> and 10<sup><i>n</i></sup> so that 1 &le; <i>a</i> {@literal <} 10. The magnitude is then represented
     * as the integer part of <i>a</i>, as a single decimal digit, followed by '{@code .}' ({@code '\u005Cu002E'}),
     * followed by decimal digits representing the fractional part of <i>a</i>, followed by the letter '{@code E}'
     * ({@code '\u005Cu0045'}), followed by a representation of <i>n</i> as a decimal integer, as produced by the method
     * {@link java.lang.Integer#toString(int)}.
     *
     * </ul>
     * </ul>
     * Handled as a float and number of significant digits is determined by {@link Float#toString(float f)
     * Float.toString(floatValue)} using results of {@link #floatValue()} call using {@code half} instance.
     *
     * <p>
     * To create localized string representations of a floating-point value, use subclasses of
     * {@link java.text.NumberFormat}.
     *
     * @param half
     *            the Half to be converted.
     *
     * @return a string representation of the argument.
     */
    public static String toString(Half half) {
        // Use float toString for now
        // Should have own toString implementation for better result.
        return Float.toString(half.floatRepresentation);
    }

    @Override
    public String toString() {
        return toString(this);
    }

    /**
     * Returns a hexadecimal string representation of the {@code half} argument. All characters mentioned below are
     * ASCII characters.
     *
     * <ul>
     * <li>If the argument is NaN, the result is the string "{@code NaN}".
     * <li>Otherwise, the result is a string that represents the sign and magnitude (absolute value) of the argument. If
     * the sign is negative, the first character of the result is '{@code -}' ({@code '\u005Cu002D'}); if the sign is
     * positive, no sign character appears in the result. As for the magnitude <i>m</i>:
     *
     * <ul>
     * <li>If <i>m</i> is infinity, it is represented by the string {@code "Infinity"}; thus, positive infinity produces
     * the result {@code "Infinity"} and negative infinity produces the result {@code "-Infinity"}.
     *
     * <li>If <i>m</i> is zero, it is represented by the string {@code "0x0.0p0"}; thus, negative zero produces the
     * result {@code "-0x0.0p0"} and positive zero produces the result {@code "0x0.0p0"}.
     *
     * <li>If <i>m</i> is a {@code half} with a normalized representation, substrings are used to represent the
     * significand and exponent fields. The significand is represented by the characters {@code "0x1."} followed by a
     * lowercase hexadecimal representation of the rest of the significand as a fraction. Trailing zeros in the
     * hexadecimal representation are removed unless all the digits are zero, in which case a single zero is used. Next,
     * the exponent is represented by {@code "p"} followed by a decimal string of the unbiased exponent as if produced
     * by a call to {@link Integer#toString(int) Integer.toString} on the exponent value.
     *
     * <li>If <i>m</i> is a {@code half} with a subnormal representation, the significand is represented by the
     * characters {@code "0x0."} followed by a hexadecimal representation of the rest of the significand as a fraction.
     * Trailing zeros in the hexadecimal representation are removed. Next, the exponent is represented by
     * {@code "p-14"}. Note that there must be at least one nonzero digit in a subnormal significand.
     *
     * </ul>
     *
     * </ul>
     *
     * <table class="striped">
     * <caption>Examples</caption> <thead>
     * <tr>
     * <th scope="col">Floating-point Value</th>
     * <th scope="col">Hexadecimal String</th> </thead> <tbody>
     * <tr>
     * <th scope="row">{@code 1.0}</th>
     * <td>{@code 0x1.0p0}</td>
     * <tr>
     * <th scope="row">{@code -1.0}</th>
     * <td>{@code -0x1.0p0}</td>
     * <tr>
     * <th scope="row">{@code 2.0}</th>
     * <td>{@code 0x1.0p1}</td>
     * <tr>
     * <th scope="row">{@code 3.0}</th>
     * <td>{@code 0x1.8p1}</td>
     * <tr>
     * <th scope="row">{@code 0.5}</th>
     * <td>{@code 0x1.0p-1}</td>
     * <tr>
     * <th scope="row">{@code 0.25}</th>
     * <td>{@code 0x1.0p-2}</td>
     * <tr>
     * <th scope="row">{@code Float.MAX_VALUE}</th>
     * <td>{@code 0x1.ffcp15}</td>
     * <tr>
     * <th scope="row">{@code Minimum Normal Value}</th>
     * <td>{@code 0x1.0p-14}</td>
     * <tr>
     * <th scope="row">{@code Maximum Subnormal Value}</th>
     * <td>{@code 0x0.ffcp-14}</td>
     * <tr>
     * <th scope="row">{@code Float.MIN_VALUE}</th>
     * <td>{@code 0x0.004p-14}</td> </tbody>
     * </table>
     *
     * @param half
     *            the {@code Half} to be converted.
     *
     * @return a hex string representation of the argument.
     */
    public static String toHexString(Half half) {
        // Check subnormal
        if (HalfMath.abs(half).compareTo(Half.MIN_NORMAL) < 0 && !HalfMath.abs(half).equals(Half.POSITIVE_ZERO)) {
            String s = Double
                    .toHexString(Math.scalb((double) half.floatValue(), Double.MIN_EXPONENT - Half.MIN_EXPONENT));
            return s.replaceFirst("p-1022$", "p-14");
        } else {
            // double string will be the same as half string
            return Double.toHexString(half.floatValue());
        }
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Half) && Float.valueOf(((Half) obj).floatRepresentation).equals(floatRepresentation);
    }

    /**
     * Returns a hash code for a {@code Half}; compatible with {@code Half.hashCode()}.
     *
     * @param half
     *            the {@code Half} to hash
     *
     * @return a hash code value for a {@code Half} value.
     */
    public static int hashCode(Half half) {
        return halfToShortBits(half);
    }

    /**
     * Returns a hash code for this {@code Half} object. The result is the short bit representation, exactly as produced
     * by the method {@link #halfToShortBits(Half)} represented by this {@code Half} object.
     *
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return hashCode(this);
    }

    /**
     * Compares the two specified {@code Half} objects. The sign of the integer value returned is the same as that of
     * the integer that would be returned by the call:
     *
     * <pre>
     * half1.compareTo(half2)
     * </pre>
     *
     * @param half1
     *            the first {@code Half} to compare.
     * @param half2
     *            the second {@code Half} to compare.
     *
     * @return the value {@code 0} if {@code half1} is numerically equal to {@code half2}; a value less than {@code 0}
     *         if {@code half1} is numerically less than {@code half2}; and a value greater than {@code 0} if
     *         {@code half1} is numerically greater than {@code half2}.
     */
    public static int compare(Half half1, Half half2) {
        return Float.compare(half1.floatRepresentation, half2.floatRepresentation);
    }

    @Override
    public int compareTo(Half anotherHalf) {
        return compare(this, anotherHalf);
    }

    /**
     * Adds two {@code Half} values together as per the + operator.
     *
     * @param a
     *            the first operand
     * @param b
     *            the second operand
     *
     * @return the sum of {@code a} and {@code b}
     *
     * @see java.util.function.BinaryOperator
     */
    public static Half sum(Half a, Half b) {
        return Half.valueOf(Float.sum(a.floatRepresentation, b.floatRepresentation));
    }

    /**
     * Returns the greater of two {@code Half} objects.<br>
     * Determined using {@link #floatValue() aFloatValue = a.floatValue()} and {@link #floatValue() bFloatValue =
     * b.floatValue()} then calling {@link Float#max(float, float) Float.max(aFloatValue, bFloatValue)}.
     *
     * @param a
     *            the first operand
     * @param b
     *            the second operand
     *
     * @return the greater of {@code a} and {@code b}
     *
     * @see java.util.function.BinaryOperator
     */
    public static Half max(Half a, Half b) {
        return a.floatRepresentation > b.floatRepresentation ? a : b;
    }

    /**
     * Returns the smaller of two {@code Half} objects.<br>
     * Determined using {@link #floatValue() aFloatValue = a.floatValue()} and {@link #floatValue() bFloatValue =
     * b.floatValue()} then calling {@link Float#min(float, float) Float.min(aFloatValue, bFloatValue)}.
     *
     * @param a
     *            the first operand
     * @param b
     *            the second operand
     *
     * @return the smaller of {@code a} and {@code b}
     *
     * @see java.util.function.BinaryOperator
     */
    public static Half min(Half a, Half b) {
        return a.floatRepresentation < b.floatRepresentation ? a : b;
    }
}
