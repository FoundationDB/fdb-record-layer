/*
 * HalfRealVector.java
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

package com.apple.foundationdb.linear;

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * Immutable vector storing 32-bit single-precision (float) components. Internally the data is
 * widened to a {@code double[]} for arithmetic compatibility with the rest of the
 * {@link RealVector} hierarchy, but every value is first truncated to {@code float} precision so
 * the storage cost and round-trip behaviour match a true float vector. Conversion to
 * {@link HalfRealVector} is memoized; conversion to {@link DoubleRealVector} is cheap and not
 * memoized (it simply wraps the existing array).
 */
public class FloatRealVector extends AbstractRealVector {
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<HalfRealVector> toHalfRealVectorSupplier = Suppliers.memoize(this::computeHalfRealVector);

    /**
     * Constructs a single-precision vector from a boxed {@code Float[]}. The input is unboxed
     * and widened into a fresh {@code double[]} backing store; subsequent changes to
     * {@code floatData} do not affect the returned vector.
     *
     * @param floatData the components of the new vector
     */
    public FloatRealVector(@Nonnull final Float[] floatData) {
        this(computeDoubleData(floatData));
    }

    /**
     * Constructs a single-precision vector from a primitive {@code float[]}. The input is
     * widened into a fresh {@code double[]} backing store; subsequent changes to
     * {@code floatData} do not affect the returned vector.
     *
     * @param floatData the components of the new vector
     */
    public FloatRealVector(@Nonnull final float[] floatData) {
        this(computeDoubleData(floatData));
    }

    /**
     * Constructs a single-precision vector from a primitive {@code double[]}. The input is
     * truncated component-wise to {@code float} precision (and re-widened to {@code double}) so
     * round-trip behaviour matches a native float vector. The original {@code data} array is
     * read but not retained.
     *
     * @param data the components of the new vector, in {@code double} precision
     */
    public FloatRealVector(@Nonnull final double[] data) {
        super(truncateDoubleData(data));
    }

    /**
     * Constructs a single-precision vector by widening each int component. The new vector owns
     * its backing array — the input is read but not retained.
     *
     * @param intData the components of the new vector
     */
    public FloatRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    /**
     * Constructs a single-precision vector by widening each long component. The new vector owns
     * its backing array — the input is read but not retained. {@code long} values outside the
     * 24-bit float mantissa lose precision; values outside the 53-bit double mantissa lose
     * precision in the intermediate widening as well.
     *
     * @param longData the components of the new vector
     */
    public FloatRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    /**
     * Returns the memoized half-precision projection of this vector.
     */
    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return toHalfRealVectorSupplier.get();
    }

    /**
     * Builds a fresh half-precision view of this vector by truncating each component into a
     * {@link HalfRealVector}. Used as the supplier behind {@link #toHalfRealVector()}.
     *
     * @return a new {@link HalfRealVector} with this vector's components
     */
    @Nonnull
    public HalfRealVector computeHalfRealVector() {
        return new HalfRealVector(data);
    }

    /**
     * Returns {@code this} — already a single-precision vector, so no conversion is needed.
     */
    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return this;
    }

    /**
     * Returns a fresh {@link DoubleRealVector} carrying this vector's already-widened components.
     * Note that the underlying values are still float-truncated; the conversion only changes the
     * runtime type, not the precision of the data.
     */
    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return new DoubleRealVector(data);
    }

    /**
     * Returns a fresh {@link FloatRealVector} carrying {@code data} (after truncation to float
     * precision inside the constructor).
     *
     * @param data the components of the new vector
     * @return a fresh immutable single-precision vector
     */
    @Nonnull
    @Override
    public FloatRealVector withData(@Nonnull final double[] data) {
        return new FloatRealVector(data);
    }

    /**
     * Returns {@code this} — instances of this class are already immutable.
     */
    @Nonnull
    @Override
    public FloatRealVector toImmutable() {
        return this;
    }

    /**
     * Serializes this single-precision vector into a leading type byte followed by big-endian
     * 32-bit IEEE-754 floats, one per component. The resulting byte array has length
     * {@code 1 + 4 * getNumDimensions()} and round-trips through {@link #fromBytes(byte[])}.
     *
     * @return a new byte array representing the serialized vector data; never {@code null}
     */
    @Nonnull
    @Override
    protected byte[] computeRawData() {
        final byte[] vectorBytes = new byte[1 + 4 * getNumDimensions()];
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte)VectorType.SINGLE.ordinal());
        for (int i = 0; i < getNumDimensions(); i ++) {
            buffer.putFloat((float)data[i]);
        }
        return vectorBytes;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector normalize() {
        return withData(RealVectorPrimitives.normalizeInto(this, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector add(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.addInto(this, other, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector add(final double scalar) {
        return withData(RealVectorPrimitives.addInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector subtract(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.subtractInto(this, other, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector subtract(final double scalar) {
        return withData(RealVectorPrimitives.subtractInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link FloatRealVector}.
     */
    @Nonnull
    @Override
    public FloatRealVector multiply(final double scalar) {
        return withData(RealVectorPrimitives.multiplyInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns a vector whose components are all zero.
     * @param numDimensions number of dimensions
     * @return a vector whose components are all zero
     */
    @Nonnull
    public static FloatRealVector zeroVector(final int numDimensions) {
        return new FloatRealVector(new float[numDimensions]);
    }

    /**
     * Unboxes a {@code Float[]} into a freshly allocated {@code double[]} (widening each
     * component). Used by the boxed-array constructor.
     *
     * @param floatData the boxed components
     * @return a new {@code double[]} of the same length, each element widened from the
     *         corresponding {@code floatData[i]}
     */
    @Nonnull
    private static double[] computeDoubleData(@Nonnull Float[] floatData) {
        double[] result = new double[floatData.length];
        for (int i = 0; i < floatData.length; i++) {
            result[i] = floatData[i];
        }
        return result;
    }

    /**
     * Widens a {@code float[]} into a freshly allocated {@code double[]}. Used by the primitive
     * float-array constructor.
     *
     * @param floatData the source components
     * @return a new {@code double[]} of the same length, each element widened from the
     *         corresponding {@code floatData[i]}
     */
    @Nonnull
    private static double[] computeDoubleData(@Nonnull float[] floatData) {
        double[] result = new double[floatData.length];
        for (int i = 0; i < floatData.length; i++) {
            result[i] = floatData[i];
        }
        return result;
    }

    /**
     * Truncates each component of {@code doubleData} to float precision and re-widens to
     * {@code double}, so the resulting backing array faithfully represents what a true
     * {@code float[]} would hold. Used by the double-array constructor to enforce
     * single-precision storage semantics.
     *
     * @param doubleData the source components in double precision
     * @return a new {@code double[]} of the same length, each element truncated through
     *         {@code float}
     */
    @Nonnull
    private static double[] truncateDoubleData(@Nonnull double[] doubleData) {
        double[] result = new double[doubleData.length];
        for (int i = 0; i < doubleData.length; i++) {
            result[i] = (float)doubleData[i];
        }
        return result;
    }

    /**
     * Creates a {@link FloatRealVector} from a byte array produced by {@link #getRawData()}.
     * <p>
     * The input is interpreted as a leading type byte (which must match
     * {@link VectorType#SINGLE}) followed by a sequence of big-endian 32-bit IEEE-754 floats,
     * one per component.
     * @param vectorBytes the non-null byte array to convert
     * @return a new {@link FloatRealVector} instance created from the byte array
     */
    @Nonnull
    public static FloatRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        Verify.verify(buffer.get() == VectorType.SINGLE.ordinal());
        final int numDimensions = vectorBytes.length >> 2;
        final double[] vectorComponents = new double[numDimensions];
        for (int i = 0; i < numDimensions; i ++) {
            vectorComponents[i] = buffer.getFloat();
        }
        return new FloatRealVector(vectorComponents);
    }
}
