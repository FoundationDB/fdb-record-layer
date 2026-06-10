/*
 * DoubleRealVector.java
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
 * Immutable vector storing 64-bit double-precision components. Conversions to the lower-precision
 * {@link HalfRealVector} and {@link FloatRealVector} representations are memoized, so callers
 * that round-trip a vector through several precisions repeatedly only pay the conversion cost
 * once per direction. The mutable counterpart with the same storage layout is
 * {@link MutableDoubleRealVector}.
 */
public class DoubleRealVector extends AbstractRealVector {
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<HalfRealVector> toHalfVectorSupplier = Suppliers.memoize(this::computeHalfRealVector);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<FloatRealVector> toFloatVectorSupplier = Suppliers.memoize(this::computeFloatRealVector);

    /**
     * Constructs a double-precision vector from a boxed {@code Double[]}. The input is
     * defensively copied into a fresh primitive array, so subsequent changes to {@code doubleData}
     * do not affect the returned vector.
     *
     * @param doubleData the components of the new vector
     */
    public DoubleRealVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    /**
     * Constructs a double-precision vector over the given primitive array. The array is stored
     * <em>by reference</em> per the {@link AbstractRealVector#AbstractRealVector(double[]) base
     * contract} — callers must not mutate {@code data} after construction. Use
     * {@code data.clone()} at the call site if you need an independent backing store.
     *
     * @param data the components of the new vector; ownership transfers to this vector
     */
    public DoubleRealVector(@Nonnull final double[] data) {
        super(data);
    }

    /**
     * Constructs a double-precision vector by widening each int component to a {@code double}.
     * The new vector owns its backing array — the input is read but not retained.
     *
     * @param intData the components of the new vector
     */
    public DoubleRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    /**
     * Constructs a double-precision vector by widening each long component to a {@code double}.
     * The new vector owns its backing array — the input is read but not retained.
     * {@code long} values outside the 53-bit double mantissa may lose precision.
     *
     * @param longData the components of the new vector
     */
    public DoubleRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    /**
     * Returns the memoized half-precision projection of this vector.
     */
    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return toHalfVectorSupplier.get();
    }

    /**
     * Builds a fresh half-precision view of this vector by truncating each component into a
     * {@link HalfRealVector}. Used as the supplier behind {@link #toHalfRealVector()}; subclasses
     * may override to plug in alternative half-precision representations.
     *
     * @return a new {@link HalfRealVector} with this vector's components
     */
    @Nonnull
    protected HalfRealVector computeHalfRealVector() {
        return new HalfRealVector(data);
    }

    /**
     * Returns the memoized single-precision projection of this vector.
     */
    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return toFloatVectorSupplier.get();
    }

    /**
     * Builds a fresh single-precision view of this vector by truncating each component into a
     * {@link FloatRealVector}. Used as the supplier behind {@link #toFloatRealVector()};
     * subclasses may override to plug in alternative single-precision representations.
     *
     * @return a new {@link FloatRealVector} with this vector's components
     */
    @Nonnull
    protected FloatRealVector computeFloatRealVector() {
        return new FloatRealVector(data);
    }

    /**
     * Returns {@code this} — already a double-precision vector, so no conversion is needed.
     */
    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return this;
    }

    /**
     * Returns a fresh {@link DoubleRealVector} backed by {@code data}. Like
     * {@link #DoubleRealVector(double[]) the array-by-reference constructor}, ownership of
     * {@code data} transfers to the returned vector.
     *
     * @param data the components of the new vector
     * @return a fresh immutable double-precision vector
     */
    @Nonnull
    @Override
    public DoubleRealVector withData(@Nonnull final double[] data) {
        return new DoubleRealVector(data);
    }

    /**
     * Returns {@code this} — instances of this class are already immutable.
     */
    @Nonnull
    @Override
    public DoubleRealVector toImmutable() {
        return this;
    }

    /**
     * Serializes this double-precision vector into a leading type byte followed by big-endian
     * 64-bit IEEE-754 doubles, one per component. The resulting byte array has length
     * {@code 1 + 8 * getNumDimensions()} and round-trips through {@link #fromBytes(byte[])}.
     *
     * @return a new byte array representing the serialized vector data; never {@code null}
     */
    @Nonnull
    @Override
    protected byte[] computeRawData() {
        final byte[] vectorBytes = new byte[1 + 8 * getNumDimensions()];
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte)VectorType.DOUBLE.ordinal());
        for (int i = 0; i < getNumDimensions(); i ++) {
            buffer.putDouble(getComponent(i));
        }
        return vectorBytes;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector normalize() {
        return withData(RealVectorPrimitives.normalizeInto(this, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector add(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.addInto(this, other, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector add(final double scalar) {
        return withData(RealVectorPrimitives.addInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector subtract(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.subtractInto(this, other, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector subtract(final double scalar) {
        return withData(RealVectorPrimitives.subtractInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link DoubleRealVector}.
     */
    @Nonnull
    @Override
    public DoubleRealVector multiply(final double scalar) {
        return withData(RealVectorPrimitives.multiplyInto(this, scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns a vector whose components are all zero.
     * @param numDimensions number of dimensions
     * @return a vector whose components are all zero
     */
    @Nonnull
    public static DoubleRealVector zeroVector(final int numDimensions) {
        return new DoubleRealVector(new double[numDimensions]);
    }

    /**
     * Unboxes a {@code Double[]} into a freshly allocated {@code double[]} suitable for handing
     * to the array-by-reference constructor. Shared between this class and
     * {@link MutableDoubleRealVector}'s boxed-array constructor.
     *
     * @param doubleData the boxed components
     * @return a new primitive array of the same length and values
     */
    @Nonnull
    protected static double[] computeDoubleData(@Nonnull Double[] doubleData) {
        double[] result = new double[doubleData.length];
        for (int i = 0; i < doubleData.length; i++) {
            result[i] = doubleData[i];
        }
        return result;
    }

    /**
     * Creates a {@link DoubleRealVector} from a byte array.
     * <p>
     * This method interprets the input byte array as a sequence of 64-bit double-precision floating-point numbers. Each
     * run of eight bytes is converted into a {@code double} value, which then becomes a component of the resulting
     * vector.
     * @param vectorBytes the non-null byte array to convert
     * @return a new {@link DoubleRealVector} instance created from the byte array
     */
    @Nonnull
    public static DoubleRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        return new DoubleRealVector(decodeDoubleBytes(vectorBytes));
    }

    /**
     * Decodes a serialized double-precision vector byte array into its raw {@code double[]}
     * components, shared between {@link DoubleRealVector#fromBytes(byte[])} and
     * {@link MutableDoubleRealVector#fromBytes(byte[])}. The first byte must be the
     * {@link VectorType#DOUBLE} ordinal; the remainder is read as big-endian 64-bit doubles.
     *
     * @param vectorBytes the non-null byte array to decode
     * @return a freshly allocated {@code double[]} with the decoded components
     */
    @Nonnull
    protected static double[] decodeDoubleBytes(@Nonnull final byte[] vectorBytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        Verify.verify(buffer.get() == VectorType.DOUBLE.ordinal());
        final int numDimensions = vectorBytes.length >> 3;
        final double[] vectorComponents = new double[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            vectorComponents[i] = buffer.getDouble();
        }
        return vectorComponents;
    }
}
