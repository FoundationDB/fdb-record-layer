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

import com.apple.foundationdb.half.Half;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Immutable vector storing 16-bit {@link Half}-precision components. Internally the data is
 * widened to a {@code double[]} for arithmetic compatibility with the rest of the
 * {@link RealVector} hierarchy, but every value is first truncated through
 * {@link Half#valueOf(double)} so storage cost and round-trip behaviour match a true half-precision
 * vector. Conversions to the higher-precision {@link FloatRealVector} and {@link DoubleRealVector}
 * are not memoized (each call wraps the already-widened backing array in a fresh instance —
 * cheap enough that caching wouldn't pay off).
 */
public class HalfRealVector extends AbstractRealVector {
    /**
     * Constructs a half-precision vector from a boxed {@link Half}{@code []}. The input is
     * unboxed and widened into a fresh {@code double[]} backing store; subsequent changes to
     * {@code halfData} do not affect the returned vector.
     *
     * @param halfData the components of the new vector
     */
    public HalfRealVector(@Nonnull final Half[] halfData) {
        this(computeDoubleData(halfData));
    }

    /**
     * Constructs a half-precision vector from a primitive {@code double[]}. The input is
     * truncated component-wise through {@link Half#valueOf(double)} (and re-widened to
     * {@code double}) so round-trip behaviour matches a native half-precision vector. The
     * original {@code data} array is read but not retained.
     *
     * @param data the components of the new vector, in {@code double} precision
     */
    public HalfRealVector(@Nonnull final double[] data) {
        super(truncateDoubleData(data));
    }

    /**
     * Constructs a half-precision vector by widening each int component. The new vector owns
     * its backing array — the input is read but not retained. {@code int} values outside
     * the ~11-bit half-precision mantissa lose precision after the truncation in the constructor
     * delegate.
     *
     * @param intData the components of the new vector
     */
    public HalfRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    /**
     * Constructs a half-precision vector by widening each long component. The new vector owns
     * its backing array — the input is read but not retained. Same precision caveats as
     * {@link #HalfRealVector(int[])}.
     *
     * @param longData the components of the new vector
     */
    public HalfRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    /**
     * Returns {@code this} — already a half-precision vector, so no conversion is needed.
     */
    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return this;
    }

    /**
     * Returns a fresh {@link FloatRealVector} carrying this vector's already-widened components.
     * Note that the underlying values are still half-truncated; the conversion only changes the
     * runtime type, not the precision of the data.
     */
    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return new FloatRealVector(data);
    }

    /**
     * Returns a fresh {@link DoubleRealVector} carrying this vector's already-widened components.
     * Note that the underlying values are still half-truncated; the conversion only changes the
     * runtime type, not the precision of the data.
     */
    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return new DoubleRealVector(data);
    }

    /**
     * Returns a fresh {@link HalfRealVector} carrying {@code data} (after truncation to half
     * precision inside the constructor).
     *
     * @param data the components of the new vector
     * @return a fresh immutable half-precision vector
     */
    @Nonnull
    @Override
    public HalfRealVector withData(@Nonnull final double[] data) {
        return new HalfRealVector(data);
    }

    /**
     * Serializes this half-precision vector into a leading type byte followed by big-endian
     * 16-bit {@link Half}-encoded components, one per dimension. The resulting byte array has
     * length {@code 1 + 2 * getNumDimensions()} and round-trips through
     * {@link #fromBytes(byte[])}.
     *
     * @return a new byte array representing the serialized vector data; never {@code null}
     */
    @Nonnull
    @Override
    protected byte[] computeRawData() {
        final byte[] vectorBytes = new byte[1 + 2 * getNumDimensions()];
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte)VectorType.HALF.ordinal());
        for (int i = 0; i < getNumDimensions(); i ++) {
            buffer.putShort(Half.floatToShortBitsCollapseNaN(Half.quantizeFloat((float)getComponent(i))));
        }
        return vectorBytes;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector normalize() {
        return withData(RealVectorPrimitives.normalizeInto(this.getData(), new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector add(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.addInto(this.getData(), other.getData(), new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector add(final double scalar) {
        return withData(RealVectorPrimitives.addInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector subtract(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.subtractInto(this.getData(), other.getData(), new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector subtract(final double scalar) {
        return withData(RealVectorPrimitives.subtractInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Narrows the return type to {@link HalfRealVector}.
     */
    @Nonnull
    @Override
    public HalfRealVector multiply(final double scalar) {
        return withData(RealVectorPrimitives.multiplyInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns a vector whose components are all zero.
     * @param numDimensions number of dimensions
     * @return a vector whose components are all zero
     */
    @Nonnull
    public static HalfRealVector zeroVector(final int numDimensions) {
        return new HalfRealVector(new double[numDimensions]);
    }

    /**
     * Unboxes a {@link Half}{@code []} into a freshly allocated {@code double[]} (widening each
     * component via {@link Half#doubleValue()}). Used by the boxed-array constructor.
     *
     * @param halfData the boxed components
     * @return a new {@code double[]} of the same length, each element widened from the
     *         corresponding {@code halfData[i]}
     */
    @Nonnull
    private static double[] computeDoubleData(@Nonnull Half[] halfData) {
        double[] result = new double[halfData.length];
        for (int i = 0; i < halfData.length; i++) {
            result[i] = halfData[i].doubleValue();
        }
        return result;
    }

    /**
     * Truncates each component of {@code doubleData} through {@link Half#valueOf(double)} and
     * re-widens to {@code double}, so the resulting backing array faithfully represents what a
     * true {@link Half}{@code []} would hold. Used by the double-array constructor to enforce
     * half-precision storage semantics.
     *
     * @param doubleData the source components in double precision
     * @return a new {@code double[]} of the same length, each element truncated through
     *         {@link Half}
     */
    @Nonnull
    private static double[] truncateDoubleData(@Nonnull double[] doubleData) {
        double[] result = new double[doubleData.length];
        for (int i = 0; i < doubleData.length; i++) {
            result[i] = Half.valueOf(doubleData[i]).doubleValue();
        }
        return result;
    }

    /**
     * Returns {@code this} — instances of this class are already immutable.
     */
    @Nonnull
    @Override
    public HalfRealVector toImmutable() {
        return this;
    }

    /**
     * Creates a {@link HalfRealVector} from a byte array produced by {@link #getRawData()}.
     * <p>
     * The input is interpreted as a leading type byte (which must match {@link VectorType#HALF})
     * followed by a sequence of big-endian 16-bit {@link Half}-encoded components, one per
     * dimension.
     * @param vectorBytes the non-null byte array to convert
     * @return a new {@link HalfRealVector} instance created from the byte array
     */
    @Nonnull
    public static HalfRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        Verify.verify(buffer.get() == VectorType.HALF.ordinal());
        final int numDimensions = vectorBytes.length >> 1;
        final double[] vectorComponents = new double[numDimensions];
        for (int i = 0; i < numDimensions; i ++) {
            vectorComponents[i] = Half.halfShortToFloat(buffer.getShort());
        }
        return new HalfRealVector(vectorComponents);
    }
}
