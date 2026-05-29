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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * A vector class encoding a vector over double components. Conversion to {@link HalfRealVector} is supported and
 * memoized.
 */
@SuppressWarnings("PMD.OverrideBothEqualsAndHashcode")
public class MutableDoubleRealVector extends DoubleRealVector {

    public MutableDoubleRealVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    public MutableDoubleRealVector(@Nonnull final double[] data) {
        super(data);
    }

    public MutableDoubleRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public MutableDoubleRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return computeHalfRealVector();
    }

    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return computeFloatRealVector();
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector toDoubleRealVector() {
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector toMutable() {
        return this;
    }

    @Nonnull
    @Override
    public DoubleRealVector toImmutable() {
        return new DoubleRealVector(getData().clone());
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector withData(@Nonnull final double[] data) {
        Preconditions.checkArgument(this.data.length == data.length);
        System.arraycopy(data, 0, this.data, 0, this.data.length);
        return this;
    }

    @Nonnull
    @Override
    public byte[] getRawData() {
        return computeRawData();
    }

    @Override
    public int hashCode() {
        // this hashCode() implementation cannot rely on a memoized hash code
        return computeHashCode();
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector normalize() {
        RealVectorPrimitives.normalizeInto(this.getData(), getData());
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector add(@Nonnull final RealVector other) {
        RealVectorPrimitives.addInto(this.getData(), other.getData(), getData());
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector add(final double scalar) {
        RealVectorPrimitives.addInto(this.getData(), scalar, getData());
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector subtract(@Nonnull final RealVector other) {
        RealVectorPrimitives.subtractInto(this.getData(), other.getData(), getData());
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector subtract(final double scalar) {
        RealVectorPrimitives.subtractInto(this.getData(), scalar, getData());
        return this;
    }

    @Nonnull
    @Override
    public MutableDoubleRealVector multiply(final double scalar) {
        RealVectorPrimitives.multiplyInto(this.getData(), scalar, getData());
        return this;
    }

    @Nonnull
    public MutableDoubleRealVector zero() {
        Arrays.fill(getData(), 0.0d);
        return this;
    }

    /**
     * Returns a vector whose components are all zero.
     * @param numDimensions number of dimensions
     * @return a vector whose components are all zero
     */
    @Nonnull
    public static MutableDoubleRealVector zeroVector(final int numDimensions) {
        return new MutableDoubleRealVector(new double[numDimensions]);
    }

    /**
     * Creates a {@link MutableDoubleRealVector} from a byte array.
     * <p>
     * This method interprets the input byte array as a sequence of 64-bit double-precision floating-point numbers. Each
     * run of eight bytes is converted into a {@code double} value, which then becomes a component of the resulting
     * vector.
     * @param vectorBytes the non-null byte array to convert
     * @return a new {@link MutableDoubleRealVector} instance created from the byte array
     */
    @Nonnull
    public static MutableDoubleRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        return new MutableDoubleRealVector(DoubleRealVector.decodeDoubleBytes(vectorBytes));
    }
}
