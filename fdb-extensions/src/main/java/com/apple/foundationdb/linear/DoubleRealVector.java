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

import com.apple.foundationdb.async.hnsw.EncodingHelpers;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A vector class encoding a vector over double components. Conversion to {@link HalfRealVector} is supported and
 * memoized.
 */
public class DoubleRealVector extends AbstractRealVector {
    @Nonnull
    private final Supplier<HalfRealVector> toHalfVectorSupplier;

    public DoubleRealVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    public DoubleRealVector(@Nonnull final double[] data) {
        super(data);
        this.toHalfVectorSupplier = Suppliers.memoize(this::computeHalfVector);
    }

    public DoubleRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public DoubleRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return toHalfVectorSupplier.get();
    }

    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return this;
    }

    @Nonnull
    public HalfRealVector computeHalfVector() {
        return new HalfRealVector(data);
    }

    @Nonnull
    @Override
    public RealVector withData(@Nonnull final double[] data) {
        return new DoubleRealVector(data);
    }

    /**
     * Converts this {@link RealVector} of {@code double} precision floating-point numbers into a byte array.
     * <p>
     * This method iterates through the input vector, converting each {@code double} element into its 16-bit short
     * representation. It then serializes this short into eight bytes, placing them sequentially into the resulting byte
     * array. The final array's length will be {@code 8 * vector.size()}.
     * @return a new byte array representing the serialized vector data. This array is never null.
     */
    @Nonnull
    @Override
    protected byte[] computeRawData() {
        final byte[] vectorBytes = new byte[1 + 8 * getNumDimensions()];
        vectorBytes[0] = (byte)VectorType.DOUBLE.ordinal();
        for (int i = 0; i < getNumDimensions(); i ++) {
            EncodingHelpers.fromLongIntoBytes(Double.doubleToLongBits(getComponent(i)), vectorBytes,
                    1 + (i << 3));
        }
        return vectorBytes;
    }

    @Nonnull
    private static double[] computeDoubleData(@Nonnull Double[] doubleData) {
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
     * @param offset to the first byte containing the vector-specific data
     * @return a new {@link DoubleRealVector} instance created from the byte array
     */
    @Nonnull
    public static DoubleRealVector fromBytes(@Nonnull final byte[] vectorBytes, int offset) {
        final int numDimensions = (vectorBytes.length - offset) >> 3;
        final double[] vectorComponents = new double[numDimensions];
        for (int i = 0; i < numDimensions; i ++) {
            vectorComponents[i] = Double.longBitsToDouble(EncodingHelpers.longFromBytes(vectorBytes, offset + (i << 3)));
        }
        return new DoubleRealVector(vectorComponents);
    }
}
