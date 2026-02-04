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
 * A vector class encoding a vector over double components. Conversion to {@link HalfRealVector} is supported and
 * memoized.
 */
public class DoubleRealVector extends AbstractRealVector {
    @Nonnull
    private final Supplier<HalfRealVector> toHalfVectorSupplier;
    @Nonnull
    private final Supplier<FloatRealVector> toFloatVectorSupplier;

    public DoubleRealVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    public DoubleRealVector(@Nonnull final double[] data) {
        super(data);
        this.toHalfVectorSupplier = Suppliers.memoize(this::computeHalfRealVector);
        this.toFloatVectorSupplier = Suppliers.memoize(this::computeFloatRealVector);
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
    public HalfRealVector computeHalfRealVector() {
        return new HalfRealVector(data);
    }

    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return toFloatVectorSupplier.get();
    }

    @Nonnull
    private FloatRealVector computeFloatRealVector() {
        return new FloatRealVector(data);
    }

    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return this;
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
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte)VectorType.DOUBLE.ordinal());
        for (int i = 0; i < getNumDimensions(); i ++) {
            buffer.putDouble(getComponent(i));
        }
        return vectorBytes;
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
     * @return a new {@link DoubleRealVector} instance created from the byte array
     */
    @Nonnull
    public static DoubleRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        Verify.verify(buffer.get() == VectorType.DOUBLE.ordinal());
        final int numDimensions = vectorBytes.length >> 3;
        final double[] vectorComponents = new double[numDimensions];
        for (int i = 0; i < numDimensions; i ++) {
            vectorComponents[i] = buffer.getDouble();
        }
        return new DoubleRealVector(vectorComponents);
    }
}
