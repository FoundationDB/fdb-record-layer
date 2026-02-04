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
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * A vector class encoding a vector over float components.
 */
public class FloatRealVector extends AbstractRealVector {
    @Nonnull
    private final Supplier<HalfRealVector> toHalfRealVectorSupplier;

    public FloatRealVector(@Nonnull final Float[] floatData) {
        this(computeDoubleData(floatData));
    }

    public FloatRealVector(@Nonnull final float[] floatData) {
        this(computeDoubleData(floatData));
    }

    @SuppressWarnings("this-escape")
    public FloatRealVector(@Nonnull final double[] data) {
        super(truncateDoubleData(data));
        this.toHalfRealVectorSupplier = Suppliers.memoize(this::computeHalfRealVector);
    }

    public FloatRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public FloatRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return toHalfRealVectorSupplier.get();
    }

    @Nonnull
    public HalfRealVector computeHalfRealVector() {
        return new HalfRealVector(data);
    }

    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return this;
    }

    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return new DoubleRealVector(data);
    }

    @Nonnull
    @Override
    public RealVector withData(@Nonnull final double[] data) {
        return new FloatRealVector(data);
    }

    /**
     * Converts this {@link RealVector} of single precision floating-point numbers into a byte array.
     * <p>
     * This method iterates through the input vector, converting each {@link Half} element into its 16-bit short
     * representation. It then serializes this short into two bytes, placing them sequentially into the resulting byte
     * array. The final array's length will be {@code 2 * vector.size()}.
     * @return a new byte array representing the serialized vector data. This array is never null.
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
     * Returns a vector whose components are all zero.
     * @param numDimensions number of dimensions
     * @return a vector whose components are all zero
     */
    @Nonnull
    public static FloatRealVector zeroVector(final int numDimensions) {
        return new FloatRealVector(new float[numDimensions]);
    }

    @Nonnull
    private static double[] computeDoubleData(@Nonnull Float[] floatData) {
        double[] result = new double[floatData.length];
        for (int i = 0; i < floatData.length; i++) {
            result[i] = floatData[i];
        }
        return result;
    }

    @Nonnull
    private static double[] computeDoubleData(@Nonnull float[] floatData) {
        double[] result = new double[floatData.length];
        for (int i = 0; i < floatData.length; i++) {
            result[i] = floatData[i];
        }
        return result;
    }

    @Nonnull
    private static double[] truncateDoubleData(@Nonnull double[] doubleData) {
        double[] result = new double[doubleData.length];
        for (int i = 0; i < doubleData.length; i++) {
            result[i] = (float)doubleData[i];
        }
        return result;
    }

    /**
     * Creates a {@link FloatRealVector} from a byte array.
     * <p>
     * This method interprets the input byte array as a sequence of 16-bit half-precision floating-point numbers. Each
     * consecutive pair of bytes is converted into a {@code Half} value, which then becomes a component of the resulting
     * vector.
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
