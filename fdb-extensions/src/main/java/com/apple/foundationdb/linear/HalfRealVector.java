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
 * A vector class encoding a vector over half components. Conversion to {@link DoubleRealVector} is supported and
 * memoized.
 */
public class HalfRealVector extends AbstractRealVector {
    public HalfRealVector(@Nonnull final Half[] halfData) {
        this(computeDoubleData(halfData));
    }

    public HalfRealVector(@Nonnull final double[] data) {
        super(truncateDoubleData(data));
    }

    public HalfRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public HalfRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return this;
    }

    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return new DoubleRealVector(data);
    }

    @Nonnull
    public DoubleRealVector computeDoubleVector() {
        return new DoubleRealVector(data);
    }

    @Nonnull
    @Override
    public RealVector withData(@Nonnull final double[] data) {
        return new HalfRealVector(data);
    }

    /**
     * Converts this {@link RealVector} of {@link Half} precision floating-point numbers into a byte array.
     * <p>
     * This method iterates through the input vector, converting each {@link Half} element into its 16-bit short
     * representation. It then serializes this short into two bytes, placing them sequentially into the resulting byte
     * array. The final array's length will be {@code 2 * vector.size()}.
     * @return a new byte array representing the serialized vector data. This array is never null.
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

    @Nonnull
    private static double[] computeDoubleData(@Nonnull Half[] halfData) {
        double[] result = new double[halfData.length];
        for (int i = 0; i < halfData.length; i++) {
            result[i] = halfData[i].doubleValue();
        }
        return result;
    }

    @Nonnull
    private static double[] truncateDoubleData(@Nonnull double[] doubleData) {
        double[] result = new double[doubleData.length];
        for (int i = 0; i < doubleData.length; i++) {
            result[i] = Half.valueOf(doubleData[i]).doubleValue();
        }
        return result;
    }

    /**
     * Creates a {@link HalfRealVector} from a byte array.
     * <p>
     * This method interprets the input byte array as a sequence of 16-bit half-precision floating-point numbers. Each
     * consecutive pair of bytes is converted into a {@code Half} value, which then becomes a component of the resulting
     * vector.
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
