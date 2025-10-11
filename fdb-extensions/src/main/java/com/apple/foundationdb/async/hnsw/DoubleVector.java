/*
 * DoubleVector.java
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

package com.apple.foundationdb.async.hnsw;

import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A vector class encoding a vector over double components. Conversion to {@link HalfVector} is supported and
 * memoized.
 */
public class DoubleVector extends AbstractVector {
    @Nonnull
    private final Supplier<HalfVector> toHalfVectorSupplier;

    public DoubleVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    public DoubleVector(@Nonnull final double[] data) {
        super(data);
        this.toHalfVectorSupplier = Suppliers.memoize(this::computeHalfVector);
    }

    public DoubleVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public DoubleVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public HalfVector toHalfVector() {
        return toHalfVectorSupplier.get();
    }

    @Nonnull
    @Override
    public DoubleVector toDoubleVector() {
        return this;
    }

    @Nonnull
    public HalfVector computeHalfVector() {
        return new HalfVector(data);
    }

    @Override
    public int precisionShift() {
        return 3;
    }

    @Nonnull
    @Override
    public Vector withData(@Nonnull final double[] data) {
        return new DoubleVector(data);
    }

    /**
     * Converts this {@link Vector} of {@code double} precision floating-point numbers into a byte array.
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
        vectorBytes[0] = (byte)precisionShift();
        for (int i = 0; i < getNumDimensions(); i ++) {
            final byte[] componentBytes = StorageAdapter.bytesFromLong(Double.doubleToLongBits(getComponent(i)));
            final int offset = 1 + (i << 3);
            vectorBytes[offset] = componentBytes[0];
            vectorBytes[offset + 1] = componentBytes[1];
            vectorBytes[offset + 2] = componentBytes[2];
            vectorBytes[offset + 3] = componentBytes[3];
            vectorBytes[offset + 4] = componentBytes[4];
            vectorBytes[offset + 5] = componentBytes[5];
            vectorBytes[offset + 6] = componentBytes[6];
            vectorBytes[offset + 7] = componentBytes[7];
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
}
