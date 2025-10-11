/*
 * HalfVector.java
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

import com.christianheina.langx.half4j.Half;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A vector class encoding a vector over half components. Conversion to {@link DoubleVector} is supported and
 * memoized.
 */
public class HalfVector extends AbstractVector {
    @Nonnull
    private final Supplier<DoubleVector> toDoubleVectorSupplier;

    public HalfVector(@Nonnull final Half[] halfData) {
        this(computeDoubleData(halfData));
    }

    public HalfVector(@Nonnull final double[] data) {
        super(data);
        this.toDoubleVectorSupplier = Suppliers.memoize(this::computeDoubleVector);
    }

    public HalfVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    public HalfVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    @Nonnull
    @Override
    public com.apple.foundationdb.async.hnsw.HalfVector toHalfVector() {
        return this;
    }

    @Nonnull
    @Override
    public DoubleVector toDoubleVector() {
        return toDoubleVectorSupplier.get();
    }

    @Nonnull
    public DoubleVector computeDoubleVector() {
        return new DoubleVector(data);
    }

    @Override
    public int precisionShift() {
        return 1;
    }

    @Nonnull
    @Override
    public Vector withData(@Nonnull final double[] data) {
        return new HalfVector(data);
    }

    /**
     * Converts this {@link Vector} of {@link Half} precision floating-point numbers into a byte array.
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
        vectorBytes[0] = (byte)precisionShift();
        for (int i = 0; i < getNumDimensions(); i ++) {
            final byte[] componentBytes = StorageAdapter.bytesFromShort(Half.halfToShortBits(Half.valueOf(getComponent(i))));
            final int offset = 1 + (i << 1);
            vectorBytes[offset] = componentBytes[0];
            vectorBytes[offset + 1] = componentBytes[1];
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
}
