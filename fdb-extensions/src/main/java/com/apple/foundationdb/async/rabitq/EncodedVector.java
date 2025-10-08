/*
 * EncodedVector.java
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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.async.hnsw.DoubleVector;
import com.apple.foundationdb.async.hnsw.HalfVector;
import com.apple.foundationdb.async.hnsw.Vector;

import javax.annotation.Nonnull;

@SuppressWarnings("checkstyle:MemberName")
public class EncodedVector implements Vector {
    @Nonnull
    private final int[] encoded;
    final double fAddEx;
    final double fRescaleEx;

    public EncodedVector(@Nonnull final int[] encoded, final double fAddEx, final double fRescaleEx) {
        this.encoded = encoded;
        this.fAddEx = fAddEx;
        this.fRescaleEx = fRescaleEx;
    }

    @Nonnull
    public int[] getEncodedData() {
        return encoded;
    }

    public double getfAddEx() {
        return fAddEx;
    }

    public double getfRescaleEx() {
        return fRescaleEx;
    }

    @Override
    public int getNumDimensions() {
        return encoded.length;
    }

    public int getEncodedComponent(final int dimension) {
        return encoded[dimension];
    }


    @Override
    public double getComponent(final int dimension) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public double[] getData() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public byte[] getRawData() {
        return new byte[0];
    }

    @Nonnull
    @Override
    public HalfVector toHalfVector() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public DoubleVector toDoubleVector() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double dot(@Nonnull final Vector other) {
        return 0;
    }

    @Override
    public double l2Norm() {
        return 0;
    }

    @Nonnull
    @Override
    public Vector normalize() {
        return null;
    }

    @Nonnull
    @Override
    public Vector add(@Nonnull final Vector other) {
        return null;
    }

    @Nonnull
    @Override
    public Vector add(final double scalar) {
        return null;
    }

    @Nonnull
    @Override
    public Vector subtract(@Nonnull final Vector other) {
        return null;
    }

    @Nonnull
    @Override
    public Vector subtract(final double scalar) {
        return null;
    }
}
