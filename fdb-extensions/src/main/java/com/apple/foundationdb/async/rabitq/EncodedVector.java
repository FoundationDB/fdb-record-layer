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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

@SuppressWarnings("checkstyle:MemberName")
public class EncodedVector implements Vector {
    private static final double EPS0 = 1.9d;

    private final int numExBits;
    @Nonnull
    private final int[] encoded;
    final double fAddEx;
    final double fRescaleEx;
    final double fErrorEx;

    final Supplier<double[]> dataSupplier;
    final Supplier<byte[]> rawDataSupplier;

    public EncodedVector(final int numExBits, @Nonnull final int[] encoded, final double fAddEx, final double fRescaleEx,
                         final double fErrorEx) {
        this.numExBits = numExBits;
        this.encoded = encoded;
        this.fAddEx = fAddEx;
        this.fRescaleEx = fRescaleEx;
        this.fErrorEx = fErrorEx;

        this.dataSupplier = Suppliers.memoize(this::computeData);
        this.rawDataSupplier = Suppliers.memoize(this::computeRawData);
    }

    public int getNumExBits() {
        return numExBits;
    }

    @Nonnull
    public int[] getEncodedData() {
        return encoded;
    }

    public double getAddEx() {
        return fAddEx;
    }

    public double getRescaleEx() {
        return fRescaleEx;
    }

    public double getErrorEx() {
        return fErrorEx;
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
        return getData()[dimension];
    }

    @Nonnull
    @Override
    public double[] getData() {
        return dataSupplier.get();
    }

    @Nonnull
    @Override
    public Vector withData(@Nonnull final double[] data) {
        // we explicitly make this a normal double vector instead of an encoded vector
        return new DoubleVector(data);
    }

    @Nonnull
    public double[] computeData() {
        final int numDimensions = getNumDimensions();
        final double cB = (1 << numExBits) - 0.5;
        final Vector z = new DoubleVector(encoded).subtract(cB);
        final double normZ = z.l2Norm();

        // Solve for rho and Δx from fErrorEx and fRescaleEx
        final double A = (2.0 * EPS0) / Math.sqrt(numDimensions - 1.0);
        final double denom = A * Math.abs(fRescaleEx) * normZ;
        Verify.verify(denom != 0.0, "degenerate parameters: denom == 0");

        final double r = Math.min(1.0, (2.0 * Math.abs(fErrorEx)) / denom); // clamp for safety
        final double rho = Math.sqrt(Math.max(0.0, 1.0 - r * r));

        final double deltaX = -0.5 * fRescaleEx * rho;

        // ô = c + Δx * r
        return z.multiply(deltaX).getData();
    }

    @Nonnull
    @Override
    public byte[] getRawData() {
        return rawDataSupplier.get();
    }

    @Nonnull
    protected byte[] computeRawData() {
        int totalBits = getNumDimensions() * (numExBits + 1); // congruency with paper

        byte[] packedComponents = new byte[(totalBits - 1) / 8 + 1];
        packEncodedComponents(packedComponents);
        final int[] unpacked = unpackComponents(packedComponents, 0, getNumDimensions(), getNumExBits());
        return packedComponents;
    }

    private void packEncodedComponents(@Nonnull byte[] bytes) {
        // big-endian
        final int bitsPerComponent = getNumExBits() + 1; // congruency with paper
        int offset = 0;
        int remainingBitsInByte = 8;
        for (int i = 0; i < getNumDimensions(); i++) {
            final int component = getEncodedComponent(i);
            int remainingBitsInComponent = bitsPerComponent;

            while (remainingBitsInComponent > 0) {
                final int remainingMask = (1 << remainingBitsInComponent) - 1;
                final int remainingComponent = component & remainingMask;

                if (remainingBitsInComponent <= remainingBitsInByte) {
                    bytes[offset] = (byte)((int)bytes[offset] | (remainingComponent << (remainingBitsInByte - remainingBitsInComponent)));
                    remainingBitsInByte -= remainingBitsInComponent;
                    if (remainingBitsInByte == 0) {
                        remainingBitsInByte = 8;
                        offset ++;
                    }
                    break;
                }

                // remainingBitsInComponent > bitOffset
                bytes[offset] = (byte)((int)bytes[offset] | (remainingComponent >> (remainingBitsInComponent - remainingBitsInByte)));
                remainingBitsInComponent -= remainingBitsInByte;
                remainingBitsInByte = 8;
                offset ++;
            }
        }
    }

    @Nonnull
    private static int[] unpackComponents(@Nonnull byte[] bytes, int offset, int numDimensions, int numExBits) {
        int[] result = new int[numDimensions];

        // big-endian
        final int bitsPerComponent = numExBits + 1; // congruency with paper
        int remainingBitsInByte = 8;
        for (int i = 0; i < numDimensions; i++) {
            int remainingBitsForComponent = bitsPerComponent;

            while (remainingBitsForComponent > 0) {
                final int mask = (1 << remainingBitsInByte) - 1;
                int maskedByte = bytes[offset] & mask;

                if (remainingBitsForComponent <= remainingBitsInByte) {
                    result[i] |= maskedByte >> (remainingBitsInByte - remainingBitsForComponent);

                    remainingBitsInByte -= remainingBitsForComponent;
                    if (remainingBitsInByte == 0) {
                        remainingBitsInByte = 8;
                        offset++;
                    }
                    break;
                }

                // remainingBitsForComponent > remainingBitsInByte
                result[i] |= maskedByte << remainingBitsForComponent - remainingBitsInByte;
                remainingBitsForComponent -= remainingBitsInByte;
                remainingBitsInByte = 8;
                offset++;
            }
        }
        return result;
    }

    @Nonnull
    @Override
    public HalfVector toHalfVector() {
        return new HalfVector(getData());
    }

    @Nonnull
    @Override
    public DoubleVector toDoubleVector() {
        return new DoubleVector(getData());
    }
}
