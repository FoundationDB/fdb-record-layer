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
import com.apple.foundationdb.async.hnsw.EncodingHelpers;
import com.apple.foundationdb.async.hnsw.HalfVector;
import com.apple.foundationdb.async.hnsw.Vector;
import com.apple.foundationdb.async.hnsw.VectorType;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.Supplier;

@SuppressWarnings("checkstyle:MemberName")
public class EncodedVector implements Vector {
    private static final double EPS0 = 1.9d;

    @Nonnull
    private final int[] encoded;
    private final double fAddEx;
    private final double fRescaleEx;
    private final double fErrorEx;

    @Nonnull
    private final Supplier<Integer> hashCodeSupplier;
    private final Supplier<double[]> dataSupplier;
    private final Supplier<byte[]> rawDataSupplier;

    public EncodedVector(final int numExBits, @Nonnull final int[] encoded, final double fAddEx, final double fRescaleEx,
                         final double fErrorEx) {
        this.encoded = encoded;
        this.fAddEx = fAddEx;
        this.fRescaleEx = fRescaleEx;
        this.fErrorEx = fErrorEx;

        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
        this.dataSupplier = Suppliers.memoize(() -> computeData(numExBits));
        this.rawDataSupplier = Suppliers.memoize(() -> computeRawData(numExBits));
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
    public final boolean equals(final Object o) {
        if (!(o instanceof EncodedVector)) {
            return false;
        }

        final EncodedVector that = (EncodedVector)o;
        return Double.compare(fAddEx, that.fAddEx) == 0 &&
                Double.compare(fRescaleEx, that.fRescaleEx) == 0 &&
                Double.compare(fErrorEx, that.fErrorEx) == 0 &&
                Arrays.equals(encoded, that.encoded);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    public int computeHashCode() {
        int result = Arrays.hashCode(encoded);
        result = 31 * result + Double.hashCode(fAddEx);
        result = 31 * result + Double.hashCode(fRescaleEx);
        result = 31 * result + Double.hashCode(fErrorEx);
        return result;
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
    public double[] computeData(final int numExBits) {
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
    protected byte[] computeRawData(final int numExBits) {
        int numBits = getNumDimensions() * (numExBits + 1); // congruency with paper
        final int length = 25 +        // RABITQ (byte) + fAddEx (double) + fRescaleEx (double) + fErrorEx (double)
                (numBits - 1) / 8 + 1; // snap byte array to the smallest length fitting all bits
        final byte[] result = new byte[length];
        result[0] = (byte)VectorType.RABITQ.ordinal();
        EncodingHelpers.fromLongIntoBytes(Double.doubleToLongBits(fAddEx), result, 1);
        EncodingHelpers.fromLongIntoBytes(Double.doubleToLongBits(fRescaleEx), result, 9);
        EncodingHelpers.fromLongIntoBytes(Double.doubleToLongBits(fErrorEx), result, 17);
        packEncodedComponents(numExBits, result, 25);
        return result;
    }

    private void packEncodedComponents(final int numExBits, @Nonnull byte[] bytes, int offset) {
        // big-endian
        final int bitsPerComponent = numExBits + 1; // congruency with paper
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
    @Override
    public HalfVector toHalfVector() {
        return new HalfVector(getData());
    }

    @Nonnull
    @Override
    public DoubleVector toDoubleVector() {
        return new DoubleVector(getData());
    }

    @Nonnull
    public static EncodedVector fromBytes(@Nonnull byte[] bytes, int offset, int numDimensions, int numExBits) {
        final double fAddEx = Double.longBitsToDouble(EncodingHelpers.longFromBytes(bytes, offset));
        final double fRescaleEx = Double.longBitsToDouble(EncodingHelpers.longFromBytes(bytes, offset + 8));
        final double fErrorEx = Double.longBitsToDouble(EncodingHelpers.longFromBytes(bytes, offset + 16));
        final int[] components = unpackComponents(bytes, offset + 24, numDimensions, numExBits);
        return new EncodedVector(numExBits, components, fAddEx, fRescaleEx, fErrorEx);
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
}
