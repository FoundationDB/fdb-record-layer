/*
 * RTreeHilbertCurveHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Utility class to compute the Hilbert value from n-dimensional points.
 */
@API(API.Status.EXPERIMENTAL)
public class RTreeHilbertCurveHelpers {
    private static final long nullReplacement = Long.MIN_VALUE;

    private RTreeHilbertCurveHelpers() {
        // do not instantiate
    }

    public static BigInteger hilbertValue(@Nonnull final RTree.Point point) {
        int numBits = 64;
        final long[] shiftedCoordinates = new long[point.getNumDimensions()];
        for (int i = 0; i < point.getNumDimensions(); i++) {
            Long coordinateAsLong = (Long)point.getCoordinateAsNumber(i);
            final long coordinate = coordinateAsLong == null ? nullReplacement : coordinateAsLong;
            shiftedCoordinates[i] = shiftCoordinate(coordinate);
        }
        return toIndex(numBits, transposedIndex(numBits, shiftedCoordinates));
    }

    private static long shiftCoordinate(long coordinate) {
        return coordinate < 0
               ? (Long.MAX_VALUE - (-coordinate - 1L))
               : (coordinate | (1L << 63));
    }

    //
    // The following code is a modified version of logic out of the class HilbertCurve
    // at https://github.com/davidmoten/hilbert-curve. That code is open-sourced under the Apache license 2.0
    // https://github.com/davidmoten/hilbert-curve/blob/master/LICENSE. The code has been adapted to compute
    // unique Hilbert values for all Java long coordinates in an n-dimensional space. (-2^63 ... 2^63 - 1).
    //

    private static BigInteger toIndex(int numBits, long... transposedIndexes) {
        int length = numBits * transposedIndexes.length;
        byte[] b = new byte[length / 8 + 1];
        int bIndex = length - 1;
        long mask = 1L << (numBits - 1);
        for (int i = 0; i < numBits; i++) {
            for (final long transposedIndex : transposedIndexes) {
                if ((transposedIndex & mask) != 0) {
                    b[b.length - 1 - bIndex / 8] |= (byte)(1 << (bIndex % 8));
                }
                bIndex--;
            }
            mask >>>= 1;
        }
        // b is expected to be BigEndian
        return new BigInteger(1, b);
    }

    private static long[] transposedIndex(int numBits, long... unsignedPoints) {
        final long m = 1L << (numBits - 1);
        final int n = unsignedPoints.length; // n: Number of dimensions
        final long[] x = Arrays.copyOf(unsignedPoints, n);
        long p;
        long q;
        long t;
        int i;
        // Inverse undo
        for (q = m; q != 1; q >>>= 1) {
            p = q - 1;
            for (i = 0; i < n; i++) {
                if ((x[i] & q) != 0) {
                    x[0] ^= p; // invert
                } else {
                    t = (x[0] ^ x[i]) & p;
                    x[0] ^= t;
                    x[i] ^= t;
                }
            }
        } // exchange
        // Gray encode
        for (i = 1; i < n; i++) {
            x[i] ^= x[i - 1];
        }
        t = 0;
        for (q = m; q != 1; q >>>= 1) {
            if ((x[n - 1] & q) != 0) {
                t ^= q - 1;
            }
        }
        for (i = 0; i < n; i++) {
            x[i] ^= t;
        }

        return x;
    }
}
