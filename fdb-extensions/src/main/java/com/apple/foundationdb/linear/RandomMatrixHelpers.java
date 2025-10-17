/*
 * RandomMatrixHelpers.java
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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class RandomMatrixHelpers {
    private RandomMatrixHelpers() {
        // nothing
    }

    @Nonnull
    public static RealMatrix randomOrthogonalMatrix(int seed, int dimension) {
        return decomposeMatrix(randomGaussianMatrix(seed, dimension, dimension));
    }

    @Nonnull
    public static RealMatrix randomGaussianMatrix(int seed, int rowDimension, int columnDimension) {
        final SecureRandom rng;
        try {
            rng = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        rng.setSeed(seed);

        final double[][] resultMatrix = new double[rowDimension][columnDimension];
        for (int row = 0; row < rowDimension; row++) {
            for (int column = 0; column < columnDimension; column++) {
                resultMatrix[row][column] = nextGaussian(rng);
            }
        }

        return new RowMajorRealMatrix(resultMatrix);
    }

    private static double nextGaussian(@Nonnull final SecureRandom rng) {
        double v1;
        double v2;
        double s;
        do {
            v1 = 2 * rng.nextDouble() - 1; // between -1 and 1
            v2 = 2 * rng.nextDouble() - 1; // between -1 and 1
            s = v1 * v1 + v2 * v2;
        } while (s >= 1 || s == 0);
        double multiplier = StrictMath.sqrt(-2 * StrictMath.log(s) / s);
        return v1 * multiplier;
    }

    @Nonnull
    private static RealMatrix decomposeMatrix(@Nonnull final RealMatrix matrix) {
        Preconditions.checkArgument(matrix.isSquare());

        final double[] rDiag = new double[matrix.getRowDimension()];
        final double[][] qrt = matrix.transpose().getData();

        for (int minor = 0; minor < matrix.getRowDimension(); minor++) {
            performHouseholderReflection(minor, qrt, rDiag);
        }

        return getQ(qrt, rDiag);
    }

    private static void performHouseholderReflection(final int minor, final double[][] qrt,
                                                     final double[] rDiag) {

        final double[] qrtMinor = qrt[minor];

        /*
         * Let x be the first column of the minor, and a^2 = |x|^2.
         * x will be in the positions qr[minor][minor] through qr[m][minor].
         * The first column of the transformed minor will be (a,0,0,..)'
         * The sign of a is chosen to be opposite to the sign of the first
         * component of x. Let's find a:
         */
        double xNormSqr = 0;
        for (int row = minor; row < qrtMinor.length; row++) {
            final double c = qrtMinor[row];
            xNormSqr += c * c;
        }
        final double a = (qrtMinor[minor] > 0) ? -Math.sqrt(xNormSqr) : Math.sqrt(xNormSqr);
        rDiag[minor] = a;

        if (a != 0.0) {

            /*
             * Calculate the normalized reflection vector v and transform
             * the first column. We know the norm of v beforehand: v = x-ae
             * so |v|^2 = <x-ae,x-ae> = <x,x>-2a<x,e>+a^2<e,e> =
             * a^2+a^2-2a<x,e> = 2a*(a - <x,e>).
             * Here <x, e> is now qr[minor][minor].
             * v = x-ae is stored in the column at qr:
             */
            qrtMinor[minor] -= a; // now |v|^2 = -2a*(qr[minor][minor])

            /*
             * Transform the rest of the columns of the minor:
             * They will be transformed by the matrix H = I-2vv'/|v|^2.
             * If x is a column vector of the minor, then
             * Hx = (I-2vv'/|v|^2)x = x-2vv'x/|v|^2 = x - 2<x,v>/|v|^2 v.
             * Therefore, the transformation is easily calculated by
             * subtracting the column vector (2<x,v>/|v|^2)v from x.
             *
             * Let 2<x,v>/|v|^2 = alpha. From above, we have
             * |v|^2 = -2a*(qr[minor][minor]), so
             * alpha = -<x,v>/(a*qr[minor][minor])
             */
            for (int col = minor + 1; col < qrt.length; col++) {
                final double[] qrtCol = qrt[col];
                double alpha = 0;
                for (int row = minor; row < qrtCol.length; row++) {
                    alpha -= qrtCol[row] * qrtMinor[row];
                }
                alpha /= a * qrtMinor[minor];

                // Subtract the column vector alpha*v from x.
                for (int row = minor; row < qrtCol.length; row++) {
                    qrtCol[row] -= alpha * qrtMinor[row];
                }
            }
        }
    }

    /**
     * Returns the transpose of the matrix Q of the decomposition.
     * <p>Q is an orthogonal matrix</p>
     * @return the Q matrix
     */
    @Nonnull
    private static RealMatrix getQ(final double[][] qrt, final double[] rDiag) {
        final int m = qrt.length;
        double[][] q = new double[m][m];

        for (int minor = m - 1; minor >= 0; minor--) {
            final double[] qrtMinor = qrt[minor];
            q[minor][minor] = 1.0d;
            if (qrtMinor[minor] != 0.0) {
                for (int col = minor; col < m; col++) {
                    double alpha = 0;
                    for (int row = minor; row < m; row++) {
                        alpha -= q[row][col] * qrtMinor[row];
                    }
                    alpha /= rDiag[minor] * qrtMinor[minor];

                    for (int row = minor; row < m; row++) {
                        q[row][col] += -alpha * qrtMinor[row];
                    }
                }
            }
        }
        return new RowMajorRealMatrix(q);
    }
}
