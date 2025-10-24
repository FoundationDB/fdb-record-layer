/*
 * QRDecomposition.java
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
import java.util.function.Supplier;

/**
 * Provides a static method to compute the QR decomposition of a matrix.
 * <p>
 * This class is a utility class and cannot be instantiated. The decomposition
 * is performed using the Householder reflection method. The result of the
 * decomposition of a matrix A is an orthogonal matrix Q and an upper-triangular
 * matrix R such that {@code A = QR}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class QRDecomposition {
    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private QRDecomposition() {
        // nothing
    }

    /**
     * Decomposes a square matrix A into an orthogonal matrix Q and an upper
     * triangular matrix R, such that A = QR.
     * <p>
     * This implementation uses the Householder reflection method to perform the
     * decomposition. The resulting Q and R matrices are not computed immediately but are
     * available through suppliers within the returned {@link Result} object, allowing for
     * lazy evaluation. The decomposition is performed on the transpose of the input matrix
     * for efficiency.
     *
     * @param matrix the square matrix to decompose. Must not be null.
     *
     * @return a {@link Result} object containing suppliers for the Q and R matrices.
     *
     * @throws IllegalArgumentException if the provided {@code matrix} is not square.
     */
    @Nonnull
    public static Result decomposeMatrix(@Nonnull final RealMatrix matrix) {
        Preconditions.checkArgument(matrix.isSquare());

        final double[] rDiagonal = new double[matrix.getRowDimension()];
        final double[][] qrt = matrix.transpose().getRowMajorData();

        for (int minor = 0; minor < matrix.getRowDimension(); minor++) {
            performHouseholderReflection(minor, qrt, rDiagonal);
        }

        return new Result(() -> getQ(qrt, rDiagonal), () -> getR(qrt, rDiagonal));
    }

    /**
     * Performs a Householder reflection on a minor of a matrix.
     * <p>
     * This method is a core step in QR decomposition. It transforms the {@code minor}-th
     * column of the {@code qrt} matrix into a vector with a single non-zero element {@code a}
     * (which becomes the new diagonal element of the R matrix), and applies the same
     * transformation to the remaining columns of the minor. The transformation is done in-place.
     * </p>
     * The reflection is defined by a matrix {@code H = I - 2vv'/|v|^2}, where the vector {@code v}
     * is derived from the {@code minor}-th column of the matrix.
     *
     * @param minor the index of the minor matrix to be transformed.
     * @param qrt the matrix to be transformed in-place. On exit, this matrix is
     *        updated to reflect the Householder transformation.
     * @param rDiagonal an array where the diagonal element of the R matrix for the
     *        current {@code minor} will be stored.
     */
    private static void performHouseholderReflection(final int minor, final double[][] qrt,
                                                     final double[] rDiagonal) {

        final double[] qrtMinor = qrt[minor];

        /*
         * Let x be the first column of the minor, and a^2 = |x|^2.
         * x will be in the positions qr[minor][minor] through qr[m][minor].
         * The first column of the transformed minor will be (a, 0, 0, ...)'
         * The sign of "a" is chosen to be opposite to the sign of the first
         * component of x. Let's find "a":
         */
        double xNormSqr = 0;
        for (int row = minor; row < qrtMinor.length; row++) {
            final double c = qrtMinor[row];
            xNormSqr += c * c;
        }
        final double a = (qrtMinor[minor] > 0) ? -Math.sqrt(xNormSqr) : Math.sqrt(xNormSqr);
        rDiagonal[minor] = a;

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
             * Hx = (I-2vv'/|v|^2)x = x-2v*v'x/|v|^2 = x - 2<x,v>/|v|^2 v.
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
     * Returns the matrix {@code Q} of the decomposition where {@code Q} is an orthogonal matrix.
     * @return the {@code Q} matrix
     */
    @Nonnull
    private static RealMatrix getQ(final double[][] qrt, final double[] rDiagonal) {
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
                    alpha /= rDiagonal[minor] * qrtMinor[minor];

                    for (int row = minor; row < m; row++) {
                        q[row][col] += -alpha * qrtMinor[row];
                    }
                }
            }
        }
        return new RowMajorRealMatrix(q);
    }

    /**
     * Constructs the upper-triangular R matrix from a QRT decomposition's packed storage.
     * <p>
     * This is a helper method that reconstructs the {@code R} matrix from the compact
     * representation used by some QR decomposition algorithms. The resulting matrix @{code R}
     * is upper-triangular.
     * </p><p>
     * The upper-triangular elements (where row index {@code i < j}) are extracted
     * from the {@code qrt} matrix at transposed indices ({@code qrt[j][i]}). The
     * diagonal elements (where {@code i == j}) are taken from the {@code rDiagonal}
     * array. All lower-triangular elements (where {@code i > j}) are set to 0.0.
     * </p>
     *
     * @param qrt The packed QRT decomposition data. The strict upper-triangular
     *        part of {@code R} is stored in this matrix.
     * @param rDiagonal An array containing the diagonal elements of the R matrix.
     *
     * @return The reconstructed upper-triangular R matrix as a {@link RealMatrix}.
     */
    @Nonnull
    private static RealMatrix getR(final double[][] qrt, final double[] rDiagonal) {
        final int m = qrt.length; // square in this helper
        final double[][] r = new double[m][m];

        // R is upper-triangular. With Commons-Math style storage:
        // - for i < j, R[i][j] is in qrt[j][i]
        // - for i == j, R[i][i] comes from rDiag[i]
        // - for i > j, R[i][j] = 0
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < m; j++) {
                if (i < j) {
                    r[i][j] = qrt[j][i];
                } else if (i == j) {
                    r[i][j] = rDiagonal[i];
                } else {
                    r[i][j] = 0.0;
                }
            }
        }
        return new RowMajorRealMatrix(r);
    }

    @SuppressWarnings("checkstyle:MemberName")
    public static class Result {
        @Nonnull
        private final Supplier<RealMatrix> qSupplier;
        @Nonnull
        private final Supplier<RealMatrix> rSupplier;

        public Result(@Nonnull final Supplier<RealMatrix> qSupplier, @Nonnull final Supplier<RealMatrix> rSupplier) {
            this.qSupplier = qSupplier;
            this.rSupplier = rSupplier;
        }

        @Nonnull
        RealMatrix getQ() {
            return qSupplier.get();
        }

        @Nonnull
        RealMatrix getR() {
            return rSupplier.get();
        }
    }
}
