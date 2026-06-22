/*
 * RowMajorRealMatrix.java
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
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.Supplier;

public class RowMajorRealMatrix implements RealMatrix {
    @Nonnull
    private final double[][] data;
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::valueBasedHashCode);

    public RowMajorRealMatrix(@Nonnull final double[][] data) {
        Preconditions.checkArgument(data.length > 0);
        Preconditions.checkArgument(data[0].length > 0);
        this.data = data;
    }

    @Nonnull
    private double[][] getData() {
        return data;
    }

    @Override
    public int getNumRowDimensions() {
        return data.length;
    }

    @Override
    public int getNumColumnDimensions() {
        return data[0].length;
    }

    @Override
    public double getEntry(final int row, final int column) {
        return data[row][column];
    }

    @Nonnull
    public double[] getRow(final int row) {
        return data[row];
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix transpose() {
        int n = getNumRowDimensions();
        int m = getNumColumnDimensions();
        double[][] result = new double[m][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                result[j][i] = getEntry(i, j);
            }
        }
        return new RowMajorRealMatrix(result);
    }

    @Nonnull
    @Override
    public DoubleRealVector apply(@Nonnull final RealVector vector) {
        Preconditions.checkArgument(getNumColumnDimensions() == vector.getNumDimensions());
        final int n = getNumRowDimensions();
        final int m = getNumColumnDimensions();
        final double[] vData = vector.getData();
        final double[] result = new double[n];
        for (int i = 0; i < n; i++) {
            result[i] = RealVectorPrimitives.dot(data[i], vData, 0, m);
        }
        return new DoubleRealVector(result);
    }

    @Nonnull
    @Override
    public DoubleRealVector transposedApply(@Nonnull final RealVector vector) {
        Preconditions.checkArgument(getNumRowDimensions() == vector.getNumDimensions());
        final int n = getNumRowDimensions();
        final int m = getNumColumnDimensions();
        final double[] vData = vector.getData();
        final double[] result = new double[m];
        // result[j] = Σ_i data[i][j] * v[i] reorganized as input-stationary AXPYs over the
        // contiguous rows of the matrix: each iteration adds v[i] · row_i into the result.
        for (int i = 0; i < n; i++) {
            RealVectorPrimitives.multiplyAddInto(vData[i], data[i], result, 0, m);
        }
        return new DoubleRealVector(result);
    }

    /**
     * Multiplies this matrix by {@code otherMatrix} and returns the row-major product.
     *
     * <p>The implementation provides <em>two</em> SIMD fast paths plus a scalar triple-loop
     * fallback. This is asymmetric to {@link ColumnMajorRealMatrix#multiply}, which gets by
     * with a <em>single</em> universal AXPY path. The asymmetry is forced by storage layout —
     * not an oversight — so the rest of this comment explains why the two methods can't share a
     * shape.
     *
     * <p>Matrix multiplication has two SIMD-able decompositions:
     * <ol>
     *   <li><b>Output-stationary dot:</b> {@code C[i][j] = dot(A.row(i), B.col(j))}. Requires
     *       {@code A.row(i)} <em>and</em> {@code B.col(j)} to both be contiguous in memory.</li>
     *   <li><b>Input-stationary AXPY:</b> {@code C.row(i) += A[i][k] · B.row(k)}. Requires
     *       {@code C.row(i)} <em>and</em> {@code B.row(k)} to both be contiguous.</li>
     * </ol>
     *
     * <p>What's contiguous depends on layout: a row-major matrix exposes contiguous rows and
     * strided columns; a column-major matrix exposes contiguous columns and strided rows. So
     * for row-major {@code A} (this method), the decompositions split cleanly along {@code B}'s
     * layout — shape&nbsp;(1) needs column-major {@code B}, shape&nbsp;(2) needs row-major
     * {@code B} — and we need both specialized branches to keep every layout combination on a
     * SIMD path. The scalar fallback below is dead code today (no other {@link RealMatrix}
     * subtypes exist), but it preserves correctness for any future layout that doesn't surface
     * a contiguous strip on either axis.
     *
     * <p>Column-major {@code A} dodges the split: {@link ColumnMajorRealMatrix#multiply} writes
     * {@code C.col(j) += B[k][j] · A.col(k)}, where the contiguous arrays it cares about
     * ({@code A.col(k)} and {@code C.col(j)}) come from {@code A} and {@code C} themselves —
     * never from {@code B}. The only thing {@code B} contributes per inner step is a single
     * scalar {@code B[k][j]}, which a virtual {@code getEntry} call delivers cheaply regardless
     * of {@code B}'s layout. One universal AXPY path covers every {@code B} there.
     *
     * @param otherMatrix the right-hand operand; must satisfy
     *        {@code otherMatrix.getNumRowDimensions() == this.getNumColumnDimensions()}
     * @return a new {@link RowMajorRealMatrix} containing the product
     */
    @Nonnull
    @Override
    public RowMajorRealMatrix multiply(@Nonnull final RealMatrix otherMatrix) {
        Preconditions.checkArgument(getNumColumnDimensions() == otherMatrix.getNumRowDimensions());
        final int n = getNumRowDimensions();
        final int m = otherMatrix.getNumColumnDimensions();
        final int common = getNumColumnDimensions();
        final double[][] result = new double[n][m];
        if (otherMatrix instanceof final ColumnMajorRealMatrix other) {
            // Output-stationary dot: rows of `this` and columns of B are both contiguous.
            for (int i = 0; i < n; i++) {
                final double[] rowI = data[i];
                for (int j = 0; j < m; j++) {
                    result[i][j] = RealVectorPrimitives.dot(rowI, other.getColumn(j));
                }
            }
        } else if (otherMatrix instanceof final RowMajorRealMatrix other) {
            // Input-stationary AXPY: each output row is built by accumulating B's contiguous
            // rows scaled by entries of the corresponding row of `this`.
            for (int i = 0; i < n; i++) {
                final double[] target = result[i];
                final double[] aRow = data[i];
                for (int k = 0; k < common; k++) {
                    RealVectorPrimitives.multiplyAddInto(aRow[k], other.getRow(k), target, 0, m);
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < m; j++) {
                    for (int k = 0; k < common; k++) {
                        result[i][j] += getEntry(i, k) * otherMatrix.getEntry(k, j);
                    }
                }
            }
        }
        return new RowMajorRealMatrix(result);
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix subMatrix(final int startRow, final int lengthRow,
                                        final int startColumn, final int lengthColumn) {
        final double[][] subData = new double[lengthRow][lengthColumn];

        for (int i = startRow; i < startRow + lengthRow; i ++) {
            System.arraycopy(data[i], startColumn, subData[i - startRow], 0, lengthColumn);
        }

        return new RowMajorRealMatrix(subData);
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix toRowMajor() {
        return this;
    }

    @Nonnull
    @Override
    public double[][] getRowMajorData() {
        return getData();
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix toColumnMajor() {
        return new ColumnMajorRealMatrix(getColumnMajorData());
    }

    @Nonnull
    @Override
    public double[][] getColumnMajorData() {
        return transpose().getData();
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix quickTranspose() {
        return new ColumnMajorRealMatrix(getRowMajorData());
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix flipMajor() {
        return (ColumnMajorRealMatrix)RealMatrix.super.flipMajor();
    }

    @Override
    public final boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof RowMajorRealMatrix) {
            final RowMajorRealMatrix that = (RowMajorRealMatrix)o;
            return Arrays.deepEquals(data, that.data);
        }
        return valueEquals(o);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }
}
