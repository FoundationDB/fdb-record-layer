/*
 * ColumnMajorRealMatrix.java
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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class ColumnMajorRealMatrix implements RealMatrix {
    @Nonnull
    private final double[][] data;
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::valueBasedHashCode);

    public ColumnMajorRealMatrix(@Nonnull final double[][] data) {
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
        return data[0].length;
    }

    @Override
    public int getNumColumnDimensions() {
        return data.length;
    }

    @Override
    public double getEntry(final int row, final int column) {
        return data[column][row];
    }

    @Nonnull
    public double[] getColumn(final int column) {
        return data[column];
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix transpose() {
        int n = getNumRowDimensions();
        int m = getNumColumnDimensions();
        double[][] result = new double[n][m];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                result[i][j] = getEntry(i, j);
            }
        }
        return new ColumnMajorRealMatrix(result);
    }

    @Nonnull
    @Override
    public DoubleRealVector apply(@Nonnull final RealVector vector) {
        Preconditions.checkArgument(getNumColumnDimensions() == vector.getNumDimensions());
        final int n = getNumRowDimensions();
        final int m = getNumColumnDimensions();
        final double[] vData = vector.getData();
        final double[] result = new double[n];
        // result[i] = Σ_j data[j][i] * v[j] reorganized as input-stationary AXPYs over the
        // contiguous columns of the matrix: each iteration adds v[j] · col_j into the result.
        for (int j = 0; j < m; j++) {
            RealVectorPrimitives.multiplyAddInto(vData[j], data[j], result, 0, n);
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
        for (int j = 0; j < m; j++) {
            result[j] = RealVectorPrimitives.dot(data[j], vData, 0, n);
        }
        return new DoubleRealVector(result);
    }

    /**
     * Multiplies this matrix by {@code otherMatrix} and returns the column-major product.
     *
     * <p>Single universal AXPY fast path: each output column is built by accumulating
     * {@code this}'s contiguous columns scaled by the corresponding entries of {@code B}'s
     * column. Works regardless of {@code B}'s layout because the only thing {@code B}
     * contributes per inner step is a scalar from {@code getEntry}. See
     * {@link RowMajorRealMatrix#multiply} for the longer story on why row-major {@code A}
     * cannot share this shape and instead needs two specialized branches.
     *
     * @param otherMatrix the right-hand operand; must satisfy
     *        {@code otherMatrix.getNumRowDimensions() == this.getNumColumnDimensions()}
     * @return a new {@link ColumnMajorRealMatrix} containing the product
     */
    @Nonnull
    @Override
    public ColumnMajorRealMatrix multiply(@Nonnull final RealMatrix otherMatrix) {
        Preconditions.checkArgument(getNumColumnDimensions() == otherMatrix.getNumRowDimensions());
        final int n = getNumRowDimensions();
        final int m = otherMatrix.getNumColumnDimensions();
        final int common = getNumColumnDimensions();
        final double[][] result = new double[m][n];
        // C.col(j) += B[k][j] · A.col(k). Both A.col(k) (= data[k]) and C.col(j) (= result[j])
        // are contiguous; B[k][j] is a single virtual getEntry call amortized over an O(n) AXPY.
        for (int j = 0; j < m; j++) {
            final double[] target = result[j];
            for (int k = 0; k < common; k++) {
                RealVectorPrimitives.multiplyAddInto(otherMatrix.getEntry(k, j), data[k], target, 0, n);
            }
        }
        return new ColumnMajorRealMatrix(result);
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix subMatrix(final int startRow, final int lengthRow,
                                           final int startColumn, final int lengthColumn) {
        final double[][] subData = new double[lengthColumn][lengthRow];

        for (int j = startColumn; j < startColumn + lengthColumn; j ++) {
            System.arraycopy(data[j], startRow, subData[j - startColumn], 0, lengthRow);
        }

        return new ColumnMajorRealMatrix(subData);
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix toRowMajor() {
        return new RowMajorRealMatrix(getRowMajorData());
    }

    @Nonnull
    @Override
    public double[][] getRowMajorData() {
        return transpose().getData();
    }

    @Nonnull
    @Override
    public ColumnMajorRealMatrix toColumnMajor() {
        return this;
    }

    @Nonnull
    @Override
    public double[][] getColumnMajorData() {
        return getData();
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix quickTranspose() {
        return new RowMajorRealMatrix(getColumnMajorData());
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix flipMajor() {
        return (RowMajorRealMatrix)RealMatrix.super.flipMajor();
    }

    @Override
    public final boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof ColumnMajorRealMatrix) {
            final ColumnMajorRealMatrix that = (ColumnMajorRealMatrix)o;
            return Arrays.deepEquals(data, that.data);
        }
        return valueEquals(o);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }
}
