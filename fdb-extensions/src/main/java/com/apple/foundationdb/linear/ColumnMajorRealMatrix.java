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
    private final Supplier<Integer> hashCodeSupplier;

    public ColumnMajorRealMatrix(@Nonnull final double[][] data) {
        Preconditions.checkArgument(data.length > 0);
        Preconditions.checkArgument(data[0].length > 0);
        this.data = data;
        this.hashCodeSupplier = Suppliers.memoize(this::valueBasedHashCode);
    }

    @Nonnull
    private double[][] getData() {
        return data;
    }

    @Override
    public int getRowDimension() {
        return data[0].length;
    }

    @Override
    public int getColumnDimension() {
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
        int n = getRowDimension();
        int m = getColumnDimension();
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
    public ColumnMajorRealMatrix multiply(@Nonnull final RealMatrix otherMatrix) {
        Preconditions.checkArgument(getColumnDimension() == otherMatrix.getRowDimension());
        int n = getRowDimension();
        int m = otherMatrix.getColumnDimension();
        int common = getColumnDimension();
        double[][] result = new double[m][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                for (int k = 0; k < common; k++) {
                    result[j][i] += getEntry(i, k) * otherMatrix.getEntry(k, j);
                }
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
        return new RowMajorRealMatrix(data);
    }

    @Nonnull
    @Override
    public RowMajorRealMatrix flipMajor() {
        return (RowMajorRealMatrix)RealMatrix.super.flipMajor();
    }

    @Override
    public final boolean equals(final Object o) {
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
