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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class RowMajorRealMatrix implements RealMatrix {
    @Nonnull
    private final double[][] data;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier;

    public RowMajorRealMatrix(@Nonnull final double[][] data) {
        this.data = data;
        this.hashCodeSupplier = Suppliers.memoize(this::valueBasedHashCode);
    }

    @Nonnull
    @Override
    public double[][] getData() {
        return data;
    }

    @Override
    public int getRowDimension() {
        return data.length;
    }

    @Override
    public int getColumnDimension() {
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
    public RealMatrix transpose() {
        int n = getRowDimension();
        int m = getColumnDimension();
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
    public RealMatrix multiply(@Nonnull final RealMatrix otherMatrix) {
        Preconditions.checkArgument(getColumnDimension() == otherMatrix.getRowDimension());
        final int n = getRowDimension();
        final int m = otherMatrix.getColumnDimension();
        final int common = getColumnDimension();
        double[][] result = new double[n][m];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                for (int k = 0; k < common; k++) {
                    result[i][j] += getEntry(i, k) * otherMatrix.getEntry(k, j);
                }
            }
        }
        return new RowMajorRealMatrix(result);
    }

    @Nonnull
    @Override
    public RealMatrix subMatrix(final int startRow, final int lengthRow, final int startColumn, final int lengthColumn) {
        final double[][] subData = new double[lengthRow][lengthColumn];

        for (int i = startRow; i < startRow + lengthRow; i ++) {
            System.arraycopy(data[i], startColumn, subData[i - startRow], 0, lengthColumn);
        }

        return new RowMajorRealMatrix(subData);
    }

    @Override
    public final boolean equals(final Object o) {
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
