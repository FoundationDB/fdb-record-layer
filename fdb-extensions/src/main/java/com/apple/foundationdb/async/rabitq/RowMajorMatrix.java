/*
 * RowMajorMatrix.java
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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class RowMajorMatrix implements Matrix {
    @Nonnull
    final double[][] data;

    public RowMajorMatrix(@Nonnull final double[][] data) {
        Preconditions.checkArgument(data.length > 0);
        Preconditions.checkArgument(data[0].length > 0);

        this.data = data;
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
    public Matrix transpose() {
        int n = getRowDimension();
        int m = getColumnDimension();
        double[][] result = new double[m][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                result[j][i] = getEntry(i, j);
            }
        }
        return new RowMajorMatrix(result);
    }

    @Nonnull
    @Override
    public Matrix multiply(@Nonnull final Matrix otherMatrix) {
        int n = getRowDimension();
        int m = otherMatrix.getColumnDimension();
        int common = getColumnDimension();
        double[][] result = new double[n][m];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                for (int k = 0; k < common; k++) {
                    result[i][j] += data[i][k] * otherMatrix.getEntry(k, j);
                }
            }
        }
        return new RowMajorMatrix(result);
    }

    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof RowMajorMatrix)) {
            return false;
        }

        final RowMajorMatrix that = (RowMajorMatrix)o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}
