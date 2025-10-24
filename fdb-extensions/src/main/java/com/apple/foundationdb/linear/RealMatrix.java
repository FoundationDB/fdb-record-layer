/*
 * RealMatrix.java
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

import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface RealMatrix extends LinearOperator {
    double getEntry(int row, int column);

    @Override
    default boolean isTransposable() {
        return true;
    }

    @Nonnull
    RealMatrix transpose();

    @Nonnull
    @Override
    default RealVector operate(@Nonnull final RealVector vector) {
        Verify.verify(getColumnDimension() == vector.getNumDimensions());
        final double[] result = new double[getRowDimension()];
        for (int i = 0; i < getRowDimension(); i ++) {
            double sum = 0.0d;
            for (int j = 0; j < getColumnDimension(); j ++) {
                sum += getEntry(i, j) * vector.getComponent(j);
            }
            result[i] = sum;
        }
        return new DoubleRealVector(result);
    }

    @Nonnull
    @Override
    default RealVector operateTranspose(@Nonnull final RealVector vector) {
        Verify.verify(getRowDimension() == vector.getNumDimensions());
        final double[] result = new double[getColumnDimension()];
        for (int j = 0; j < getColumnDimension(); j ++) {
            double sum = 0.0d;
            for (int i = 0; i < getRowDimension(); i ++) {
                sum += getEntry(i, j) * vector.getComponent(i);
            }
            result[j] = sum;
        }
        return new DoubleRealVector(result);
    }

    @Nonnull
    RealMatrix multiply(@Nonnull RealMatrix otherMatrix);

    @Nonnull
    RealMatrix subMatrix(int startRow, int lengthRow, int startColumn, int lengthColumn);

    @Nonnull
    RowMajorRealMatrix toRowMajor();

    @Nonnull
    double[][] getRowMajorData();

    @Nonnull
    ColumnMajorRealMatrix toColumnMajor();

    @Nonnull
    double[][] getColumnMajorData();

    @Nonnull
    RealMatrix quickTranspose();

    @Nonnull
    default RealMatrix flipMajor() {
        return transpose().quickTranspose();
    }

    default boolean valueEquals(@Nullable final Object o) {
        if (!(o instanceof RealMatrix)) {
            return false;
        }

        final RealMatrix that = (RealMatrix)o;
        if (getRowDimension() != that.getRowDimension() ||
                getColumnDimension() != that.getColumnDimension()) {
            return false;
        }

        for (int i = 0; i < getRowDimension(); i ++) {
            for (int j = 0; j < getColumnDimension(); j ++) {
                if (getEntry(i, j) != that.getEntry(i, j)) {
                    return false;
                }
            }
        }
        return true;
    }

    default int valueBasedHashCode() {
        int hashCode = 0;
        for (int i = 0; i < getRowDimension(); i ++) {
            for (int j = 0; j < getColumnDimension(); j ++) {
                hashCode += 31 * Double.hashCode(getEntry(i, j));
            }
        }
        return hashCode;
    }
}
