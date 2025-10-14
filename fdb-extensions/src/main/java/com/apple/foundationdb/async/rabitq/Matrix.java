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

import com.apple.foundationdb.async.hnsw.DoubleVector;
import com.apple.foundationdb.async.hnsw.Vector;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

public interface Matrix extends LinearOperator {
    @Nonnull
    double[][] getData();

    double getEntry(final int row, final int column);

    @Override
    default boolean isTransposable() {
        return true;
    }

    @Nonnull
    Matrix transpose();

    @Nonnull
    @Override
    default Vector operate(@Nonnull final Vector vector) {
        Verify.verify(getColumnDimension() == vector.getNumDimensions());
        final double[] result = new double[vector.getNumDimensions()];
        for (int i = 0; i < getRowDimension(); i ++) {
            double sum = 0.0d;
            for (int j = 0; j < getColumnDimension(); j ++) {
                sum += getEntry(i, j) * vector.getComponent(j);
            }
            result[i] = sum;
        }
        return new DoubleVector(result);
    }

    @Nonnull
    @Override
    default Vector operateTranspose(@Nonnull final Vector vector) {
        Verify.verify(getRowDimension() == vector.getNumDimensions());
        final double[] result = new double[vector.getNumDimensions()];
        for (int j = 0; j < getColumnDimension(); j ++) {
            double sum = 0.0d;
            for (int i = 0; i < getRowDimension(); i ++) {
                sum += getEntry(i, j) * vector.getComponent(i);
            }
            result[j] = sum;
        }
        return new DoubleVector(result);
    }

    @Nonnull
    Matrix multiply(@Nonnull Matrix otherMatrix);
}
