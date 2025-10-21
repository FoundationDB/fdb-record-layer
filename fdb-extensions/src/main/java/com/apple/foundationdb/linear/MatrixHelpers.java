/*
 * MatrixHelpers.java
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

import javax.annotation.Nonnull;
import java.util.Random;

public class MatrixHelpers {

    private MatrixHelpers() {
        // nothing
    }
    
    @Nonnull
    public static RealMatrix randomOrthogonalMatrix(@Nonnull final Random random, final int dimension) {
        return QRDecomposition.decomposeMatrix(randomGaussianMatrix(random, dimension, dimension)).getQ();
    }

    @Nonnull
    public static RealMatrix randomGaussianMatrix(@Nonnull final Random random,
                                                  final int rowDimension,
                                                  final int columnDimension) {
        final double[][] resultMatrix = new double[rowDimension][columnDimension];
        for (int row = 0; row < rowDimension; row++) {
            for (int column = 0; column < columnDimension; column++) {
                resultMatrix[row][column] = random.nextGaussian();
            }
        }
        return new RowMajorRealMatrix(resultMatrix);
    }
}
