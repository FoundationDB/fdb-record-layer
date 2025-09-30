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

import javax.annotation.Nonnull;

public interface Matrix {
    @Nonnull
    double[][] getData();

    int getRowDimension();

    int getColumnDimension();

    double getEntry(final int row, final int column);

    default boolean isSquare() {
        return getRowDimension() == getColumnDimension();
    }

    @Nonnull
    Matrix transpose();

    @Nonnull
    Matrix multiply(@Nonnull Matrix otherMatrix);
}
