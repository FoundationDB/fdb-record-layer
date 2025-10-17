/*
 * RealMatrixTest.java
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class RealMatrixTest {
    @Test
    void transposeRowMajorMatrix() {
        final RealMatrix m = new RowMajorRealMatrix(new double[][]{{0, 1, 2}, {3, 4, 5}});
        final RealMatrix expected = new RowMajorRealMatrix(new double[][]{{0, 3}, {1, 4}, {2, 5}});

        Assertions.assertThat(m.transpose()).isEqualTo(expected);
    }

    @Test
    void transposeColumnMajorMatrix() {
        final RealMatrix m = new ColumnMajorRealMatrix(new double[][]{{0, 3}, {1, 4}, {2, 5}});
        final RealMatrix expected = new ColumnMajorRealMatrix(new double[][]{{0, 1, 2}, {3, 4, 5}});

        Assertions.assertThat(m.transpose()).isEqualTo(expected);
    }
}
