/*
 * RealMatrixParityTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Asserts that the active backend (SIMD when loadable, scalar otherwise) produces the same matrix
 * results as a reference computed with a fresh {@link ScalarBackend}, within an O(N·ε) tolerance.
 * Mirrors the parity pattern in {@link RealVectorPrimitivesParityTest}, but lifted to the
 * matrix-multiply fast path and the four {@code apply}/{@code transposedApply} overrides.
 * <p>
 * Under the default {@code test} task this verifies SIMD-vs-scalar parity; under
 * {@code testScalarFallback} (where {@link RealVectorPrimitives} resolves to scalar) it
 * degenerates into scalar-vs-scalar — still useful as a smoke check that the matrix paths
 * produce identical results to the lower-level primitive reference.
 */
class RealMatrixParityTest {
    private static final ImmutableList<Integer> DIMS = ImmutableList.of(3, 7, 16, 17, 65, 128);

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("RowMajor × ColumnMajor multiply hits backend.dot per (i, j)")
    void multiplyRowByColumn(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                for (final int l : DIMS) {
                    final RowMajorRealMatrix a = MatrixHelpers.randomGaussianMatrix(rnd, n, l).toRowMajor();
                    final ColumnMajorRealMatrix b = MatrixHelpers.randomGaussianMatrix(rnd, l, m).toColumnMajor();

                    final RealMatrix product = a.multiply(b);

                    for (int i = 0; i < n; i++) {
                        final double[] rowI = a.getRow(i);
                        for (int j = 0; j < m; j++) {
                            final double expected = scalar.dot(rowI, b.getColumn(j));
                            assertThat(product.getEntry(i, j))
                                    .as("n=%d, m=%d, l=%d, i=%d, j=%d", n, m, l, i, j)
                                    .isCloseTo(expected, within(reductionTol(l, expected)));
                        }
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("RowMajor × RowMajor multiply hits backend.multiplyAddInto per (i, k)")
    void multiplyRowByRow(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                for (final int l : DIMS) {
                    final RowMajorRealMatrix a = MatrixHelpers.randomGaussianMatrix(rnd, n, l).toRowMajor();
                    final RowMajorRealMatrix b = MatrixHelpers.randomGaussianMatrix(rnd, l, m).toRowMajor();
                    assertProductParity(scalar, n, m, l, a, b, a.multiply(b));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("ColumnMajor × RowMajor multiply via universal AXPY")
    void multiplyColumnByRow(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                for (final int l : DIMS) {
                    final ColumnMajorRealMatrix a = MatrixHelpers.randomGaussianMatrix(rnd, n, l).toColumnMajor();
                    final RowMajorRealMatrix b = MatrixHelpers.randomGaussianMatrix(rnd, l, m).toRowMajor();
                    assertProductParity(scalar, n, m, l, a, b, a.multiply(b));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("ColumnMajor × ColumnMajor multiply via universal AXPY")
    void multiplyColumnByColumn(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                for (final int l : DIMS) {
                    final ColumnMajorRealMatrix a = MatrixHelpers.randomGaussianMatrix(rnd, n, l).toColumnMajor();
                    final ColumnMajorRealMatrix b = MatrixHelpers.randomGaussianMatrix(rnd, l, m).toColumnMajor();
                    assertProductParity(scalar, n, m, l, a, b, a.multiply(b));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("RowMajorRealMatrix.apply(v): dot per row")
    void rowMajorApply(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                final RowMajorRealMatrix matrix = MatrixHelpers.randomGaussianMatrix(rnd, n, m).toRowMajor();
                final DoubleRealVector v = RealVectorTest.createRandomDoubleVector(rnd, m);

                final RealVector actual = matrix.apply(v);

                final double[] vData = v.getData();
                for (int i = 0; i < n; i++) {
                    final double expected = scalar.dot(matrix.getRow(i), vData);
                    assertThat(actual.getComponent(i))
                            .as("n=%d, m=%d, i=%d", n, m, i)
                            .isCloseTo(expected, within(reductionTol(m, expected)));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("RowMajorRealMatrix.transposedApply(v): row-wise AXPY accumulation")
    void rowMajorTransposedApply(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                final RowMajorRealMatrix matrix = MatrixHelpers.randomGaussianMatrix(rnd, n, m).toRowMajor();
                final DoubleRealVector v = RealVectorTest.createRandomDoubleVector(rnd, n);

                final RealVector actual = matrix.transposedApply(v);

                final double[] vData = v.getData();
                final double[] expected = new double[m];
                for (int i = 0; i < n; i++) {
                    scalar.multiplyAddInto(vData[i], matrix.getRow(i), expected, expected, 0, m);
                }
                for (int j = 0; j < m; j++) {
                    assertThat(actual.getComponent(j))
                            .as("n=%d, m=%d, j=%d", n, m, j)
                            .isCloseTo(expected[j], within(reductionTol(n, expected[j])));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("ColumnMajorRealMatrix.apply(v): column-wise AXPY accumulation")
    void columnMajorApply(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                final ColumnMajorRealMatrix matrix = MatrixHelpers.randomGaussianMatrix(rnd, n, m).toColumnMajor();
                final DoubleRealVector v = RealVectorTest.createRandomDoubleVector(rnd, m);

                final RealVector actual = matrix.apply(v);

                final double[] vData = v.getData();
                final double[] expected = new double[n];
                for (int j = 0; j < m; j++) {
                    scalar.multiplyAddInto(vData[j], matrix.getColumn(j), expected, expected, 0, n);
                }
                for (int i = 0; i < n; i++) {
                    assertThat(actual.getComponent(i))
                            .as("n=%d, m=%d, i=%d", n, m, i)
                            .isCloseTo(expected[i], within(reductionTol(m, expected[i])));
                }
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    @DisplayName("ColumnMajorRealMatrix.transposedApply(v): dot per column")
    void columnMajorTransposedApply(final long seed) {
        final Random rnd = new Random(seed);
        final ScalarBackend scalar = new ScalarBackend();
        for (final int n : DIMS) {
            for (final int m : DIMS) {
                final ColumnMajorRealMatrix matrix = MatrixHelpers.randomGaussianMatrix(rnd, n, m).toColumnMajor();
                final DoubleRealVector v = RealVectorTest.createRandomDoubleVector(rnd, n);

                final RealVector actual = matrix.transposedApply(v);

                final double[] vData = v.getData();
                for (int j = 0; j < m; j++) {
                    final double expected = scalar.dot(matrix.getColumn(j), vData);
                    assertThat(actual.getComponent(j))
                            .as("n=%d, m=%d, j=%d", n, m, j)
                            .isCloseTo(expected, within(reductionTol(n, expected)));
                }
            }
        }
    }

    /**
     * Runs the per-entry parity check for an arbitrary pair of operand layouts. Materializes
     * {@code a} as row-major and {@code b} as column-major (free if they're already in that
     * orientation) so the scalar reference can use contiguous {@code dot} reads regardless of
     * which fast path the active backend exercised.
     */
    private static void assertProductParity(@Nonnull final ScalarBackend scalar,
                                            final int n, final int m, final int l,
                                            @Nonnull final RealMatrix a,
                                            @Nonnull final RealMatrix b,
                                            @Nonnull final RealMatrix product) {
        final RowMajorRealMatrix aRowMajor = a.toRowMajor();
        final ColumnMajorRealMatrix bColMajor = b.toColumnMajor();
        for (int i = 0; i < n; i++) {
            final double[] rowI = aRowMajor.getRow(i);
            for (int j = 0; j < m; j++) {
                final double expected = scalar.dot(rowI, bColMajor.getColumn(j));
                assertThat(product.getEntry(i, j))
                        .as("n=%d, m=%d, l=%d, i=%d, j=%d", n, m, l, i, j)
                        .isCloseTo(expected, within(reductionTol(l, expected)));
            }
        }
    }

    /** Tolerance for reductions; scales with vector length since accumulation order differs. */
    private static double reductionTol(final int len, final double reference) {
        return Math.max(1.0e-12d, Math.abs(reference) * 1.0e-12d * len);
    }
}
