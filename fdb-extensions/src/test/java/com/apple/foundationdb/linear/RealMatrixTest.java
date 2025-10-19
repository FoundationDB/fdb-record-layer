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

import com.apple.foundationdb.async.hnsw.RealVectorSerializationTest;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class RealMatrixTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> ImmutableSet.of(3, 5, 10, 128, 768, 1000).stream()
                        .map(numDimensions -> Arguments.of(seed, numDimensions)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testTranspose(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final int numRows = random.nextInt(numDimensions) + 1;
        final int numColumns = random.nextInt(numDimensions) + 1;
        final RealMatrix matrix = RandomMatrixHelpers.randomGaussianMatrix(random, numRows, numColumns);
        final RealMatrix otherMatrix = flip(matrix);
        assertThat(otherMatrix).isEqualTo(matrix);
        final RealMatrix anotherMatrix = flip(otherMatrix);
        assertThat(anotherMatrix).isEqualTo(otherMatrix);
        assertThat(anotherMatrix).isEqualTo(matrix);
        assertThat(anotherMatrix.getClass()).isSameAs(matrix.getClass());
    }

    @Nonnull
    private static RealMatrix flip(@Nonnull final RealMatrix matrix) {
        assertThat(matrix)
                .satisfiesAnyOf(m -> assertThat(m).isInstanceOf(RowMajorRealMatrix.class),
                        m -> assertThat(m).isInstanceOf(ColumnMajorRealMatrix.class));
        final double[][] data = matrix.transpose().getData();
        if (matrix instanceof RowMajorRealMatrix) {
            return new ColumnMajorRealMatrix(data);
        } else {
            return new RowMajorRealMatrix(data);
        }
    }

    @Test
    void transposeRowMajorMatrix() {
        final RealMatrix m = new RowMajorRealMatrix(new double[][]{{0, 1, 2}, {3, 4, 5}});
        final RealMatrix expected = new RowMajorRealMatrix(new double[][]{{0, 3}, {1, 4}, {2, 5}});

        assertThat(m.isTransposable()).isTrue();
        assertThat(m.transpose()).isEqualTo(expected);
    }

    @Test
    void transposeColumnMajorMatrix() {
        final RealMatrix m = new ColumnMajorRealMatrix(new double[][]{{0, 3}, {1, 4}, {2, 5}});
        final RealMatrix expected = new ColumnMajorRealMatrix(new double[][]{{0, 1, 2}, {3, 4, 5}});

        assertThat(m.isTransposable()).isTrue();
        assertThat(m.transpose()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testOperateAndBack(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final RealMatrix matrix = RandomMatrixHelpers.randomOrthogonalMatrix(random, numDimensions);
        assertThat(matrix.isTransposable()).isTrue();
        final RealVector x = RealVectorSerializationTest.createRandomDoubleVector(random, numDimensions);
        final RealVector y = matrix.operate(x);
        final RealVector z = matrix.operateTranspose(y);

        assertThat(Metric.EUCLIDEAN_METRIC.distance(x, z)).isCloseTo(0, within(2E-10));
    }

    /**
     * Tests the multiplication of two (dense) row-major matrices. We don't want to use just a fixed set of matrices,
     * but rather want to run the multiplication for a lot of different arguments. The goal is to not replicate
     * the actual multiplication algorithm for the test. We employ the following trick. We first generate a random
     * orthogonal matrix of {@code d} dimensions. For that matrix, let's call it {@code r}, {@code r*r^T = I_d} which
     * exercises the multiplication code path and whose result is easily verifiable. The problem is, though, that doing
     * just that would only test the multiplication of square matrices. In order, to similarly test the multiplication
     * of any kinds of rectangular matrices, we can still start out from a square matrix, but then randomly cut off the
     * last {@code k} rows (and respectively {@code l} columns from the transpose) and only then multiply the results.
     * The final resulting matrix is of dimensionality {@code d - k} by {@code d - l} which is a rectangular identity
     * matrix, i.e.:
     * {@code (I_(d-k, d-l))_ij = 1, if i == j, 0 otherwise}.
     *
     * @param seed a seed for a random number generator
     * @param d the number of dimensions we should use when generating the random orthogonal matrix
     */
    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testMultiplyRowMajorMatrix(final long seed, final int d) {
        final Random random = new Random(seed);
        final RealMatrix r = RandomMatrixHelpers.randomOrthogonalMatrix(random, d);
        assertMultiplyMxMT(d, random, r);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testMultiplyColumnMajorMatrix(final long seed, final int d) {
        final Random random = new Random(seed);
        final RealMatrix r = flip(RandomMatrixHelpers.randomOrthogonalMatrix(random, d));
        assertMultiplyMxMT(d, random, r);
    }

    private static void assertMultiplyMxMT(final int d, final Random random, final RealMatrix r) {
        final int k = random.nextInt(d);
        final int l = random.nextInt(d);

        final int numResultRows = d - k;
        final int numResultColumns = d - l;

        final RealMatrix m = r.subMatrix(0, numResultRows, 0, d);
        final RealMatrix mT = r.transpose().subMatrix(0, d, 0, numResultColumns);

        final RealMatrix product = m.multiply(mT);

        assertThat(product)
                .satisfies(p -> assertThat(p.getRowDimension()).isEqualTo(numResultRows),
                        p -> assertThat(p.getColumnDimension()).isEqualTo(numResultColumns));

        for (int i = 0; i < product.getRowDimension(); i++) {
            for (int j = 0; j < product.getColumnDimension(); j++) {
                double expected = (i == j) ? 1.0 : 0.0;
                assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .isCloseTo(0, within(2E-14));
            }
        }
    }
}
