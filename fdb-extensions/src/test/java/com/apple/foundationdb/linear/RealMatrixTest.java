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

import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableSet;
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
        final RealMatrix matrix = MatrixHelpers.randomGaussianMatrix(random, numRows, numColumns);
        final RealMatrix otherMatrix = matrix.flipMajor();
        assertThat(otherMatrix).isEqualTo(matrix);
        final RealMatrix anotherMatrix = otherMatrix.flipMajor();
        assertThat(anotherMatrix).isEqualTo(otherMatrix);
        assertThat(anotherMatrix).isEqualTo(matrix);
        assertThat(anotherMatrix.getClass()).isSameAs(matrix.getClass());
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testQuickTranspose(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final int numRows = random.nextInt(numDimensions) + 1;
        final int numColumns = random.nextInt(numDimensions) + 1;
        final RealMatrix matrix = MatrixHelpers.randomGaussianMatrix(random, numRows, numColumns);
        final RealMatrix otherMatrix = matrix.quickTranspose().transpose();
        assertThat(otherMatrix).isEqualTo(matrix);
        final RealMatrix anotherMatrix = matrix.quickTranspose().quickTranspose();
        assertThat(anotherMatrix).isEqualTo(matrix);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testDifferentMajor(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final int numRows = random.nextInt(numDimensions) + 1;
        final int numColumns = random.nextInt(numDimensions) + 1;
        final RealMatrix matrix = MatrixHelpers.randomGaussianMatrix(random, numRows, numColumns);
        assertThat(matrix).isInstanceOf(RowMajorRealMatrix.class);
        final RealMatrix otherMatrix = matrix.toColumnMajor();
        assertThat(otherMatrix.toColumnMajor()).isSameAs(otherMatrix);
        assertThat(otherMatrix.hashCode()).isEqualTo(matrix.hashCode());
        assertThat(otherMatrix).isEqualTo(matrix);
        final RealMatrix anotherMatrix = otherMatrix.toRowMajor();
        assertThat(anotherMatrix.toRowMajor()).isSameAs(anotherMatrix);
        assertThat(anotherMatrix.hashCode()).isEqualTo(matrix.hashCode());
        assertThat(anotherMatrix).isEqualTo(matrix);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testApplyAndBack(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final RealMatrix matrix = MatrixHelpers.randomOrthogonalMatrix(random, numDimensions);
        assertThat(matrix.isTransposable()).isTrue();
        final RealVector x = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        final RealVector y = matrix.apply(x);
        final RealVector z = matrix.applyTranspose(y);
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
        final RealMatrix r = MatrixHelpers.randomOrthogonalMatrix(random, d);
        assertMultiplyMxMT(d, random, r);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testMultiplyColumnMajorMatrix(final long seed, final int d) {
        final Random random = new Random(seed);
        final RealMatrix r = MatrixHelpers.randomOrthogonalMatrix(random, d).flipMajor();
        assertMultiplyMxMT(d, random, r);
    }

    private static void assertMultiplyMxMT(final int d, @Nonnull final Random random, @Nonnull final RealMatrix r) {
        final int k = random.nextInt(d);
        final int l = random.nextInt(d);

        final int numResultRows = d - k;
        final int numResultColumns = d - l;

        final RealMatrix m = r.subMatrix(0, numResultRows, 0, d);
        final RealMatrix mT = r.transpose().subMatrix(0, d, 0, numResultColumns);

        final RealMatrix product = m.multiply(mT);

        assertThat(product)
                .satisfies(p -> assertThat(p.getNumRowDimensions()).isEqualTo(numResultRows),
                        p -> assertThat(p.getNumColumnDimensions()).isEqualTo(numResultColumns));

        for (int i = 0; i < product.getNumRowDimensions(); i++) {
            for (int j = 0; j < product.getNumColumnDimensions(); j++) {
                double expected = (i == j) ? 1.0 : 0.0;
                assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .isCloseTo(0, within(2E-14));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testMultiplyMatrix2(final long seed, final int d) {
        final Random random = new Random(seed);
        final int k = random.nextInt(d) + 1;
        final int l = random.nextInt(d) + 1;

        final RowMajorRealMatrix m1 = MatrixHelpers.randomGaussianMatrix(random, k, l).toRowMajor();
        final ColumnMajorRealMatrix m2 = MatrixHelpers.randomGaussianMatrix(random, l, k).toColumnMajor();

        final RealMatrix product = m1.multiply(m2);

        for (int i = 0; i < product.getNumRowDimensions(); i++) {
            for (int j = 0; j < product.getNumColumnDimensions(); j++) {
                final double expected = new DoubleRealVector(m1.getRow(i)).dot(new DoubleRealVector(m2.getColumn(j)));
                assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .isCloseTo(0, within(2E-14));
            }
        }
    }
}
