/*
 * QRDecompositionTest.java
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class QRDecompositionTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> ImmutableSet.of(3, 5, 10, 128, 768).stream()
                        .map(numDimensions -> Arguments.of(seed, numDimensions)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testQREqualsM(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final RealMatrix m = MatrixHelpers.randomOrthogonalMatrix(random, numDimensions);
        final QRDecomposition.Result result = QRDecomposition.decomposeMatrix(m);
        final RealMatrix product = result.getQ().multiply(result.getR());
        for (int i = 0; i < product.getNumRowDimensions(); i++) {
            for (int j = 0; j < product.getNumColumnDimensions(); j++) {
                assertThat(product.getEntry(i, j)).isCloseTo(m.getEntry(i, j), within(2E-14));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testRepeatedQR(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final RealMatrix m = MatrixHelpers.randomOrthogonalMatrix(random, numDimensions);
        final QRDecomposition.Result firstResult = QRDecomposition.decomposeMatrix(m);
        final QRDecomposition.Result secondResult = QRDecomposition.decomposeMatrix(firstResult.getQ());

        final RealMatrix r = secondResult.getR();
        for (int i = 0; i < r.getNumRowDimensions(); i++) {
            for (int j = 0; j < r.getNumColumnDimensions(); j++) {
                assertThat(Math.abs(r.getEntry(i, j))).isCloseTo((i == j) ? 1.0d : 0.0d, within(2E-14));
            }
        }
    }

    @Test
    void testZeroes() {
        double[][] mData = new double[5][5];

        // Fill the top-left 2x5 however you like (non-zero):
        mData[0] = new double[] { 1,  2,  3,  4,  5 };
        mData[1] = new double[] {-1,  0,  7,  8,  9 };

        // Make rows 2..4 all zeros:
        mData[2] = new double[] { 0, 0, 0, 0, 0 };
        mData[3] = new double[] { 0, 0, 0, 0, 0 };
        mData[4] = new double[] { 0, 0, 0, 0, 0 };

        // => For any minor k ≥ 2, the column segment x (rows k...end) is all zeros => a == 0.0 branch taken.
        final RealMatrix m = new RowMajorRealMatrix(mData);
        final QRDecomposition.Result result = QRDecomposition.decomposeMatrix(m);
        final RealMatrix product = result.getQ().multiply(result.getR());

        for (int i = 0; i < product.getNumRowDimensions(); i++) {
            for (int j = 0; j < product.getNumColumnDimensions(); j++) {
                assertThat(product.getEntry(i, j)).isCloseTo(m.getEntry(i, j), within(2E-14));
            }
        }
    }
}
