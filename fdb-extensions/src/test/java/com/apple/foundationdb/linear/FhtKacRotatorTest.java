/*
 * FhtKacRotatorTest.java
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.within;

class FhtKacRotatorTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> ImmutableSet.of(3, 5, 10, 128, 768, 1000).stream()
                        .map(numDimensions -> Arguments.of(seed, numDimensions)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testSimpleRotationAndBack(final long seed, final int numDimensions) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);

        final Random random = new Random(seed);
        final RealVector x = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        final RealVector y = rotator.apply(x);
        final RealVector z = rotator.invertedApply(y);

        Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(x, z)).isCloseTo(0, within(2E-10));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testRotationIsStable(final long seed, final int numDimensions) {
        final FhtKacRotator rotator1 = new FhtKacRotator(seed, numDimensions, 10);
        final FhtKacRotator rotator2 = new FhtKacRotator(seed, numDimensions, 10);
        Assertions.assertThat(rotator1.hashCode()).isEqualTo(rotator2.hashCode());
        Assertions.assertThat(rotator1).isEqualTo(rotator2);

        final Random random = new Random(seed);
        final RealVector x = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        final RealVector x_ = rotator1.apply(x);
        final RealVector x__ = rotator2.apply(x);

        Assertions.assertThat(x_).isEqualTo(x__);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testOrthogonality(final long seed, final int numDimensions) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);
        final ColumnMajorRealMatrix p = rotator.computeP().transpose().quickTranspose();

        for (int j = 0; j < numDimensions; j ++) {
            final RealVector rotated = rotator.invertedApply(new DoubleRealVector(p.getColumn(j)));
            for (int i = 0; i < numDimensions; i++) {
                double expected = (i == j) ? 1.0 : 0.0;
                Assertions.assertThat(Math.abs(rotated.getComponent(i) - expected))
                                .isCloseTo(0, within(2E-14));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testOrthogonalityWithP(final long seed, final int dimensionality) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, dimensionality, 10);
        final RealMatrix p = rotator.computeP();
        final RealMatrix product = p.transpose().multiply(p);

        for (int i = 0; i < dimensionality; i++) {
            for (int j = 0; j < dimensionality; j++) {
                double expected = (i == j) ? 1.0 : 0.0;
                Assertions.assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .isCloseTo(0, within(2E-14));
            }
        }
    }
}
