/*
 * AffineOperatorTest.java
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

class AffineOperatorTest {
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
        final RealVector translation = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        final AffineOperator affineOperator = new AffineOperator(rotator, translation);

        final RealVector x = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        final RealVector y = affineOperator.apply(x);
        final RealVector z = affineOperator.invertedApply(y);

        Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(x, z)).isCloseTo(0, within(2E-10));
    }
}
