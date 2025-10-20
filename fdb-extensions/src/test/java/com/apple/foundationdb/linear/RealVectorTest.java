/*
 * RealVectorTest.java
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
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

public class RealVectorTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> ImmutableSet.of(3, 5, 10, 128, 768, 1000).stream()
                        .map(numDimensions -> Arguments.of(seed, numDimensions)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testPrecisionRoundTrips(final long seed, final int numDimensions) {
        for (int i = 0; i < 1000; i ++) {
            final Random random = new Random(seed);
            final DoubleRealVector doubleVector = new DoubleRealVector(createRandomVectorData(random, numDimensions));
            Assertions.assertThat(doubleVector.toDoubleRealVector()).isEqualTo(doubleVector);

            final FloatRealVector floatVector = doubleVector.toFloatRealVector();
            Assertions.assertThat(floatVector.toFloatRealVector()).isEqualTo(floatVector);
            Assertions.assertThat(floatVector.toDoubleRealVector().toFloatRealVector()).isEqualTo(floatVector);

            final HalfRealVector halfVector = floatVector.toHalfRealVector();
            Assertions.assertThat(halfVector.toHalfRealVector()).isEqualTo(halfVector);
            Assertions.assertThat(halfVector.toFloatRealVector().toHalfRealVector()).isEqualTo(halfVector);

            final HalfRealVector halfVector2 = doubleVector.toHalfRealVector();
            Assertions.assertThat(halfVector2).isEqualTo(halfVector);
            Assertions.assertThat(halfVector2.toDoubleRealVector().toHalfRealVector()).isEqualTo(halfVector2);

            Assertions.assertThat(halfVector2.toDoubleRealVector().toFloatRealVector())
                    .isEqualTo(doubleVector.toHalfRealVector().toFloatRealVector());
        }
    }

    @Test
    void testAlternativeConstructors() {
        Assertions.assertThat(new DoubleRealVector(new int[] {-3, 0, 2}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new DoubleRealVector(new long[] {-3L, 0L, 2L}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new FloatRealVector(new int[] {-3, 0, 2}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new FloatRealVector(new long[] {-3L, 0L, 2L}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new HalfRealVector(new int[] {-3, 0, 2}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new HalfRealVector(new long[] {-3L, 0L, 2L}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testNorm(final long seed, final int numDimensions) {
        final DoubleRealVector zeroVector = new DoubleRealVector(new double[numDimensions]);
        for (int i = 0; i < 1000; i ++) {
            final Random random = new Random(seed);
            final DoubleRealVector doubleVector = new DoubleRealVector(createRandomVectorData(random, numDimensions));
            Assertions.assertThat(doubleVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(doubleVector, zeroVector), Offset.offset(2E-14));

            final FloatRealVector floatVector = new FloatRealVector(createRandomVectorData(random, numDimensions));
            Assertions.assertThat(floatVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(floatVector, zeroVector), Offset.offset(2E-14));

            final HalfRealVector halfVector = new HalfRealVector(createRandomVectorData(random, numDimensions));
            Assertions.assertThat(halfVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(halfVector, zeroVector), Offset.offset(2E-14));
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    void testEqualityAndHashCode(final long seed, final int numDimensions) {
        for (int i = 0; i < 1000; i ++) {
            final Random random = new Random(seed);
            final HalfRealVector halfVector = new HalfRealVector(createRandomVectorData(random, numDimensions));
            Assertions.assertThat(halfVector.toDoubleRealVector().hashCode()).isEqualTo(halfVector.hashCode());
            Assertions.assertThat(halfVector.toDoubleRealVector()).isEqualTo(halfVector);
            Assertions.assertThat(halfVector.toFloatRealVector().hashCode()).isEqualTo(halfVector.hashCode());
            Assertions.assertThat(halfVector.toFloatRealVector()).isEqualTo(halfVector);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testDot(final long seed, final int numDimensions) {
        for (int i = 0; i < 1000; i ++) {
            final Random random = new Random(seed);
            final DoubleRealVector doubleVector1 = new DoubleRealVector(createRandomVectorData(random, numDimensions));
            final DoubleRealVector doubleVector2 = new DoubleRealVector(createRandomVectorData(random, numDimensions));
            double dot = doubleVector1.dot(doubleVector2);
            Assertions.assertThat(dot).isEqualTo(doubleVector2.dot(doubleVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(doubleVector1, doubleVector2), Offset.offset(2E-14));

            final FloatRealVector floatVector1 = new FloatRealVector(createRandomVectorData(random, numDimensions));
            final FloatRealVector floatVector2 = new FloatRealVector(createRandomVectorData(random, numDimensions));
            dot = floatVector1.dot(floatVector2);
            Assertions.assertThat(dot).isEqualTo(floatVector2.dot(floatVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(floatVector1, floatVector2), Offset.offset(2E-14));

            final HalfRealVector halfVector1 = new HalfRealVector(createRandomVectorData(random, numDimensions));
            final HalfRealVector halfVector2 = new HalfRealVector(createRandomVectorData(random, numDimensions));
            dot = halfVector1.dot(halfVector2);
            Assertions.assertThat(dot).isEqualTo(halfVector2.dot(halfVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(halfVector1, halfVector2), Offset.offset(2E-14));
        }
    }

    @Nonnull
    public static double[] createRandomVectorData(@Nonnull final Random random, final int dims) {
        final double[] components = new double[dims];
        for (int d = 0; d < dims; d ++) {
            components[d] = random.nextDouble() * (random.nextBoolean() ? -1 : 1);
        }
        return components;
    }
}
