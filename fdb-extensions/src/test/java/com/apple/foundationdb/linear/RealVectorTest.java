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

import com.apple.foundationdb.half.Half;
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
        final Random random = new Random(seed);
        for (int i = 0; i < 1000; i ++) {
            final DoubleRealVector doubleVector = createRandomDoubleVector(random, numDimensions);
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
        Assertions.assertThat(new DoubleRealVector(new Double[] {-3.0d, 0.0d, 2.0d}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new DoubleRealVector(new int[] {-3, 0, 2}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new DoubleRealVector(new long[] {-3L, 0L, 2L}))
                .satisfies(vector -> Assertions.assertThat(vector.getComponent(0)).isCloseTo(-3.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(1)).isCloseTo(0.0d, Offset.offset(2E-14)),
                        vector -> Assertions.assertThat(vector.getComponent(2)).isCloseTo(2.0d, Offset.offset(2E-14)));

        Assertions.assertThat(new FloatRealVector(new Float[] {-3.0f, 0.0f, 2.0f}))
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

        Assertions.assertThat(new HalfRealVector(new Half[] {Half.valueOf(-3.0d), Half.valueOf(0.0d), Half.valueOf(2.0d)}))
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
    void testDirectSerializationDeserialization(final long seed, final int numDimensions) {
        final Random random = new Random(seed);

        final DoubleRealVector doubleVector = RealVectorTest.createRandomDoubleVector(random, numDimensions);
        RealVector deserializedVector = DoubleRealVector.fromBytes(doubleVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(DoubleRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(doubleVector);

        final FloatRealVector floatVector = RealVectorTest.createRandomFloatVector(random, numDimensions);
        deserializedVector = FloatRealVector.fromBytes(floatVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(FloatRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(floatVector);

        final HalfRealVector halfVector = RealVectorTest.createRandomHalfVector(random, numDimensions);
        deserializedVector = HalfRealVector.fromBytes(halfVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(HalfRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(halfVector);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testNorm(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final DoubleRealVector zeroVector = new DoubleRealVector(new double[numDimensions]);
        for (int i = 0; i < 1000; i ++) {
            final DoubleRealVector doubleVector = createRandomDoubleVector(random, numDimensions);
            Assertions.assertThat(doubleVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(doubleVector, zeroVector), Offset.offset(2E-14));

            final FloatRealVector floatVector = createRandomFloatVector(random, numDimensions);
            Assertions.assertThat(floatVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(floatVector, zeroVector), Offset.offset(2E-5));

            final HalfRealVector halfVector = createRandomHalfVector(random, numDimensions);
            Assertions.assertThat(halfVector.l2Norm())
                    .isCloseTo(Metric.EUCLIDEAN_METRIC.distance(halfVector, zeroVector), Offset.offset(2E-2));
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testNormalize(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        for (int i = 0; i < 1000; i ++) {
            final DoubleRealVector doubleVector = createRandomDoubleVector(random, numDimensions);
            RealVector normalizedVector  = doubleVector.normalize();
            Assertions.assertThat(normalizedVector.multiply(doubleVector.l2Norm()))
                    .satisfies(v -> Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(doubleVector, v))
                            .isCloseTo(0, Offset.offset(2E-14)));
            final FloatRealVector floatVector = createRandomFloatVector(random, numDimensions);
            normalizedVector  = floatVector.normalize();
            Assertions.assertThat(normalizedVector.multiply(floatVector.l2Norm()))
                    .satisfies(v -> Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(floatVector, v))
                            .isCloseTo(0, Offset.offset(2E-5)));
            final HalfRealVector halfVector = createRandomHalfVector(random, numDimensions);
            normalizedVector  = halfVector.normalize();
            Assertions.assertThat(normalizedVector.multiply(halfVector.l2Norm()))
                    .satisfies(v -> Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(halfVector, v))
                            .isCloseTo(0, Offset.offset(2E-2)));
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testAdd(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final DoubleRealVector zeroVector = new DoubleRealVector(new double[numDimensions]);
        for (int i = 0; i < 1000; i ++) {
            final DoubleRealVector doubleVector = createRandomDoubleVector(random, numDimensions);

            Assertions.assertThat(doubleVector.add(doubleVector))
                    .satisfies(v ->
                            Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(doubleVector.multiply(2.0d), v))
                                    .isCloseTo(0, Offset.offset(2E-14)));

            Assertions.assertThat(doubleVector.add(1.0d).add(-1.0d))
                    .satisfies(v ->
                            Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(doubleVector, v))
                                    .isCloseTo(0, Offset.offset(2E-14)));
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    void testEqualityAndHashCode(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        for (int i = 0; i < 1000; i ++) {
            final HalfRealVector halfVector = createRandomHalfVector(random, numDimensions);
            Assertions.assertThat(halfVector.toDoubleRealVector().hashCode()).isEqualTo(halfVector.hashCode());
            Assertions.assertThat(halfVector.toDoubleRealVector()).isEqualTo(halfVector);
            Assertions.assertThat(halfVector.toFloatRealVector().hashCode()).isEqualTo(halfVector.hashCode());
            Assertions.assertThat(halfVector.toFloatRealVector()).isEqualTo(halfVector);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testDot(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        for (int i = 0; i < 1000; i ++) {
            final DoubleRealVector doubleVector1 = createRandomDoubleVector(random, numDimensions);
            final DoubleRealVector doubleVector2 = createRandomDoubleVector(random, numDimensions);
            double dot = doubleVector1.dot(doubleVector2);
            Assertions.assertThat(dot).isEqualTo(doubleVector2.dot(doubleVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(doubleVector1, doubleVector2), Offset.offset(2E-14));

            final FloatRealVector floatVector1 = createRandomFloatVector(random, numDimensions);
            final FloatRealVector floatVector2 = createRandomFloatVector(random, numDimensions);
            dot = floatVector1.dot(floatVector2);
            Assertions.assertThat(dot).isEqualTo(floatVector2.dot(floatVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(floatVector1, floatVector2), Offset.offset(2E-14));

            final HalfRealVector halfVector1 = createRandomHalfVector(random, numDimensions);
            final HalfRealVector halfVector2 = createRandomHalfVector(random, numDimensions);
            dot = halfVector1.dot(halfVector2);
            Assertions.assertThat(dot).isEqualTo(halfVector2.dot(halfVector1));
            Assertions.assertThat(dot)
                    .isCloseTo(-Metric.DOT_PRODUCT_METRIC.distance(halfVector1, halfVector2), Offset.offset(2E-14));
        }
    }

    @Nonnull
    public static DoubleRealVector createRandomDoubleVector(@Nonnull final Random random, final int numDimensions) {
        return new DoubleRealVector(createRandomVectorData(random, numDimensions));
    }

    @Nonnull
    public static FloatRealVector createRandomFloatVector(@Nonnull final Random random, final int numDimensions) {
        return new FloatRealVector(createRandomVectorData(random, numDimensions));
    }

    @Nonnull
    public static HalfRealVector createRandomHalfVector(@Nonnull final Random random, final int numDimensions) {
        return new HalfRealVector(createRandomVectorData(random, numDimensions));
    }

    @Nonnull
    public static double[] createRandomVectorData(@Nonnull final Random random, final int numDimensions) {
        final double[] components = new double[numDimensions];
        for (int d = 0; d < numDimensions; d ++) {
            components[d] = random.nextDouble() * (random.nextBoolean() ? -1 : 1);
        }
        return components;
    }
}
