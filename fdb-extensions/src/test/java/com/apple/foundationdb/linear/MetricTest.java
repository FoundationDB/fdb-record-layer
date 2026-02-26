/*
 * MetricTest.java
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
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.linear.Metric.COSINE_METRIC;
import static com.apple.foundationdb.linear.Metric.DOT_PRODUCT_METRIC;
import static com.apple.foundationdb.linear.Metric.EUCLIDEAN_METRIC;
import static com.apple.foundationdb.linear.Metric.EUCLIDEAN_SQUARE_METRIC;

class MetricTest {
    static Stream<Arguments> metricAndExpectedDistance() {
        // Distance between (1.0, 2.0) and (4.0, 6.0)
        final RealVector v1 = v(1.0, 2.0);
        final RealVector v2 = v(4.0, 6.0);
        return Stream.of(
                Arguments.of(EUCLIDEAN_METRIC, v1, v2, 5.0),         // sqrt((1-4)^2 + (2-6)^2) = sqrt(9 + 16) = 5
                Arguments.of(EUCLIDEAN_SQUARE_METRIC, v1, v2, 25.0), // (1-4)^2 + (2-6)^2 = 9 + 16 = 25
                Arguments.of(COSINE_METRIC, v1, v2, 0.007722),       // ((1 * 4) + (2 * 6)) / (sqrt(1^2 + 2^2) * sqrt(4^2 + 6^2) = 16 / (sqrt(5) * sqrt(52)) ≈ 1 - 0.992277 ≈ 0.007722
                Arguments.of(DOT_PRODUCT_METRIC, v1, v2, -16.0)      // -((1 * 4) + (2 * 6)) = -(4 + 12) = -16
        );
    }

    @ParameterizedTest
    @MethodSource("metricAndExpectedDistance")
    void basicMetricTest(@Nonnull final Metric metric, @Nonnull final RealVector v1, @Nonnull final RealVector v2, final double expectedDistance) {
        Assertions.assertThat(metric.distance(v1, v2)).isCloseTo(expectedDistance, Offset.offset(2E-4d));
    }

    @Nonnull
    private static Stream<Arguments> randomSeedsWithMetrics() {
        return RandomizedTestUtils.randomSeeds(12345, 987654, 423, 18378195)
                .flatMap(seed ->
                        Sets.cartesianProduct(
                                ImmutableSet.of(EUCLIDEAN_METRIC,
                                        EUCLIDEAN_SQUARE_METRIC,
                                        COSINE_METRIC,
                                        DOT_PRODUCT_METRIC), ImmutableSet.of(3, 5, 128, 768)).stream()
                                .map(metricsAndNumDimensions ->
                                        Arguments.of(seed, metricsAndNumDimensions.get(0), metricsAndNumDimensions.get(1))));
    }

    @Test
    void testMetricDefinitionBasics() {
        Assertions.assertThat(EUCLIDEAN_METRIC.toString()).contains(MetricDefinition.EuclideanMetric.class.getSimpleName());
        Assertions.assertThat(EUCLIDEAN_METRIC.isTrueMetric()).isTrue();
        Assertions.assertThat(EUCLIDEAN_SQUARE_METRIC.toString()).contains(MetricDefinition.EuclideanSquareMetric.class.getSimpleName());
        Assertions.assertThat(EUCLIDEAN_SQUARE_METRIC.isTrueMetric()).isFalse();
        Assertions.assertThat(COSINE_METRIC.toString()).contains(MetricDefinition.CosineMetric.class.getSimpleName());
        Assertions.assertThat(COSINE_METRIC.isTrueMetric()).isFalse();
        Assertions.assertThat(DOT_PRODUCT_METRIC.toString()).contains(MetricDefinition.DotProductMetric.class.getSimpleName());
        Assertions.assertThat(DOT_PRODUCT_METRIC.isTrueMetric()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithMetrics")
    void basicPropertyTest(final long seed, @Nonnull final Metric metric, final int numDimensions) {
        final Random random = new Random(seed);

        for (int i = 0; i < 1000; i ++) {
            // use vectors that draw from [0, 1)
            final RealVector x = randomV(random, numDimensions);
            final RealVector y = randomV(random, numDimensions);
            final RealVector z = randomV(random, numDimensions);

            final double distanceXX = metric.distance(x, x);
            final double distanceXY = metric.distance(x, y);
            final double distanceYX = metric.distance(y, x);
            final double distanceYZ = metric.distance(y, z);
            final double distanceXZ = metric.distance(x, z);

            if (!Double.isFinite(distanceXX) || !Double.isFinite(distanceXY)) {
                //
                // Some metrics are numerically unstable across the entire numerical range.
                // For instance COSINE_METRIC may return Double.NaN or Double.POSITIVE_INFINITY which is ok.
                //
                continue;
            }

            Assertions.assertThat(distanceXX).satisfiesAnyOf(
                    d -> Assertions.assertThat(metric.satisfiesZeroSelfDistance()).isFalse(),
                    d -> Assertions.assertThat(d).isCloseTo(0, Offset.offset(2E-10d)));

            Assertions.assertThat(distanceXY).satisfiesAnyOf(
                    d -> Assertions.assertThat(metric.satisfiesPositivity()).isFalse(),
                    d -> Assertions.assertThat(d).isGreaterThanOrEqualTo(-2E-10));

            Assertions.assertThat(distanceXY).satisfiesAnyOf(
                    d -> Assertions.assertThat(metric.satisfiesSymmetry()).isFalse(),
                    d -> Assertions.assertThat(d).isCloseTo(distanceYX, Offset.offset(2E-10d)));

            Assertions.assertThat(distanceXY).satisfiesAnyOf(
                    d -> Assertions.assertThat(metric.satisfiesTriangleInequality()).isFalse(),
                    d -> Assertions.assertThat(d + distanceYZ).isGreaterThanOrEqualTo(distanceXZ - 2E-10d),
                    d -> Assertions.assertThat(triangleHolds(distanceXY, distanceYZ, distanceXZ, numDimensions * 3)).isTrue());

            if (i % 2 == 1) {
                // we can only check this for non-degenerate component values
                final RealVector xt = x.add(z);
                final RealVector yt = y.add(z);
                final double distanceXtYt = metric.distance(xt, yt);
                Assertions.assertThat(distanceXtYt).satisfiesAnyOf(
                        d -> Assertions.assertThat(metric.satisfiesPreservedUnderTranslation()).isFalse(),
                        d -> Assertions.assertThat(d).isCloseTo(distanceXY, Offset.offset(2E-10d)));
            }
        }
    }

    static boolean triangleHolds(double distanceXY, double distanceYZ, double distanceXZ, int numOperations) {
        double u = 0x1p-53;                                           // relative error ~1.11e-16
        double gamma = (numOperations * u) / (1 - numOperations * u); // ~ n*u
        double scale = distanceXY + distanceYZ + distanceXZ;          // magnitude to scale tol
        double tol = 4 * gamma * scale +
                (Math.ulp(distanceXY + distanceYZ) + Math.ulp(distanceXZ)); // small guard
        return distanceXZ <= distanceXY + distanceYZ + tol;
    }

    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    private static RealVector v(final double... components) {
        return new DoubleRealVector(components);
    }

    @Nonnull
    @SuppressWarnings("checkstyle:MethodName")
    private static RealVector randomV(@Nonnull final Random random, final int numDimensions) {
        final double[] components = new double[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            components[i] = random.nextDouble();
        }
        return new DoubleRealVector(components);
    }
}
