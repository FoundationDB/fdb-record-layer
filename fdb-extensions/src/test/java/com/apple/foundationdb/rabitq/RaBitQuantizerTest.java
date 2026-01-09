/*
 * RaBitQuantizerTest.java
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

package com.apple.foundationdb.rabitq;

import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.linear.Transformed;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomDoubleVector;

public class RaBitQuantizerTest {
    private static final Logger logger = LoggerFactory.getLogger(RaBitQuantizerTest.class);

    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensionsAndNumExBits() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed ->
                        Sets.cartesianProduct(ImmutableSet.of(3, 5, 10, 128, 768, 1000),
                                        ImmutableSet.of(4, 5, 6, 7, 8))
                                .stream()
                                .map(arguments -> Arguments.of(seed, arguments.get(0), arguments.get(1))));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void basicEncodeTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final RealVector v = createRandomDoubleVector(random, numDimensions);

        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);
        final EncodedRealVector encodedVector = quantizer.encode(v);

        // v and the re-centered encoded vector should be pointing into the same direction
        final double[] reCenteredData = new double[numDimensions];
        final double cb = -(((1 << numExBits) - 0.5));
        for (int i = 0; i < numDimensions; i ++) {
            reCenteredData[i] = (double)encodedVector.getEncodedComponent(i) + cb;
        }
        final RealVector reCentered = new DoubleRealVector(reCenteredData);

        // normalize both vectors so their dot product should be 1
        final RealVector v_bar = v.normalize();
        final RealVector reCenteredBar = reCentered.normalize();
        Assertions.assertThat(v_bar.dot(reCenteredBar)).isCloseTo(1, Offset.offset(0.01));
    }

    /**
     * Create a random vector {@code v}, encode it into {@code encodedV} and estimate the distance between {@code v} and
     * {@code encodedV} which should be very close to {@code 0}.
     * @param seed a seed
     * @param numDimensions the number of dimensions
     * @param numExBits the number of bits per dimension used for encoding
     */
    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void basicEncodeWithEstimationTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final RealVector v = createRandomDoubleVector(random, numDimensions);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);
        final EncodedRealVector encodedV = quantizer.encode(v);
        final RaBitEstimator estimator = quantizer.estimator();
        final double estimatedDistance = estimator.distance(v, encodedV);
        Assertions.assertThat(estimatedDistance).isCloseTo(0.0d, Offset.offset(0.01));
    }

    /**
     * Create a random vector {@code v}, encode it into {@code encodedV} and estimate the distance between {@code v} and
     * {@code encodedV} which should be very close to {@code 0}.
     * @param seed a seed
     * @param numDimensions the number of dimensions
     * @param numExBits the number of bits per dimension used for encoding
     */
    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void basicEncodeWithEstimationCosineMetricTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final RealVector v = createRandomDoubleVector(random, numDimensions);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.COSINE_METRIC, numExBits);
        final EncodedRealVector encodedV = quantizer.encode(v);
        final RaBitEstimator estimator = quantizer.estimator();
        final double estimatedDistance = estimator.distance(v, encodedV);
        Assertions.assertThat(estimatedDistance).isCloseTo(0.0d, Offset.offset(0.01));
    }

    @Nonnull
    private static Stream<Arguments> estimationArgs() {
        return Stream.of(
                Arguments.of(new double[]{0.5d, 0.5d}, new double[]{1.0d, 1.0d}, new double[]{-1.0d, 1.0d}, 4.0d),
                Arguments.of(new double[]{0.0d, 0.0d}, new double[]{1.0d, 0.0d}, new double[]{0.0d, 1.0d}, 2.0d),
                Arguments.of(new double[]{0.0d, 0.0d}, new double[]{0.0d, 0.0d}, new double[]{1.0d, 1.0d}, 2.0d)
                );
    }

    @ParameterizedTest
    @MethodSource("estimationArgs")
    void basicEncodeWithEstimationTestSpecialValues(final double[] centroidData, final double[] vData,
                                                    final double[] qData, final double expectedDistance) {
        final RealVector centroid = new DoubleRealVector(centroidData);
        final AffineOperator operator = new AffineOperator(null, centroid.multiply(-1.0d));
        final Transformed<RealVector> v = operator.transform(new DoubleRealVector(vData));
        final Transformed<RealVector> q = operator.transform(new DoubleRealVector(qData));

        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, 7);
        final Transformed<RealVector> encodedVector = quantizer.encode(v);
        final RaBitEstimator estimator = quantizer.estimator();
        final RaBitEstimator.Result estimatedDistanceResult =
                estimator.estimateDistanceAndErrorBound(q.getUnderlyingVector(),
                        (EncodedRealVector)encodedVector.getUnderlyingVector());
        logger.info("estimated distance result = {}", estimatedDistanceResult);
        Assertions.assertThat(estimatedDistanceResult.getDistance())
                .isCloseTo(expectedDistance, Offset.offset(0.01d));

        final Transformed<RealVector> encodedVector2 = quantizer.encode(v);
        Assertions.assertThat(encodedVector2.hashCode()).isEqualTo(encodedVector.hashCode());
        Assertions.assertThat(encodedVector2).isEqualTo(encodedVector);
        Assertions.assertThat(encodedVector.hashCode()).isEqualTo(encodedVector.getUnderlyingVector().hashCode());
        Assertions.assertThat(encodedVector.toString()).isEqualTo(encodedVector.getUnderlyingVector().toString());
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void encodeManyWithEstimationsTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);
        final int numRounds = 500;
        int numEstimationWithinBounds = 0;
        int numEstimationBetter = 0;
        double sumRelativeError = 0.0d;
        for (int round = 0; round < numRounds; round ++) {
            RealVector v = null;
            RealVector q = null;
            RealVector sum = null;
            final int numVectorsForCentroid = 10;
            for (int i = 0; i < numVectorsForCentroid; i++) {
                if (q == null) {
                    if (v != null) {
                        q = v;
                    }
                }

                v = RealVectorTest.createRandomDoubleVector(random, numDimensions);
                if (sum == null) {
                    sum = v;
                } else {
                    sum.add(v);
                }
            }
            Objects.requireNonNull(v);
            Objects.requireNonNull(q);

            final RealVector centroid = sum.multiply(1.0d / numVectorsForCentroid);

            logger.trace("q = {}", q);
            logger.trace("v = {}", v);
            logger.trace("centroid = {}", centroid);

            final RealVector centroidRot = rotator.apply(centroid);
            final AffineOperator operator = new AffineOperator(rotator, centroidRot.multiply(-1.0d));
            final Transformed<RealVector> qTrans = operator.transform(q);
            final Transformed<RealVector> vTrans = operator.transform(v);

            logger.trace("qTrans = {}", qTrans);
            logger.trace("vTrans = {}", vTrans);
            logger.trace("centroidRot = {}", centroidRot);

            final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);
            final Transformed<RealVector> encodedV = quantizer.encode(vTrans);
            final Transformed<RealVector> encodedQ = quantizer.encode(qTrans);
            final RaBitEstimator estimator = quantizer.estimator();
            final RealVector reconstructedQ = operator.untransform(encodedQ);
            final RealVector reconstructedV = operator.untransform(encodedV);
            final RaBitEstimator.Result estimatedDistance =
                    estimator.estimateDistanceAndErrorBound(qTrans.getUnderlyingVector(),
                            (EncodedRealVector)encodedV.getUnderlyingVector());
            logger.trace("estimated ||qRot - vRot||^2 = {}", estimatedDistance);
            final double trueDistance =
                    Metric.EUCLIDEAN_SQUARE_METRIC.distance(vTrans.getUnderlyingVector(),
                            qTrans.getUnderlyingVector());
            logger.trace("true ||qRot - vRot||^2 = {}", trueDistance);
            if (trueDistance >= estimatedDistance.getDistance() - estimatedDistance.getErr() &&
                    trueDistance < estimatedDistance.getDistance() + estimatedDistance.getErr()) {
                numEstimationWithinBounds++;
            }
            logger.trace("reconstructed q = {}", reconstructedQ);
            logger.trace("reconstructed v = {}", reconstructedV);
            logger.trace("true ||qDec - vDec||^2 = {}", Metric.EUCLIDEAN_SQUARE_METRIC.distance(reconstructedV, reconstructedQ));
            final double reconstructedDistance = Metric.EUCLIDEAN_SQUARE_METRIC.distance(reconstructedV, q);
            logger.trace("true ||q - vDec||^2 = {}", reconstructedDistance);
            double error = Math.abs(estimatedDistance.getDistance() - trueDistance);
            if (error < Math.abs(reconstructedDistance - trueDistance)) {
                numEstimationBetter ++;
            }
            sumRelativeError += error / trueDistance;
        }
        logger.info("estimator within bounds = {}%", String.format(Locale.ROOT, "%.2f", (double)numEstimationWithinBounds * 100.0d / numRounds));
        logger.info("estimator better than reconstructed distance = {}%", String.format(Locale.ROOT, "%.2f", (double)numEstimationBetter * 100.0d / numRounds));
        logger.info("relative error = {}%", String.format(Locale.ROOT, "%.2f", sumRelativeError * 100.0d / numRounds));

        Assertions.assertThat((double)numEstimationWithinBounds / numRounds).isGreaterThan(0.8);
        Assertions.assertThat((double)numEstimationBetter / numRounds).isBetween(0.3, 0.7);
        Assertions.assertThat(sumRelativeError / numRounds).isLessThan(0.1d);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void encodeManyWithEstimationsCosineMetricTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);
        final int numRounds = 500;
        int numEstimationWithinBounds = 0;
        int numEstimationBetter = 0;
        double sumRelativeError = 0.0d;
        for (int round = 0; round < numRounds; round ++) {
            RealVector v = RealVectorTest.createRandomDoubleVector(random, numDimensions);
            RealVector q = RealVectorTest.createRandomDoubleVector(random, numDimensions);
            v = v.normalize();
            q = q.normalize();

            logger.info("q = {}", q);
            logger.info("v = {}", v);

            final AffineOperator operator = new AffineOperator(rotator, null);
            final Transformed<RealVector> qRot = operator.transform(q);
            final Transformed<RealVector> vRot = operator.transform(v);

            logger.info("qRot = {}", qRot);
            logger.info("vRot = {}", vRot);

            final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.COSINE_METRIC, numExBits);
            final Transformed<RealVector> encodedV = quantizer.encode(vRot);
            final Transformed<RealVector> encodedQ = quantizer.encode(qRot);
            final RaBitEstimator estimator = quantizer.estimator();
            final RealVector reconstructedQ = operator.untransform(encodedQ);
            final RealVector reconstructedV = operator.untransform(encodedV);
            final RaBitEstimator.Result estimatedDistance =
                    estimator.estimateDistanceAndErrorBound(qRot.getUnderlyingVector(),
                            (EncodedRealVector)encodedV.getUnderlyingVector());
            logger.info("estimated 1 - cos(qRot, vRot) = {}", estimatedDistance);
            final double trueDistance =
                    Metric.COSINE_METRIC.distance(vRot.getUnderlyingVector(),
                            qRot.getUnderlyingVector());
            logger.info("true 1 - cos(qRot, vRot) = {}", trueDistance);
            if (trueDistance >= estimatedDistance.getDistance() - estimatedDistance.getErr() &&
                    trueDistance < estimatedDistance.getDistance() + estimatedDistance.getErr()) {
                numEstimationWithinBounds++;
            }
            logger.info("reconstructed q = {}", reconstructedQ);
            logger.info("reconstructed v = {}", reconstructedV);
            logger.info("true 1 - cos(qDec, vDec) = {}", Metric.COSINE_METRIC.distance(reconstructedV, reconstructedQ));
            final double reconstructedDistance = Metric.COSINE_METRIC.distance(reconstructedV, q);
            logger.info("true 1 - cos(q, vDec) = {}", reconstructedDistance);
            double error = Math.abs(estimatedDistance.getDistance() - trueDistance);
            if (error < Math.abs(reconstructedDistance - trueDistance)) {
                numEstimationBetter ++;
            }
            sumRelativeError += error / trueDistance;
        }
        logger.info("(cosine metric) estimator within bounds = {}%", String.format(Locale.ROOT, "%.2f", (double)numEstimationWithinBounds * 100.0d / numRounds));
        logger.info("(cosine metric) estimator better than reconstructed distance = {}%", String.format(Locale.ROOT, "%.2f", (double)numEstimationBetter * 100.0d / numRounds));
        logger.info("(cosine metric) relative error = {}%", String.format(Locale.ROOT, "%.2f", sumRelativeError * 100.0d / numRounds));

        Assertions.assertThat((double)numEstimationWithinBounds / numRounds).isGreaterThan(0.8);
        Assertions.assertThat((double)numEstimationBetter / numRounds).isBetween(0.3, 0.7);
        Assertions.assertThat(sumRelativeError / numRounds).isLessThan(0.1d);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void serializationRoundTripTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final RealVector v = createRandomDoubleVector(random, numDimensions);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);
        final EncodedRealVector encodedVector = quantizer.encode(v);
        final byte[] rawData = encodedVector.getRawData();
        final EncodedRealVector deserialized = EncodedRealVector.fromBytes(rawData, numDimensions, numExBits);
        Assertions.assertThat(deserialized).isEqualTo(encodedVector);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensionsAndNumExBits")
    void precisionTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final RealVector v = createRandomDoubleVector(random, numDimensions);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);
        final EncodedRealVector encodedVector = quantizer.encode(v);
        final DoubleRealVector reconstructedDoubleVector = encodedVector.toDoubleRealVector();
        Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(encodedVector.toFloatRealVector(),
                        reconstructedDoubleVector.toFloatRealVector())).isCloseTo(0, Offset.offset(0.1));
        Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(encodedVector.toHalfRealVector(),
                reconstructedDoubleVector.toHalfRealVector())).isCloseTo(0, Offset.offset(0.1));
    }
}
