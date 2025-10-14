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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.async.hnsw.DoubleVector;
import com.apple.foundationdb.async.hnsw.Metrics;
import com.apple.foundationdb.async.hnsw.Vector;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class RaBitQuantizerTest {
    private static final Logger logger = LoggerFactory.getLogger(RaBitQuantizerTest.class);

    @Nonnull
    private static Stream<Arguments> randomSeedsWithDimensionalityAndNumExBits() {
        return Sets.cartesianProduct(ImmutableSet.of(3, 5, 10, 128, 768, 1000),
                        ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8))
                .stream()
                .flatMap(arguments ->
                        LongStream.generate(() -> new Random().nextLong())
                                .limit(3)
                                .mapToObj(seed -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @Test
    void basicEncodeTest() {
        final int dims = 768;
        final Random random = new Random(System.nanoTime());
        final Vector v = new DoubleVector(createRandomVector(random, dims));
        final Vector centroid = new DoubleVector(new double[dims]);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroid, 4);
        final EncodedVector encodedVector = quantizer.encode(v);
        final Vector v_bar = v.normalize();
        final double[] reCenteredData = new double[dims];
        for (int i = 0; i < dims; i ++) {
            reCenteredData[i] = (double)encodedVector.getEncodedComponent(i) - 15.5d;
        }
        final Vector reCentered = new DoubleVector(reCenteredData);
        final Vector reCenteredBar = reCentered.normalize();
        System.out.println(v_bar.dot(reCenteredBar));
    }

    @Test
    void basicEncodeWithEstimationTest() {
        final int dims = 768;
        final Random random = new Random(System.nanoTime());
        final Vector v = new DoubleVector(createRandomVector(random, dims));
        final Vector centroid = new DoubleVector(new double[dims]);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroid, 4);
        final EncodedVector encodedVector = quantizer.encode(v);
        final RaBitEstimator estimator = quantizer.estimator();
        final RaBitEstimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(v, encodedVector);
        System.out.println("estimated distance = " + estimatedDistance);
    }

    @Test
    void basicEncodeWithEstimationTest1() {
        final Vector v = new DoubleVector(new double[]{1.0d, 1.0d});
        final Vector centroid = new DoubleVector(new double[]{0.5d, 0.5d});
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroid, 4);
        final EncodedVector encodedVector = quantizer.encode(v);

        final Vector q = new DoubleVector(new double[]{1.0d, 1.0d});
        final RaBitEstimator estimator = quantizer.estimator();
        final RaBitEstimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(q, encodedVector);
        System.out.println("estimated distance = " + estimatedDistance);
        System.out.println(encodedVector);
    }

    @Test
    void encodeWithEstimationTest() {
        final long seed = 0;
        final int numDimensions = 3000;
        final int numExBits = 7;
        final Random random = new Random(seed);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);

        Vector v = null;
        Vector sum = null;
        final int numVectorsForCentroid = 10;
        for (int i = 0; i < numVectorsForCentroid; i ++) {
            v = new DoubleVector(createRandomVector(random, numDimensions));
            if (sum == null) {
                sum = v;
            } else {
                sum.add(v);
            }
        }

        final Vector centroid = sum.multiply(1.0d / numVectorsForCentroid);

        System.out.println("v =" + v);
        final Vector vRot = rotator.operateTranspose(v);
        final Vector centroidRot = rotator.operateTranspose(centroid);
        final Vector vTrans = vRot.subtract(centroidRot);

        final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroidRot, numExBits);
        final EncodedVector encodedVector = quantizer.encode(vTrans);
        final Vector reconstructedV = rotator.operate(encodedVector.add(centroidRot));
        System.out.println("reconstructed v = " + reconstructedV);
        final RaBitEstimator estimator = quantizer.estimator();
        final RaBitEstimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(vTrans, encodedVector);
        System.out.println("estimated distance = " + estimatedDistance);
        System.out.println("true distance = " + Metrics.EUCLIDEAN_SQUARE_METRIC.distance(v, reconstructedV));
    }

    @ParameterizedTest(name = "seed={0} dimensionality={1} numExBits={2}")
    @MethodSource("randomSeedsWithDimensionalityAndNumExBits")
    void encodeWithEstimationTest2(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);
        final int numRounds = 500;
        int numEstimationWithinBounds = 0;
        int numEstimationBetter = 0;
        double sumRelativeError = 0.0d;
        for (int round = 0; round < numRounds; round ++) {
            Vector v = null;
            Vector q = null;
            Vector sum = null;
            final int numVectorsForCentroid = 10;
            for (int i = 0; i < numVectorsForCentroid; i++) {
                if (q == null) {
                    if (v != null) {
                        q = v;
                    }
                }

                v = new DoubleVector(createRandomVector(random, numDimensions));
                if (sum == null) {
                    sum = v;
                } else {
                    sum.add(v);
                }
            }
            Objects.requireNonNull(v);
            Objects.requireNonNull(q);

            final Vector centroid = sum.multiply(1.0d / numVectorsForCentroid);

            logger.trace("q = {}", q);
            logger.trace("v = {}", v);
            logger.trace("centroid = {}", centroid);

            final Vector centroidRot = rotator.operateTranspose(centroid);
            final Vector qTrans = rotator.operateTranspose(q).subtract(centroidRot);
            final Vector vTrans = rotator.operateTranspose(v).subtract(centroidRot);

            logger.trace("qTrans = {}", qTrans);
            logger.trace("vTrans = {}", vTrans);
            logger.trace("centroidRot = {}", centroidRot);

            final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroidRot, numExBits);
            final RaBitQuantizer.Result resultV = quantizer.encodeInternal(vTrans);
            final EncodedVector encodedV = resultV.encodedVector;
            logger.trace("fAddEx vor v = {}", encodedV.getAddEx());
            logger.trace("fRescaleEx vor v = {}", encodedV.getRescaleEx());
            logger.trace("fErrorEx vor v = {}", encodedV.getErrorEx());

            final EncodedVector encodedQ = quantizer.encode(qTrans);
            final RaBitEstimator estimator = quantizer.estimator();
            final Vector reconstructedQ = rotator.operate(encodedQ.add(centroidRot));
            final Vector reconstructedV = rotator.operate(encodedV.add(centroidRot));
            final RaBitEstimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(qTrans, encodedV);
            logger.trace("estimated ||qRot - vRot||^2 = {}", estimatedDistance);
            final double trueDistance = Metrics.EUCLIDEAN_SQUARE_METRIC.distance(vTrans, qTrans);
            logger.trace("true ||qRot - vRot||^2 = {}", trueDistance);
            if (trueDistance >= estimatedDistance.getDistance() - estimatedDistance.getErr() &&
                    trueDistance < estimatedDistance.getDistance() + estimatedDistance.getErr()) {
                numEstimationWithinBounds++;
            }
            logger.trace("reconstructed q = {}", reconstructedQ);
            logger.trace("reconstructed v = {}", reconstructedV);
            logger.trace("true ||qDec - vDec||^2 = {}", Metrics.EUCLIDEAN_SQUARE_METRIC.distance(reconstructedV, reconstructedQ));
            final double reconstructedDistance = Metrics.EUCLIDEAN_SQUARE_METRIC.distance(reconstructedV, q);
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
    }

    @ParameterizedTest(name = "seed={0} dimensionality={1} numExBits={2}")
    @MethodSource("randomSeedsWithDimensionalityAndNumExBits")
    void serializationRoundTripTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final Vector v = new DoubleVector(createRandomVector(random, numDimensions));
        final Vector centroid = new DoubleVector(new double[numDimensions]);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metrics.EUCLIDEAN_SQUARE_METRIC, centroid, numExBits);
        final EncodedVector encodedVector = quantizer.encode(v);
        final byte[] rawData = encodedVector.getRawData();
        final EncodedVector deserialized = EncodedVector.fromBytes(rawData, 1, numDimensions, numExBits);
        Assertions.assertThat(deserialized).isEqualTo(encodedVector);
    }

    private static double[] createRandomVector(final Random random, final int dims) {
        final double[] components = new double[dims];
        for (int d = 0; d < dims; d ++) {
            components[d] = random.nextDouble() * (random.nextBoolean() ? -1 : 1);
        }
        return components;
    }
}
