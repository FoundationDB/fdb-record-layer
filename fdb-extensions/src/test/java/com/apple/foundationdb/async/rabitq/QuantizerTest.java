/*
 * QuantizerTest.java
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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class QuantizerTest {
    @Test
    void basicEncodeTest() {
        final int dims = 768;
        final Random random = new Random(System.nanoTime());
        final Vector v = new DoubleVector(createRandomVector(random, dims));
        final Vector centroid = new DoubleVector(new double[dims]);
        final Quantizer quantizer = new Quantizer(centroid, 4, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result result = quantizer.encode(v);
        final EncodedVector encodedVector = result.encodedVector;
        final Vector v_bar = v.normalize();
        final double[] recentered_data = new double[dims];
        for (int i = 0; i < dims; i ++) {
            recentered_data[i] = (double)encodedVector.getEncodedComponent(i) - 15.5d;
        }
        final Vector recentered = new DoubleVector(recentered_data);
        final Vector recentered_bar = recentered.normalize();
        System.out.println(v_bar.dot(recentered_bar));
    }

    @Test
    void basicEncodeWithEstimationTest() {
        final int dims = 768;
        final Random random = new Random(System.nanoTime());
        final Vector v = new DoubleVector(createRandomVector(random, dims));
        final Vector centroid = new DoubleVector(new double[dims]);
        final Quantizer quantizer = new Quantizer(centroid, 4, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result result = quantizer.encode(v);
        final Estimator estimator = quantizer.estimator();
        final Estimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(v, result.encodedVector);
        System.out.println("estimated distance = " + estimatedDistance);
    }

    @Test
    void basicEncodeWithEstimationTest1() {
        final Vector v = new DoubleVector(new double[]{1.0d, 1.0d});
        final Vector centroid = new DoubleVector(new double[]{0.5d, 0.5d});
        final Quantizer quantizer = new Quantizer(centroid, 4, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result result = quantizer.encode(v);

        final Vector q = new DoubleVector(new double[]{1.0d, 1.0d});
        final Estimator estimator = quantizer.estimator();
        final EncodedVector encodedVector = result.encodedVector;
        final Estimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(q, encodedVector);
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

        final Quantizer quantizer = new Quantizer(centroidRot, numExBits, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result result = quantizer.encode(vRot);
        final EncodedVector encodedVector = result.encodedVector;
        final Vector reconstructedV = rotator.operate(encodedVector.add(centroidRot));
        System.out.println("reconstructed v = " + reconstructedV);
        final Estimator estimator = quantizer.estimator();
        final Estimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(vRot, encodedVector);
        System.out.println("estimated distance = " + estimatedDistance);
        System.out.println("true distance = " + Vector.distance(Metrics.EUCLIDEAN_SQUARE_METRIC, v, reconstructedV));
    }

    @Test
    void encodeWithEstimationTest2() {
        final long seed = 10;
        final int numDimensions = 3000;
        final int numExBits = 4;
        final Random random = new Random(seed);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);

        Vector v = null;
        Vector q = null;
        Vector sum = null;
        final int numVectorsForCentroid = 10;
        for (int i = 0; i < numVectorsForCentroid; i ++) {
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

//        System.out.println("q =" + q);
//        System.out.println("v =" + v);
//        System.out.println("centroid =" + centroid);

        final Vector vRot = rotator.operateTranspose(v);
        final Vector centroidRot = rotator.operateTranspose(centroid);
        final Vector qRot = rotator.operateTranspose(q);
//        System.out.println("qRot =" + qRot);
//        System.out.println("vRot =" + vRot);
//        System.out.println("centroidRot =" + centroidRot);

        final Quantizer quantizer = new Quantizer(centroidRot, numExBits, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result resultV = quantizer.encode(vRot);
        final EncodedVector encodedV = resultV.encodedVector;
//        System.out.println("fAddEx vor v = " + encodedV.fAddEx);
//        System.out.println("fRescaleEx vor v = " + encodedV.fRescaleEx);
//        System.out.println("fErrorEx vor v = " + encodedV.fErrorEx);


        final Quantizer.Result resultQ = quantizer.encode(qRot);
        final EncodedVector encodedQ = resultQ.encodedVector;

        final Estimator estimator = quantizer.estimator();
        final Estimator.Result estimatedDistance = estimator.estimateDistanceAndErrorBound(qRot, encodedV);
        System.out.println("estimated ||qRot - vRot||^2 = " + estimatedDistance);
        System.out.println("true ||qRot - vRot||^2 = " + Vector.distance(Metrics.EUCLIDEAN_SQUARE_METRIC, vRot, qRot));

        final Vector reconstructedV = rotator.operate(encodedV.add(centroidRot));
        System.out.println("reconstructed v = " + reconstructedV);

        final Vector reconstructedQ = rotator.operate(encodedQ.add(centroidRot));
        System.out.println("reconstructed q = " + reconstructedQ);

        System.out.println("true ||qDec - vDec||^2 = " + Vector.distance(Metrics.EUCLIDEAN_SQUARE_METRIC, reconstructedV, reconstructedQ));

        encodedV.getRawData();
    }

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

    @ParameterizedTest(name = "seed={0} dimensionality={1} numExBits={2}")
    @MethodSource("randomSeedsWithDimensionalityAndNumExBits")
    void serializationRoundTripTest(final long seed, final int numDimensions, final int numExBits) {
        final Random random = new Random(seed);
        final Vector v = new DoubleVector(createRandomVector(random, numDimensions));
        final Vector centroid = new DoubleVector(new double[numDimensions]);
        final Quantizer quantizer = new Quantizer(centroid, numExBits, Metrics.EUCLIDEAN_SQUARE_METRIC);
        final Quantizer.Result result = quantizer.encode(v);
        final EncodedVector encodedVector = result.encodedVector;
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
