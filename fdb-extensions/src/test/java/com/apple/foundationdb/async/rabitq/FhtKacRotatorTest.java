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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.async.hnsw.DoubleVector;
import com.apple.foundationdb.async.hnsw.Vector;
import com.apple.foundationdb.async.hnsw.VectorTest;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class FhtKacRotatorTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithDimensionality() {
        return Sets.cartesianProduct(ImmutableSet.of(3, 5, 10, 128, 768, 1000))
                .stream()
                .flatMap(arguments ->
                        LongStream.generate(() -> new Random().nextLong())
                                .limit(3)
                                .mapToObj(seed -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @ParameterizedTest(name = "seed={0} dimensionality={1}")
    @MethodSource("randomSeedsWithDimensionality")
    void testSimpleTest(final long seed, final int dimensionality) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, dimensionality, 10);

        final Random random = new Random(seed);
        final Vector x = VectorTest.createRandomDoubleVector(random, dimensionality);

        final Vector y = rotator.operate(x);
        final Vector z = rotator.operateTranspose(y);

        // Verify ||x|| ≈ ||y|| and P^T P ≈ I
        double nx = norm2(x);
        double ny = norm2(y);
        double maxErr = maxAbsDiff(x, z);
        System.out.printf("||x|| = %.6f  ||Px|| = %.6f  max|x - P^T P x|=%.3e%n", nx, ny, maxErr);
    }

    @ParameterizedTest(name = "seed={0} dimensionality={1}")
    @MethodSource("randomSeedsWithDimensionality")
    void testOrthogonality(final long seed, final int dimensionality) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, dimensionality, 10);
        final ColumnMajorMatrix p = new ColumnMajorMatrix(rotator.computeP().transpose().getData());

        for (int j = 0; j < dimensionality; j ++) {
            final Vector rotated = rotator.operateTranspose(new DoubleVector(p.getColumn(j)));
            for (int i = 0; i < dimensionality; i++) {
                double expected = (i == j) ? 1.0 : 0.0;
                Assertions.assertThat(Math.abs(rotated.getComponent(i) - expected))
                        .satisfies(difference -> Assertions.assertThat(difference).isLessThan(10E-9d));
            }
        }
    }

    @ParameterizedTest(name = "seed={0} dimensionality={1}")
    @MethodSource("randomSeedsWithDimensionality")
    void testOrthogonalityWithP(final long seed, final int dimensionality) {
        final FhtKacRotator rotator = new FhtKacRotator(seed, dimensionality, 10);
        final Matrix p = rotator.computeP();
        final Matrix product = p.transpose().multiply(p);

        for (int i = 0; i < dimensionality; i++) {
            for (int j = 0; j < dimensionality; j++) {
                double expected = (i == j) ? 1.0 : 0.0;
                Assertions.assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .satisfies(difference -> Assertions.assertThat(difference).isLessThan(10E-9d));
            }
        }
    }

    private static double norm2(@Nonnull final Vector a) {
        double s = 0;
        for (double v : a.getData()) {
            s += v * v;
        }
        return Math.sqrt(s);
    }

    private static double maxAbsDiff(@Nonnull final Vector a, @Nonnull final Vector b) {
        double m = 0;
        for (int i = 0; i < a.getNumDimensions(); i++) {
            m = Math.max(m, Math.abs(a.getComponent(i) - b.getComponent(i)));
        }
        return m;
    }
}
