/*
 * RealVectorExactReductionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that the {@code …Exact} reduction variants on {@link RealVectorPrimitives} and
 * {@link RealVector} reproduce {@link ScalarBackend} <em>bit-for-bit</em>, independent of the
 * ambient backend.
 * <p>
 * This is deliberately stricter than {@link RealVectorPrimitivesParityTest}, which allows a
 * few-ULP tolerance because SIMD and scalar reductions legitimately disagree in their low bits.
 * The exact variants exist precisely so that a caller who persists a result and later compares it
 * byte-for-byte gets the same value on every host; a tolerance-based assertion would not catch a
 * regression that accidentally routed an exact variant through the SIMD backend. Under the default
 * {@code test} task the ambient backend is SIMD, so this proves the exact variants ignore it;
 * under {@code scalarFallbackTest} it degenerates into a scalar-vs-scalar smoke check.
 */
@Tag(Tags.DualScalarSIMD)
class RealVectorExactReductionTest {
    private static final ImmutableList<Integer> LENGTHS =
            ImmutableList.of(1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024);

    @Nonnull
    private static Stream<Arguments> randomSeedsWithLengths() {
        return RandomizedTestUtils.randomSeeds(0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L)
                .flatMap(seed -> LENGTHS.stream().map(len -> Arguments.of(seed, len)));
    }

    @ParameterizedTest(name = "seed={0}, len={1}")
    @MethodSource("randomSeedsWithLengths")
    @DisplayName("dotExact matches the scalar backend bit-for-bit")
    void dotExactMatchesScalarBackendBitForBit(final long seed, final int len) {
        final Random rnd = new Random(seed);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);
        final double reference = new ScalarBackend().dot(a, b);

        assertThat(RealVectorPrimitives.dotExact(a, b)).isEqualTo(reference);
        assertThat(new DoubleRealVector(a).dotExact(new DoubleRealVector(b))).isEqualTo(reference);
    }

    @ParameterizedTest(name = "seed={0}, len={1}")
    @MethodSource("randomSeedsWithLengths")
    @DisplayName("l2SquaredNormExact / l2NormExact match the scalar backend bit-for-bit")
    void l2NormExactMatchesScalarBackendBitForBit(final long seed, final int len) {
        final Random rnd = new Random(seed);
        final double[] a = randomVector(rnd, len);
        final double referenceSquared = new ScalarBackend().l2SquaredNorm(a);

        assertThat(RealVectorPrimitives.l2SquaredNormExact(a)).isEqualTo(referenceSquared);
        assertThat(new DoubleRealVector(a).l2SquaredNormExact()).isEqualTo(referenceSquared);
        assertThat(new DoubleRealVector(a).l2NormExact()).isEqualTo(Math.sqrt(referenceSquared));
    }

    @ParameterizedTest(name = "seed={0}, len={1}")
    @MethodSource("randomSeedsWithLengths")
    @DisplayName("l2SquaredDistanceExact matches the scalar backend bit-for-bit")
    void l2SquaredDistanceExactMatchesScalarBackendBitForBit(final long seed, final int len) {
        final Random rnd = new Random(seed);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);
        final double reference = new ScalarBackend().euclideanSquared(a, b);

        assertThat(RealVectorPrimitives.euclideanSquaredExact(a, b)).isEqualTo(reference);
        assertThat(new DoubleRealVector(a).l2SquaredDistanceExact(new DoubleRealVector(b))).isEqualTo(reference);
    }

    @ParameterizedTest(name = "seed={0}, len={1}")
    @MethodSource("randomSeedsWithLengths")
    @DisplayName("normalizeExact matches the scalar backend bit-for-bit")
    void normalizeExactMatchesScalarBackendBitForBit(final long seed, final int len) {
        final Random rnd = new Random(seed);
        final double[] a = randomVector(rnd, len);
        // Ensure the vector isn't degenerate so the norm is well-defined.
        a[0] += 1.0d;

        final double[] reference = new double[len];
        final double norm = Math.sqrt(new ScalarBackend().l2SquaredNorm(a));
        new ScalarBackend().multiplyInto(a, 1.0d / norm, reference);

        final double[] primitiveActual = RealVectorPrimitives.normalizeIntoExact(a, new double[len]);
        final double[] vectorActual = new DoubleRealVector(a).normalizeExact().getData();
        for (int i = 0; i < len; i++) {
            assertThat(primitiveActual[i]).as("primitive element[%d]", i).isEqualTo(reference[i]);
            assertThat(vectorActual[i]).as("vector element[%d]", i).isEqualTo(reference[i]);
        }
    }

    @Nonnull
    private static double[] randomVector(@Nonnull final Random rnd, final int len) {
        final double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = (rnd.nextDouble() - 0.5d) * 10.0d;
        }
        return v;
    }
}
