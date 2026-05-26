/*
 * RealVectorPrimitivesParityTest.java
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Asserts that every primitive in {@link RealVectorPrimitives} produces results equivalent to the
 * scalar implementation ({@link ScalarBackend}) across a representative range of vector sizes.
 * <p>
 * Comparison uses absolute and relative tolerances rather than bit-exact equality because SIMD
 * reductions sum partial lanes in a different order than scalar accumulation; small differences in
 * the low-order bits are expected and not a bug.
 * <p>
 * The test runs against whichever backend {@link RealVectorPrimitives} resolves — so under the
 * default {@code test} task it validates SIMD-vs-scalar parity, and under
 * {@code testScalarFallback} it degenerates into scalar-vs-scalar (still a useful smoke check of
 * the new {@code double[]} primitives).
 */
class RealVectorPrimitivesParityTest {
    private static final long SEED = 0x0fdbL;

    /** Per-element absolute tolerance for elementwise results. SIMD vs scalar should match within ULPs. */
    private static final double ELEMENT_ABS_TOL = 1.0e-12d;

    /** Tolerance for reductions; scales with vector length since accumulation order differs. */
    private static double reductionTol(final int len, final double reference) {
        return Math.max(1.0e-12d, Math.abs(reference) * 1.0e-12d * len);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("add(a, b)")
    void add(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);

        final double[] actual = new double[len];
        RealVectorPrimitives.addInto(a, b, actual);

        final double[] expected = new double[len];
        new ScalarBackend().addInto(a, b, expected);

        assertVectorClose(expected, actual);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("addScalar(a, s)")
    void addScalar(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double s = rnd.nextDouble();

        final double[] actual = new double[len];
        RealVectorPrimitives.addInto(a, s, actual);

        final double[] expected = new double[len];
        new ScalarBackend().addInto(a, s, expected);

        assertVectorClose(expected, actual);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("subtract(a, b)")
    void subtract(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);

        final double[] actual = new double[len];
        RealVectorPrimitives.subtractInto(a, b, actual);

        final double[] expected = new double[len];
        new ScalarBackend().subtractInto(a, b, expected);

        assertVectorClose(expected, actual);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("subtractScalar(a, s)")
    void subtractScalar(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double s = rnd.nextDouble();

        final double[] actual = new double[len];
        RealVectorPrimitives.subtractInto(a, s, actual);

        final double[] expected = new double[len];
        new ScalarBackend().subtractInto(a, s, expected);

        assertVectorClose(expected, actual);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("multiplyScalar(a, s)")
    void multiplyScalar(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double s = rnd.nextDouble();

        final double[] actual = new double[len];
        RealVectorPrimitives.multiplyInto(a, s, actual);

        final double[] expected = new double[len];
        new ScalarBackend().multiplyInto(a, s, expected);

        assertVectorClose(expected, actual);
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("dot(a, b)")
    void dot(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);

        final double actual = RealVectorPrimitives.dot(a, b);
        final double expected = new ScalarBackend().dot(a, b);

        assertThat(actual).isCloseTo(expected, within(reductionTol(len, expected)));
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("l2SquaredNorm(a)")
    void l2SquaredNorm(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);

        final double actual = RealVectorPrimitives.l2SquaredNorm(a);
        final double expected = new ScalarBackend().l2SquaredNorm(a);

        assertThat(actual).isCloseTo(expected, within(reductionTol(len, expected)));
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("euclideanSquared(a, b)")
    void euclideanSquared(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        final double[] b = randomVector(rnd, len);

        final double actual = RealVectorPrimitives.euclideanSquared(a, b);
        final double expected = new ScalarBackend().euclideanSquared(a, b);

        assertThat(actual).isCloseTo(expected, within(reductionTol(len, expected)));
    }

    @ParameterizedTest(name = "len={0}")
    @ValueSource(ints = {1, 2, 3, 7, 8, 15, 16, 17, 64, 65, 128, 1024})
    @DisplayName("normalize(a)")
    void normalize(final int len) {
        final Random rnd = new Random(SEED ^ len);
        final double[] a = randomVector(rnd, len);
        // Ensure the vector isn't degenerate.
        a[0] += 1.0d;

        final double[] actual = new double[len];
        RealVectorPrimitives.normalizeInto(a, actual);

        final double[] expected = new double[len];
        // Scalar reference: compute norm via scalar backend and divide.
        final double norm = Math.sqrt(new ScalarBackend().l2SquaredNorm(a));
        new ScalarBackend().multiplyInto(a, 1.0d / norm, expected);

        assertVectorClose(expected, actual);
    }

    private static void assertVectorClose(@Nonnull final double[] expected, @Nonnull final double[] actual) {
        assertThat(actual).hasSize(expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(actual[i])
                    .as("element[%d]", i)
                    .isCloseTo(expected[i], within(ELEMENT_ABS_TOL));
        }
    }

    @Nonnull
    private static double[] randomVector(@Nonnull final Random rnd, final int len) {
        final double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            // Range that exercises both small and large magnitudes without overflowing reductions.
            v[i] = (rnd.nextDouble() - 0.5d) * 10.0d;
        }
        return v;
    }
}
