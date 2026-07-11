/*
 * RunningStatsTest.java
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

package com.apple.foundationdb.async.guardiann;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link RunningStats}: the parallel {@link RunningStats#subtract} inverse, the sample
 * (Bessel-corrected) statistics, the deliberately-stale {@link RunningStats#maxEver()}, and the raw-field
 * {@code toString}. The incremental {@code add}/{@code remove}/{@code combine} path is exercised indirectly by the
 * guardiann scenario tests; these focus on the branches those tests don't pin.
 */
class RunningStatsTest {
    @Test
    void subtractInvertsCombine() {
        final RunningStats a = RunningStats.of(1).add(2).add(3);
        final RunningStats b = RunningStats.of(10).add(20);
        final RunningStats combined = a.combine(b);

        final RunningStats back = combined.subtract(b);

        assertThat(back.numElements()).isEqualTo(a.numElements());
        assertThat(back.mean()).isCloseTo(a.mean(), within(1.0e-9));
        assertThat(back.populationVariance()).isCloseTo(a.populationVariance(), within(1.0e-9));
        // maxEver is intentionally NOT restored on subtract: it stays the combined (stale) upper bound, so it
        // over-estimates a's true max of 3.0.
        assertThat(back.maxEver()).isEqualTo(combined.maxEver());
        assertThat(back.maxEver()).isGreaterThan(a.maxEver());
    }

    @Test
    void subtractOfEmptyReturnsSameInstance() {
        final RunningStats a = RunningStats.of(1).add(2).add(3);
        assertThat(a.subtract(RunningStats.identity())).isSameAs(a);
    }

    @Test
    void subtractOfEqualSizeReturnsIdentity() {
        final RunningStats a = RunningStats.of(1).add(2).add(3);
        assertThat(a.subtract(a)).isSameAs(RunningStats.identity());
    }

    @Test
    void subtractOfLargerThrows() {
        final RunningStats small = RunningStats.of(1).add(2);
        final RunningStats large = RunningStats.of(1).add(2).add(3);
        assertThatThrownBy(() -> small.subtract(large)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subtractClampsTinyNegativeM2ToZero() {
        // The subtracted M2 exceeds this M2 only within floating-point roundoff (5e-13), so the result must clamp
        // to exactly 0.0 rather than a tiny negative. Built via the canonical constructor to force the boundary.
        final RunningStats base = new RunningStats(3L, 2.0d, 1.0d, 9.0d);
        final RunningStats subset = new RunningStats(2L, 2.0d, 1.0d + 5.0e-13, 9.0d);

        assertThat(base.subtract(subset).runningSumSquaredDeviations()).isEqualTo(0.0d);
    }

    @Test
    void subtractOfNonSubsetThrows() {
        // A subtracted M2 far larger than this one can only happen if "other" was never a subset; the negative-M2
        // guard turns that into an IllegalStateException rather than silently producing garbage statistics.
        final RunningStats base = new RunningStats(3L, 2.0d, 1.0d, 9.0d);
        final RunningStats bogus = new RunningStats(2L, 2.0d, 1000.0d, 9.0d);
        assertThatThrownBy(() -> base.subtract(bogus)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sampleVarianceAppliesBesselCorrection() {
        // Classic dataset {2,4,4,4,5,5,7,9}: mean 5, sum of squared deviations 32.
        final RunningStats stats = RunningStats.of(2).add(4).add(4).add(4).add(5).add(5).add(7).add(9);

        assertThat(stats.sampleVariance()).isCloseTo(32.0d / 7.0d, within(1.0e-9));
        assertThat(stats.sampleVariance())
                .isCloseTo(stats.populationVariance() * 8.0d / 7.0d, within(1.0e-9));
        assertThat(RunningStats.of(1).add(3).sampleVariance()).isCloseTo(2.0d, within(1.0e-9));
    }

    @Test
    void sampleVarianceRequiresTwoValues() {
        assertThatThrownBy(() -> RunningStats.identity().sampleVariance()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RunningStats.of(5.0d).sampleVariance()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sampleStandardDeviationIsSqrtOfSampleVariance() {
        final RunningStats stats = RunningStats.of(2).add(4).add(4).add(4).add(5).add(5).add(7).add(9);
        assertThat(stats.sampleStandardDeviation()).isCloseTo(Math.sqrt(32.0d / 7.0d), within(1.0e-9));
        assertThat(RunningStats.of(1).add(3).sampleStandardDeviation()).isCloseTo(Math.sqrt(2.0d), within(1.0e-9));
    }

    @Test
    void sampleStandardDeviationRequiresTwoValues() {
        assertThatThrownBy(() -> RunningStats.of(5.0d).sampleStandardDeviation())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void maxEverTracksMaximumAndReturnsNaNWhenEmpty() {
        assertThat(RunningStats.identity().maxEver()).isNaN();
        assertThat(RunningStats.of(5.0d).maxEver()).isEqualTo(5.0d);
        assertThat(RunningStats.of(2).add(9).add(4).maxEver()).isEqualTo(9.0d);
    }

    @Test
    void maxEverStaysStaleAfterRemove() {
        // remove() deliberately does not lower the max, so it remains an upper bound even after the max value goes.
        assertThat(RunningStats.of(2).add(9).add(4).remove(9.0d).maxEver()).isEqualTo(9.0d);
    }

    @Test
    void maxEverHandlesNegativeAndInfiniteValues() {
        // The empty sentinel is -Infinity; the guard must only fire for that, never for a legitimately negative max.
        assertThat(RunningStats.of(-3).add(-1).maxEver()).isEqualTo(-1.0d);
        assertThat(RunningStats.of(5).add(Double.POSITIVE_INFINITY).maxEver())
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void toStringRendersRawFields() {
        // Locks the raw runningMaxEver sentinel (-Infinity), which differs from what maxEver() surfaces (NaN).
        assertThat(RunningStats.identity().toString()).isEqualTo("RunningStats[0, 0.0, 0.0, -Infinity]");
        assertThat(RunningStats.of(5.0d).toString()).isEqualTo("RunningStats[1, 5.0, 0.0, 5.0]");
    }
}
