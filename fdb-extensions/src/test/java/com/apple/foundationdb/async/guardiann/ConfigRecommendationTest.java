/*
 * ConfigRecommendationTest.java
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

import com.apple.foundationdb.linear.Metric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the relationships {@link ConfigRecommendation} exists to maintain. These are not assertions about particular
 * numbers — the point is that they keep holding as {@code primaryClusterMax} changes, since each one describes a way
 * the structure misbehaves when the ratio is wrong.
 */
class ConfigRecommendationTest {
    private static final int NUM_DIMENSIONS = 128;

    @ParameterizedTest
    @ValueSource(ints = {50, 128, 512, 1000, 4096, 20_000})
    void mergingACoupleOfClustersCannotImmediatelySplitTheResult(final int primaryClusterMax) {
        final Config config = recommend(primaryClusterMax);
        // A 2->1 merge of two threshold-sized clusters yields roughly 2 * primaryClusterMin. If that reached
        // primaryClusterMax the structure would ping-pong between merging and splitting the same vectors.
        assertThat(2 * config.primaryClusterMin())
                .as("a merged pair must land well clear of the split ceiling")
                .isLessThan(config.primaryClusterMax());
    }

    @ParameterizedTest
    @ValueSource(ints = {50, 128, 512, 1000, 4096, 20_000})
    void theMaxEverFractionCanActuallyEngage(final int primaryClusterMax) {
        final Config config = recommend(primaryClusterMax);
        // mergeThreshold is max(primaryClusterMin, fraction * maxEver), so the fraction only ever matters for
        // clusters whose peak exceeds primaryClusterMin / fraction. If the floor sat above fraction * max, no
        // cluster could ever reach that peak and the hysteresis would be dead code.
        final int peakAtWhichFractionEngages =
                (int)Math.ceil(config.primaryClusterMin() / config.mergeMaxEverFraction());
        assertThat(peakAtWhichFractionEngages)
                .as("some attainable cluster peak must make the max-ever fraction bind")
                .isLessThan(config.primaryClusterMax());
    }

    @ParameterizedTest
    @ValueSource(ints = {50, 128, 512, 1000, 4096, 20_000})
    void smallestPermittedChildIsNotBornWantingToMerge(final int primaryClusterMax) {
        final Config config = recommend(primaryClusterMax);
        // A split fires at no fewer than primaryClusterMax vectors, so the smallest child a split may produce holds
        // minChildFraction * primaryClusterMax. Below primaryClusterMin it would be merge-eligible on arrival, which
        // makes the split self-defeating.
        final int smallestPermittedChild = (int)Math.floor(config.minChildFraction() * config.primaryClusterMax());
        assertThat(smallestPermittedChild)
                .as("the smallest child a split may produce must not already be undersized")
                .isGreaterThanOrEqualTo(config.primaryClusterMin());
    }

    @ParameterizedTest
    @ValueSource(ints = {50, 128, 512, 1000, 4096, 20_000})
    void everyRecommendationIsSelfConsistent(final int primaryClusterMax) {
        final Config config = recommend(primaryClusterMax);
        // Config's own compact constructor enforces these; asserting them here means a bad ratio shows up as a
        // readable failure rather than as an IllegalArgumentException from deep inside build().
        assertThat(config.primaryClusterHardMax()).isGreaterThan(config.primaryClusterMax());
        assertThat(config.collapseMinDuplicates()).isLessThan(config.primaryClusterMax());
        assertThat(config.primaryClusterMin()).isPositive();
        assertThat(config.toBuilder().build(NUM_DIMENSIONS))
                .as("a recommendation must survive a builder round trip")
                .isEqualTo(config);
    }

    @Test
    void theRecommendationForTheMeasuredDatasetMatchesWhatWasMeasured() {
        // 512 is the cluster size the SIFT drain workload was tuned against; these are the values that produced a
        // clean run, recorded here so a change to the ratios has to confront them.
        final Config config = recommend(512);
        assertThat(config.primaryClusterMin()).isEqualTo(51);
        assertThat(config.primaryClusterHardMax()).isEqualTo(1024);
        assertThat(config.mergeMaxEverFraction()).isEqualTo(0.2d);
        assertThat(config.minChildFraction()).isCloseTo(0.0996d, org.assertj.core.data.Offset.offset(1.0e-4d));
    }

    @Test
    void clusterSizeTooSmallToBeMeaningfulIsRejected() {
        assertThatThrownBy(() -> ConfigRecommendation.forClusterMax(Metric.EUCLIDEAN_METRIC, 49))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primaryClusterMax");
    }

    private static Config recommend(final int primaryClusterMax) {
        return ConfigRecommendation.forClusterMax(Metric.EUCLIDEAN_METRIC, primaryClusterMax)
                .build(NUM_DIMENSIONS);
    }
}
