/*
 * ClusterMetadataTest.java
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

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link ClusterMetadata#maxEverNumPrimaryVectors()} high-water mark and the
 * {@link ClusterMetadata#mergeThreshold(Config)} it feeds. The mark is maintained monotonically by the compact
 * constructor (raised to the current primary count on every construction, never decremented on shrink) and threaded
 * through the {@code with*} methods, which is what lets the merge trigger be expressed as a fraction of a cluster's
 * lifetime peak rather than an absolute floor.
 */
class ClusterMetadataTest {
    private static final UUID ID = new UUID(0x1234L, 0x5678L);
    private static final int NUM_DIMENSIONS = 128;

    @Test
    void freshClusterMaxEverIsBirthCount() {
        // Seeded with a maxEver of 0; the compact ctor clamps it up to the current primary count.
        final ClusterMetadata cm = clusterMetadata(3, 0);
        assertThat(cm.getNumPrimaryVectors()).isEqualTo(3);
        assertThat(cm.maxEverNumPrimaryVectors()).isEqualTo(3);
    }

    @Test
    void constructorClampsMaxEverUpToCurrentCount() {
        // A maxEver below the current count is not representable; it is corrected up.
        final ClusterMetadata cm = clusterMetadata(7, 2);
        assertThat(cm.maxEverNumPrimaryVectors()).isEqualTo(7);
    }

    @Test
    void withNewVectorsGrowsMaxEverOnGrowth() {
        final ClusterMetadata grown = clusterMetadata(3, 3)
                .withNewVectors(0, 0, stats(10), EnumSet.noneOf(ClusterMetadata.State.class));
        assertThat(grown.getNumPrimaryVectors()).isEqualTo(10);
        assertThat(grown.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void withNewVectorsPreservesMaxEverOnShrink() {
        // Mirrors the reassign target: its stats are rebuilt to a smaller count, but the peak must be preserved so
        // the merge trigger can compare the shrunk count against the pre-shrink peak.
        final ClusterMetadata shrunk = clusterMetadata(10, 10)
                .withNewVectors(0, 0, stats(4), EnumSet.noneOf(ClusterMetadata.State.class));
        assertThat(shrunk.getNumPrimaryVectors()).isEqualTo(4);
        assertThat(shrunk.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void withAdditionalVectorsPreservesMaxEverOnDelete() {
        // A delete removes one distance sample; the peak stays put.
        final ClusterMetadata afterDelete = clusterMetadata(10, 10)
                .withAdditionalVectors(0, 0, stats(9));
        assertThat(afterDelete.getNumPrimaryVectors()).isEqualTo(9);
        assertThat(afterDelete.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void mergeThresholdTakesTheLargerOfTheFloorAndTheFraction() {
        // The threshold is max(primaryClusterMin, floor(mergeMaxEverFraction * maxEverNumPrimaryVectors)).
        final Config config = config(50, 0.2d);
        assertThat(clusterMetadata(0, 100).mergeThreshold(config)).isEqualTo(50);   // floor(0.2*100)=20, floor wins
        assertThat(clusterMetadata(0, 1000).mergeThreshold(config)).isEqualTo(200); // floor(0.2*1000)=200, fraction
        assertThat(clusterMetadata(0, 254).mergeThreshold(config)).isEqualTo(50);   // floor(0.2*254)=50, ties the floor
        assertThat(clusterMetadata(0, 255).mergeThreshold(config)).isEqualTo(51);   // floor(0.2*255)=51, fraction
        assertThat(clusterMetadata(0, 0).mergeThreshold(config)).isEqualTo(50);     // empty peak clamps to the floor
    }

    @Test
    void mergeThresholdFollowsThePeakNotTheCurrentCount() {
        // The whole point of the high-water mark: two clusters holding the *same* number of primaries get different
        // verdicts because their peaks differ. The one still at its own peak is not merge-eligible — a cluster at its
        // peak only is when it falls below primaryClusterMin, since the fraction of a peak is always less than the
        // peak — while the one that shrank to the same size from far higher is.
        final Config config = config(10, 0.5d);

        final ClusterMetadata atItsPeak = clusterMetadata(40, 0);
        assertThat(atItsPeak.getNumPrimaryVectors()).isEqualTo(40);
        assertThat(atItsPeak.mergeThreshold(config)).isEqualTo(20);
        assertThat(atItsPeak.getNumPrimaryVectors()).isGreaterThanOrEqualTo(atItsPeak.mergeThreshold(config));

        final ClusterMetadata shrunken = clusterMetadata(400, 400)
                .withNewVectors(0, 0, stats(40), EnumSet.noneOf(ClusterMetadata.State.class));
        assertThat(shrunken.getNumPrimaryVectors()).isEqualTo(40);
        assertThat(shrunken.mergeThreshold(config)).isEqualTo(200);
        assertThat(shrunken.getNumPrimaryVectors()).isLessThan(shrunken.mergeThreshold(config));
    }

    @Test
    void clusterAtItsPeakIsOnlyMergeEligibleBelowTheFloor() {
        // The fraction term cannot make a cluster sitting at its own peak merge-eligible, however large the fraction:
        // fraction * peak < peak for any fraction below one, so the comparison collapses to peak < primaryClusterMin
        // and the floor decides alone. This is why the fraction does not shield a freshly split child from being born
        // merge-eligible — Config.minChildFraction is what bounds that.
        final Config config = config(50, 0.9d);

        final ClusterMetadata aboveTheFloor = clusterMetadata(60, 0);
        assertThat(aboveTheFloor.mergeThreshold(config)).isEqualTo(54);
        assertThat(aboveTheFloor.getNumPrimaryVectors())
                .as("a cluster at its own peak clears its threshold even under an extreme fraction")
                .isGreaterThanOrEqualTo(aboveTheFloor.mergeThreshold(config));

        final ClusterMetadata belowTheFloor = clusterMetadata(40, 0);
        // 0.9 * 40 = 36 would have left this cluster alone; the floor of 50 is what makes it merge-eligible.
        assertThat(belowTheFloor.mergeThreshold(config)).isEqualTo(50);
        assertThat(belowTheFloor.getNumPrimaryVectors())
                .isLessThan(belowTheFloor.mergeThreshold(config));
    }

    @Nonnull
    private static Config config(final int primaryClusterMin, final double mergeMaxEverFraction) {
        return Guardiann.newConfigBuilder()
                .setPrimaryClusterMin(primaryClusterMin)
                .setMergeMaxEverFraction(mergeMaxEverFraction)
                .build(NUM_DIMENSIONS);
    }

    @Nonnull
    private static ClusterMetadata clusterMetadata(final int numPrimaryVectors, final int maxEverSeed) {
        return new ClusterMetadata(ID, 0, 0, stats(numPrimaryVectors),
                EnumSet.noneOf(ClusterMetadata.State.class), maxEverSeed);
    }

    @Nonnull
    private static RunningStats stats(final int numElements) {
        RunningStats stats = RunningStats.identity();
        for (int i = 0; i < numElements; i++) {
            stats = stats.add(1.0d);
        }
        return stats;
    }
}
