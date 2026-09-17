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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@link ClusterMetadata#maxEverNumPrimaryVectors()} high-water mark and the
 * {@link ClusterMetadata#mergeThreshold(Config)} it feeds. Whoever replaces the running statistics raises the mark,
 * and the compact constructor rejects any instance whose mark would sit below its own primary count; between them the
 * mark is monotone, which is what lets the merge trigger be expressed as a fraction of a cluster's lifetime peak
 * rather than an absolute floor.
 */
class ClusterMetadataTest {
    private static final UUID ID = new UUID(0x1234L, 0x5678L);
    private static final int NUM_DIMENSIONS = 128;

    @Test
    void freshClusterMaxEverIsBirthCount() {
        final ClusterMetadata cm = atPeak(3);
        assertThat(cm.getNumPrimaryVectors()).isEqualTo(3);
        assertThat(cm.maxEverNumPrimaryVectors()).isEqualTo(3);
    }

    @Test
    void constructorRejectsMaxEverBelowCurrentCount() {
        // The mark is checked rather than quietly raised, so a caller that forgot to raise it fails here instead of
        // producing a cluster whose merge threshold has silently degraded to the bare primaryClusterMin floor.
        assertThatThrownBy(() -> new ClusterMetadata(ID, 0, 0, stats(7),
                EnumSet.noneOf(ClusterMetadata.State.class), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxEverNumPrimaryVectors");
    }

    @Test
    void withNewVectorsGrowsMaxEverOnGrowth() {
        final ClusterMetadata grown = atPeak(3)
                .withNewVectors(0, 0, stats(10), EnumSet.noneOf(ClusterMetadata.State.class));
        assertThat(grown.getNumPrimaryVectors()).isEqualTo(10);
        assertThat(grown.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void withNewVectorsPreservesMaxEverOnShrink() {
        // Mirrors the reassign target: its stats are rebuilt to a smaller count, but the peak must be preserved so
        // the merge trigger can compare the shrunk count against the pre-shrink peak.
        final ClusterMetadata shrunk = atPeak(10)
                .withNewVectors(0, 0, stats(4), EnumSet.noneOf(ClusterMetadata.State.class));
        assertThat(shrunk.getNumPrimaryVectors()).isEqualTo(4);
        assertThat(shrunk.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void withAdditionalVectorsPreservesMaxEverOnDelete() {
        // A delete removes one distance sample; the peak stays put.
        final ClusterMetadata afterDelete = atPeak(10)
                .withAdditionalVectors(0, 0, stats(9));
        assertThat(afterDelete.getNumPrimaryVectors()).isEqualTo(9);
        assertThat(afterDelete.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void withAdditionalVectorsRaisesMaxEverOnInsert() {
        // The insert direction, which the delete case above cannot cover: the extra distance sample raises the count,
        // so withAdditionalVectors must raise the mark with it. Were that raise dropped the peak would freeze and
        // mergeThreshold would degrade to the bare primaryClusterMin floor — with the delete test above still
        // passing, which is why this case is worth its own test.
        final ClusterMetadata afterInsert = atPeak(10)
                .withAdditionalVectors(0, 0, stats(11));
        assertThat(afterInsert.getNumPrimaryVectors()).isEqualTo(11);
        assertThat(afterInsert.maxEverNumPrimaryVectors()).isEqualTo(11);
    }

    @Test
    void withAdditionalVectorsLeavesMaxEverOnReplicaInsert() {
        // A replica carries no distance sample, so the statistics are handed back unchanged and the primary count does
        // not move. The high-water mark tracks primaries only, so it must not move either — confirming the replicated
        // delta parameter has no bearing on it.
        final ClusterMetadata before = atPeak(10);
        final ClusterMetadata afterReplica =
                before.withAdditionalVectors(0, 1, before.runningStandardDeviation());
        assertThat(afterReplica.getNumPrimaryVectors()).isEqualTo(10);
        assertThat(afterReplica.numReplicatedVectors()).isEqualTo(before.numReplicatedVectors() + 1);
        assertThat(afterReplica.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void mergeThresholdTakesTheLargerOfTheFloorAndTheFraction() {
        // The threshold is max(primaryClusterMin, floor(mergeMaxEverFraction * maxEverNumPrimaryVectors)).
        final Config config = config(50, 0.2d);
        assertThat(shrunkFrom(100, 0).mergeThreshold(config)).isEqualTo(50);   // floor(0.2*100)=20, floor wins
        assertThat(shrunkFrom(1000, 0).mergeThreshold(config)).isEqualTo(200); // floor(0.2*1000)=200, fraction
        assertThat(shrunkFrom(254, 0).mergeThreshold(config)).isEqualTo(50);   // floor(0.2*254)=50, ties the floor
        assertThat(shrunkFrom(255, 0).mergeThreshold(config)).isEqualTo(51);   // floor(0.2*255)=51, fraction
        assertThat(atPeak(0).mergeThreshold(config)).isEqualTo(50);     // an empty cluster falls to the floor
    }

    @Test
    void mergeThresholdFollowsThePeakNotTheCurrentCount() {
        // The whole point of the high-water mark: two clusters holding the *same* number of primaries get different
        // verdicts because their peaks differ. The one still at its own peak is not merge-eligible — a cluster at its
        // peak only is when it falls below primaryClusterMin, since the fraction of a peak is always less than the
        // peak — while the one that shrank to the same size from far higher is.
        final Config config = config(10, 0.5d);

        final ClusterMetadata atItsPeak = atPeak(40);
        assertThat(atItsPeak.getNumPrimaryVectors()).isEqualTo(40);
        assertThat(atItsPeak.mergeThreshold(config)).isEqualTo(20);
        assertThat(atItsPeak.getNumPrimaryVectors()).isGreaterThanOrEqualTo(atItsPeak.mergeThreshold(config));

        final ClusterMetadata shrunken = atPeak(400)
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

        final ClusterMetadata aboveTheFloor = atPeak(60);
        assertThat(aboveTheFloor.mergeThreshold(config)).isEqualTo(54);
        assertThat(aboveTheFloor.getNumPrimaryVectors())
                .as("a cluster at its own peak clears its threshold even under an extreme fraction")
                .isGreaterThanOrEqualTo(aboveTheFloor.mergeThreshold(config));

        final ClusterMetadata belowTheFloor = atPeak(40);
        // 0.9 * 40 = 36 would have left this cluster alone; the floor of 50 is what makes it merge-eligible.
        assertThat(belowTheFloor.mergeThreshold(config)).isEqualTo(50);
        assertThat(belowTheFloor.getNumPrimaryVectors())
                .isLessThan(belowTheFloor.mergeThreshold(config));
    }

    @Test
    void zeroFractionLeavesPrimaryClusterMinAsTheWholeTrigger() {
        // At a fraction of zero the threshold is primaryClusterMin, no matter how far a cluster has fallen from its
        // peak. That is how the trigger behaved before the high-water mark existed.
        final Config config = config(50, 0.0d);
        assertThat(shrunkFrom(1000, 60).mergeThreshold(config)).isEqualTo(50);
        assertThat(atPeak(60).mergeThreshold(config)).isEqualTo(50);

        // Paired with a primaryClusterMin of 1 this is how a test turns merges off: only an empty cluster qualifies.
        final Config mergesOff = config(1, 0.0d);
        final ClusterMetadata drained = shrunkFrom(1000, 1);
        assertThat(drained.mergeThreshold(mergesOff)).isEqualTo(1);
        assertThat(drained.getNumPrimaryVectors())
                .as("at a fraction of zero and primaryClusterMin of 1, even a cluster drained from 1000 is ineligible")
                .isGreaterThanOrEqualTo(drained.mergeThreshold(mergesOff));
    }

    @Nonnull
    private static Config config(final int primaryClusterMin, final double mergeMaxEverFraction) {
        return Guardiann.newConfigBuilder()
                .setPrimaryClusterMin(primaryClusterMin)
                .setMergeMaxEverFraction(mergeMaxEverFraction)
                .build(NUM_DIMENSIONS);
    }

    /** A cluster sitting at its own high-water mark, i.e. one that has never shrunk. */
    @Nonnull
    private static ClusterMetadata atPeak(final int numPrimaryVectors) {
        return shrunkFrom(numPrimaryVectors, numPrimaryVectors);
    }

    /** A cluster that once held {@code peak} primaries and now holds {@code numPrimaryVectors}. */
    @Nonnull
    private static ClusterMetadata shrunkFrom(final int peak, final int numPrimaryVectors) {
        return new ClusterMetadata(ID, 0, 0, stats(numPrimaryVectors),
                EnumSet.noneOf(ClusterMetadata.State.class), peak);
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
