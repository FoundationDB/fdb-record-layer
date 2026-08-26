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
 * Unit tests for the {@link ClusterMetadata#maxEverNumPrimaryVectors()} high-water mark. It is maintained
 * monotonically by the compact constructor (raised to the current primary count on every construction, never
 * decremented on shrink) and threaded through the {@code with*} methods, which is what lets the merge trigger be
 * expressed as a fraction of a cluster's lifetime peak rather than an absolute floor.
 */
class ClusterMetadataTest {
    private static final UUID ID = new UUID(0x1234L, 0x5678L);

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
