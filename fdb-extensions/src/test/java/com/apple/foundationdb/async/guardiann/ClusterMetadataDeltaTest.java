/*
 * ClusterMetadataDeltaTest.java
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

import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the append-friendly cluster-metadata value format: a base record followed by zero or more appended
 * {@link ClusterMetadataDelta}s, folded back together on read.
 * <p>
 * The load-bearing property is that FDB's tuple encoding is a concatenation of self-delimiting elements, so
 * appending one packed tuple onto another (which is what {@code APPEND_IF_FITS} does) yields bytes that still parse
 * as a single tuple. These tests simulate the mutation with plain byte concatenation, so they pin that property
 * without needing a database.
 */
class ClusterMetadataDeltaTest {
    private static final UUID CLUSTER_ID = new UUID(0xABCDL, 0x1234L);

    @Test
    void deltaSurvivesATupleRoundTrip() {
        final ClusterMetadataDelta delta = new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.ADD, 0.375d,
                1, -2, EnumSet.of(ClusterMetadata.State.REASSIGN),
                EnumSet.of(ClusterMetadata.State.SPLIT_MERGE, ClusterMetadata.State.COLLAPSE));

        final Tuple appended = StorageAdapter.valueTupleFromClusterMetadataDelta(delta);
        assertThat(appended.size()).as("a delta must serialize as exactly one nested element").isEqualTo(1);

        assertThat(StorageAdapter.clusterMetadataDeltaFromTuple(appended.getNestedTuple(0))).isEqualTo(delta);
    }

    /**
     * The central invariant: parsing {@code base ++ delta1 ++ delta2 ++ …} must yield exactly what applying the same
     * deltas one at a time in memory yields. This is what lets a writer append instead of reading, folding and
     * rewriting.
     */
    @ParameterizedTest
    @RandomSeedSource
    void foldingAppendedDeltasMatchesApplyingThemInMemory(final long seed) {
        final Random random = new Random(seed);
        final ClusterMetadata base = baseMetadata(random);

        final List<ClusterMetadataDelta> deltas = randomDeltaSequence(random, base.getNumPrimaryVectors());

        // In memory: apply each delta to the running result.
        ClusterMetadata expected = base;
        for (final ClusterMetadataDelta delta : deltas) {
            expected = delta.applyTo(expected);
        }

        // On disk: the base value with every delta appended, then parsed back.
        final ClusterMetadata actual =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));

        assertThat(actual)
                .as("folding %d appended deltas must equal applying them in memory", deltas.size())
                .isEqualTo(expected);
        // Spot-check the Welford components explicitly: record equality would also pass if both sides were broken
        // in the same way, but these are the values the whole exercise is meant to preserve exactly.
        assertThat(actual.getNumPrimaryVectors()).isEqualTo(expected.getNumPrimaryVectors());
        assertThat(actual.meanDistance()).isEqualTo(expected.meanDistance());
        assertThat(actual.standardDeviation()).isEqualTo(expected.standardDeviation());
        assertThat(actual.maxEverNumPrimaryVectors()).isEqualTo(expected.maxEverNumPrimaryVectors());
    }

    @Test
    void pendingDeltaCountReflectsTheNumberOfAppends() {
        final ClusterMetadata base = baseMetadata(new Random(1L));
        assertThat(StorageAdapter.pendingClusterMetadataDeltas(
                StorageAdapter.valueTupleFromClusterMetadata(base)))
                .as("a freshly written (compacted) value has no pending deltas")
                .isZero();

        final List<ClusterMetadataDelta> deltas = List.of(statsDelta(ClusterMetadataDelta.StatsOp.ADD, 1.0d),
                statsDelta(ClusterMetadataDelta.StatsOp.ADD, 2.0d),
                statsDelta(ClusterMetadataDelta.StatsOp.ADD, 3.0d));
        assertThat(StorageAdapter.pendingClusterMetadataDeltas(Tuple.fromBytes(valueWith(base, deltas))))
                .isEqualTo(3);
    }

    @Test
    void compactingIsFoldPreserving() {
        final ClusterMetadata base = baseMetadata(new Random(7L));
        final List<ClusterMetadataDelta> deltas = List.of(statsDelta(ClusterMetadataDelta.StatsOp.ADD, 0.5d),
                new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.ADD, 0.25d, 1, 2,
                        EnumSet.of(ClusterMetadata.State.REASSIGN), EnumSet.noneOf(ClusterMetadata.State.class)));

        final ClusterMetadata folded =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));
        // Compaction is just writing the folded value back out, so re-parsing it must be a no-op.
        final Tuple compacted = StorageAdapter.valueTupleFromClusterMetadata(folded);

        assertThat(StorageAdapter.pendingClusterMetadataDeltas(compacted)).isZero();
        assertThat(StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, compacted)).isEqualTo(folded);
    }

    @Test
    void maxEverRecordsThePeakThePendingDeltasPassedThrough() {
        // Grow to 10 primaries then shrink back to 3, all within the pending deltas: the peak must survive even
        // though no compaction observed it.
        final ClusterMetadata base = emptyMetadata();
        final List<ClusterMetadataDelta> deltas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            deltas.add(statsDelta(ClusterMetadataDelta.StatsOp.ADD, 1.0d + i));
        }
        for (int i = 0; i < 7; i++) {
            deltas.add(statsDelta(ClusterMetadataDelta.StatsOp.REMOVE, 1.0d + i));
        }

        final ClusterMetadata folded =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));
        assertThat(folded.getNumPrimaryVectors()).isEqualTo(3);
        assertThat(folded.maxEverNumPrimaryVectors()).isEqualTo(10);
    }

    @Test
    void overRemovingIsClampedRatherThanThrown() {
        // The fold runs on every read, so an inconsistent delta sequence must not make the cluster unreadable.
        final ClusterMetadata base = emptyMetadata();
        final List<ClusterMetadataDelta> deltas = List.of(statsDelta(ClusterMetadataDelta.StatsOp.ADD, 1.0d),
                statsDelta(ClusterMetadataDelta.StatsOp.REMOVE, 1.0d),
                statsDelta(ClusterMetadataDelta.StatsOp.REMOVE, 1.0d));

        final ClusterMetadata folded =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));
        assertThat(folded.getNumPrimaryVectors()).isZero();
    }

    @Test
    void negativeCountsAreClampedRatherThanThrown() {
        final ClusterMetadata base = emptyMetadata();
        final List<ClusterMetadataDelta> deltas = List.of(new ClusterMetadataDelta(
                ClusterMetadataDelta.StatsOp.NONE, 0.0d, -5, -5,
                EnumSet.noneOf(ClusterMetadata.State.class), EnumSet.noneOf(ClusterMetadata.State.class)));

        final ClusterMetadata folded =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));
        assertThat(folded.numPrimaryUnderreplicatedVectors()).isZero();
        assertThat(folded.numReplicatedVectors()).isZero();
    }

    @Test
    void statesAreSetThenClearedInDeltaOrder() {
        final ClusterMetadata base = emptyMetadata();
        final List<ClusterMetadataDelta> deltas = List.of(
                new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.NONE, 0.0d, 0, 0,
                        EnumSet.of(ClusterMetadata.State.SPLIT_MERGE, ClusterMetadata.State.REASSIGN),
                        EnumSet.noneOf(ClusterMetadata.State.class)),
                new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.NONE, 0.0d, 0, 0,
                        EnumSet.noneOf(ClusterMetadata.State.class),
                        EnumSet.of(ClusterMetadata.State.SPLIT_MERGE)));

        final ClusterMetadata folded =
                StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(valueWith(base, deltas)));
        assertThat(folded.states()).containsExactly(ClusterMetadata.State.REASSIGN);
    }

    @Test
    void valueHoldingOnlyDeltasIsRejected() {
        // What an APPEND_IF_FITS would produce if it created the key because the base record was missing. A delta is
        // a single nested element, so such a value is shorter than a base record and is caught on arity alone.
        final byte[] deltaOnly = StorageAdapter.valueTupleFromClusterMetadataDelta(
                statsDelta(ClusterMetadataDelta.StatsOp.ADD, 1.0d)).pack();

        assertThatThrownBy(() -> StorageAdapter.clusterMetadataFromTuple(CLUSTER_ID, Tuple.fromBytes(deltaOnly)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("malformed cluster metadata value");
    }

    @Test
    void fullDeltaLogStaysWellInsideTheValueSizeCeiling() {
        // The byte ceiling is a backstop; with the default threshold it must never be the binding constraint.
        final ClusterMetadata base = baseMetadata(new Random(11L));
        final List<ClusterMetadataDelta> deltas = new ArrayList<>();
        for (int i = 0; i < Config.DEFAULT_CLUSTER_METADATA_MAX_PENDING_DELTAS; i++) {
            deltas.add(new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.ADD, 123.456789d, 1, 1,
                    EnumSet.of(ClusterMetadata.State.REASSIGN), EnumSet.of(ClusterMetadata.State.COLLAPSE)));
        }

        assertThat(valueWith(base, deltas).length)
                .as("base plus a full delta log must stay far below the append ceiling")
                .isLessThan(StorageAdapter.CLUSTER_METADATA_MAX_VALUE_SIZE / 2);
    }

    // -------------------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------------------

    /** Simulates {@code APPEND_IF_FITS}: the base value with each delta's packed bytes concatenated onto the end. */
    @Nonnull
    private static byte[] valueWith(@Nonnull final ClusterMetadata base,
                                    @Nonnull final List<ClusterMetadataDelta> deltas) {
        byte[] value = StorageAdapter.valueTupleFromClusterMetadata(base).pack();
        for (final ClusterMetadataDelta delta : deltas) {
            value = ByteArrayUtil.join(value, StorageAdapter.valueTupleFromClusterMetadataDelta(delta).pack());
        }
        return value;
    }

    @Nonnull
    private static ClusterMetadataDelta statsDelta(@Nonnull final ClusterMetadataDelta.StatsOp statsOp,
                                                   final double distance) {
        return new ClusterMetadataDelta(statsOp, distance, 0, 0,
                EnumSet.noneOf(ClusterMetadata.State.class), EnumSet.noneOf(ClusterMetadata.State.class));
    }

    @Nonnull
    private static ClusterMetadata emptyMetadata() {
        return new ClusterMetadata(CLUSTER_ID, 0, 0, RunningStats.identity(),
                EnumSet.noneOf(ClusterMetadata.State.class), 0);
    }

    @Nonnull
    private static ClusterMetadata baseMetadata(@Nonnull final Random random) {
        RunningStats stats = RunningStats.identity();
        final int numElements = 5 + random.nextInt(20);
        for (int i = 0; i < numElements; i++) {
            stats = stats.add(random.nextDouble() * 100.0d);
        }
        return new ClusterMetadata(CLUSTER_ID, random.nextInt(3), random.nextInt(5), stats,
                EnumSet.noneOf(ClusterMetadata.State.class), 0);
    }

    /**
     * A realistic delta sequence: mostly primary inserts, with removals only of distances that were actually added
     * (mirroring what the insert/delete paths will emit) plus the occasional replica and state change.
     */
    @Nonnull
    private static List<ClusterMetadataDelta> randomDeltaSequence(@Nonnull final Random random,
                                                                  final int initialNumPrimaryVectors) {
        final List<Double> live = new ArrayList<>();
        for (int i = 0; i < initialNumPrimaryVectors; i++) {
            live.add(random.nextDouble() * 100.0d);
        }
        final List<ClusterMetadataDelta> deltas = new ArrayList<>();
        final int numDeltas = 1 + random.nextInt(40);
        for (int i = 0; i < numDeltas; i++) {
            final boolean remove = !live.isEmpty() && random.nextInt(3) == 0;
            final ClusterMetadataDelta.StatsOp statsOp;
            final double distance;
            if (remove) {
                statsOp = ClusterMetadataDelta.StatsOp.REMOVE;
                distance = live.remove(random.nextInt(live.size()));
            } else if (random.nextInt(8) == 0) {
                statsOp = ClusterMetadataDelta.StatsOp.NONE;
                distance = 0.0d;
            } else {
                statsOp = ClusterMetadataDelta.StatsOp.ADD;
                distance = random.nextDouble() * 100.0d;
                live.add(distance);
            }
            deltas.add(new ClusterMetadataDelta(statsOp, distance,
                    random.nextInt(2), random.nextInt(3) - 1,
                    randomStates(random), randomStates(random)));
        }
        return deltas;
    }

    @Nonnull
    private static EnumSet<ClusterMetadata.State> randomStates(@Nonnull final Random random) {
        final EnumSet<ClusterMetadata.State> states = EnumSet.noneOf(ClusterMetadata.State.class);
        for (final ClusterMetadata.State state : ClusterMetadata.State.values()) {
            if (random.nextInt(6) == 0) {
                states.add(state);
            }
        }
        return states;
    }
}
