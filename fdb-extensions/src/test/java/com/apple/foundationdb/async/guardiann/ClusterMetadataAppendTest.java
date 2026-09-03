/*
 * ClusterMetadataAppendTest.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link Primitives#applyClusterMetadataDelta} against a real database, which is where the
 * {@code APPEND_IF_FITS} mutation and the append-versus-compact decision actually run.
 * <p>
 * {@link ClusterMetadataDeltaTest} covers the format and the fold by concatenating bytes in memory; what only a
 * database can confirm is that FDB's append really does produce a value the parser still understands, and that
 * crossing {@link Config#clusterMetadataMaxPendingDeltas()} collapses the accumulated deltas back into the base.
 */
public class ClusterMetadataAppendTest implements BaseTest {
    private static final int MAX_PENDING_DELTAS = 4;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    final TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;

    @Nonnull
    @Override
    public Database getDb() {
        return Objects.requireNonNull(db);
    }

    @Nonnull
    @Override
    public Subspace getSubspace() {
        return subspaceExtension.getSubspace();
    }

    @Nonnull
    @Override
    public Path getTempDir() {
        return tempDir;
    }

    @BeforeAll
    public static void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @Test
    void deltasAccumulateAsAppendsAndThenCompact() {
        final Guardiann guardiann = newGuardiann();
        final Primitives primitives = guardiann.getLocator().primitives();
        final UUID clusterId = new UUID(0x5150L, 0x1234L);

        // Start from a written (compacted) base holding a single primary.
        db.run(transaction -> {
            primitives.writeClusterMetadata(transaction,
                    new ClusterMetadata(clusterId, 0, 0, RunningStats.of(1.0d),
                            EnumSet.noneOf(ClusterMetadata.State.class), 0));
            return null;
        });
        assertThat(pendingDeltas(guardiann, clusterId)).isZero();

        // Each of the first MAX_PENDING_DELTAS updates must land as an append, growing the pending count.
        for (int i = 1; i <= MAX_PENDING_DELTAS; i++) {
            applyDelta(primitives, clusterId, ClusterMetadataDelta.StatsOp.ADD, 1.0d + i);
            assertThat(pendingDeltas(guardiann, clusterId))
                    .as("update %d must have been appended rather than compacted", i)
                    .isEqualTo(i);
            assertThat(fetch(primitives, clusterId).getNumPrimaryVectors())
                    .as("the fold must see every appended delta")
                    .isEqualTo(1 + i);
        }

        // The next update finds the log full, so it compacts: the deltas collapse into the base and the fold result
        // still accounts for every one of them.
        applyDelta(primitives, clusterId, ClusterMetadataDelta.StatsOp.ADD, 100.0d);
        assertThat(pendingDeltas(guardiann, clusterId))
                .as("crossing clusterMetadataMaxPendingDeltas must compact")
                .isZero();

        final ClusterMetadata compacted = fetch(primitives, clusterId);
        assertThat(compacted.getNumPrimaryVectors()).isEqualTo(2 + MAX_PENDING_DELTAS);
        assertThat(compacted.maxEverNumPrimaryVectors()).isEqualTo(2 + MAX_PENDING_DELTAS);
    }

    @Test
    void appendedDeltasSurviveARemovalAndKeepThePeak() {
        final Guardiann guardiann = newGuardiann();
        final Primitives primitives = guardiann.getLocator().primitives();
        final UUID clusterId = new UUID(0x600dL, 0xf00dL);

        db.run(transaction -> {
            primitives.writeClusterMetadata(transaction,
                    new ClusterMetadata(clusterId, 0, 0, RunningStats.identity(),
                            EnumSet.noneOf(ClusterMetadata.State.class), 0));
            return null;
        });

        // Two appends up, one append down: the peak of 2 was never observed by a compaction, so it can only come
        // from the fold replaying the deltas in stored (commit) order.
        applyDelta(primitives, clusterId, ClusterMetadataDelta.StatsOp.ADD, 3.0d);
        applyDelta(primitives, clusterId, ClusterMetadataDelta.StatsOp.ADD, 5.0d);
        applyDelta(primitives, clusterId, ClusterMetadataDelta.StatsOp.REMOVE, 3.0d);

        final ClusterMetadata folded = fetch(primitives, clusterId);
        assertThat(pendingDeltas(guardiann, clusterId)).isEqualTo(3);
        assertThat(folded.getNumPrimaryVectors()).isEqualTo(1);
        assertThat(folded.meanDistance()).isEqualTo(5.0d);
        assertThat(folded.maxEverNumPrimaryVectors()).isEqualTo(2);
    }

    @Test
    void appendedStateChangesAreVisibleToTheFold() {
        final Guardiann guardiann = newGuardiann();
        final Primitives primitives = guardiann.getLocator().primitives();
        final UUID clusterId = new UUID(0xBEEFL, 0xCAFEL);

        db.run(transaction -> {
            primitives.writeClusterMetadata(transaction,
                    new ClusterMetadata(clusterId, 0, 0, RunningStats.of(1.0d),
                            EnumSet.noneOf(ClusterMetadata.State.class), 0));
            return null;
        });

        db.run(transaction -> {
            final ClusterMetadataForUpdate forUpdate =
                    primitives.fetchClusterMetadataForUpdate(transaction, clusterId).join();
            primitives.applyClusterMetadataDelta(transaction, Objects.requireNonNull(forUpdate),
                    new ClusterMetadataDelta(ClusterMetadataDelta.StatsOp.NONE, 0.0d, 0, 1,
                            EnumSet.of(ClusterMetadata.State.REASSIGN),
                            EnumSet.noneOf(ClusterMetadata.State.class)));
            return null;
        });

        final ClusterMetadata folded = fetch(primitives, clusterId);
        assertThat(folded.states()).containsExactly(ClusterMetadata.State.REASSIGN);
        assertThat(folded.numReplicatedVectors()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------------------

    private void applyDelta(@Nonnull final Primitives primitives, @Nonnull final UUID clusterId,
                            @Nonnull final ClusterMetadataDelta.StatsOp statsOp, final double distance) {
        db.run(transaction -> {
            final ClusterMetadataForUpdate forUpdate =
                    primitives.fetchClusterMetadataForUpdate(transaction, clusterId).join();
            primitives.applyClusterMetadataDelta(transaction, Objects.requireNonNull(forUpdate),
                    new ClusterMetadataDelta(statsOp, distance, 0, 0,
                            EnumSet.noneOf(ClusterMetadata.State.class),
                            EnumSet.noneOf(ClusterMetadata.State.class)));
            return null;
        });
    }

    /**
     * Counts the deltas pending in the stored value by reading the raw bytes. Deliberately goes to the bytes rather
     * than asking {@code fetchClusterMetadataForUpdate}: the point of these tests is what FDB's append actually left
     * on disk, and the fetch only reports a verdict derived from it.
     */
    private int pendingDeltas(@Nonnull final Guardiann guardiann, @Nonnull final UUID clusterId) {
        final byte[] key = guardiann.getLocator().primitives().getClusterMetadataSubspace()
                .pack(Tuple.from(clusterId));
        return db.run(transaction -> StorageAdapter.pendingClusterMetadataDeltas(
                Tuple.fromBytes(Objects.requireNonNull(transaction.get(key).join()))));
    }

    @Nonnull
    private ClusterMetadata fetch(@Nonnull final Primitives primitives, @Nonnull final UUID clusterId) {
        return db.run(transaction ->
                Objects.requireNonNull(primitives.fetchClusterMetadata(transaction, clusterId).join()));
    }

    @Nonnull
    private Guardiann newGuardiann() {
        final Config config = Guardiann.newConfigBuilder()
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setDeterministicRandomness(true)
                .setClusterMetadataMaxPendingDeltas(MAX_PENDING_DELTAS)
                .build(128);
        return new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                new TestHelpers.TestOnWriteListener(),
                new TestHelpers.TestOnReadListener());
    }
}
