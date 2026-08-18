/*
 * GuardiannVectorIndexBackPressureTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests the Guardiann primary-cluster hard cap ({@code guardiannPrimaryClusterHardMax}). When deferred maintenance
 * tasks are not drained in the writing transaction, an insert that would push a cluster above its hard cap must
 * back-pressure the writer with a {@link VectorIndexClusterTooLargeException} instead of letting the cluster grow
 * without bound; when they are drained in-transaction the write self-maintains and is never capped.
 * <p>
 * The cluster-size knobs are tiny and the hard cap low so a short insert burst reaches the cap: with splits deferred,
 * primaries pile into a single over-grown cluster, so a handful of inserts crosses the cap.
 */
class GuardiannVectorIndexBackPressureTest extends VectorIndexTestBase {
    private static final String INDEX_NAME = "UngroupedVectorIndex";
    // Comfortably above the hard cap below; with splits deferred these all target one cluster, so the cap is crossed
    // well before this many inserts (the reactive test stops at the first back-pressure). Also a whole number of
    // BATCH_SIZE batches for the interleaving test.
    private static final int HARD_CAP_FORCING_INSERTS = 64;
    // Cluster-size knobs kept tiny (and the hard cap low) so a short insert burst reaches the cap. Invariant is
    // MIN < MAX < HARD_MAX. The back-pressure gate is strict ({@code numPrimary + 1 > HARD_MAX}), so a cluster may sit
    // at exactly HARD_MAX without refusing — which is what makes BATCH_SIZE == MAX safe below.
    private static final int PRIMARY_CLUSTER_MIN = 2;
    private static final int PRIMARY_CLUSTER_MAX = 8;
    private static final int PRIMARY_CLUSTER_HARD_MAX = 16;
    private static final int COLLAPSE_MIN_DUPLICATES = 4;
    // A merge to completion re-bounds every primary cluster to <= MAX, so a following batch of at most MAX new primaries
    // can push a lone cluster only to MAX + MAX == HARD_MAX — still allowed by the strict-'>' gate. Merging between
    // batches keeps that ceiling, so periodic merges admit the whole load without ever back-pressuring.
    private static final int BATCH_SIZE = PRIMARY_CLUSTER_MAX;

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.<String, String>builder()
                .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.GUARDIANN.name())
                .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                // tiny clusters and a low hard cap (which must stay strictly above the max) so a short burst trips it
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, Integer.toString(PRIMARY_CLUSTER_MIN))
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, Integer.toString(PRIMARY_CLUSTER_MAX))
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_HARD_MAX, Integer.toString(PRIMARY_CLUSTER_HARD_MAX))
                .put(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES, Integer.toString(COLLAPSE_MIN_DUPLICATES))
                .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                // maintainIndexesInTransaction left at its false default so the split backlog accrues
                .build();
    }

    /**
     * With deferred draining (the default), inserts that outrun the split backlog push a cluster past its hard cap and
     * must back-pressure with {@link VectorIndexClusterTooLargeException}.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void insertPastHardCapBackPressures(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            try {
                // read-your-writes keeps the cluster's primary count in step within this transaction, so it climbs to
                // the cap over successive inserts and the crossing insert throws.
                for (int i = 0; i < HARD_CAP_FORCING_INSERTS; i++) {
                    recordStore.saveRecord(generator.apply((long)i));
                }
                fail("inserting past the cluster hard cap should have back-pressured");
            } catch (final Exception e) {
                assertThat(FDBExceptions.isOrHasCause(e, VectorIndexClusterTooLargeException.class))
                        .as("back-pressure must surface as VectorIndexClusterTooLargeException, but got: %s", e)
                        .isTrue();
            }
        }
    }

    /**
     * An index that drains deferred tasks in-transaction self-maintains: splits are applied inline so the cluster stays
     * bounded and the hard-cap check is disabled, so the same insert burst never back-pressures.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL})
    void inTransactionDrainingNeverBackPressures(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
            for (int i = 0; i < HARD_CAP_FORCING_INSERTS; i++) {
                recordStore.saveRecord(generator.apply((long)i));
            }
            commit(context);
        }
    }

    /**
     * A merge relieves the hard-cap back-pressure. Inserting one record per committed transaction climbs a single
     * cluster's primary count by exactly one each time (read-your-writes spans one insert), so the first refused insert
     * is precisely the one that would push an at-hard-cap cluster over the top; its transaction rolls back, leaving the
     * store in the pre-refusal state. Draining the deferred split backlog to completion re-bounds that cluster to at
     * most {@code MAX}, after which the very same record — the one just refused — inserts and persists.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void mergeRelievesTheBackPressure(final long seed) throws Exception {
        final RecordMetaData metaData = metaDataFor(this::addUngroupedVectorIndex);
        final var generator = getRecordGenerator(new Random(seed), 0.0d);

        VectorRecord refused = null;
        for (int recNo = 0; recNo < HARD_CAP_FORCING_INSERTS; recNo++) {
            final VectorRecord record = generator.apply((long)recNo);
            try (FDBRecordContext context = openContext()) {
                openStore(context, metaData).saveRecord(record);
                context.commit();
            } catch (final RuntimeException e) {
                assertThat(FDBExceptions.isOrHasCause(e, VectorIndexClusterTooLargeException.class))
                        .as("the first refused insert must be hard-cap back-pressure, but got: %s", e).isTrue();
                refused = record; // rolled back, so not persisted — this is exactly the state we retry after merging
                break;
            }
        }
        assertThat(refused).as("an insert must have back-pressured within %d inserts", HARD_CAP_FORCING_INSERTS)
                .isNotNull();

        // Draining the deferred split backlog to completion splits the over-grown cluster back down to <= MAX.
        mergeVectorIndexToCompletion(metaData, INDEX_NAME);
        assertThat(vectorIndexHasOutstandingWork(metaData, INDEX_NAME))
                .as("the merge must drain the split backlog").isFalse();

        // The very record that was refused now inserts and persists — the merge relieved the cap.
        final FDBStoredRecord<Message> stored;
        try (FDBRecordContext context = openContext()) {
            stored = openStore(context, metaData).saveRecord(refused);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(openStore(context, metaData).loadRecord(stored.getPrimaryKey()))
                    .as("the once-refused record must be present after the merge let it in").isNotNull();
        }
    }

    /**
     * Periodic merging keeps the hard cap from ever triggering. Each committed insert adds one primary to a single
     * cluster; draining to completion between batches re-bounds every cluster to at most {@code MAX}, so a following
     * batch of at most {@code BATCH_SIZE == MAX} primaries can push a lone cluster only to {@code MAX + MAX == HARD_MAX},
     * which the strict-{@code >} gate still allows. So the same {@link #HARD_CAP_FORCING_INSERTS} burst that
     * back-pressures without merging goes in entirely when merges are interleaved.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void interleavingMergesAvoidsBackPressure(final long seed) throws Exception {
        final RecordMetaData metaData = metaDataFor(this::addUngroupedVectorIndex);
        final var generator = getRecordGenerator(new Random(seed), 0.0d);

        int committed = 0;
        while (committed < HARD_CAP_FORCING_INSERTS) {
            final int batchEnd = Math.min(committed + BATCH_SIZE, HARD_CAP_FORCING_INSERTS);
            for (; committed < batchEnd; committed++) {
                final VectorRecord record = generator.apply((long)committed);
                try (FDBRecordContext context = openContext()) {
                    openStore(context, metaData).saveRecord(record);
                    context.commit();
                } catch (final RuntimeException e) {
                    if (FDBExceptions.isOrHasCause(e, VectorIndexClusterTooLargeException.class)) {
                        fail("periodic merging must keep clusters bounded, but insert " + committed
                                + " back-pressured");
                    }
                    throw e;
                }
            }
            // Re-bound every cluster to <= MAX before the next batch, so no batch can push one past the hard cap.
            mergeVectorIndexToCompletion(metaData, INDEX_NAME);
        }

        assertThat(committed).as("every record must have been inserted via insert-then-merge")
                .isEqualTo(HARD_CAP_FORCING_INSERTS);
        assertThat(vectorIndexHasOutstandingWork(metaData, INDEX_NAME))
                .as("the final merge leaves no outstanding work").isFalse();
    }
}
