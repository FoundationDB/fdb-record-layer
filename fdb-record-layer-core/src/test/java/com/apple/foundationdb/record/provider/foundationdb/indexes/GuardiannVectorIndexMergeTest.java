/*
 * GuardiannVectorIndexMergeTest.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for {@link VectorIndexMaintainer#mergeIndex()} against the Guardiann engine, which pays down the deferred
 * maintenance backlog (split/merge/reassign/collapse tasks) that inserts and deletes only nibble at.
 * <p>
 * The cluster-size knobs are tightened well below production so a modest write load forces many splits — and hence a
 * queue of deferred tasks (and their follow-ups) that inserts cannot keep up with — giving merge something real to
 * drain. Draining runs to completion by re-invoking {@code mergeIndex()} across transactions, because executing a task
 * can enqueue follow-up tasks, so the queue converges over several passes rather than in one.
 */
class GuardiannVectorIndexMergeTest extends VectorIndexTestBase {
    private static final int NUM_RECORDS = 1000;
    // Per-transaction drain budget while merging to completion (mergeControl.getMergesLimit()).
    private static final int MERGE_BATCH = 100;
    // Safety bound on the drive loop; convergence for this dataset needs far fewer passes.
    private static final int MAX_MERGE_PASSES = 200;
    private static final ImmutableList<String> INDEX_NAMES =
            ImmutableList.of("UngroupedVectorIndex", "GroupedVectorIndex");

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.<String, String>builder()
                .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())
                .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                // small clusters -> frequent splits -> a real deferred-task backlog
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, "64")
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, "8")
                .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                .build();
    }

    /**
     * A write-heavy load leaves outstanding tasks; merging to completion drains every partition's queue (both the
     * ungrouped, empty-prefix index and the grouped, partitioned one) so no outstanding work — and no counter total —
     * remains.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void mergeDrainsGuardiannBacklog(final long seed) throws Exception {
        saveRandomRecords(false, this::addVectorIndexes, new Random(seed), NUM_RECORDS);

        for (final String indexName : INDEX_NAMES) {
            assertThat(hasOutstandingWork(indexName))
                    .as("index %s should have a deferred-maintenance backlog after a write-heavy load", indexName)
                    .isTrue();

            drainToCompletion(indexName);

            assertThat(hasOutstandingWork(indexName))
                    .as("index %s should have no outstanding work once merge runs to completion", indexName)
                    .isFalse();
        }
    }

    /**
     * A single {@code mergeIndex()} honors {@code mergesLimit} (drains at most that many tasks) and, while a backlog
     * remains, reports {@code mergesFound > mergesTried} so {@code IndexingMerger} keeps looping.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL})
    void mergeRespectsBudget(final long seed) throws Exception {
        saveRandomRecords(false, this::addVectorIndexes, new Random(seed), NUM_RECORDS);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            final VectorIndexMaintainer maintainer = maintainerFor("GroupedVectorIndex");
            assertThat(maintainer.hasOutstandingWork().get())
                    .as("test needs a backlog to exercise the per-transaction budget")
                    .isTrue();

            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            mergeControl.setMergesLimit(1);
            maintainer.mergeIndex().get();

            assertThat(mergeControl.getMergesTried())
                    .as("a budget of 1 drains at most one task").isEqualTo(1L);
            // The drain is uncommitted but read-your-writes still reflects the decremented counter here.
            if (maintainer.hasOutstandingWork().get()) {
                assertThat(mergeControl.getMergesFound())
                        .as("with work still queued the driver must be told to loop")
                        .isGreaterThan(mergeControl.getMergesTried());
            }
        }
    }

    /**
     * HNSW does everything inline and enqueues no deferred tasks, so its {@code mergeIndex()} is a clean no-op — it must
     * complete (the merge driver calls it for every target index) and never report outstanding work.
     */
    @Test
    void hnswMergeIsNoOp() throws Exception {
        final Map<String, String> hnswOptions = ImmutableMap.of(
                IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.HNSW.name(),
                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                IndexOptions.VECTOR_NUM_DIMENSIONS, "128");
        final RecordMetaDataHook hook = metaDataBuilder -> addUngroupedVectorIndex(metaDataBuilder, hnswOptions);
        saveRandomRecords(false, hook, new Random(0x1234L), 100);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            final VectorIndexMaintainer maintainer = maintainerFor("UngroupedVectorIndex");
            assertThat(maintainer.hasOutstandingWork().get()).isFalse();
            maintainer.mergeIndex().get();
            assertThat(maintainer.hasOutstandingWork().get()).isFalse();
            commit(context);
        }
    }

    /**
     * Drives {@code mergeIndex()} to completion for one index, each pass in its own transaction (executing a task can
     * enqueue follow-ups, so the queue drains over several passes), until no outstanding work remains.
     */
    private void drainToCompletion(@Nonnull final String indexName) throws Exception {
        for (int pass = 0; pass < MAX_MERGE_PASSES; pass++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, this::addVectorIndexes);
                recordStore.getIndexDeferredMaintenanceControl().setMergesLimit(MERGE_BATCH);
                maintainerFor(indexName).mergeIndex().get();
                commit(context);
            }
            if (!hasOutstandingWork(indexName)) {
                return;
            }
        }
        fail(String.format("merge did not drain the backlog for %s within %d passes", indexName, MAX_MERGE_PASSES));
    }

    private boolean hasOutstandingWork(@Nonnull final String indexName) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            return maintainerFor(indexName).hasOutstandingWork().get();
        }
    }

    @Nonnull
    private VectorIndexMaintainer maintainerFor(@Nonnull final String indexName) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        return (VectorIndexMaintainer)maintainer;
    }
}
