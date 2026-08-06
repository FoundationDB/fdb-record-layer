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
import java.util.UUID;

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
    // Safety bound on the drive loop. Higher than the pure-drain version because the per-prefix merge lease splits
    // claiming from draining: each partition costs an extra "claim" pass before its drain passes.
    private static final int MAX_MERGE_PASSES = 500;
    private static final ImmutableList<String> INDEX_NAMES =
            ImmutableList.of("UngroupedVectorIndex", "GroupedVectorIndex");
    // A single-transaction insert load comfortably above PRIMARY_CLUSTER_MAX (64), so one open transaction overflows a
    // cluster and enqueues a deferred split — letting a test observe the merge-required flag before the write commits.
    private static final int SINGLE_TXN_SPLIT_FORCING_INSERTS = 200;

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
                .put(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES, "32")
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
     * remains, reports {@code mergesFound > mergesTried} so {@code IndexingMerger} keeps looping. With the per-partition
     * lease, claiming and draining are separate invocations: the first claims (drains nothing), the second drains.
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
            mergeControl.setMergeSessionId(UUID.randomUUID());
            mergeControl.setMergesLimit(1);

            // First invocation only claims a partition's lease (the claim must commit before any drain), so it drains
            // nothing; the second, now owning that partition (read-your-writes sees its own claim), drains one task.
            maintainer.mergeIndex().get();
            assertThat(mergeControl.getMergesTried())
                    .as("the claim invocation drains no tasks").isEqualTo(0L);

            maintainer.mergeIndex().get();
            assertThat(mergeControl.getMergesTried())
                    .as("a budget of 1 drains at most one task").isEqualTo(1L);
            if (maintainer.hasOutstandingWork().get()) {
                assertThat(mergeControl.getMergesFound())
                        .as("with work still queued the driver must be told to loop")
                        .isGreaterThan(mergeControl.getMergesTried());
            }
        }
    }

    /**
     * With a per-partition lease, a second concurrent merge (a different session) that finds a partition already held
     * by a live owner skips it rather than racing into the same drain. The ungrouped index has a single partition, so
     * once owner A claims it (committing the claim without draining), owner B has nothing it can do and stops — leaving
     * the backlog for A to finish.
     */
    @Test
    void secondOwnerSkipsAPrefixHeldByTheFirst() throws Exception {
        saveRandomRecords(false, this::addUngroupedVectorIndex, new Random(0x5ca1ab1eL), NUM_RECORDS);
        final UUID ownerA = UUID.randomUUID();
        final UUID ownerB = UUID.randomUUID();

        // Owner A claims the single prefix: a commit with no drain.
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            assertThat(maintainerFor("UngroupedVectorIndex").hasOutstandingWork().get()).isTrue();
            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            mergeControl.setMergeSessionId(ownerA);
            mergeControl.setMergesLimit(MERGE_BATCH);
            maintainerFor("UngroupedVectorIndex").mergeIndex().get();
            assertThat(mergeControl.getMergesTried()).as("A only claims, does not drain").isEqualTo(0L);
            commit(context);
        }

        // Owner B, a different session, finds the only prefix held live by A: it drains nothing and stops
        // (found == tried), so the backlog is untouched and waits for A.
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            mergeControl.setMergeSessionId(ownerB);
            mergeControl.setMergesLimit(MERGE_BATCH);
            maintainerFor("UngroupedVectorIndex").mergeIndex().get();
            assertThat(mergeControl.getMergesTried()).isEqualTo(0L);
            assertThat(mergeControl.getMergesFound())
                    .as("B skips A's live-held prefix and stops").isEqualTo(0L);
            assertThat(maintainerFor("UngroupedVectorIndex").hasOutstandingWork().get())
                    .as("B drained nothing, so the backlog remains").isTrue();
            commit(context);
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
     * With deferred tasks NOT drained in-transaction (the default), an insert that overflows a cluster enqueues a
     * deferred split and must flag its index as needing a background merge — the same signal Lucene raises for its
     * pending-write queue, which a caller's commit hook reads to schedule the merge. The flag is asserted within the
     * writing transaction, before commit, because the {@link IndexDeferredMaintenanceControl} lives on the record store.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void insertEnqueuingDeferredTaskSignalsMergeRequired(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            for (int i = 0; i < SINGLE_TXN_SPLIT_FORCING_INSERTS; i++) {
                recordStore.saveRecord(generator.apply((long)i));
            }
            assertThat(isFlaggedForMerge(mergeControl, index))
                    .as("an insert that enqueues a deferred split must flag the index for a background merge")
                    .isTrue();
            commit(context);
        }
    }

    /**
     * The delete leg of the signal mirrors the insert leg — {@code updateIndexEntry} hands the same composed register to
     * both branches. A delete-driven task is only enqueued once a primary cluster drops below its minimum <em>and</em> a
     * mergeable neighbor exists and the cluster carries no pending task, so the split backlog is first drained to a
     * settled multi-cluster state; then primaries are deleted (in one open transaction) until a cluster underflows, at
     * which point the index must be flagged for a background merge — read before commit.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void deleteEnqueuingDeferredTaskSignalsMergeRequired(final long seed) throws Exception {
        final var saved = saveRandomRecords(false, this::addVectorIndexes, new Random(seed), NUM_RECORDS);
        // Drain the split backlog so clusters settle into many neighbors, none carrying a pending SPLIT_MERGE task —
        // the preconditions a delete-driven merge needs (a lone or already-tasked cluster never enqueues one).
        drainToCompletion("UngroupedVectorIndex");
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            // Delete primaries until a cluster underflows PRIMARY_CLUSTER_MIN with neighbors still present; stop at the
            // first flag so plenty of clusters survive (cardinality stays MULTIPLE, the merge precondition).
            boolean flagged = false;
            for (int i = 0; i < saved.size() && !flagged; i++) {
                recordStore.deleteRecordAsync(saved.get(i).getPrimaryKey()).get();
                flagged = isFlaggedForMerge(mergeControl, index);
            }
            assertThat(flagged)
                    .as("a delete that drops a cluster below its minimum (with a mergeable neighbor) must flag the index")
                    .isTrue();
            commit(context);
        }
    }

    /**
     * When a write drains its deferred maintenance in the same transaction (autoMergeDuringCommit), there is no
     * background merge for the caller to run, so the index must not be flagged as merge-required — even by an insert
     * that enqueues a split. This is the opposite of the default (deferred) case, where the same load does flag it.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL})
    void insertDoesNotSignalMergeRequiredWhenDrainingInTransaction(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
            mergeControl.setAutoMergeDuringCommit(true);
            for (int i = 0; i < SINGLE_TXN_SPLIT_FORCING_INSERTS; i++) {
                recordStore.saveRecord(generator.apply((long)i));
            }
            assertThat(isFlaggedForMerge(mergeControl, index))
                    .as("an index draining deferred tasks in-transaction is self-maintaining; no caller merge is needed")
                    .isFalse();
            commit(context);
        }
    }

    /**
     * Draining the backlog pays down merge work rather than creating it: the follow-up tasks a drain enqueues flow
     * through the bare task-count register, never the merge-signal one, so a merge in progress must never (re-)flag its
     * index. Flagging is the insert/delete path's job, done once; a drain that re-flagged would make a merge perpetually
     * reschedule itself.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL})
    void drainingNeverSignalsMergeRequired(final long seed) throws Exception {
        saveRandomRecords(false, this::addVectorIndexes, new Random(seed), NUM_RECORDS);
        final String indexName = "UngroupedVectorIndex";
        final UUID sessionId = UUID.randomUUID();
        long tasksDrained = 0L;
        for (int pass = 0; pass < MAX_MERGE_PASSES; pass++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, this::addVectorIndexes);
                final Index index = recordStore.getRecordMetaData().getIndex(indexName);
                final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
                mergeControl.setMergeSessionId(sessionId);
                mergeControl.setMergesLimit(MERGE_BATCH);
                maintainerFor(indexName).mergeIndex().get();
                assertThat(isFlaggedForMerge(mergeControl, index))
                        .as("a merge draining the backlog must not (re-)flag the index for a caller-driven merge")
                        .isFalse();
                tasksDrained += mergeControl.getMergesTried();
                commit(context);
            }
            if (!hasOutstandingWork(indexName)) {
                // mergeIndex()'s first pass only claims the prefix and drains nothing, so the never-reflag assertion
                // above only bites once a pass has actually executed tasks (whose follow-up enqueues could otherwise
                // re-flag). Require at least one real drain, so the assertion is not vacuously satisfied by claim passes.
                assertThat(tasksDrained)
                        .as("the drain loop must execute at least one deferred task, else the never-reflag check is vacuous")
                        .isGreaterThan(0L);
                return;
            }
        }
        fail(String.format("merge did not drain the backlog for %s within %d passes", indexName, MAX_MERGE_PASSES));
    }

    /**
     * Whether {@code index} is flagged for a background merge on {@code mergeControl}, treating the lazily-initialized
     * (null-until-first-set) merge-required set as "nothing flagged".
     */
    private static boolean isFlaggedForMerge(@Nonnull final IndexDeferredMaintenanceControl mergeControl,
                                             @Nonnull final Index index) {
        final var mergeRequired = mergeControl.getMergeRequiredIndexes();
        return mergeRequired != null && mergeRequired.contains(index);
    }

    /**
     * Drives {@code mergeIndex()} to completion for one index, each pass in its own transaction (executing a task can
     * enqueue follow-ups, so the queue drains over several passes), until no outstanding work remains.
     */
    private void drainToCompletion(@Nonnull final String indexName) throws Exception {
        // One stable session id for the whole drive loop so the maintainer recognizes and keeps its own per-partition
        // lease across passes — a direct mergeIndex() call gets no id from the (absent) indexing session, so without
        // this a fresh id each pass would skip its own just-claimed prefix and never drain.
        final UUID sessionId = UUID.randomUUID();
        for (int pass = 0; pass < MAX_MERGE_PASSES; pass++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, this::addVectorIndexes);
                final IndexDeferredMaintenanceControl mergeControl = recordStore.getIndexDeferredMaintenanceControl();
                mergeControl.setMergeSessionId(sessionId);
                mergeControl.setMergesLimit(MERGE_BATCH);
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
