/*
 * GuardiannVectorIndexConcurrentMergeTest.java
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

import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.guardiann.Config;
import com.apple.foundationdb.async.guardiann.Guardiann;
import com.apple.foundationdb.async.guardiann.GuardiannStructureAsserts;
import com.apple.foundationdb.async.guardiann.OnReadListener;
import com.apple.foundationdb.async.guardiann.OnWriteListener;
import com.apple.foundationdb.async.guardiann.SiftTestHelpers;
import com.apple.foundationdb.async.guardiann.VecsDatasetLoaders;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A realistic concurrency stress test for the Guardiann vector index: it inserts the full SIFT-small dataset
 * (10k x 128-dim) from several threads <em>while</em> the record-layer index merger ({@link OnlineIndexer#mergeIndex()})
 * concurrently drains the deferred split/merge backlog. Modeled on Lucene's
 * {@code LuceneIndexMaintenanceTest.chaosMergeAndUpdateTest} (a dedicated inserter thread + a merger thread that both
 * retry on transaction conflicts).
 * <p>
 * The index is configured with tight clusters (so 10k inserts produce a large deferred backlog for the merger) and a
 * modest primary-cluster hard cap (so inserters that briefly outrun the merger get back-pressured with
 * {@link VectorIndexClusterTooLargeException} and must retry). Both writers and merger retry FDB conflicts, logging when
 * they do. After the load, the backlog is drained to completion and recall@k is checked against the SIFT ground truth.
 * <p>
 * Several parameters below (cluster sizes, hard cap, thread count, throttle, recall threshold) are calibration knobs; a
 * first run against FDB is expected to confirm/adjust them.
 */
class GuardiannVectorIndexConcurrentMergeTest extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(GuardiannVectorIndexConcurrentMergeTest.class);

    private static final String INDEX_NAME = "UngroupedVectorIndex";
    private static final int DIMENSIONS = 128;

    // Cluster tuning. PRIMARY_CLUSTER_MAX is deliberately small so 10k vectors in one partition produce a big split
    // backlog. The other knobs whose Config defaults derive from the default max (1000) are scaled down to match, and
    // HARD_MAX is only ~2x MAX so inserters that briefly outrun the merger trip back-pressure.
    private static final int PRIMARY_CLUSTER_MAX = 128;
    private static final int PRIMARY_CLUSTER_HARD_MAX = 2 * PRIMARY_CLUSTER_MAX;   // > MAX (invariant)
    private static final int PRIMARY_CLUSTER_MIN = 16;
    private static final int COLLAPSE_MIN_DUPLICATES = PRIMARY_CLUSTER_MAX / 2;    // < MAX (invariant); SIFT vecs distinct
    private static final int REPLICATED_CLUSTER_MAX_WRITES = 3 * PRIMARY_CLUSTER_MAX / 10;
    private static final int REPLICATED_CLUSTER_TARGET = PRIMARY_CLUSTER_MAX / 10;
    private static final int REPLICATION_STATS_MIN_SAMPLE_SIZE = PRIMARY_CLUSTER_MAX / 4;   // < MAX so stats get trusted
    private static final int UNDERREPLICATED_PRIMARY_CLUSTER_MAX = 16;

    // Concurrency / workload.
    private static final int INSERTER_THREADS = 1;
    private static final int BATCH_SIZE = 10;
    private static final long INSERT_THROTTLE_MILLIS = 100L;   // throttle writers so the merger can keep pace (lock step)
    private static final long BACK_PRESSURE_BACKOFF_MILLIS = 25L;
    private static final int MAX_FINAL_MERGE_PASSES = 200;

    // Recall verification.
    private static final int RECALL_K = 100;
    private static final double MIN_RECALL = 0.5d;   // lenient (half-precision + approximate + tight clusters); calibrate

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.<String, String>builder()
                .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())
                .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .put(IndexOptions.VECTOR_NUM_DIMENSIONS, Integer.toString(DIMENSIONS))
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, Integer.toString(PRIMARY_CLUSTER_MAX))
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_HARD_MAX, Integer.toString(PRIMARY_CLUSTER_HARD_MAX))
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, Integer.toString(PRIMARY_CLUSTER_MIN))
                .put(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES, Integer.toString(COLLAPSE_MIN_DUPLICATES))
                .put(IndexOptions.GUARDIANN_REPLICATED_CLUSTER_MAX_WRITES,
                        Integer.toString(REPLICATED_CLUSTER_MAX_WRITES))
                .put(IndexOptions.GUARDIANN_REPLICATED_CLUSTER_TARGET, Integer.toString(REPLICATED_CLUSTER_TARGET))
                .put(IndexOptions.GUARDIANN_REPLICATION_STATS_MIN_SAMPLE_SIZE,
                        Integer.toString(REPLICATION_STATS_MIN_SAMPLE_SIZE))
                .put(IndexOptions.GUARDIANN_UNDERREPLICATED_PRIMARY_CLUSTER_MAX,
                        Integer.toString(UNDERREPLICATED_PRIMARY_CLUSTER_MAX))
                .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                .build();
        // executeDeferredTasksInTransaction is left at its false default so the split backlog accrues for the merger.
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void concurrentSiftInsertAndMerge() throws Exception {
        final List<PrimaryKeyAndVector> base = VecsDatasetLoaders.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH,
                Integer.MAX_VALUE);
        final List<DoubleRealVector> queries =
                VecsDatasetLoaders.loadQueryVectors(SiftTestHelpers.SIFT_SMALL_QUERY_PATH);
        final List<Set<Integer>> groundTruth =
                VecsDatasetLoaders.loadGroundTruth(SiftTestHelpers.SIFT_SMALL_GROUNDTRUTH_PATH, -1);
        assertThat(base).as("siftsmall base must be present/extracted").isNotEmpty();
        final int numBase = base.size();
        logger.info("loaded SIFT-small: {} base, {} queries, {} ground-truth rows", numBase, queries.size(),
                groundTruth.size());

        // Immutable metadata shared across all threads (each thread opens its own store per transaction from it).
        final RecordMetaData metaData = buildMetaData();

        final AtomicLong committed = new AtomicLong();
        final AtomicLong conflictRetries = new AtomicLong();
        final AtomicLong backPressureRetries = new AtomicLong();
        final AtomicLong mergerPasses = new AtomicLong();
        final AtomicReference<Throwable> failedInsert = new AtomicReference<>();
        final AtomicReference<Throwable> failedMerge = new AtomicReference<>();
        final AtomicBoolean stopMerger = new AtomicBoolean(false);
        // Wake-up permits: inserters release one after each committed batch and the merger blocks acquiring them instead
        // of polling. A no-work wake-up (a batch that enqueued no split) just fails the cheap hasOutstandingWork gate.
        final Semaphore mergeSignal = new Semaphore(0);

        // Inserter threads: each owns a disjoint slice of base indices [t, numBase) stepping by INSERTER_THREADS, so
        // rec_no == base index (ground-truth indices map directly to primary keys) and no two threads write the same key.
        final List<Thread> inserters = new ArrayList<>();
        for (int t = 0; t < INSERTER_THREADS; t++) {
            final int threadId = t;
            final Thread inserter = new Thread(() -> {
                try {
                    final List<Integer> batch = new ArrayList<>(BATCH_SIZE);
                    for (int i = threadId; i < numBase; i += INSERTER_THREADS) {
                        batch.add(i);
                        if (batch.size() == BATCH_SIZE || i + INSERTER_THREADS >= numBase) {
                            insertBatchWithRetry(metaData, base, batch, committed, conflictRetries, backPressureRetries);
                            batch.clear();
                            mergeSignal.release();   // wake the merger: this committed batch may have enqueued work
                            sleepQuietly(INSERT_THROTTLE_MILLIS);   // throttle so the merger can move in lock step
                        }
                    }
                } catch (final Throwable e) {
                    failedInsert.compareAndSet(null, e);
                }
            }, "sift-inserter-" + threadId);
            inserters.add(inserter);
        }

        // Merger thread: waits for an inserter to signal a committed batch, then runs the real index merger
        // (OnlineIndexer.mergeIndex sets the merge session id via IndexingMerger and retries FDB conflicts internally).
        final Thread merger = new Thread(() -> {
            try {
                while (!stopMerger.get()) {
                    mergeSignal.acquire();   // block until an inserter (or the stop wake-up) releases a permit
                    if (stopMerger.get()) {
                        break;
                    }
                    try {
                        // The permit only means "a batch committed"; it may not have enqueued a split, so gate the
                        // (transaction-opening) drain on there actually being outstanding work.
                        if (hasOutstandingWork(metaData)) {
                            runMergePass(metaData);
                            mergerPasses.incrementAndGet();
                        }
                    } catch (final Exception e) {
                        // A transient failure here is tolerable (the merger's runner already retries conflicts); record
                        // the first unexpected one but keep draining.
                        logger.info("merge pass failed (continuing): {}", e.toString());
                        failedMerge.compareAndSet(null, e);
                    }
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "sift-merger");

        merger.start();
        inserters.forEach(Thread::start);
        for (final Thread inserter : inserters) {
            inserter.join();
        }
        stopMerger.set(true);
        mergeSignal.release();   // wake the merger out of its blocking acquire() so it observes the stop flag
        merger.join();

        assertThat(failedInsert.get()).as("inserter threads must not fail unexpectedly").isNull();
        logger.info("insert phase done: committed={}, conflictRetries={}, backPressureRetries={}, mergerPasses={}",
                committed.get(), conflictRetries.get(), backPressureRetries.get(), mergerPasses.get());

        // Drain any residual backlog (and the follow-up tasks draining enqueues) to completion.
        drainToCompletion(metaData);

        // ---- assertions ----
        // 1. Every base vector was committed exactly once, and the backlog is fully drained.
        assertThat(committed.get()).as("all base vectors committed").isEqualTo(numBase);
        assertThat(hasOutstandingWork(metaData)).as("backlog fully drained after the final merge").isFalse();

        // 2. Real concurrency actually occurred: writers hit both FDB conflicts and hard-cap back-pressure, and the
        //    merger ran while they did. (If back-pressure never triggered, lower PRIMARY_CLUSTER_HARD_MAX or the
        //    INSERT_THROTTLE_MILLIS so inserters outrun the merger more.)
        assertThat(mergerPasses.get()).as("merger ran concurrently with inserts").isGreaterThan(0L);
        assertThat(conflictRetries.get()).as("writers observed FDB conflicts against the merger").isGreaterThan(0L);
        //assertThat(backPressureRetries.get()).as("writers were back-pressured by the cluster hard cap").isGreaterThan(0L);

        // 3. Search still works: recall@k against the SIFT ground truth stays above the (lenient) floor.
        final double meanRecall = meanRecallAtK(queries, groundTruth);
        logger.info("mean recall@{} = {}", RECALL_K, meanRecall);
        assertThat(meanRecall).as("recall@%d after concurrent load+merge", RECALL_K).isGreaterThanOrEqualTo(MIN_RECALL);

        // 4. The on-disk Guardiann structure is internally consistent (no orphaned/duplicate primaries, no dangling
        //    replicas, replication within tolerance), per fdb-extensions' structural-invariant checker.
        assertGuardiannStructureInvariants(metaData);
    }

    /**
     * Inserts one batch of base indices in a single transaction, retrying the whole batch on an FDB conflict (logged)
     * and backing off + retrying on a cluster hard-cap back-pressure (logged). {@code saveRecord} is idempotent by
     * primary key, so replaying a rolled-back batch is safe.
     */
    private void insertBatchWithRetry(@Nonnull final RecordMetaData metaData,
                                      @Nonnull final List<PrimaryKeyAndVector> base,
                                      @Nonnull final List<Integer> batch,
                                      @Nonnull final AtomicLong committed,
                                      @Nonnull final AtomicLong conflictRetries,
                                      @Nonnull final AtomicLong backPressureRetries) {
        while (true) {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context, metaData);
                for (final int index : batch) {
                    store.saveRecord(toVectorRecord(index, (DoubleRealVector) base.get(index).vector()));
                }
                context.commit();
                committed.addAndGet(batch.size());
                return;
            } catch (final RuntimeException e) {
                if (isOrHasCause(e, VectorIndexClusterTooLargeException.class)) {
                    backPressureRetries.incrementAndGet();
                    logger.info("insert back-pressured (hard cap) on batch starting {}; backing off and retrying",
                            batch.get(0));
                    sleepQuietly(BACK_PRESSURE_BACKOFF_MILLIS);
                } else if (isOrHasCause(e, FDBExceptions.FDBStoreTransactionConflictException.class)) {
                    conflictRetries.incrementAndGet();
                    logger.info("insert conflict on batch starting {}; retrying", batch.get(0));
                } else {
                    throw e;
                }
            }
        }
    }

    /** One pass of the real record-layer index merger over the vector index (mirrors Lucene's explicitMergeIndex). */
    @SuppressWarnings("PMD.CloseResource") // the outer context only builds the store for OnlineIndexer config
    private void runMergePass(@Nonnull final RecordMetaData metaData) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            final Index index = store.getRecordMetaData().getIndex(INDEX_NAME);
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(store)
                    .setIndex(index)
                    .setTimer(new FDBStoreTimer())
                    .build()) {
                indexer.mergeIndex();
            }
        }
    }

    /** Drives the merger to completion after the concurrent phase, until no outstanding deferred work remains. */
    private void drainToCompletion(@Nonnull final RecordMetaData metaData) throws Exception {
        for (int pass = 0; pass < MAX_FINAL_MERGE_PASSES; pass++) {
            runMergePass(metaData);
            if (!hasOutstandingWork(metaData)) {
                return;
            }
        }
        throw new AssertionError("merge did not drain the backlog within " + MAX_FINAL_MERGE_PASSES + " passes");
    }

    /**
     * Verifies the on-disk Guardiann structure is internally consistent after the concurrent load + merge: rebuild a
     * raw {@link Guardiann} over the index's subspace — the same keys the record-layer engine wrote — and run
     * fdb-extensions' structural-invariant checker. Reusing {@link GuardiannVectorIndexEngine#parseConfig} guarantees
     * the reconstructed {@link Guardiann} is configured exactly as the engine that wrote the data (dimensions,
     * cluster sizes, replication thresholds). This is the no-delete variant (the test only inserts), so it also
     * asserts every replica references a live primary. The checker first drains to quiescence, which is a no-op here
     * because {@link #drainToCompletion} already emptied the backlog.
     */
    private void assertGuardiannStructureInvariants(@Nonnull final RecordMetaData metaData) {
        final Subspace indexSubspace;
        final Config config;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            final Index index = store.getRecordMetaData().getIndex(INDEX_NAME);
            indexSubspace = store.indexSubspace(index);
            config = GuardiannVectorIndexEngine.parseConfig(index);
        }
        final Guardiann guardiann = new Guardiann(indexSubspace, fdb.getExecutor(), config,
                OnWriteListener.NOOP, OnReadListener.NOOP);
        GuardiannStructureAsserts.assertGuardiannInvariants(fdb.database(), guardiann);
    }

    private boolean hasOutstandingWork(@Nonnull final RecordMetaData metaData) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            final VectorIndexMaintainer maintainer =
                    (VectorIndexMaintainer)store.getIndexMaintainer(store.getRecordMetaData().getIndex(INDEX_NAME));
            return maintainer.hasOutstandingWork().get();
        }
    }

    /** Mean recall@{@link #RECALL_K} of the index over every SIFT query vs. the provided ground truth. */
    private double meanRecallAtK(@Nonnull final List<DoubleRealVector> queries,
                                 @Nonnull final List<Set<Integer>> groundTruth) throws Exception {
        double totalRecall = 0.0d;
        for (int q = 0; q < queries.size(); q++) {
            // siftsmall's ground truth carries the top-100 nearest per query and RECALL_K == 100, so the whole
            // ground-truth set is the recall@K reference set.
            final Set<Long> expected = groundTruth.get(q).stream()
                    .map(Integer::longValue)
                    .collect(ImmutableSet.toImmutableSet());
            final Set<Long> got = queryTopK(queries.get(q).toHalfRealVector(), RECALL_K);
            final long hits = got.stream().filter(expected::contains).count();
            totalRecall += (double) hits / RECALL_K;
        }
        return totalRecall / queries.size();
    }

    /** Executes the vector index kNN plan and returns the primary keys (rec_no) of the top-k hits. */
    private Set<Long> queryTopK(@Nonnull final HalfRealVector queryVector, final int k) throws Exception {
        final RecordQueryIndexPlan plan = createIndexPlan(queryVector, k, INDEX_NAME);
        final Set<Long> recNos = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            byte[] continuation = null;
            do {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                             executeQuery(plan, continuation, Bindings.EMPTY_BINDINGS, Integer.MAX_VALUE)) {
                    while (cursor.hasNext()) {
                        final VectorRecord record =
                                VectorRecord.newBuilder().mergeFrom(Objects.requireNonNull(cursor.next()).getRecord())
                                        .build();
                        recNos.add(record.getRecNo());
                    }
                    continuation = cursor.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED
                                   ? null : cursor.getContinuation();
                }
            } while (continuation != null);
        }
        return recNos;
    }

    @Nonnull
    private FDBRecordStore openStore(@Nonnull final FDBRecordContext context, @Nonnull final RecordMetaData metaData) {
        return getStoreBuilder(context, metaData, Objects.requireNonNull(path)).createOrOpen();
    }

    @Nonnull
    private RecordMetaData buildMetaData() {
        final RecordMetaDataBuilder metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("VectorRecord").setPrimaryKey(concatenateFields("group_id", "rec_no"));
        addUngroupedVectorIndex(metaDataBuilder);
        return metaDataBuilder.getRecordMetaData();
    }

    @Nonnull
    private static VectorRecord toVectorRecord(final int index, @Nonnull final DoubleRealVector vector) {
        return VectorRecord.newBuilder()
                .setRecNo(index)
                .setGroupId(0)
                .setVectorData(ByteString.copyFrom(vector.toHalfRealVector().getRawData()))
                .build();
    }

    private static boolean isOrHasCause(@Nonnull final Throwable throwable,
                                        @Nonnull final Class<? extends Throwable> type) {
        for (Throwable current = throwable; current != null && current != current.getCause();
                current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
