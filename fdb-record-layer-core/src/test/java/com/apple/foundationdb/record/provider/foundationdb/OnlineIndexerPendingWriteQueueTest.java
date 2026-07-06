/*
 * OnlineIndexerPendingWriteQueueTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.CloseException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building an index with a pending writes queue.
 */
class OnlineIndexerPendingWriteQueueTest extends OnlineIndexerTest {

    @Test
    void testDrainPendingQueueWhileBuilding() throws Exception {
        // Add new records during online indexing session
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);

        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        final List<Integer> queuedRecNos = List.of(1, 3, 5, 7);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    // Write the new records. Because the index is in the queue state, these saves are enqueued rather than indexed.
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        assertTrue(store.isIndexWriteOnlyWithQueue(index));
                        for (int recNo : queuedRecNos) {
                            store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                                    .setRecNo(recNo)
                                    .setNumValue2(recNo * 19)
                                    .setNumValueUnique(recNo * 1139)
                                    .build());
                        }
                        context.commit();
                    }
                    // The saves were deferred to the queue, not written to the index.
                    assertEquals(queuedRecNos.size(), writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));

                    // The pending writes queue should now hold exactly the deferred records (and nothing else), before the drain.
                    try (FDBRecordContext context = openContext()) {
                        final PendingWritesQueue<IndexBuildProto.PendingWritesQueueEntry> queue =
                                PendingWriteQueueIndexingFactory.getIndexingQueue(recordStore, index);
                        final Long queueSize = queue.getQueueSizeNoConflict(context).join();
                        assertEquals(queuedRecNos.size(), queueSize == null ? 0L : queueSize);

                        final List<Long> queuedRecordNos = queue
                                .getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                                .asList().join().stream()
                                .map(entry -> {
                                    final IndexBuildProto.PendingWritesQueueEntry payload = entry.getPayload();
                                    final Message record = recordStore.getSerializer().deserialize(recordStore.getRecordMetaData(),
                                            TupleHelpers.EMPTY, payload.getNewRecord().toByteArray(), recordStore.getTimer());
                                    return (Long)record.getField(record.getDescriptorForType().findFieldByName("rec_no"));
                                })
                                .toList();
                        assertEquals(queuedRecNos.stream().map(Integer::longValue).toList(), queuedRecordNos);
                        context.commit();
                    }
                });

        assertReadable(index);
        // The index must contain an entry for every record, including the ones that were only ever added to the queue.
        assertEquals(numInitialRecords + queuedRecNos.size(), indexEntryCount(index));
        // A scrub confirms there are no missing (or dangling) index entries.
        scrubAndValidate(List.of(index));
    }

    @Test
    void testIndexBuildStateWhileBuildingWithQueue() throws Exception {
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        final IndexBuildState buildState =
                                IndexBuildState.loadIndexBuildStateAsync(recordStore, index).join();
                        assertTrue(buildState.getIndexState().isWriteOnlyWithQueue(),
                                "the index should be building with a pending writes queue");
                        // WRITE_ONLY_WITH_QUEUE is a write-only state, so progress counts are loaded (not skipped)...
                        assertNotNull(buildState.getRecordsScanned(),
                                "records-scanned progress should be loaded for a queue-state build");
                        // ...and toString() renders those counts (the branch guarded by isAnyWriteOnly()).
                        final String rendered = buildState.toString();
                        assertTrue(rendered.contains("scannedRecords="), rendered);
                        assertTrue(rendered.contains("totalRecords="), rendered);
                        context.commit();
                    }
                });

        assertReadable(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void testDrainQueueForDeletedRecords() throws Exception {
        // Delete records during online indexing session
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        // recNo 4 falls in the range indexed by the first pass; recNo 30 has not been scanned yet when we delete it.
        final List<Integer> deletedRecNos = List.of(4, 30);
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        for (int recNo : deletedRecNos) {
                            assertTrue(store.deleteRecord(Tuple.from(recNo)), "record should have existed: " + recNo);
                        }
                        context.commit();
                    }
                });

        assertEquals(deletedRecNos.size(), writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(index);
        assertEquals(numInitialRecords - deletedRecNos.size(), indexEntryCount(index));
        assertNull(queueSizeCounter(index));
        scrubAndValidate(List.of(index));
    }

    @Test
    void testDrainQueueForUpdatedRecords() throws Exception {
        // Update records during online indexing session
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 24;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final int updatedRecNo = 30; // not yet scanned when the first pass pauses
        final int newNumValue2 = 999_999;
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        saveSimpleRecord(store, updatedRecNo, newNumValue2);
                        context.commit();
                    }
                });

        assertEquals(1, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(index);
        assertEquals(numInitialRecords, indexEntryCount(index));
        // The index entry must reflect the new value, not the original one.
        assertEquals(List.of((long)updatedRecNo), indexPrimaryKeysForValue(index, newNumValue2));
        assertEquals(List.of(), indexPrimaryKeysForValue(index, updatedRecNo * 19));
        assertNull(queueSizeCounter(index));
        scrubAndValidate(List.of(index));
    }

    @Test
    void testDrainQueueReappliesIdempotentlyForScannedRecords() throws Exception {
        // Update records during online indexing session (after already indexed)
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 21;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final int updatedRecNo = 4; // within the range indexed by the first pass
        final int newNumValue2 = 123_456;
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        saveSimpleRecord(recordStore, updatedRecNo, newNumValue2);
                        context.commit();
                    }
                });

        assertReadable(index);
        assertEquals(numInitialRecords, indexEntryCount(index));
        assertEquals(List.of((long)updatedRecNo), indexPrimaryKeysForValue(index, newNumValue2));
        assertEquals(List.of(), indexPrimaryKeysForValue(index, updatedRecNo * 19));
        assertNull(queueSizeCounter(index));
        scrubAndValidate(List.of(index));
    }

    @Test
    void testNonIdempotentIndexFallsBackToWriteOnly() throws Exception {
        // Non-idempotent index must fall back to a plain write-only build (for now, at least)
        final Index index = new Index("simple$count_queue", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        assertTrue(store.isIndexWriteOnly(index));
                        assertFalse(store.isIndexWriteOnlyWithQueue(index));
                        saveSimpleRecord(store, 1, 19);
                        saveSimpleRecord(store, 3, 57);
                        context.commit();
                    }
                });

        assertEquals(0, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(index);
    }

    @Test
    void testQueryCannotUseIndexInQueueState() throws Exception {
        // While an index is being built with a queue it is not readable and queries must not be able to scan it.
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        assertTrue(recordStore.isIndexWriteOnlyWithQueue(index));
                        assertFalse(recordStore.isIndexReadable(index));
                        assertThrows(ScanNonReadableIndexException.class, () ->
                                        recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN),
                                "queries must not be able to scan the index while it is in the queue state");
                        context.commit();
                    }
                });

        assertReadable(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void testMixedTargetsSomeQueuedSomeNot() throws Exception {
        // Build two value indexes together, only one of which uses a queue. Each index should be marked with its own
        // state and end up correctly built.
        final Index queuedIndex = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final Index directIndex = new Index("simple$num_value_3_direct", field("num_value_3_indexed"), IndexTypes.VALUE);
        final List<Index> indexes = List.of(queuedIndex, directIndex);
        final int numInitialRecords = 30;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        final List<Integer> newRecNos = List.of(1, 3, 5);
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(indexes, List.of(queuedIndex)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        assertTrue(store.isIndexWriteOnlyWithQueue(queuedIndex));
                        assertTrue(store.isIndexWriteOnly(directIndex));
                        assertFalse(store.isIndexWriteOnlyWithQueue(directIndex));
                        for (int recNo : newRecNos) {
                            saveSimpleRecord(store, recNo, recNo * 19);
                        }
                        context.commit();
                    }
                });

        // Only the queued index defers writes; the direct index is written normally (one deferred write per record).
        assertEquals(newRecNos.size(), writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(indexes);
        assertEquals(numInitialRecords + newRecNos.size(), indexEntryCount(queuedIndex));
        assertEquals(numInitialRecords + newRecNos.size(), indexEntryCount(directIndex));
        assertNull(queueSizeCounter(queuedIndex));
        scrubAndValidate(indexes);
    }

    @Test
    void testMixedIdempotentAndNonIdempotentQueued() throws Exception {
        // When both an idempotent and a non-idempotent index are requested for the queue, only the idempotent one is
        // built with the queue; the non-idempotent one falls back to plain write-only.
        final Index valueIndex = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final Index countIndex = new Index("simple$count_queue", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        final List<Index> indexes = List.of(valueIndex, countIndex);
        final int numInitialRecords = 24;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        final AtomicBoolean statesAsExpected = new AtomicBoolean(false);
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(indexes, indexes),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        statesAsExpected.set(recordStore.isIndexWriteOnlyWithQueue(valueIndex)
                                && recordStore.isIndexWriteOnly(countIndex)
                                && !recordStore.isIndexWriteOnlyWithQueue(countIndex));
                        context.commit();
                    }
                });

        assertTrue(statesAsExpected.get(), "the idempotent index should use the queue and the non-idempotent one should not");
        assertReadable(indexes);
        assertEquals(numInitialRecords, indexEntryCount(valueIndex));
        scrubAndValidate(indexes);
    }

    @Test
    void testUniqueIndexQueueConflictRecordedDuringDrain() throws Exception {
        // A uniqueness conflict among the deferred writes must be added to the UV list during the drain (like a normal
        // write-only build)
        final Index index = new Index("simple$num_value_2_unique_queue", field("num_value_2"),
                EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final int numInitialRecords = 20; // recNos 0,2,..,38 with distinct num_value_2 = recNo * 19 (no initial conflict)
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        buildIndexPausingOnceForWrites(
                newIndexerBuilder(index)
                        .setLimit(10)
                        .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                                .setUsePendingWriteQueue(List.of(index))
                                .allowUniquePendingState()
                                .build()),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        // recNo 1 duplicates recNo 0's num_value_2 (both 0) -> a uniqueness conflict once drained.
                        saveSimpleRecord(recordStore, 1, 0);
                        context.commit();
                    }
                });

        // The drain recorded the violation and the build completed (no crash) into readable-unique-pending.
        openSimpleMetaData(allIndexesHook(List.of(index)));
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadableUniquePending(index),
                    "the index should be readable-unique-pending after a conflict among drained writes");
            assertTrue(recordStore.scanUniquenessViolations(index).getCount().join() > 0,
                    "the uniqueness conflict should have been recorded, not thrown, during the drain");
            context.commit();
        }
    }

    @Test
    void testFallsBackToWriteOnlyBelowFormatVersion() throws Exception {
        formatVersion = FormatVersion.FULL_STORE_LOCK; // one below WRITE_ONLY_WITH_QUEUE
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final List<Integer> newRecNos = List.of(1, 3);
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        assertFalse(store.isIndexWriteOnlyWithQueue(index),
                                "below the required format version the index must not enter the queue state");
                        assertTrue(store.isIndexWriteOnly(index), "the index should fall back to plain write-only");
                        for (int recNo : newRecNos) {
                            saveSimpleRecord(store, recNo, recNo * 19);
                        }
                        context.commit();
                    }
                });

        assertEquals(0, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE),
                "no writes should be deferred to a queue below the required format version");
        assertReadable(index);
        assertEquals(numInitialRecords + newRecNos.size(), indexEntryCount(index));
        scrubAndValidate(List.of(index));
    }

    @Test
    void testDrainQueueAcrossMultiplePasses() throws Exception {
        // Enqueue writes at two distinct points during the build so the queue is drained across more than one pass.
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 30;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final Semaphore pausedSemaphore = new Semaphore(0);
        final Semaphore resumeSemaphore = new Semaphore(0);
        final AtomicBoolean firstPass = new AtomicBoolean(true);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();

        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = queueIndexerBuilder(index, List.of(index))
                    .setLimit(3)
                    .setConfigLoader(old -> pauseThenResume(old, firstPass, pausedSemaphore, resumeSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();

        final List<Integer> firstBatch = List.of(1, 3);
        final List<Integer> secondBatch = List.of(5, 7, 9);
        // First drain cycle.
        pausedSemaphore.acquire();
        writeNewRecords(firstBatch);
        resumeSemaphore.release();
        // Second drain cycle (maybe).
        pausedSemaphore.acquire();
        writeNewRecords(secondBatch);
        // Let the build run to completion; flood the resume permits so any further pauses proceed immediately.
        resumeSemaphore.release(1_000_000);
        indexerThread.join();
        assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());

        assertReadable(index);
        assertEquals(numInitialRecords + firstBatch.size() + secondBatch.size(), indexEntryCount(index));
        assertNull(queueSizeCounter(index), "the queue data should have been erased once the index became readable");
        scrubAndValidate(List.of(index));
    }

    @Test
    void testResumeBuildDrainsQueue() throws Exception {
        // Crash a build mid-way while records sit in the queue, then resume with a fresh indexer and confirm the
        // resumed build drains the queue and completes correctly.
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 22;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final Semaphore pausedSemaphore = new Semaphore(0);
        final Semaphore resumeSemaphore = new Semaphore(0);
        final AtomicBoolean firstPass = new AtomicBoolean(true);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();
        final String intentionally = "Intentionally crash during test";

        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = queueIndexerBuilder(index, List.of(index))
                    .setConfigLoader(old -> {
                        // Let the first pass run and mark the index write-only-with-queue, then pause so the driver can
                        // enqueue writes, and only then crash - leaving records sitting in the queue mid-build.
                        if (firstPass.compareAndSet(true, false)) {
                            return old;
                        }
                        pausedSemaphore.release();
                        Assertions.assertDoesNotThrow(() -> resumeSemaphore.acquire());
                        throw new RecordCoreException(intentionally);
                    })
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();

        final List<Integer> queuedRecNos = List.of(1, 3, 5, 7);
        pausedSemaphore.acquire();
        writeNewRecords(queuedRecNos);
        resumeSemaphore.release();
        indexerThread.join();
        assertInstanceOf(RecordCoreException.class, buildFailure.get(), "the build should have crashed");
        assertEquals(intentionally, buildFailure.get().getMessage());

        // The queue should still hold the deferred writes after the crash.
        assertEquals(queuedRecNos.size(), queueSizeCounter(index).intValue());

        // Resume with a fresh indexer (still requesting the queue) and let it finish.
        try (OnlineIndexer indexer = queueIndexerBuilder(index, List.of(index)).build()) {
            indexer.buildIndex(true);
        }

        assertReadable(index);
        assertEquals(numInitialRecords + queuedRecNos.size(), indexEntryCount(index));
        assertNull(queueSizeCounter(index));
        scrubAndValidate(List.of(index));
    }

    @Test
    void testPendingWriteQueueNotEmptyWhileMarkingReadableExceptionCarriesIndexName() {
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final IndexingBase.PendingWriteQueueNotEmptyWhileMarkingReadable ex =
                new IndexingBase.PendingWriteQueueNotEmptyWhileMarkingReadable(index);
        assertEquals("Pending write queue is not empty while marking index as readable", ex.getMessage());
        assertTrue(ex.getLogInfo().containsValue(index.getName()),
                "the exception should carry the offending index name in its log info");
    }

    @Test
    void testPendingWriteQueueDrainExceptionWrapsCause() {
        final CloseException cause = new CloseException(new RuntimeException("boom"));
        final PendingWriteQueueDrainer.PendingWriteQueueDrainException ex =
                new PendingWriteQueueDrainer.PendingWriteQueueDrainException(cause);
        assertEquals("Pending write queue drain had failed", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    private void populateEvenRecords(final int numRecords) {
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                final int recNo = 2 * i;
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setNumValue2(recNo * 19)
                        .setNumValueUnique(recNo * 1139)
                        .build());
            }
            context.commit();
        }
    }

    /**
     * An indexer builder configured to build {@code targetIndexes} with a {@link OnlineIndexer.IndexingPolicy}
     * requesting a write pending queue for {@code queuedIndexes}, at a small scan limit.
     */
    @Nonnull
    private OnlineIndexer.Builder queueIndexerBuilder(@Nonnull final List<Index> targetIndexes, @Nonnull final List<Index> queuedIndexes) {
        return newIndexerBuilder(targetIndexes)
                .setLimit(10)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setUsePendingWriteQueue(queuedIndexes)
                        .build());
    }

    @Nonnull
    private OnlineIndexer.Builder queueIndexerBuilder(@Nonnull final Index targetIndex, @Nonnull final List<Index> queuedIndexes) {
        return queueIndexerBuilder(List.of(targetIndex), queuedIndexes);
    }

    /**
     * Run {@code indexerBuilder.buildIndex} on a background thread, pausing after the first scan pass so
     * {@code whilePaused} can perform concurrent writes that get deferred to the pending writes queue, then let the
     * build finish. Asserts the build completed without error.
     */
    private void buildIndexPausingOnceForWrites(@Nonnull final OnlineIndexer.Builder indexerBuilder,
                                                @Nonnull final Runnable whilePaused) throws InterruptedException {
        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicBoolean passed = new AtomicBoolean(false);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();

        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = indexerBuilder
                    .setConfigLoader(old -> pauseAfterOnePass(old, passed, startBuildingSemaphore, pauseSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();
        // Wait until the indexer thread has started building and paused.
        startBuildingSemaphore.acquire();
        startBuildingSemaphore.release();

        whilePaused.run();

        pauseSemaphore.release();
        indexerThread.join();
        assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());
    }

    /**
     * A config loader that lets the first scan pass run unimpeded, then on every subsequent invocation signals the
     * driver thread (via {@code pausedSemaphore}) and blocks until it is allowed to resume (via {@code resumeSemaphore}).
     * Unlike {@link #pauseAfterOnePass}, this pauses on <i>every</i> pass, so the driver can control several distinct
     * pause points.
     */
    private static OnlineIndexOperationConfig pauseThenResume(final OnlineIndexOperationConfig oldConfig,
                                                              final AtomicBoolean firstPass,
                                                              final Semaphore pausedSemaphore,
                                                              final Semaphore resumeSemaphore) {
        if (firstPass.compareAndSet(true, false)) {
            return oldConfig;
        }
        pausedSemaphore.release();
        Assertions.assertDoesNotThrow(() -> resumeSemaphore.acquire());
        return oldConfig;
    }

    private void saveSimpleRecord(@Nonnull final FDBRecordStore store, final int recNo, final int numValue2) {
        store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .setNumValueUnique(recNo * 1139)
                .build());
    }

    private void writeNewRecords(@Nonnull final List<Integer> recNos) {
        try (FDBRecordContext context = openContext()) {
            for (int recNo : recNos) {
                saveSimpleRecord(recordStore, recNo, recNo * 19);
            }
            context.commit();
        }
    }

    private long indexEntryCount(@Nonnull final Index index) {
        try (FDBRecordContext context = openContext()) {
            final long count = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .getCount().join();
            context.commit();
            return count;
        }
    }

    /**
     * The primary keys of the index entries whose leading (indexed) value equals {@code value}, for a value index over
     * a single field.
     */
    @Nonnull
    private List<Long> indexPrimaryKeysForValue(@Nonnull final Index index, final int value) {
        try (FDBRecordContext context = openContext()) {
            final List<Long> primaryKeys = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(value)), null, ScanProperties.FORWARD_SCAN)
                    .map(entry -> entry.getKey().getLong(entry.getKey().size() - 1))
                    .asList().join();
            context.commit();
            return primaryKeys;
        }
    }

    /**
     * The value of the pending writes queue size counter for {@code index}, or {@code null} if the counter key has
     * been cleared (e.g. after the queue data was erased when the index became readable).
     */
    private Long queueSizeCounter(@Nonnull final Index index) {
        try (FDBRecordContext context = openContext()) {
            final Long size = PendingWriteQueueIndexingFactory.getIndexingQueue(recordStore, index)
                    .getQueueSizeNoConflict(context).join();
            context.commit();
            return size;
        }
    }

}
