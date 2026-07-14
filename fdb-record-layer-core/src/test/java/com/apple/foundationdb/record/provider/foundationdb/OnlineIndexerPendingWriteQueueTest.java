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

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.CloseException;
import com.apple.test.SuperSlow;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5})
    void testDrainPendingQueueWhileBuilding(int limit) throws Exception {
        // Add new records during online indexing session
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);

        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        final List<Integer> queuedRecNos = List.of(1, 3, 5, 7);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)).setLimit(limit),
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
                                IndexingPendingWriteQueue.getIndexingQueue(recordStore, index);
                        final Long queueSize = queue.getQueueSizeNoConflict(context).join();
                        assertEquals(queuedRecNos.size(), queueSize == null ? 0L : queueSize);

                        final List<Long> queuedRecordNos = queue
                                .getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                                .asList().join().stream()
                                .map(entry -> {
                                    final IndexBuildProto.PendingWritesQueueEntry payload = entry.getPayload();
                                    final Message rec = recordStore.getSerializer().deserialize(recordStore.getRecordMetaData(),
                                            TupleHelpers.EMPTY, payload.getNewRecord().toByteArray(), recordStore.getTimer());
                                    return (Long)rec.getField(rec.getDescriptorForType().findFieldByName("rec_no"));
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testDrainPendingQueueWhileBuildingAfterNPasses(final int passesCount) throws Exception {
        // Same as testDrainPendingQueueWhileBuilding, but the writes are injected after exactly passesCount scan
        // passes have run rather than after the first. The scan limit is kept small (and there are plenty of initial
        // records) so the build always has more than three passes, guaranteeing the pause point is reached.
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);

        final int numInitialRecords = 30;
        populateEvenRecords(numInitialRecords);
        final List<Integer> queuedRecNos = List.of(1, 3, 5, 7);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingAfterNPassesForWrites(
                queueIndexerBuilder(index, List.of(index)).setLimit(3),
                passesCount,
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
                                IndexingPendingWriteQueue.getIndexingQueue(recordStore, index);
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
                        assertTrue(buildState.getRecordsScanned() > 0);
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

        final List<Integer> updatedRecNos = List.of(30, 32, 34, 36, 38); // not yet scanned when the first pass pauses
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)).setLimit(10),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        for (int recNo : updatedRecNos) {
                            saveSimpleRecord(store, recNo, 900_000 + recNo);
                        }
                        context.commit();
                    }
                });

        assertEquals(updatedRecNos.size(), writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(index);
        assertEquals(numInitialRecords, indexEntryCount(index));
        // Each index entry must reflect the new value, not the original one.
        for (int recNo : updatedRecNos) {
            assertEquals(List.of((long)recNo), indexPrimaryKeysForValue(index, 900_000 + recNo));
            assertEquals(List.of(), indexPrimaryKeysForValue(index, recNo * 19));
        }
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
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        // The first pass has already indexed updatedRecNo, so its primary key is in the built range.
                        assertTrue(store.getIndexMaintainer(index).addedRangeWithKey(Tuple.from(updatedRecNo)).join(),
                                "updatedRecNo should be within the range indexed by the first pass");
                        saveSimpleRecord(store, updatedRecNo, newNumValue2);
                        context.commit();
                    }
                });

        // The update should have been enqueued rather than written directly to the index.
        assertEquals(1, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
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
                        assertTrue(store.isIndexWriteOnlyNoQueue(index));
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
    void testVersionIndexFallsBackToWriteOnly() throws Exception {
        // An index whose key contains a record version should fall back to a plain write-only build (at least for now)
        final Index index = new Index("simple$num2_version_queue",
                concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        // A version index requires the store to persist record versions.
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.setStoreRecordVersions(true);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(hook);
        disableAll(List.of(index));

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        assertTrue(store.isIndexWriteOnlyNoQueue(index));
                        assertFalse(store.isIndexWriteOnlyWithQueue(index));
                        saveSimpleRecord(store, 1, 19);
                        saveSimpleRecord(store, 3, 57);
                        context.commit();
                    }
                });

        assertEquals(0, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        // The build still completes as a normal write-only version index.
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadable(index));
            context.commit();
        }
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
                        assertTrue(store.isIndexWriteOnlyNoQueue(directIndex));
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
    void testMultiTargetTwoQueuedTwoDirect() throws Exception {
        // Build five indexes together: two value indexes use a pending writes queue, two value indexes are built
        // directly (plain write-only), and a non-idempotent SUM index is also built directly (it cannot use the queue).
        // Each index should carry its own state during the build and end up correctly built.
        final Index queuedA = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final Index queuedB = new Index("simple$num_value_unique_queue", field("num_value_unique"), IndexTypes.VALUE);
        final Index directA = new Index("simple$num_value_3_direct", field("num_value_3_indexed"), IndexTypes.VALUE);
        final Index directB = new Index("simple$str_value_direct", field("str_value_indexed"), IndexTypes.VALUE);
        final Index sumIndex = new Index("simple$sum_num_value_2_direct", field("num_value_2").ungrouped(), IndexTypes.SUM);
        final List<Index> queuedIndexes = List.of(queuedA, queuedB);
        // Every index that is not queued (the two direct value indexes plus the non-idempotent SUM index).
        final List<Index> directIndexes = List.of(directA, directB, sumIndex);
        // The value indexes only, for the per-record entry-count and scrub assertions.
        final List<Index> valueIndexes = List.of(queuedA, queuedB, directA, directB);
        final List<Index> indexes = List.of(queuedA, queuedB, directA, directB, sumIndex);
        final int numInitialRecords = 30;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        final List<Integer> newRecNos = List.of(1, 3, 5);
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(indexes, queuedIndexes),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        // The two queued indexes enter the queue state; every other index (including the SUM index) is
                        // plain write-only.
                        for (Index queued : queuedIndexes) {
                            assertTrue(store.isIndexWriteOnlyWithQueue(queued), queued.getName());
                        }
                        for (Index direct : directIndexes) {
                            assertTrue(store.isIndexWriteOnlyNoQueue(direct), direct.getName());
                            assertFalse(store.isIndexWriteOnlyWithQueue(direct), direct.getName());
                        }
                        for (int recNo : newRecNos) {
                            saveSimpleRecord(store, recNo, recNo * 19);
                        }
                        context.commit();
                    }
                });

        // Each of the two queued indexes defers one write per new record; the direct indexes are written normally.
        assertEquals((long)newRecNos.size() * queuedIndexes.size(),
                writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertReadable(indexes);
        // Every value index - queued or direct - ends up with an entry for every record, including the new ones.
        for (Index index : valueIndexes) {
            assertEquals(numInitialRecords + newRecNos.size(), indexEntryCount(index), index.getName());
        }
        // The SUM index must reflect the sum of num_value_2 across all records (initial and concurrently written).
        long expectedSum = 0;
        for (int i = 0; i < numInitialRecords; i++) {
            expectedSum += (long)(2 * i) * 19; // populateEvenRecords: num_value_2 == recNo * 19, recNo == 2 * i
        }
        for (int recNo : newRecNos) {
            expectedSum += (long)recNo * 19;
        }
        final IndexAggregateFunction sumFunction =
                new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName());
        try (FDBRecordContext context = openContext()) {
            final long sum = recordStore.evaluateAggregateFunction(List.of("MySimpleRecord"), sumFunction,
                    TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0);
            assertEquals(expectedSum, sum, sumIndex.getName());
            context.commit();
        }
        // The queued indexes' queue data is erased once they become readable.
        for (Index queued : queuedIndexes) {
            assertNull(queueSizeCounter(queued), queued.getName());
        }
        scrubAndValidate(valueIndexes);
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
                                && recordStore.isIndexWriteOnlyNoQueue(countIndex)
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
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numInitialRecords; i++) {
                final int recNo = 2 * i;
                saveSimpleRecord(recordStore, recNo, recNo * 19);
            }
            context.commit();
        }
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
    void testReDrainsWhenWritesArriveDuringDrain() throws Exception {
        // Enqueues a record from inside updateWhileWriteOnly, i.e. *during* the drain. This save is pushed
        // back into the pending writes queue.
        final Index index = new Index("simple$num_value_2_reenqueue", field("num_value_2"),
                ReEnqueueDuringDrainIndexMaintainer.INDEX_TYPE);
        final int numInitialRecords = 20; // recNos 0,2,..,38 -- neither the trigger (7) nor injected (9) record exists yet
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        // The trigger and the record it injects during the drain share this num_value_2 (the injected record is a
        // copy of the trigger with a different rec_no), so both land under the same index key.
        final int sharedNumValue2 = (int) ReEnqueueDuringDrainIndexMaintainer.TRIGGER_REC_NO * 19;
        buildIndexPausingOnceForWrites(
                queueIndexerBuilder(index, List.of(index)),
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        // Enqueue the trigger record; replaying it during the drain enqueues INJECTED_REC_NO in turn.
                        saveSimpleRecord(recordStore, (int) ReEnqueueDuringDrainIndexMaintainer.TRIGGER_REC_NO, sharedNumValue2);
                        context.commit();
                    }
                });

        assertReadable(index);
        // The re-drain must have applied the record injected mid-drain: the trigger and the injected record are both
        // indexed under the shared value, and the queue is fully drained.
        assertEquals(numInitialRecords + 2, indexEntryCount(index));
        assertEquals(
                List.of(ReEnqueueDuringDrainIndexMaintainer.TRIGGER_REC_NO, ReEnqueueDuringDrainIndexMaintainer.INJECTED_REC_NO),
                indexPrimaryKeysForValue(index, sharedNumValue2));
        assertNull(queueSizeCounter(index), "the queue must be fully drained before the index becomes readable");
    }

    @Test
    void testDrainsWritesEnqueuedDuringIndexingPass() {
        // Enqueues a record from inside update(), i.e. during the indexing pass
        final Index index = new Index("simple$num_value_2_enqueue_indexing", field("num_value_2"),
                EnqueueDuringIndexingIndexMaintainer.INDEX_TYPE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        // A small scan limit forces several passes, so the record injected while indexing recNo TRIGGER_REC_NO is
        // enqueued mid-build and drained by a subsequent pass rather than only at mark-readable time.
        try (OnlineIndexer indexer = queueIndexerBuilder(index, List.of(index)).setLimit(3).build()) {
            indexer.buildIndex(true);
        }

        assertReadable(index);
        // The record enqueued mid-pass must have been drained and indexed: every initial record plus the injected one.
        assertEquals(numInitialRecords + 1, indexEntryCount(index));
        assertEquals(
                List.of(EnqueueDuringIndexingIndexMaintainer.INJECTED_REC_NO),
                indexPrimaryKeysForValue(index, (int) (EnqueueDuringIndexingIndexMaintainer.INJECTED_REC_NO * 19)));
        assertNull(queueSizeCounter(index), "the queue must be fully drained before the index becomes readable");
        scrubAndValidate(List.of(index));
    }

    @Test
    void testFallsBackToWriteOnlyBelowFormatVersion() throws Exception {
        formatVersion = FormatVersionTestUtils.previous(FormatVersion.WRITE_ONLY_WITH_QUEUE);
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
                        assertTrue(store.isIndexWriteOnlyNoQueue(index), "the index should fall back to plain write-only");
                        for (int recNo : newRecNos) {
                            saveSimpleRecord(store, recNo, recNo * 19);
                        }
                        context.commit();
                    }
                });

        assertTrue(formatVersion.compareTo(FormatVersion.WRITE_ONLY_WITH_QUEUE) < 0);
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
    void testEnqueuedWriteFailsToCommitWhenIndexBecomesReadable() throws Exception {
        // A user transaction that updates the pending write queue must fail to commit if the index becomes readable
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final Semaphore pausedSemaphore = new Semaphore(0);
        final Semaphore resumeSemaphore = new Semaphore(0);
        final AtomicBoolean firstPass = new AtomicBoolean(true);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();

        // Build the index with the queue on a background thread, pausing after the first pass
        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = queueIndexerBuilder(index, List.of(index))
                    .setConfigLoader(old -> pauseThenResume(old, firstPass, pausedSemaphore, resumeSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();

        // Wait until the indexer has paused; the index state is now WRITE_ONLY_WITH_QUEUE.
        pausedSemaphore.acquire();

        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        try (FDBRecordContext userContext = fdb.openContext(null, writeTimer)) {
            final FDBRecordStore userStore = createStoreBuilder().setContext(userContext)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            assertTrue(userStore.getIndexState(index).isWriteOnlyWithQueue());
            // The update is deferred to the pending writes queue
            saveSimpleRecord(userStore, 1, 19);
            assertEquals(1, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));

            // Let the indexer run to completion and marks the index READABLE (after drain)
            resumeSemaphore.release(Integer.MAX_VALUE);
            indexerThread.join();
            assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());
            assertReadable(index);

            // The user transaction must now fail: the index state it read while enqueuing changed underneath it.
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, userContext::commit,
                    "enqueuing a write should not commit once the index has been marked readable");
        }

        // The rejected write was never applied: only the initial (even) records are indexed.
        assertEquals(numInitialRecords, indexEntryCount(index));
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
        final IndexingPendingWriteQueue.PendingWriteQueueDrainException ex =
                new IndexingPendingWriteQueue.PendingWriteQueueDrainException(cause);
        assertEquals("Pending write queue drain had failed", ex.getMessage());
        assertSame(cause, ex.getCause());
    }


    @ParameterizedTest
    @CsvSource({
            // numInitialRecords, limit, maxWriteIterations
            "20, 1, 100",
            "50, 2, 200",
    })
    void testConcurrentWritesWhileBuildingWithQueue(final int numInitialRecords, final int limit, final int maxWriteIterations) throws InterruptedException {
        concurrentWritesWhileBuildingWithQueue(numInitialRecords, limit, maxWriteIterations);
    }

    @Test
    @SuperSlow
    void testConcurrentWritesWhileBuildingWithQueueManyRecords() throws InterruptedException {
        // Nightly-only: overlap a great many concurrent writes.
        concurrentWritesWhileBuildingWithQueue(10_000, 10, 1200);
    }

    @Test
    void testMutualIndexingFallsBackToWriteOnly() throws Exception {
        // Mutual indexing does not support the pending writes queue
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        final int numInitialRecords = 30;
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final List<Integer> newRecNos = List.of(1, 3, 5);
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        buildIndexPausingOnceForWrites(
                newIndexerBuilder(index)
                        .setLimit(5)
                        .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                                .setMutualIndexing()
                                .setUsePendingWriteQueue(List.of(index))
                                .build()),
                () -> {
                    try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        // Despite requesting the queue, mutual indexing marks the index plain write-only.
                        assertTrue(store.isIndexWriteOnlyNoQueue(index),
                                "a mutually-built index should fall back to plain write-only");
                        assertFalse(store.isIndexWriteOnlyWithQueue(index),
                                "mutual indexing must not enter the queue state");
                        for (int recNo : newRecNos) {
                            saveSimpleRecord(store, recNo, recNo * 19);
                        }
                        context.commit();
                    }
                });

        // No writes were deferred to a queue, and the queue counter was never created.
        assertEquals(0, writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE));
        assertNull(queueSizeCounter(index));
        assertReadable(index);
        assertEquals(numInitialRecords + newRecNos.size(), indexEntryCount(index));
        scrubAndValidate(List.of(index));
    }

    private void concurrentWritesWhileBuildingWithQueue(final int numInitialRecords, final int limit, final int maxWriteIterations) throws InterruptedException {
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);
        populateEvenRecords(numInitialRecords);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final AtomicInteger writtenCount = new AtomicInteger(0);
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final FDBStoreTimer writeTimer = new FDBStoreTimer();

        final Thread writerThread = new Thread(() -> {
            try {
                int recNo = 1;
                while (writtenCount.get() < maxWriteIterations && !isIndexReadableInNewContext(index)) {
                    final int thisRecNo = recNo;
                    // Use a retrying runner: the write reads the index state, which the build mutates when it marks the
                    // index readable, so a lock-step raw transaction would occasionally lose a conflict and abort.
                    fdb.run(writeTimer, null, context -> {
                        final FDBRecordStore store = createStoreBuilder().setContext(context)
                                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
                        saveSimpleRecord(store, thisRecNo, thisRecNo * 19);
                        return null;
                    });
                    writtenCount.incrementAndGet();
                    recNo += 2;
                    // Throttle a touch so the writer cannot indefinitely outrun the drainer's bounded mark-readable
                    // retries during the final drain.
                    snooze(3);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            }
        });
        writerThread.start();

        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setUsePendingWriteQueue(List.of(index))
                        .setPendingWriteQueueIndexesMaxDrainAttempts(maxWriteIterations + 4))
                .setLimit(limit)
                .build()) {
            indexer.buildIndex(true);
        }
        writerThread.join();

        assertNull(writerFailure.get(), () -> "the concurrent writer failed: " + writerFailure.get());
        assertTrue(writtenCount.get() > 0, "the writer should have committed at least one record");
        // Some of the concurrent writes must have been deferred to the queue while the index was in the queue state.
        assertTrue(writeTimer.getCount(FDBStoreTimer.Counts.PENDING_WRITES_QUEUE_WRITE) > 0,
                "at least some concurrent writes should have been deferred to the pending writes queue");

        assertReadable(index);
        // Every initial record plus every concurrently written record must be present in the index.
        assertEquals(numInitialRecords + writtenCount.get(), indexEntryCount(index));
        assertNull(queueSizeCounter(index), "the queue data should have been erased once the index became readable");
        scrubAndValidate(List.of(index));
    }

    private boolean isIndexReadableInNewContext(@Nonnull final Index index) {
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore store = createStoreBuilder().setContext(context)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            final boolean readable = store.isIndexReadable(index);
            context.commit();
            return readable;
        }
    }

    private void populateEvenRecords(final int numRecords) {
        openSimpleMetaData();
        // Commit in batches so a large initial data set does not overflow a single transaction's byte limit.
        final int batchSize = 100;
        for (int start = 0; start < numRecords; start += batchSize) {
            final int end = Math.min(start + batchSize, numRecords);
            try (FDBRecordContext context = openContext()) {
                for (int i = start; i < end; i++) {
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
     * Like {@link #buildIndexPausingOnceForWrites}, but pauses after exactly {@code passesCount} scan passes rather
     * than after the first. The caller must ensure the build has more than {@code passesCount} passes, otherwise the
     * pause point is never reached and this method deadlocks waiting for it.
     */
    private void buildIndexPausingAfterNPassesForWrites(@Nonnull final OnlineIndexer.Builder indexerBuilder,
                                                        final int passesCount,
                                                        @Nonnull final Runnable whilePaused) throws InterruptedException {
        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicInteger passCounter = new AtomicInteger(0);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();

        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = indexerBuilder
                    .setConfigLoader(old -> pauseAfterNthPass(old, passesCount, passCounter, startBuildingSemaphore, pauseSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();
        // Wait until the indexer thread has run passesCount passes and paused.
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
            final Long size = IndexingPendingWriteQueue.getIndexingQueue(recordStore, index)
                    .getQueueSizeNoConflict(context).join();
            context.commit();
            return size;
        }
    }

    public static class ReEnqueueDuringDrainIndexMaintainer extends ValueIndexMaintainer {
        public static final String INDEX_TYPE = "value_reenqueue_during_drain";
        /** The queued record whose replay triggers a mid-drain enqueue. */
        public static final long TRIGGER_REC_NO = 7L;
        /** The record injected into the queue during the drain of {@link #TRIGGER_REC_NO}. */
        public static final long INJECTED_REC_NO = 9L;

        public ReEnqueueDuringDrainIndexMaintainer(final IndexMaintainerState state) {
            super(state);
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                                                @Nullable final FDBIndexableRecord<M> newRecord) {
            final CompletableFuture<Void> superFuture = super.updateWhileWriteOnly(oldRecord, newRecord);
            if (newRecord == null) {
                return superFuture;
            }
            final Message rec = newRecord.getRecord();
            final Descriptors.Descriptor descriptor = rec.getDescriptorForType();
            final Descriptors.FieldDescriptor recNoField = descriptor.findFieldByName("rec_no");
            final long recNo = ((Number)rec.getField(recNoField)).longValue();
            if (recNo != TRIGGER_REC_NO) {
                return superFuture;
            }
            // Simulate a concurrent writer enqueueing a new item while the drain is in progress. The save runs in its
            // own (asynchronous) transaction, separate from this drain transaction: the index is still
            // WRITE_ONLY_WITH_QUEUE, so that save is deferred back into the pending writes queue rather than indexed.
            final Message.Builder builder = rec.toBuilder().setField(recNoField, INJECTED_REC_NO);
            final Descriptors.FieldDescriptor uniqueField = descriptor.findFieldByName("num_value_unique");
            if (uniqueField != null && rec.hasField(uniqueField)) {
                builder.setField(uniqueField, (int)(INJECTED_REC_NO * 1139));
            }
            final Message injected = builder.build();
            final FDBDatabase database = state.context.getDatabase();
            final FDBRecordStore.Builder storeBuilder = state.store.asBuilder();
            final CompletableFuture<Void> asyncSave = database.runAsync(context ->
                    storeBuilder.copyBuilder().setContext(context)
                            .createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.NONE)
                            .thenCompose(store -> store.saveRecordAsync(injected))
                            .thenApply(ignore -> null));
            return CompletableFuture.allOf(superFuture, asyncSave);
        }

        @AutoService(IndexMaintainerFactory.class)
        public static class Factory implements IndexMaintainerFactory {
            private static final Set<String> INDEX_TYPES = Collections.singleton(INDEX_TYPE);
            private static final ValueIndexMaintainerFactory underlying = new ValueIndexMaintainerFactory();

            @Nonnull
            @Override
            public Iterable<String> getIndexTypes() {
                return INDEX_TYPES;
            }

            @Nonnull
            @Override
            public IndexValidator getIndexValidator(final Index index) {
                return underlying.getIndexValidator(index);
            }

            @Nonnull
            @Override
            public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
                return new ReEnqueueDuringDrainIndexMaintainer(state);
            }
        }
    }

    /**
     * A value index maintainer that, while indexing a specific record during the build scan, enqueues a new
     * record into the pending writes queue in a separate transaction. This injects a queued write <i>during the
     * indexing pass</i>
     */
    public static class EnqueueDuringIndexingIndexMaintainer extends ValueIndexMaintainer {
        public static final String INDEX_TYPE = "value_enqueue_during_indexing";
        /** The (pre-existing) record whose indexing triggers the mid-pass enqueue. */
        public static final long TRIGGER_REC_NO = 6L;
        /** The new record injected into the queue while {@link #TRIGGER_REC_NO} is being indexed. */
        public static final long INJECTED_REC_NO = 5L;

        public EnqueueDuringIndexingIndexMaintainer(final IndexMaintainerState state) {
            super(state);
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                                  @Nullable final FDBIndexableRecord<M> newRecord) {
            final CompletableFuture<Void> superFuture = super.update(oldRecord, newRecord);
            if (newRecord == null) {
                return superFuture;
            }
            final Message rec = newRecord.getRecord();
            final Descriptors.FieldDescriptor recNoField = rec.getDescriptorForType().findFieldByName("rec_no");
            final long recNo = ((Number)rec.getField(recNoField)).longValue();
            if (recNo != TRIGGER_REC_NO) {
                return superFuture;
            }
            // Inject a new record into the pending writes queue mid-pass.
            final Message injected = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(INJECTED_REC_NO)
                    .setNumValue2((int)(INJECTED_REC_NO * 19))
                    .setNumValueUnique((int)(INJECTED_REC_NO * 1139))
                    .build();
            final FDBDatabase database = state.context.getDatabase();
            final FDBRecordStore.Builder storeBuilder = state.store.asBuilder();
            final CompletableFuture<Void> asyncSave = database.runAsync(context ->
                    storeBuilder.copyBuilder().setContext(context)
                            .createOrOpenAsync(FDBRecordStoreBase.StoreExistenceCheck.NONE)
                            .thenCompose(store -> store.saveRecordAsync(injected))
                            .thenApply(ignore -> null));
            return CompletableFuture.allOf(superFuture, asyncSave);
        }

        @AutoService(IndexMaintainerFactory.class)
        public static class Factory implements IndexMaintainerFactory {
            private static final Set<String> INDEX_TYPES = Collections.singleton(INDEX_TYPE);
            private static final ValueIndexMaintainerFactory underlying = new ValueIndexMaintainerFactory();

            @Nonnull
            @Override
            public Iterable<String> getIndexTypes() {
                return INDEX_TYPES;
            }

            @Nonnull
            @Override
            public IndexValidator getIndexValidator(final Index index) {
                return underlying.getIndexValidator(index);
            }

            @Nonnull
            @Override
            public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
                return new EnqueueDuringIndexingIndexMaintainer(state);
            }
        }
    }

}
