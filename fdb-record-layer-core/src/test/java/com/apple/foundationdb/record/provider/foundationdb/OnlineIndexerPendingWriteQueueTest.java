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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building an index with a pending writes queue.
 */
class OnlineIndexerPendingWriteQueueTest extends OnlineIndexerTest {

    @Test
    void testDrainPendingQueueWhileBuilding() throws Exception {
        final Index index = new Index("simple$num_value_2_queue", field("num_value_2"), IndexTypes.VALUE);

        final int numInitialRecords = 20;
        populateEvenRecords(numInitialRecords);
        final List<Integer> queuedRecNos = List.of(1, 3, 5, 7);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));

        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicBoolean passed = new AtomicBoolean(false);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();

        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = newIndexerBuilder(index)
                    .setLimit(10)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setUseWritePendingQueue(List.of(index))
                            .build())
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

        // Write the new records. Because the index is in the queue state, these saves are enqueued rather than indexed.
        final FDBStoreTimer writeTimer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, writeTimer)) {
            final FDBRecordStore store = createStoreBuilder().setContext(context)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            assertTrue(store.isIndexWriteOnlyWithQueue(index), "the index should be building with a pending writes queue");
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
            assertEquals((long)queuedRecNos.size(), queueSize == null ? 0L : queueSize.longValue(),
                    "the queue size counter should reflect every deferred write");

            final List<Long> queuedRecordNos = queue
                    .getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .asList().join().stream()
                    .map(entry -> {
                        final IndexBuildProto.PendingWritesQueueEntry payload = entry.getPayload();
                        final Message record = recordStore.getSerializer().deserialize(recordStore.getRecordMetaData(),
                                TupleHelpers.EMPTY, payload.getNewRecord().toByteArray(), recordStore.getTimer());
                        return (Long)record.getField(record.getDescriptorForType().findFieldByName("rec_no"));
                    })
                    .sorted()
                    .toList();
            assertEquals(queuedRecNos.stream().map(Integer::longValue).sorted().collect(Collectors.toList()), queuedRecordNos,
                    "the queue should contain exactly the deferred records");
            context.commit();
        }

        pauseSemaphore.release();
        indexerThread.join();
        assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());

        assertReadable(index);
        // The index must contain an entry for every record, including the ones that were only ever added to the queue.
        try (FDBRecordContext context = openContext()) {
            final long indexEntries = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .getCount().join();
            assertEquals(numInitialRecords + queuedRecNos.size(), indexEntries);
            context.commit();
        }
        // A scrub confirms there are no missing (or dangling) index entries.
        scrubAndValidate(List.of(index));
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
}
