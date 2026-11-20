/*
 * OnlineIndexerPendingWriteQueueAutoQATest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainerWithQueue;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueueEntry;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for building an index with a pending writes queue.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerPendingWriteQueueAutoQATest extends OnlineIndexerTest {

    private final Index myIndex = new Index("simple$num_value_2_queue", field("num_value_2"), ValueIndexMaintainerWithQueue.Factory.INDEX_TYPE);

    @Test
    void getQueueCursorWithSkipSkipsLeadingEntries() {
        // BUG AQA-30: PendingWritesQueue.getQueueCursor clears skip on the inner cursor and never applies it again.
        // Spec: getQueueCursor comment "the caller's row/time/byte/skip limits are honored at the entry level".
        // The queue cursor must honor the skip of the given scan properties at the entry level.
        enqueueRecords(List.of(1, 3, 5, 7, 9));
        final ScanProperties skipTwo = new ScanProperties(ExecuteProperties.newBuilder().setSkip(2).build());
        assertThat(queuedRecNos(skipTwo)).containsExactly(5L, 7L, 9L);
    }

    @Nonnull
    private static TestRecords1Proto.MySimpleRecord largeRecord(final int recNo, final int numValue2) {
        final TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .setNumValueUnique(recNo * 1139);
        for (int i = 0; i < 70_000; i++) {
            builder.addRepeater(1);
        }
        return builder.build();
    }

    private void enqueueRecords(@Nonnull final List<Integer> recNos) {
        openSimpleMetaData(allIndexesHook(List.of(myIndex)));
        disableAll(List.of(myIndex));
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(myIndex, IndexState.WRITE_ONLY_WITH_QUEUE).join();
            context.commit();
        }
        // Each record is saved in its own transaction so the queue entries have distinct commit versions.
        for (int recNo : recNos) {
            try (FDBRecordContext context = openContext()) {
                saveSimpleRecord(recordStore, recNo, recNo * 19);
                context.commit();
            }
        }
    }

    @Nonnull
    private List<Long> queuedRecNos(@Nonnull final ScanProperties scanProperties) {
        try (FDBRecordContext context = openContext()) {
            final List<Long> recNos = IndexingPendingWriteQueue.getIndexingQueue(recordStore, myIndex)
                    .getQueueCursor(context, scanProperties, null)
                    .map(this::recNoOf)
                    .asList().join();
            context.commit();
            return recNos;
        }
    }

    private long recNoOf(@Nonnull final PendingWritesQueueEntry<IndexBuildProto.PendingWritesQueueEntry> entry) {
        final IndexBuildProto.OldAndNewRecords records;
        try {
            records = entry.getPayload().getData().unpack(IndexBuildProto.OldAndNewRecords.class);
        } catch (InvalidProtocolBufferException e) {
            throw new RecordCoreException("failed to parse pending write queue entry data", e);
        }
        final Message rec = recordStore.getSerializer().deserialize(recordStore.getRecordMetaData(),
                TupleHelpers.EMPTY, records.getNewRecord().toByteArray(), recordStore.getTimer());
        return (Long)rec.getField(rec.getDescriptorForType().findFieldByName("rec_no"));
    }

    private void runInStore(@Nonnull final Consumer<FDBRecordStore> action) {
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore store = createStoreBuilder().setContext(context)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            action.accept(store);
            context.commit();
        }
    }

    @Nonnull
    private Map<Long, Long> indexContents(@Nonnull final Index index) {
        try (FDBRecordContext context = openContext()) {
            final Map<Long, Long> contents = new TreeMap<>();
            recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .forEach(entry -> {
                        final Long previous = contents.put(entry.getKey().getLong(1), entry.getKey().getLong(0));
                        assertThat(previous).as("duplicate index entry for record %s", entry.getKey().getLong(1)).isNull();
                    }).join();
            context.commit();
            return contents;
        }
    }

    private void populateEvenRecords(final int numRecords) {
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                final int recNo = 2 * i;
                saveSimpleRecord(recordStore, recNo, recNo * 19);
            }
            context.commit();
        }
    }

    private void saveSimpleRecord(@Nonnull final FDBRecordStore store, final int recNo, final int numValue2) {
        store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .setNumValueUnique(recNo * 1139)
                .build());
    }

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
        startBuildingSemaphore.acquire();
        startBuildingSemaphore.release();

        @Nullable Throwable writeFailure = null;
        try {
            whilePaused.run();
        } catch (Throwable t) {
            writeFailure = t;
        }

        pauseSemaphore.release();
        indexerThread.join();
        assertThat(writeFailure).as("the concurrent writes failed").isNull();
        assertThat(buildFailure.get()).as("the build failed").isNull();
    }
}
