/*
 * PendingWriteQueueTest.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LucenePartitionInfoProto;
import com.apple.foundationdb.record.lucene.LucenePartitioner;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration Tests for {@link PendingWriteQueue} size limit feature.
 * These tests verify the integrated behavior of the queue through index operations: save record, merge index and
 * queries.
 */
@Tag(Tags.RequiresFDB)
public class PendingWriteQueueSizeIntegrationTest extends FDBRecordStoreTestBase {
    private final Index index = SIMPLE_TEXT_SUFFIXES;

    @Test
    void testQueueLimitsSingleTransaction() {
        // Test that too many entries in a single transaction fail to queue
        // Set "ongoing merge" indicator
        setOngoingMergeIndicator(index, null, null, simpleMetadataHook());

        // Write too many records in a single transaction
        final int maxQueueSize = 5;
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            for (int i = 0; i < maxQueueSize; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(100L + i, "test document", 1));
            }
            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class,
                    () -> recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(999L, "test document", 1)));
            commit(context);
        }

        // only 5 records written
        verifyExpectedDocIds(index, Set.of(100L, 101L, 102L, 103L, 104L));
    }

    @Test
    void testQueueLimitsMultipleTransactions() {
        // Test that too many entries in 2 transactions fail to queue
        // Set "ongoing merge" indicator
        setOngoingMergeIndicator(index, null, null, simpleMetadataHook());

        final int maxQueueSize = 5;
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            for (int i = 0; i < maxQueueSize; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(100L + i, "test document", 1));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class,
                    () -> recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(999L, "test document", 1)));
            commit(context);
        }

        verifyExpectedQueueAndIndicator(index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT),
                simpleMetadataHook());

        // only 5 records written
        verifyExpectedDocIds(index, Set.of(100L, 101L, 102L, 103L, 104L));
    }

    @Test
    void testQueueLimitsAfterMerge() {
        // Test that too many entries are cleared after merge
        // Set "ongoing merge" indicator
        setOngoingMergeIndicator(index, null, null, simpleMetadataHook());

        final int maxQueueSize = 5;
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            for (int i = 0; i < maxQueueSize; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(100L + i, "test document", 1));
            }
            commit(context);
        }

        mergeIndexNow(index, simpleMetadataHook());

        // Merge cleared the indicator, queue is now empty
        setOngoingMergeIndicator(index, null, null, simpleMetadataHook());

        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(999L, "test document", 1));
            commit(context);
        }
        verifyExpectedQueueAndIndicator(index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT),
                simpleMetadataHook());

        // 5 docs are in the index, one in the queue
        verifyExpectedDocIds(index, Set.of(100L, 101L, 102L, 103L, 104L, 999L));
    }

    @Test
    void testQueueLimitsWithUpdates() {
        // Test that each update counts for 2 entries in the queue
        // Write 5 records
        final int maxQueueSize = 5;
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            for (int i = 0; i < maxQueueSize; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(100L + i, "test document", 1));
            }
            commit(context);
        }

        // Set "ongoing merge" indicator
        setOngoingMergeIndicator(index, null, null, simpleMetadataHook());

        // Update 3 documents, third update fails
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, simpleMetadataHook());
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(100L, "test document updated", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(101L, "test document updated", 1));
            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class, () ->
                    recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(102L, "test document updated", 1)));
            commit(context);
        }

        // Since we have a failure in the middle of the "save" (DELETE enqueued INSERT failed), the queue is inconsistent.
        // We don't make any guarantees in that case, the transaction should have been rolled back
        verifyExpectedQueueAndIndicator(index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE),
                simpleMetadataHook());

        // And the result is an inconsistent index, where the 102 doc is missing
        verifyExpectedDocIds(index, Set.of(100L, 101L, 103L, 104L));
    }

    @Test
    void testQueueLimitMultiplePartitions() {
        // Test pending queue size limit with partitioned index - documents in different partitions
        final int partitionSize = 3;
        final Index complexIndex = complexPartitionedIndex(partitionSize);
        final RecordMetaDataHook hook = complexMetadataHook(complexIndex);
        final Tuple groupingKey = Tuple.from(1L);  // All documents in group 1
        final int maxQueueSize = 5;
        // Insert a few documents when "ongoing merge" indicator is clear.
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            for (int i = 0; i < 6; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2000L + i, "document foo", 1L, 30L + i));
            }
            commit(context);
        }

        mergeIndexNow(complexIndex, hook);
        // Expected partition count
        verifyPartitionCount(complexIndex, groupingKey, List.of(3, 3), hook);

        final Integer partition0 = 0;
        final Integer partition1 = 1;

        // Enable queue mode for both partitions
        setOngoingMergeIndicator(complexIndex, groupingKey, partition0, hook);
        setOngoingMergeIndicator(complexIndex, groupingKey, partition1, hook);

        // Insert documents when "ongoing merge" indicator is set.
        try (FDBRecordContext context = openContext(getContextProperties(maxQueueSize))) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);

            for (int i = 0; i < maxQueueSize; i++) {
                // save record to new partition
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(100L + i, "second document", 1L, 100L + i));
                // save record to old partition
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(200L + i, "third document", 1L, 30L - i));
            }
            // additional docs fail on both queues
            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class, () ->
                    recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(999, "second document", 1L, 100L + 9)));
            // save record to old partition
            assertThrows(PendingWriteQueue.PendingWritesQueueTooLargeException.class, () ->
                    recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(999, "third document", 1L, 30L - 9)));

            commit(context);
        }

        // verify records found in query (from queue)
        verifyExpectedDocIds(complexIndex, "*:*", groupingKey.getLong(0),
                Set.of(2000L, 2001L, 2002L, 2003L, 2004L, 2005L, 100L, 101L, 102L, 103L, 104L, 200L, 201L, 202L, 203L, 204L),
                hook);

        // Merge index - drains all partition queues
        mergeIndexNow(complexIndex, hook);

        // Verify both queues are empty and indicators are cleared
        verifyClearedQueueAndIndicator(complexIndex, groupingKey, partition0, hook);
        verifyClearedQueueAndIndicator(complexIndex, groupingKey, partition1, hook);
    }

    private void verifyClearedQueueAndIndicator(Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);

            assertFalse(directory.shouldUseQueue(), "Ongoing merge indicator should be cleared");

            PendingWriteQueue queue = directory.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries = new ArrayList<>();
            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries::add).join();
            assertTrue(entries.isEmpty(), "Pending queue should have been empty");
            commit(context);
        }
    }

    private void verifyExpectedQueueAndIndicator(Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId,
                                                 List<LucenePendingWriteQueueProto.PendingWriteItem.OperationType> expectedOperations, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);

            assertTrue(directory.shouldUseQueue(),
                    "Ongoing merge indicator should have been set");

            PendingWriteQueue queue = directory.createPendingWritesQueue();

            RecordCursor<PendingWriteQueue.QueueEntry> queueCursor = queue.getQueueCursor(
                    recordStore.getContext(), ScanProperties.FORWARD_SCAN, null);

            assertEquals(expectedOperations,
                    queueCursor.asList().join().stream()
                            .map(PendingWriteQueue.QueueEntry::getOperationType)
                            .collect(Collectors.toList()));

            commit(context);
        }
    }

    private void verifyPartitionCount(Index index, @Nullable Tuple groupingKey, List<Integer> expectedCount, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            final LucenePartitioner partitioner = indexMaintainer.getPartitioner();

            final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos =
                    partitioner.getAllPartitionMetaInfo(groupingKey == null ? Tuple.from() : groupingKey).join();

            assertEquals(expectedCount,
                    partitionInfos.stream()
                            .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                            .collect(Collectors.toList()));
        }
    }

    private void verifyExpectedDocIds(Index index, Set<Long> expectedDocIds) {
        // Query all documents using wildcard search
        verifyExpectedDocIds(index, "*:*", null, expectedDocIds, simpleMetadataHook());
    }

    private void verifyExpectedDocIds(Index index, String query, @Nullable Object group, Set<Long> expectedDocIds, final RecordMetaDataHook hook) {
        // location of the doc_id within the Tuple returned from the cursor
        int pkLocation = (group == null) ? 0 : 1;

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, query, false, 0, group);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                List<Long> primaryKeys = cursor
                        .map(IndexEntry::getPrimaryKey)
                        .map(tuple -> tuple.getLong(pkLocation))
                        .asList()
                        .join();

                assertEquals(expectedDocIds, new HashSet<>(primaryKeys));
                context.commit();
            }
        }
    }

    private void setOngoingMergeIndicator(Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            // Get directory and set ongoing merge indicator
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index, recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);
            directory.setOngoingMergeIndicator();
            commit(context);
        }
    }

    private void mergeIndexNow(Index index, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }
    }

    @Nonnull
    private LuceneIndexMaintainer getIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer)store.getIndexMaintainer(index);
    }

    RecordMetaDataHook simpleMetadataHook() {
        return metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(TestRecordsTextProto.SimpleDocument.getDescriptor().getName(), SIMPLE_TEXT_SUFFIXES);
        };
    }

    RecordMetaDataHook complexMetadataHook(Index index) {
        return metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(TestRecordsTextProto.ComplexDocument.getDescriptor().getName(), index);
        };
    }

    @Nonnull
    public Index complexPartitionedIndex(int highWatermark) {
        final Map<String, String> options = Map.of(
                ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE, "true",
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(highWatermark));

        return complexPartitionedIndex(options);
    }

    @Nonnull
    public Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private RecordLayerPropertyStorage getContextProperties(int maxQueueSize) {
        return RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_PENDING_QUEUE_SIZE, maxQueueSize)
                .build();
    }

}
