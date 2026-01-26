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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LucenePartitionInfoProto;
import com.apple.foundationdb.record.lucene.LucenePartitioner;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.Streams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link PendingWriteQueue}.
 */
@Tag(Tags.RequiresFDB)
public class PendingWriteQueueTest extends FDBRecordStoreTestBase {
    @ParameterizedTest
    @EnumSource
    void testEnqueueAndIterate(LucenePendingWriteQueueProto.PendingWriteItem.OperationType operationType) {
        List<TestDocument> docs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            docs.forEach(doc -> {
                switch (operationType) {
                    case INSERT:
                        queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
                        break;
                    case UPDATE:
                        queue.enqueueUpdate(context, doc.getPrimaryKey(), doc.getFields());
                        break;
                    case DELETE:
                        queue.enqueueDelete(context, doc.getPrimaryKey());
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown operation " + operationType);
                }
            });
            commit(context);
        }

        assertQueueEntries(queue, docs, operationType);
    }

    @Test
    void testEnqueueMultipleTransactions() {
        List<TestDocument> docs = createTestDocuments();
        List<TestDocument> moreDocs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            docs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            moreDocs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            commit(context);
        }

        assertQueueEntries(queue, Stream.concat(docs.stream(), moreDocs.stream()).collect(Collectors.toList()), LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT);
    }

    @Test
    void testEnqueueAndDelete() {
        List<TestDocument> docs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            docs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            assertEquals(docs.size(), context.getTimer().getCount(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_WRITE));
            commit(context);
        }

        List<PendingWriteQueue.QueueEntry> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .asList().join();
        }

        // Delete the 3rd entry
        try (FDBRecordContext context = openContext()) {
            queue.clearEntry(context, entries.get(2));
            assertEquals(1, context.getTimer().getCount(LuceneEvents.Counts.LUCENE_PENDING_QUEUE_CLEAR));
            commit(context);
        }

        docs = new ArrayList<>(docs);
        docs.remove(2);
        assertQueueEntries(queue, docs, LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT);
    }

    @Test
    void testDeleteAll() {
        List<TestDocument> docs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            docs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            commit(context);
        }

        List<PendingWriteQueue.QueueEntry> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .asList().join();
        }

        // Delete All
        try (FDBRecordContext context = openContext()) {
            entries.forEach(entry -> queue.clearEntry(context, entry));
            commit(context);
        }

        assertQueueEntries(queue, Collections.emptyList(), LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT);
    }

    @Test
    void testIterateEmptyQueue() {
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));
        assertQueueEntries(queue, Collections.emptyList(), LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT);
    }

    @Test
    void testWrongValueType() {
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));
        final LuceneDocumentFromRecord.DocumentField fieldWithWrongType =
                createField("f", 5, LuceneIndexExpressions.DocumentFieldType.STRING, true, true);

        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(() -> queue.enqueueInsert(context, Tuple.from(1), List.of(fieldWithWrongType)))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Test
    void testUnsupportedFieldConfigType() {
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));
        final LuceneDocumentFromRecord.DocumentField fieldWithWrongConfig =
                createField("f", 5, LuceneIndexExpressions.DocumentFieldType.INT, true, true, Map.of("Double", 5.42D));

        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(() -> queue.enqueueInsert(context, Tuple.from(1), List.of(fieldWithWrongConfig)))
                    .isInstanceOf(RecordCoreArgumentException.class);
        }
    }

    @Test
    void testIterateWithContinuations() {
        List<TestDocument> docs = createTestDocuments();
        List<TestDocument> moreDocs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            docs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            moreDocs.forEach(doc -> {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            });
            commit(context);
        }

        // There are 8 documents, reading with limit=3 will create 2 continuations
        List<PendingWriteQueue.QueueEntry> allResults = new ArrayList<>();
        RecordCursorContinuation continuation;

        ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(3)
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .build());

        // First iteration - 3 elements
        try (FDBRecordContext context = openContext()) {
            final RecordCursor<PendingWriteQueue.QueueEntry> cursor = queue.getQueueCursor(context, scanProperties, null);
            final RecordCursorResult<PendingWriteQueue.QueueEntry> lastResult = cursor.forEachResult(result -> {
                allResults.add(result.get());
            }).join();
            continuation = lastResult.getContinuation();
        }
        assertEquals(3, allResults.size());

        // Second iteration - 3 elements
        try (FDBRecordContext context = openContext()) {
            final RecordCursor<PendingWriteQueue.QueueEntry> cursor = queue.getQueueCursor(context, scanProperties, continuation.toBytes());
            final RecordCursorResult<PendingWriteQueue.QueueEntry> lastResult = cursor.forEachResult(result -> {
                allResults.add(result.get());
            }).join();
            continuation = lastResult.getContinuation();
        }
        assertEquals(6, allResults.size());

        // Third iteration - 2 elements
        try (FDBRecordContext context = openContext()) {
            final RecordCursor<PendingWriteQueue.QueueEntry> cursor = queue.getQueueCursor(context, scanProperties, continuation.toBytes());
            final RecordCursorResult<PendingWriteQueue.QueueEntry> lastResult = cursor.forEachResult(result -> {
                allResults.add(result.get());
            }).join();
            continuation = lastResult.getContinuation();
        }
        assertEquals(8, allResults.size());

        // Ensure all documents show up in the results
        List<TestDocument> allDocs = Streams.concat(docs.stream(), moreDocs.stream()).collect(Collectors.toList());
        Assertions.assertThat(allResults).zipSatisfy(allDocs, (entry, doc) -> entryEquals(entry, doc, LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT));
    }

    @Test
    void testIsQueueEmpty() {
        List<TestDocument> docs = createTestDocuments();
        PendingWriteQueue queue = new PendingWriteQueue(new Subspace(Tuple.from(UUID.randomUUID().toString())));

        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.isQueueEmpty(context).join());
            commit(context);
        }

        docs.forEach(doc -> {
            try (FDBRecordContext context = openContext()) {
                queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
                commit(context);
            }
            // the enqueue is finalized only after the commit. Verifying should be done with another context
            try (FDBRecordContext context = openContext()) {
                assertFalse(queue.isQueueEmpty(context).join(), "Expected isQueueEmpty to return false");
                commit(context);
            }
        });
    }


    @Test
    void testPendingQueueSimple() {
        // Test simple non-partitioned pending queue life cycle
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Set "ongoing merge" indicator
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // Write a record
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "test document", 1)
            );
            commit(context);
        }

        // Verify record is in queue
        verifyExpectedQueueAndIndicator(schemaSetup, index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT));

        // call merge - this should call drain and remove the "ongoing merge" indicator
        mergeIndexNow(schemaSetup, index);

        // Verify empty queue
        verifyClearedQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void testPendingQueueSimpleWithDelete() {
        // Test pending queue with both INSERT and DELETE operations
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Set ongoing merge indicator
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // Write multiple records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "first document", 1)
            );
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1002L, "second document", 1)
            );
            commit(context);
        }

        // Delete one of the records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(Tuple.from(1001L));
            commit(context);
        }

        // Verify records are in queue (2 INSERTs + 1 DELETE)
        verifyExpectedQueueAndIndicator(schemaSetup, index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));

        // Call merge - this should drain the queue and remove the ongoing merge indicator
        mergeIndexNow(schemaSetup, index);

        // Verify empty queue and correct final state
        verifyClearedQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void testPendingQueueMultiDelete() {
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();


        // Write multiple records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "first document", 1)
            );
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1002L, "second document", 1)
            );
            commit(context);
        }

        // Set ongoing merge indicator
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // Write multiple records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1003L, "third document", 1)
            );
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1004L, "fourth document", 1)
            );
            commit(context);
        }

        // Delete of two records, one written before and one during the merge
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(Tuple.from(1001L));
            recordStore.deleteRecord(Tuple.from(1004L));
            commit(context);
        }

        // Verify records are in queue
        verifyExpectedQueueAndIndicator(schemaSetup, index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));

        // Call merge - this should drain the queue and remove the ongoing merge indicator
        mergeIndexNow(schemaSetup, index);

        // Verify empty queue and correct final state
        verifyClearedQueueAndIndicator(schemaSetup, index, null, null);

        // Verify that only docs 1002L and 1003L remain in the index after merge
        verifyExpectedDocIds(schemaSetup, index, Set.of(1002L, 1003L));
    }

    @Test
    void pendingQueueTestMultiplePartitions() {
        // Test pending queue with partitioned index - documents in different partitions
        final Map<String, String> options = Map.of(
                ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE, "true",
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(3));

        final Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.ComplexDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Insert a few documents when "ongoing merge" indicator is clear.
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2001L, "document foo", 1L, 30L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2002L, "document bar", 1L, 130L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2003L, "document zoo", 1L, 35L));
            commit(context);
        }

        final Tuple groupingKey = Tuple.from(1L);  // All documents in group 1
        // Expected partition count
        verifyPartitionCount(schemaSetup, index, groupingKey, List.of(3));

        final Integer partition0 = 0;
        final Integer partition1 = 1;

        // Enable queue mode for both partitions
        setOngoingMergeIndicator(schemaSetup, index, groupingKey, partition0);

        // Store primary keys for later deletion
        Tuple primaryKey1;

        // Insert documents when "ongoing merge" indicator is set.
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            primaryKey1 = recordStore.saveRecord(
                    LuceneIndexTestUtils.createComplexDocument(1001L, "first document", 1L, 50L)
            ).getPrimaryKey();
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1002L, "second document", 1L, 150L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1003L, "third document", 1L, 75L));
            commit(context);
        }

        verifyPartitionCount(schemaSetup, index, groupingKey, List.of(6));

        // Delete one document from partition 0
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(primaryKey1);
            commit(context);
        }

        // Verify empty queue in partition 1 - which does not exist yet
        verifyClearedQueueAndIndicator(schemaSetup, index, groupingKey, partition1);

        // Verify queue in partition 0
        verifyExpectedQueueAndIndicator(schemaSetup, index, groupingKey, partition0,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));

        // Merge index - drains all partition queues
        // Note: This test sets the "ongoing merge" indicator ahead of the real merge. Merge allows
        // the indicator being set as a way to recover from a leftover set indicator - which also enables this test.
        mergeIndexNow(schemaSetup, index);

        // Verify both queues are empty and indicators are cleared
        verifyClearedQueueAndIndicator(schemaSetup, index, groupingKey, partition0);
        verifyClearedQueueAndIndicator(schemaSetup, index, groupingKey, partition1);

        // After the drain the count should be reduced by 1 to 5 + repartition should split partition 0
        verifyPartitionCount(schemaSetup, index, groupingKey, List.of(3, 2));
    }

    @Test
    void testPendingQueueAcrossMultiplePartitions() {
        // Test pending queue with operations queued across multiple partitions
        final Map<String, String> options = Map.of(
                ENABLE_PENDING_WRITE_QUEUE_DURING_MERGE, "true",
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(5));  // Small watermark for simple test

        final Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.ComplexDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        final Tuple groupingKey = Tuple.from(1L);
        final Integer partition0 = 0;
        final Integer partition1 = 1;

        // Write initial documents to create 2 partitions (before queue enabled)
        Map<Long, Tuple> primaryKeys = new HashMap<>();
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            for (long i = 3001L; i <= 3005L; i++) {
                Tuple primaryKey = recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, "doc p1-" + i, 1L, 10L + i)).getPrimaryKey();
                primaryKeys.put(i, primaryKey);
            }
            for (long i = 2001L; i <= 2005L; i++) {
                Tuple primaryKey = recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, "doc p0-" + i, 1L, 10L + i)).getPrimaryKey();
                primaryKeys.put(i, primaryKey);
            }
            commit(context);
        }

        // Verify initial state.
        verifyPartitionCount(schemaSetup, index, groupingKey, List.of(5, 5));

        // Set merge indicators for BOTH partitions (shouldn't happen in real life)
        setOngoingMergeIndicator(schemaSetup, index, groupingKey, partition0);
        setOngoingMergeIndicator(schemaSetup, index, groupingKey, partition1);

        // Delete one document from each partition (will be queued)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(primaryKeys.get(2001L)); // Delete from partition 0
            recordStore.deleteRecord(primaryKeys.get(3001L)); // Delete from partition 1
            commit(context);
        }

        // Verify queues
        verifyExpectedQueueAndIndicator(schemaSetup, index, groupingKey, partition0,
                List.of(// LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));
        verifyExpectedQueueAndIndicator(schemaSetup, index, groupingKey, partition1,
                List.of(// LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));

        // Merge - drains both partition queues
        mergeIndexNow(schemaSetup, index);

        // Verify both queues cleared and indicators removed
        verifyClearedQueueAndIndicator(schemaSetup, index, groupingKey, partition0);
        verifyClearedQueueAndIndicator(schemaSetup, index, groupingKey, partition1);

        // Verify final document counts
        verifyPartitionCount(schemaSetup, index, groupingKey, List.of(4, 4));
    }

    @Test
    void testPendingQueueWithUpdate() {
        // Test update operation in pending queue
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Insert document before queue is enabled
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "original text", 1)
            );
            commit(context);
        }

        // Mark "ongoing merge" indicator
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // Update the document
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "updated text", 1)
            );
            commit(context);
        }

        // Verify INSERT and DELETE in queue
        verifyExpectedQueueAndIndicator(schemaSetup, index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT));

        // Merge and verify
        mergeIndexNow(schemaSetup, index);

        // Verify queue cleared
        verifyClearedQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void testPendingQueueMixedOperations() {
        // Test INSERT, UPDATE, DELETE sequence on same document
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Mark "ongoing merge" indicator
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // INSERT
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1001L, "version 1", 1));
            commit(context);
        }

        // UPDATE (generates DELETE + INSERT since doc was inserted while queue was active)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1001L, "version 2", 1));
            commit(context);
        }

        // DELETE
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(Tuple.from(1001L));
            commit(context);
        }

        // Verify INSERT, DELETE, INSERT, DELETE in queue
        verifyExpectedQueueAndIndicator(schemaSetup, index, null, null,
                List.of(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                        LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE));

        // Merge
        mergeIndexNow(schemaSetup, index);

        // Verify that the "ongoing merge" indicator is cleared
        verifyClearedQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void pendingQueueTestDrainException() {
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Enable queue
        setOngoingMergeIndicator(schemaSetup, index, null, null);

        // Set "ongoing merge" indicator and write corrupted queue entry
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Write invalid protobuf directly to queue subspace
            Subspace queueSubspace = recordStore.indexSubspace(index)
                    .subspace(Tuple.from(FDBDirectory.PENDING_WRITE_QUEUE_SUBSPACE));
            context.ensureActive().set(queueSubspace.pack(Tuple.from(0L)),
                    new byte[]{(byte)0xFF, (byte)0xFF}); // Corrupted protobuf

            commit(context);
        }

        // Merge attempts to drain corrupted queue and throws PendingQueueDrainException
        CompletionException thrownException = assertThrows(CompletionException.class, () ->
                mergeIndexNow(schemaSetup, index)
        );

        final FDBDirectoryWrapper.PendingQueueDrainException queueDrainException = TestHelpers.findCause(thrownException, FDBDirectoryWrapper.PendingQueueDrainException.class);
        assertNotNull(queueDrainException);
        assertThat(thrownException.getMessage(), containsString("Pending queue drain had failed"));
    }

    private void verifyClearedQueueAndIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
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

    private void verifyExpectedQueueAndIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId,
                                                 List<LucenePendingWriteQueueProto.PendingWriteItem.OperationType> expectedOperations) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
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

    private void verifyPartitionCount(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey,
                                      List<Integer> expectedCount) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
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

    private void verifyExpectedDocIds(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index,
                                      Set<Long> expectedDocIds) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Query all documents using wildcard search
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, "*:*", false);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                List<Long> primaryKeys = cursor
                        .map(IndexEntry::getPrimaryKey)
                        .map(tuple -> tuple.getLong(0))
                        .asList()
                        .join();

                assertEquals(expectedDocIds, new HashSet<>(primaryKeys));
            }
        }
    }

    private void setOngoingMergeIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Get directory and set ongoing merge indicator
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);
            directory.setOngoingMergeIndicator();
            commit(context);
        }
    }

    private void mergeIndexNow(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }
    }

    @Nonnull
    private static LuceneIndexMaintainer getIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer)store.getIndexMaintainer(index);
    }

    @Nonnull
    public static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private void assertQueueEntries(final PendingWriteQueue queue, final List<TestDocument> docs, LucenePendingWriteQueueProto.PendingWriteItem.OperationType operationType) {
        List<PendingWriteQueue.QueueEntry> entries;
        try (FDBRecordContext context = openContext()) {
            entries = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .asList().join();
        }

        // Ensure the documents read match the given docs
        Assertions.assertThat(entries).zipSatisfy(docs, (entry, doc) -> entryEquals(entry, doc, operationType));
    }

    private void entryEquals(PendingWriteQueue.QueueEntry queueEntry, TestDocument testDocument, LucenePendingWriteQueueProto.PendingWriteItem.OperationType operationType) {
        assertTrue(queueEntry.getVersionstamp().isComplete());
        assertTrue(queueEntry.getEnqueuedTimeStamp() > 0);
        assertEquals(testDocument.getPrimaryKey(), Tuple.fromBytes(queueEntry.getPrimaryKey()));
        assertEquals(operationType, queueEntry.getOperationType());
        if (operationType.equals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE)) {
            assertTrue(queueEntry.getDocumentFields().isEmpty());
        } else {
            Assertions.assertThat(queueEntry.getDocumentFields()).zipSatisfy(
                    testDocument.getFields(),
                    (entryField, docField) -> fieldEquals(entryField, docField));
        }
    }

    private void fieldEquals(LucenePendingWriteQueueProto.DocumentField entryField, LuceneDocumentFromRecord.DocumentField docField) {
        assertEquals(docField.getFieldName(), entryField.getFieldName());
        Object entryValue = assertType(entryField, docField.getType());
        assertEquals(docField.getValue(), entryValue);
        assertEquals(docField.isSorted(), entryField.getSorted());
        assertEquals(docField.isStored(), entryField.getStored());
        Map<String, Object> entryFieldConfigs = convertToObjects(entryField.getFieldConfigsMap());
        assertEquals(docField.getFieldConfigs(), entryFieldConfigs);
    }

    private Map<String, Object> convertToObjects(Map<String, LucenePendingWriteQueueProto.FieldConfigValue> fieldConfigsMap) {
        Map<String, Object> result = new HashMap<>(fieldConfigsMap.size());
        fieldConfigsMap.forEach((key, value) -> {
            switch (value.getValueCase()) {
                case INT_VALUE:
                    result.put(key, value.getIntValue());
                    break;
                case BOOLEAN_VALUE:
                    result.put(key, value.getBooleanValue());
                    break;
                case STRING_VALUE:
                    result.put(key, value.getStringValue());
                    break;
                default:
                    fail("Unknown type");
            }
        });
        return result;
    }

    private Object assertType(LucenePendingWriteQueueProto.DocumentField entryField, LuceneIndexExpressions.DocumentFieldType type) {
        switch (type) {
            case INT:
                assertTrue(entryField.hasIntValue());
                return entryField.getIntValue();
            case LONG:
                assertTrue(entryField.hasLongValue());
                return entryField.getLongValue();
            case DOUBLE:
                assertTrue(entryField.hasDoubleValue());
                return entryField.getDoubleValue();
            case BOOLEAN:
                assertTrue(entryField.hasBooleanValue());
                return entryField.getBooleanValue();
            case STRING:
                assertTrue(entryField.hasStringValue());
                return entryField.getStringValue();
            case TEXT:
                assertTrue(entryField.hasTextValue());
                return entryField.getTextValue();
            default:
                fail("Unknown type");
        }
        return null;
    }

    private List<TestDocument> createTestDocuments() {
        TestDocument docWithNoFields = new TestDocument(primaryKey("No Fields"),
                Collections.emptyList());

        TestDocument docWithOneFields = new TestDocument(primaryKey("One Field"),
                List.of(createField("f0", 5, LuceneIndexExpressions.DocumentFieldType.INT, true, true)));

        TestDocument docWithMultipleFields = new TestDocument(primaryKey("Multiple Fields"),
                List.of(
                        createField("f1", 5, LuceneIndexExpressions.DocumentFieldType.INT, true, true),
                        createField("f2", "Hello", LuceneIndexExpressions.DocumentFieldType.STRING, false, false),
                        createField("f3", true, LuceneIndexExpressions.DocumentFieldType.BOOLEAN, true, false)));

        Map<String, Object> m1 = Map.of("1", 1);
        Map<String, Object> m2 = Map.of("2", 2, "str", "str");
        Map<String, Object> m3 = Map.of("3", 3, "string", "string", "bool", true);
        TestDocument docWithAllFieldTypes = new TestDocument(primaryKey("Many Fields"),
                List.of(
                        createField("int field", 5, LuceneIndexExpressions.DocumentFieldType.INT, true, true, m1),
                        createField("str field", "Hello", LuceneIndexExpressions.DocumentFieldType.STRING, false, false, m2),
                        createField("bool field", true, LuceneIndexExpressions.DocumentFieldType.BOOLEAN, true, false, m3),
                        createField("text field", "some text", LuceneIndexExpressions.DocumentFieldType.TEXT, false, true, m3),
                        createField("long field", 6L, LuceneIndexExpressions.DocumentFieldType.LONG, true, false),
                        createField("double field", 3.14D, LuceneIndexExpressions.DocumentFieldType.DOUBLE, true, true)));

        return List.of(docWithNoFields, docWithOneFields, docWithMultipleFields, docWithAllFieldTypes);
    }

    @Nonnull
    private static Tuple primaryKey(String text) {
        return Tuple.from(text, System.nanoTime());
    }

    private LuceneDocumentFromRecord.DocumentField createField(
            String fieldName, Object fieldValue, LuceneIndexExpressions.DocumentFieldType fieldType,
            boolean stored, boolean sorted) {
        return createField(fieldName, fieldValue, fieldType, stored, sorted, Collections.emptyMap());
    }

    private LuceneDocumentFromRecord.DocumentField createField(
            String fieldName, Object fieldValue, LuceneIndexExpressions.DocumentFieldType fieldType,
            boolean stored, boolean sorted, Map<String, Object> fieldConfigs) {

        return new LuceneDocumentFromRecord.DocumentField(
                fieldName, fieldValue, fieldType,
                stored, sorted, fieldConfigs);
    }

    private static class TestDocument {
        private final Tuple primaKey;
        private final List<LuceneDocumentFromRecord.DocumentField> fields;

        private TestDocument(Tuple primaKey, final List<LuceneDocumentFromRecord.DocumentField> fields) {
            this.primaKey = primaKey;
            this.fields = fields;
        }

        public Tuple getPrimaryKey() {
            return primaKey;
        }

        public List<LuceneDocumentFromRecord.DocumentField> getFields() {
            return fields;
        }
    }
}
