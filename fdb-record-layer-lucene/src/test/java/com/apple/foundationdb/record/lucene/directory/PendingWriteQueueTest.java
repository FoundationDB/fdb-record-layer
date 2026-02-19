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
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.Streams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link PendingWriteQueue} in isolation.
 * These tests assert the functionality of the PendingWriteQueue with no other system components in play.
 */
@Tag(Tags.RequiresFDB)
class PendingWriteQueueTest extends FDBRecordStoreTestBase {
    LuceneSerializer serializer;

    @BeforeEach
    void setup() {
        serializer = new LuceneSerializer(true, false, null, true);
    }
    @ParameterizedTest
    @EnumSource
    void testEnqueueAndIterate(LucenePendingWriteQueueProto.PendingWriteItem.OperationType operationType) {
        // don't deal with the "unspecified" operation type
        Assumptions.assumeFalse(operationType.equals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.OPERATION_TYPE_UNSPECIFIED));

        List<TestDocument> docs = createTestDocuments();

        PendingWriteQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            docs.forEach(doc -> {
                switch (operationType) {
                    case INSERT:
                        queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
                        break;
                    case DELETE:
                        queue.enqueueDelete(context, doc.getPrimaryKey());
                        break;
                    case OPERATION_TYPE_UNSPECIFIED:
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
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
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
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
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
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
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
        PendingWriteQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
        }

        assertQueueEntries(queue, Collections.emptyList(), LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT);
    }

    @Test
    void testWrongValueType() {
        final LuceneDocumentFromRecord.DocumentField fieldWithWrongType =
                createField("f", 5, LuceneIndexExpressions.DocumentFieldType.STRING, true, true);

        try (FDBRecordContext context = openContext()) {
            PendingWriteQueue queue = getQueue(context);
            Assertions.assertThatThrownBy(() -> queue.enqueueInsert(context, Tuple.from(1), List.of(fieldWithWrongType)))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Test
    void testUnsupportedFieldConfigType() {
        final LuceneDocumentFromRecord.DocumentField fieldWithWrongConfig =
                createField("f", 5, LuceneIndexExpressions.DocumentFieldType.INT, true, true, Map.of("Double", 5.42D));

        try (FDBRecordContext context = openContext()) {
            PendingWriteQueue queue = getQueue(context);
            Assertions.assertThatThrownBy(() -> queue.enqueueInsert(context, Tuple.from(1), List.of(fieldWithWrongConfig)))
                    .isInstanceOf(RecordCoreArgumentException.class);
        }
    }

    @Test
    void testIterateWithContinuations() {
        List<TestDocument> docs = createTestDocuments();
        List<TestDocument> moreDocs = createTestDocuments();

        PendingWriteQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
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
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
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
    void testFailToSerialize() {
        List<TestDocument> docs = createTestDocuments();
        LuceneSerializer failingSerializer = new FailingLuceneSerializer();
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, failingSerializer);
            final TestDocument doc = docs.get(0);
            Assertions.assertThatThrownBy(() -> queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields()))
                    .isInstanceOf(RecordCoreInternalException.class)
                    .hasMessageContaining("Failing to encode");

            // Commit here should do nothing as the queue should still be empty
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(queue.isQueueEmpty(context).join(), "Expected isQueueEmpty to return true");
            commit(context);
        }
    }

    @Test
    void testFailToDeserialize() {
        List<TestDocument> docs = createTestDocuments();
        LuceneSerializer failingSerializer = new FailingLuceneSerializer();
        PendingWriteQueue queue;
        PendingWriteQueue failingQueue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            failingQueue = getQueue(context, failingSerializer);
            final TestDocument doc = docs.get(0);
            // save a single doc using the good queue
            queue.enqueueInsert(context, doc.getPrimaryKey(), doc.getFields());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            RecordCursor<PendingWriteQueue.QueueEntry> queueCursor = failingQueue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null);
            Assertions.assertThatThrownBy(() -> queueCursor.asList().get())
                    .hasCauseInstanceOf(RecordCoreInternalException.class)
                    .hasMessageContaining("Failing to decode");
        }
    }

    @Test
    void testQueueItemTooLarge() throws Exception {
        StringBuilder builder = new StringBuilder();
        for (int i = 0 ; i < 100_000 ; i++) {
            builder.append("Hello ");
        }
        String hugeString = builder.toString();
        TestDocument docWithHugeString = new TestDocument(primaryKey("Huge"),
                List.of(createField("f2", hugeString.toString(), LuceneIndexExpressions.DocumentFieldType.STRING, false, false)));

        LuceneSerializer passThroughSerializer = new PassThroughLuceneSerializer();
        PendingWriteQueue queue;

        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            // save a single doc using the good queue (should succeed since the serializer compresses the data)
            queue.enqueueInsert(context, docWithHugeString.getPrimaryKey(), docWithHugeString.getFields());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            RecordCursor<PendingWriteQueue.QueueEntry> queueCursor = queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null);
            List<PendingWriteQueue.QueueEntry> list = queueCursor.asList().get();
            assertEquals(1, list.size());
            assertEquals(hugeString, list.get(0).getDocumentFields().get(0).getStringValue());
        }

        try (FDBRecordContext context = openContext()) {
            PendingWriteQueue failingQueue = getQueue(context, passThroughSerializer);
            // save a single doc using the bad serializer (should fail as the entry will be too large)
            failingQueue.enqueueInsert(context, docWithHugeString.getPrimaryKey(), docWithHugeString.getFields());
            Assertions.assertThatThrownBy(() -> commit(context))
                    .isInstanceOf(FDBExceptions.FDBStoreValueSizeException.class);
        }
    }

    private PendingWriteQueue getQueue(FDBRecordContext context) {
        return getQueue(context, serializer);
    }

    private PendingWriteQueue getQueue(FDBRecordContext context, LuceneSerializer serializer) {
        Subspace queueSpace = path.toSubspace(context).subspace(Tuple.from(0));
        Subspace counterSpace = path.toSubspace(context).subspace(Tuple.from(1));
        return new PendingWriteQueue(queueSpace, counterSpace, serializer);
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
        assertEquals(testDocument.getPrimaryKey(), queueEntry.getPrimaryKeyParsed());
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

    private static class FailingLuceneSerializer extends LuceneSerializer {
        public FailingLuceneSerializer() {
            super(true, false, null, true);
        }

        @Nullable
        @Override
        public byte[] encode(@Nullable final byte[] data) {
            throw new RecordCoreInternalException("Failing to encode");
        }

        @Nullable
        @Override
        public byte[] decode(@Nullable final byte[] data) {
            throw new RecordCoreInternalException("Failing to decode");
        }
    }

    private static class PassThroughLuceneSerializer extends LuceneSerializer {
        public PassThroughLuceneSerializer() {
            super(true, false, null, true);
        }

        @Nullable
        @Override
        public byte[] encode(@Nullable final byte[] data) {
            return data;
        }

        @Nullable
        @Override
        public byte[] decode(@Nullable final byte[] data) {
            return data;
        }
    }
}
