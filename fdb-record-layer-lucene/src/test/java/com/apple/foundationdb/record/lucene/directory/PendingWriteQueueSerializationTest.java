/*
 * PendingWriteQueueSerializationTest.java
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
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneRecordCursor;
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Test end-to-end serialization and deserialization through the pending write queue.
 * Verifies that records written through the queue are identical to those written directly.
 */
@Tag(Tags.RequiresFDB)
class PendingWriteQueueSerializationTest extends FDBRecordStoreTestBase {

    @ParameterizedTest
    @BooleanSource
    void testEndToEndSerialization(boolean overwrite) {
        // Compare two identical records - one of them was written through the pending write queue
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        final long directRecordId = 1001L;
        final long queuedRecordId = 2002L;
        final String textContent = "The quick brown fox jumps over the lazy dog";
        final String tempTextContent = "The slow blue rabbit crawled under the hard-working donkey";
        final List<String> searchTerms = List.of("text:quick", "text:lazy", "*:*");

        // Write record directly to index (no queue)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(directRecordId, textContent, 1));
            if (overwrite) {
                recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(queuedRecordId, tempTextContent, 1));
            }
            commit(context);
        }

        // Enable pending queue mode
        setOngoingMergeIndicator(schemaSetup, index);

        // Write identical record through queue
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(queuedRecordId, textContent, 1));
            commit(context);
        }

        // Verify both records queryable: direct from index + replayed from queue
        assertQueryFindsRecords(schemaSetup, index, searchTerms, List.of(directRecordId, queuedRecordId), false);

        // Drain the queue via merge
        mergeIndex(schemaSetup, index);

        // Verify both records still queryable: both now from index
        assertQueryFindsRecords(schemaSetup, index, searchTerms, List.of(directRecordId, queuedRecordId), false);

        // Verify that the records are identical (sort of)
        assertRecordsIdenticalExceptIds(schemaSetup, index);
    }

    @ParameterizedTest
    @BooleanSource
    void testEndToEndSerializationComplex(boolean overwrite) {
        // compare two identical records - one of them was written through the pending write queue, but use complex index
        final Index index = TEXT_AND_STORED_COMPLEX;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path, COMPLEX_DOC, index, useCascadesPlanner).getLeft();

        final long directRecordId = 1001L;
        final long queuedRecordId = 2002L;
        final String text = "The quick brown fox jumps over the lazy dog";
        final String text2 = "inbox message";
        final long group = 5L;
        final int score = 42;
        final boolean isSeen = false;
        final double time = 123.456;
        final List<String> searchTerms = List.of("score:42", "text:quick", "text:dog", "time:123.456", "is_seen:false", "group:5", "*:*");

        // Write record directly to index (no queue)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                    directRecordId, text, text2, group, score, isSeen, time));
            if (overwrite) {
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                        queuedRecordId, "green eggs and ham", "old text two", group, 24, true, 654.321));
            }
            commit(context);
        }

        // Enable pending queue mode
        setOngoingMergeIndicator(schemaSetup, index);

        // Write identical record through queue
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                    queuedRecordId, text, text2, group, score, isSeen, time));
            commit(context);
        }

        // Verify both records queryable: direct from index + replayed from queue
        assertQueryFindsRecords(schemaSetup, index, searchTerms, List.of(directRecordId, queuedRecordId), true);

        // Drain the queue via merge
        mergeIndex(schemaSetup, index);

        // Verify both records still queryable: both now from index
        assertQueryFindsRecords(schemaSetup, index, searchTerms, List.of(directRecordId, queuedRecordId), true);

        // Verify that the records are identical (sort of)
        assertComplexRecordsIdenticalExceptIds(schemaSetup);
    }

    private void assertComplexRecordsIdenticalExceptIds(Function<FDBRecordContext, FDBRecordStore> schemaSetup) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, TEXT_AND_STORED_COMPLEX, "*:*", false);

            // Get all index entries from Lucene
            List<IndexEntry> entries;
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                entries = cursor.asList().join();
            }

            // Should have exactly 2 entries
            assertEquals(2, entries.size());

            LuceneRecordCursor.ScoreDocIndexEntry entry1 = (LuceneRecordCursor.ScoreDocIndexEntry) entries.get(0);
            LuceneRecordCursor.ScoreDocIndexEntry entry2 = (LuceneRecordCursor.ScoreDocIndexEntry) entries.get(1);

            // ComplexDocument primary key is (group, doc_id)
            Tuple primaryKey1 = entry1.getPrimaryKey();
            Tuple primaryKey2 = entry2.getPrimaryKey();

            long group1 = primaryKey1.getLong(0);
            long group2 = primaryKey2.getLong(0);
            long docId1 = primaryKey1.getLong(1);
            long docId2 = primaryKey2.getLong(1);

            // Verify group values match between both entries
            assertEquals(group1, group2, "Group fields should match");

            // Verify doc IDs are different
            assertNotEquals(docId1, docId2, "Document IDs should be different");

            // Compare index key elements to verify stored fields are identical
            Tuple key1 = entry1.getKey();
            Tuple key2 = entry2.getKey();

            assertEquals(key1.size(), key2.size(), "Key tuples should have same size");

            // Count positions where the keys differ
            int differenceCount = 0;
            List<Integer> differingPositions = new java.util.ArrayList<>();

            for (int i = 0; i < key1.size(); i++) {
                Object obj1 = key1.get(i);
                Object obj2 = key2.get(i);

                if (!Objects.equals(obj1, obj2)) {
                    differenceCount++;
                    differingPositions.add(i);
                }
            }

            // The keys should differ in exactly one position (the doc_id component)
            // This verifies that all stored fields (text2, group, score, time, is_seen)
            // were correctly serialized and deserialized through the pending write queue
            assertEquals(1, differenceCount,
                    "Keys should differ in exactly one position (doc_id). Differing positions: " + differingPositions);

            commit(context);
        }
    }

    private void assertRecordsIdenticalExceptIds(Function<FDBRecordContext, FDBRecordStore> schemaSetup,
                                                 Index index) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, "*:*", false);

            // Get all index entries from Lucene
            List<IndexEntry> entries;
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                entries = cursor.asList().join();
            }

            // Should have exactly 2 entries
            assertEquals(2, entries.size());

            // Extract primary keys (doc_ids) from both entries
            Tuple primaryKey1 = entries.get(0).getPrimaryKey();
            Tuple primaryKey2 = entries.get(1).getPrimaryKey();

            long docId1 = primaryKey1.getLong(0);
            long docId2 = primaryKey2.getLong(0);

            // Verify doc IDs are different
            assertNotEquals(docId1, docId2);

            commit(context);
        }
    }

    private void assertQueryFindsRecords(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index,
                                         List<String> searchTerms, List<Long> expectedDocIds, boolean isComplex) {
        for (String searchTerm: searchTerms) {
            try (FDBRecordContext context = openContext()) {
                FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
                LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, searchTerm, false);
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                    List<Long> actualDocIds = cursor
                            .map(IndexEntry::getPrimaryKey)
                            .map(tuple -> tuple.getLong(isComplex ? 1 : 0))
                            .asList()
                            .join();

                    HashSet<Long> expected = new HashSet<>(expectedDocIds);
                    assertEquals(expected, new HashSet<>(actualDocIds), "failed for: " + searchTerm);
                }
                commit(context);
            }
        }
    }

    private void setOngoingMergeIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index) {
        setOngoingMergeIndicator(schemaSetup, index, null, null);
    }

    private void setOngoingMergeIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index,
                                          @Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);
            directory.setOngoingMergeIndicator();
            commit(context);
        }
    }

    private void mergeIndex(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            LuceneIndexMaintainer indexMaintainer = getLuceneIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }
    }

    @Nonnull
    private static LuceneIndexMaintainer getLuceneIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer) store.getIndexMaintainer(index);
    }
}
