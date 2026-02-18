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
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test end-to-end serialization and deserialization through the pending write queue.
 * Verifies that records written through the queue are identical to those written directly.
 */
@Tag(Tags.RequiresFDB)
public class PendingWriteQueueSerializationTest extends FDBRecordStoreTestBase {

    @Test
    void testEndToEndSerialization() throws InvalidProtocolBufferException {
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        final long directRecordId = 1001L;
        final long queuedRecordId = 2002L;
        final String textContent = "The quick brown fox jumps over the lazy dog";

        // Write record directly to index (no queue)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(directRecordId, textContent, 1));
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
        assertQueryFindsRecords(schemaSetup, index, textContent, directRecordId, queuedRecordId);

        // Drain the queue via merge
        mergeIndex(schemaSetup, index);

        // Verify both records still queryable: both now from index
        assertQueryFindsRecords(schemaSetup, index, textContent, directRecordId, queuedRecordId);

        // Verify records are identical field by field
        assertRecordsIdenticalExceptIds(schemaSetup, directRecordId, queuedRecordId, textContent);
    }

    private void assertRecordsIdenticalExceptIds(Function<FDBRecordContext, FDBRecordStore> schemaSetup,
                                                 long recordId1, long recordId2, String expectedText) throws InvalidProtocolBufferException {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Load both records from FDB
            var storedRecord1 = recordStore.loadRecord(Tuple.from(recordId1));
            var storedRecord2 = recordStore.loadRecord(Tuple.from(recordId2));

            assertNotNull(storedRecord1, "Record " + recordId1 + " should exist");
            assertNotNull(storedRecord2, "Record " + recordId2 + " should exist");

            // Parse as SimpleDocument
            TestRecordsTextProto.SimpleDocument doc1 =
                    TestRecordsTextProto.SimpleDocument.parseFrom(storedRecord1.getRecord().toByteArray());
            TestRecordsTextProto.SimpleDocument doc2 =
                    TestRecordsTextProto.SimpleDocument.parseFrom(storedRecord2.getRecord().toByteArray());

            // Compare field by field (excluding doc_id and timestamp)
            assertEquals(doc1.getText(), doc2.getText(), "text field should be identical");
            assertEquals(expectedText, doc2.getText(), "text should match expected value");

            assertEquals(doc1.getGroup(), doc2.getGroup(), "group field should be identical");

            // Verify doc IDs are different as expected
            assertEquals(recordId1, doc1.getDocId(), "doc_id should match first record ID");
            assertEquals(recordId2, doc2.getDocId(), "doc_id should match second record ID");

            commit(context);
        }
    }

    private void assertQueryFindsRecords(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index,
                                         String fullText, long... expectedDocIds) {
        for (String searchTerm: fullText.split(" ")) {
            if (searchTerm.compareToIgnoreCase("the") >= 0) {
                // not indexed
                continue;
            }

            try (FDBRecordContext context = openContext()) {
                FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

                LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, searchTerm, false);

                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                    List<Long> actualDocIds = cursor
                            .map(IndexEntry::getPrimaryKey)
                            .map(tuple -> tuple.getLong(0))
                            .asList()
                            .join();

                    HashSet<Long> expected = new HashSet<>();
                    for (long id : expectedDocIds) {
                        expected.add(id);
                    }

                    assertEquals(expected, new HashSet<>(actualDocIds),
                            "Search for '" + searchTerm + "' should find both records");
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
            LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }
    }

    @Nonnull
    private static LuceneIndexMaintainer getIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer) store.getIndexMaintainer(index);
    }
}
