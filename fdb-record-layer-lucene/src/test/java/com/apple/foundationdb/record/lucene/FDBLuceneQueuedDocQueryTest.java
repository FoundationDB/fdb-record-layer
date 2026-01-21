/*
 * FDBLuceneQueuedDocQueryTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryWrapper;
import com.apple.foundationdb.record.lucene.directory.PendingWriteQueue;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag(Tags.RequiresFDB)
public class FDBLuceneQueuedDocQueryTest extends FDBRecordStoreTestBase {

    private static final List<TestRecordsTextProto.SimpleDocument> DOCUMENTS = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
            TextSamples.ANGSTROM,
            TextSamples.AETHELRED,
            TextSamples.PARTIAL_ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.FRENCH,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE_END
    ));

    @Test
    void scanAllDocsInQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        enqueueInsertAllDocs(index);

        // Query for all from the queue
        scanAndCompareDocs(index, "*:*", Set.of(0L, 1L, 2L, 3L, 4L, 5L));

        // ensure nothing gets committed to the index
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "*:*", Set.of());
    }

    @Test
    void scanSomeDocsInQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        enqueueInsertAllDocs(index);

        // Query for some docs in the queue
        scanAndCompareDocs(index, "text:households", Set.of(2L, 4L));

        // ensure nothing gets committed to the index
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "*:*", Set.of());
    }

    @Test
    void scanAllDocsInQueueAndIndex() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        saveSomeDocsInQueueAndSomeInIndex(index);

        // Query for all docs - mixed in queue and index
        scanAndCompareDocs(index, "*:*", Set.of(0L, 1L, 2L, 3L, 4L, 5L));

        // ensure nothing gets committed to the index (other than the original docs)
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "*:*", Set.of(3L, 4L, 5L));
    }

    @Test
    void saveToIndexAndDeleteSomeDocsFromQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        saveAllDocsInIndex(index);
        enqueueDeleteSomeDocs(index);

        // We deleted doc #2 in the queue, so only #4 remain to be queried
        scanAndCompareDocs(index, "text:households", Set.of(4L));

        // ensure nothing gets committed to the index (other than the original docs)
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "*:*", Set.of(0L, 1L, 2L, 3L, 4L, 5L));
    }

    @Test
    void saveToIndexAndDeleteAllDocsFromQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        saveAllDocsInIndex(index);
        enqueueDeleteAllDocs(index);

        // We deleted all docs in the queue, none can be queries
        scanAndCompareDocs(index, "*:*", Set.of());

        // ensure nothing gets committed to the index (other than the original docs)
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "*:*", Set.of(0L, 1L, 2L, 3L, 4L, 5L));
    }

    @Test
    void saveToIndexAndUpdateSomeDocsInQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        saveAllDocsInIndex(index);
        enqueueUpdateDoc(index, 1, 2);

        // We updated doc #1 in the queue to have same text as #2, so should match query
        scanAndCompareDocs(index, "text:households", Set.of(1L, 2L, 4L));
        // And the original value in doc #1 should be gone
        scanAndCompareDocs(index, "text:Æthelred", Set.of());

        // ensure nothing gets committed to the index (other than the original docs)
        clearAllDocsFromQueue(index);
        scanAndCompareDocs(index, "text:households", Set.of(2L, 4L));
    }

    @Test
    void saveToIndexAndUpdateInQueueMultipleTimes() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        saveAllDocsInIndex(index);
        enqueueUpdateDoc(index, 1, 2);
        // restore the original text for the doc
        enqueueUpdateDoc(index, 1, 1);

        // We restored doc #1 in the queue to have the original text
        scanAndCompareDocs(index, "text:households", Set.of(2L, 4L));
        // And the original value in doc #1 should be there
        scanAndCompareDocs(index, "text:Æthelred", Set.of(1L));
    }

    @Test
    void saveToQueueAndEmptyQueue() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES;
        enqueueInsertAllDocs(index);
        clearAllDocsFromQueue(index);

        // Since we emptied the queue there should be no docs to find
        scanAndCompareDocs(index, "*:*", Set.of());
    }

    @Test
    void tooManyPendingWritesInQueue() throws Exception {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_PENDING_WRITES_REPLAYED_FOR_QUERY, 1)
                .build();
        Index index = SIMPLE_TEXT_SUFFIXES;
        enqueueInsertAllDocs(index);

        try (FDBRecordContext context = openContext(contextProps)) {
            openRecordStore(context, index);
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, "*:*", false);
            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, scanProperties)) {
                assertThatThrownBy(() -> cursor.asList().get()).hasCauseInstanceOf(PendingWriteQueue.TooManyPendingWritesException.class);
            }
            commit(context);
        }
    }

    private void enqueueInsertAllDocs(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            DOCUMENTS.forEach(doc -> enqueueInsert(context, queue, doc));
            commit(context);
        }
    }

    private void enqueueDeleteAllDocs(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            DOCUMENTS.forEach(doc -> enqueueDelete(context, queue, doc));
            commit(context);
        }
    }

    private void enqueueDeleteSomeDocs(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            enqueueDelete(context, queue, DOCUMENTS.get(2));
            commit(context);
        }
    }

    private void enqueueUpdateDoc(Index index, int docToUpdate, int textToUse) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            enqueueUpdate(context, queue, DOCUMENTS.get(docToUpdate).getDocId(), DOCUMENTS.get(textToUse).getText());
            commit(context);
        }
    }

    private void saveAllDocsInIndex(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            DOCUMENTS.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void saveSomeDocsInQueueAndSomeInIndex(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            DOCUMENTS.subList(0, 3).forEach(doc -> enqueueInsert(context, queue, doc));
            DOCUMENTS.subList(3, 6).forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void clearAllDocsFromQueue(Index index) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            PendingWriteQueue queue = getPendingWriteQueue(recordStore, index);
            emptyQueue(context, queue);
            commit(context);
        }
    }

    private void scanAndCompareDocs(Index index, String searchTerm, Set<Long> expectedPKs) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, index);
            LuceneScanBounds scanBounds = LuceneIndexTestUtils.fullTextSearch(recordStore, index, searchTerm, false);
            ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, scanBounds, null, scanProperties)) {
                final List<IndexEntry> indexEntries = cursor.asList().get();
                final Set<Long> actualKeys = indexEntries.stream().map(entry -> entry.getKey().getLong(1)).collect(Collectors.toSet());
                assertThat(actualKeys).isEqualTo(expectedPKs);
            }
            commit(context);
        }
    }

    private void enqueueInsert(FDBRecordContext context, PendingWriteQueue queue, TestRecordsTextProto.SimpleDocument doc) {
        List<LuceneDocumentFromRecord.DocumentField> fields = toDocumentFields(doc);
        queue.enqueueInsert(context, Tuple.from(doc.getDocId()), fields);
    }

    private void enqueueUpdate(FDBRecordContext context, PendingWriteQueue queue, long docId, String text) {
        List<LuceneDocumentFromRecord.DocumentField> fields = toDocumentFields(text);
        queue.enqueueUpdate(context, Tuple.from(docId), fields);
    }

    private void enqueueDelete(FDBRecordContext context, PendingWriteQueue queue, TestRecordsTextProto.SimpleDocument doc) {
        queue.enqueueDelete(context, Tuple.from(doc.getDocId()));
    }

    private void emptyQueue(FDBRecordContext context, PendingWriteQueue queue) {
        queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                .forEach(entry -> queue.clearEntry(context, entry)).join();
    }

    private List<LuceneDocumentFromRecord.DocumentField> toDocumentFields(TestRecordsTextProto.SimpleDocument doc) {
        // Simulate the document field creation for the simple doc and simple index
        LuceneDocumentFromRecord.DocumentField field = new LuceneDocumentFromRecord.DocumentField("text", doc.getText(), LuceneIndexExpressions.DocumentFieldType.TEXT, true, false, Map.of());
        return List.of(field);
    }

    private List<LuceneDocumentFromRecord.DocumentField> toDocumentFields(String text) {
        // Simulate the document field creation for the simple text
        LuceneDocumentFromRecord.DocumentField field = new LuceneDocumentFromRecord.DocumentField("text", text, LuceneIndexExpressions.DocumentFieldType.TEXT, true, false, Map.of());
        return List.of(field);
    }

    private LuceneIndexMaintainer getIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer)store.getIndexMaintainer(index);
    }

    private static FDBDirectoryWrapper getDirectoryWrapper(final LuceneIndexMaintainer indexMaintainer) {
        Tuple groupingKey = null;
        Integer partitionId = null;
        return indexMaintainer.getDirectoryManager().getDirectoryWrapper(groupingKey, partitionId);
    }

    private PendingWriteQueue getPendingWriteQueue(FDBRecordStore store, Index index) {
        LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(store, index);
        final FDBDirectoryWrapper directoryWrapper = getDirectoryWrapper(indexMaintainer);
        return directoryWrapper.getPendingWriteQueue();
    }

    protected void openRecordStore(FDBRecordContext context, Index index) {
        recordStore = LuceneIndexTestUtils.openRecordStore(context, path, mdb -> {
            if (index != null) {
                mdb.removeIndex("SimpleDocument$text");
                mdb.addIndex(TextIndexTestUtils.SIMPLE_DOC, index);
            }
        });
    }
}
