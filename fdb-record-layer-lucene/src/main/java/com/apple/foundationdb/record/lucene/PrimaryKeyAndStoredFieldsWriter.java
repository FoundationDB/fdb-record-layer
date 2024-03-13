/*
 * PrimaryKeyAndStoredFieldsWriter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Accountable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

class PrimaryKeyAndStoredFieldsWriter extends StoredFieldsWriter {
    private final LucenePrimaryKeySegmentIndexV2 lucenePrimaryKeySegmentIndexV2;
    @Nonnull
    private final StoredFieldsWriter inner;
    private final long segmentId;
    @Nonnull
    private final FDBDirectory directory;

    private int documentId;

    PrimaryKeyAndStoredFieldsWriter(final LucenePrimaryKeySegmentIndexV2 lucenePrimaryKeySegmentIndexV2,
                                    @Nonnull StoredFieldsWriter inner, long segmentId,
                                    @Nonnull final FDBDirectory directory) {
        this.lucenePrimaryKeySegmentIndexV2 = lucenePrimaryKeySegmentIndexV2;
        this.inner = inner;
        this.segmentId = segmentId;
        this.directory = directory;
    }

    @Override
    public void startDocument() throws IOException {
        inner.startDocument();
    }

    @Override
    public void finishDocument() throws IOException {
        inner.finishDocument();
        documentId++;
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
        inner.writeField(info, field);
        if (info.name.equals(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME)) {
            final byte[] primaryKey = field.binaryValue().bytes;
            lucenePrimaryKeySegmentIndexV2.addOrDeletePrimaryKeyEntry(primaryKey, segmentId, documentId, true);
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public int merge(MergeState mergeState) throws IOException {
        final int docCount = inner.merge(mergeState);

        final int segmentCount = mergeState.storedFieldsReaders.length;
        /* TODO implement merge
        final LucenePrimaryKeySegmentIndexV2.PrimaryKeyVisitor visitor = new LucenePrimaryKeySegmentIndexV2.PrimaryKeyVisitor();
        for (int i = 0; i < segmentCount; i++) {
            final StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[i];
            final SegmentInfo mergedSegmentInfo = ((LucenePrimaryKeySegmentIndexV2.StoredFieldsReaderSegmentInfo)storedFieldsReader).getSegmentInfo();
            final long mergedSegmentId = directory.primaryKeySegmentId(mergedSegmentInfo.name, false);
            final Bits liveDocs = mergeState.liveDocs[i];
            final MergeState.DocMap docMap = mergeState.docMaps[i];
            final int maxDoc = mergeState.maxDocs[i];
            for (int j = 0; j < maxDoc; j++) {
                storedFieldsReader.visitDocument(j, visitor);
                final byte[] primaryKey = visitor.getPrimaryKey();
                if (primaryKey != null) {
                    if (liveDocs == null || liveDocs.get(j)) {
                        int docId = docMap.get(j);
                        if (docId >= 0) {
                            lucenePrimaryKeySegmentIndexV2.addOrDeletePrimaryKeyEntry(primaryKey, segmentId, docId, true);
                        }
                    }
                    // Deleting the index entry at worst triggers a fallback to search.
                    // Ordinarily, though, transaction isolation means that the entry is there along with the pre-merge segment.
                    lucenePrimaryKeySegmentIndexV2.addOrDeletePrimaryKeyEntry(primaryKey, mergedSegmentId, j, false);
                    visitor.reset();
                }
            }
        }
        */

        directory.getAgilityContext().increment(LuceneEvents.Counts.LUCENE_MERGE_DOCUMENTS, docCount);
        directory.getAgilityContext().increment(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS, segmentCount);

        return docCount;
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
        inner.finish(fis, numDocs);
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public long ramBytesUsed() {
        return inner.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return inner.getChildResources();
    }
}
