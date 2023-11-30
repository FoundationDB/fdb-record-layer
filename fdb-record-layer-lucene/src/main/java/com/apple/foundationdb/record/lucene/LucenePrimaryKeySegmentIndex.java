/*
 * LucenePrimaryKeySegmentIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Maintain a B-tree index of primary key to segment and doc id.
 * This allows for efficient deleting of a document given that key, such as when doing index maintenance from an update.
 */
public class LucenePrimaryKeySegmentIndex {
    @Nonnull
    private final FDBDirectory directory;
    @Nonnull
    private final Subspace subspace;

    public LucenePrimaryKeySegmentIndex(@Nonnull FDBDirectory directory, @Nonnull Subspace subspace) {
        this.directory = directory;
        this.subspace = subspace;
    }

    /**
     * Get all stored primary key index entries.
     * Really only useful for small-scale debugging.
     * @return a list of Tuple-decoded key entries
     */
    @VisibleForTesting
    public List<List<Object>> readAllEntries() {
        List<Tuple> tuples;
        try (KeyValueCursor kvs = KeyValueCursor.Builder.newBuilder(subspace)
                .setContext(directory.getContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
                RecordCursor<Tuple> entries = kvs.map(kv -> subspace.unpack(kv.getKey()))) {
            tuples = directory.getContext().asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY, entries.asList());
        }
        return tuples.stream().map(t -> {
            List<Object> items = t.getItems();
            // Replace segment id with segment name to make inspection easier.
            String name = directory.primaryKeySegmentName((Long)items.get(items.size() - 2));
            if (name != null) {
                items.set(items.size() - 2, name);
            }
            items.set(items.size() - 1, ((Long)items.get(items.size() - 1)).intValue());
            return items;
        }).collect(Collectors.toList());
    }

    /**
     * Result of {@link #findDocument}.
     */
    // TODO: Can be a record.
    public static class DocumentIndexEntry {
        @Nonnull
        public final Tuple primaryKey;
        @Nonnull
        public final byte[] entryKey;
        @Nonnull
        public final IndexReader indexReader;
        public final int docId;

        public DocumentIndexEntry(@Nonnull final Tuple primaryKey, @Nonnull final byte[] entryKey, @Nonnull final IndexReader indexReader, final int docId) {
            this.primaryKey = primaryKey;
            this.entryKey = entryKey;
            this.indexReader = indexReader;
            this.docId = docId;
        }
    }

    /**
     * Find document in index for direct delete.
     * @param directoryReader a NRT reader
     * @param primaryKey the document's record's primary key
     * @return an entry with the leaf reader and document id in that segment or {@code null} if not found
     * @see IndexWriter#tryDeleteDocument
     */
    @Nullable
    public DocumentIndexEntry findDocument(@Nonnull DirectoryReader directoryReader, @Nonnull Tuple primaryKey) {
        final SegmentInfos segmentInfos = ((StandardDirectoryReader)FilterDirectoryReader.unwrap(directoryReader)).getSegmentInfos();
        final Subspace keySubspace = subspace.subspace(primaryKey);
        try (KeyValueCursor kvs = KeyValueCursor.Builder.newBuilder(keySubspace)
                .setContext(directory.getContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
                RecordCursor<DocumentIndexEntry> documents = kvs.map(kv -> {
                    final Tuple segdoc = keySubspace.unpack(kv.getKey());
                    final long segid = segdoc.getLong(0);
                    final String segmentName = directory.primaryKeySegmentName(segid);
                    if (segmentName == null) {
                        return null;
                    }
                    for (int i = 0; i < segmentInfos.size(); i++) {
                        SegmentInfo segmentInfo = segmentInfos.info(i).info;
                        if (segmentInfo.name.equals(segmentName)) {
                            final int docid = (int)segdoc.getLong(1);
                            return new DocumentIndexEntry(primaryKey, kv.getKey(),
                                    directoryReader.leaves().get(i).reader(), docid);
                        }
                    }
                    return null;
                }).filter(Objects::nonNull)) {
            return directory.getContext().asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY,
                    documents.first()).orElse(null);
        }
    }

    /**
     * Hook for getting back segment info during merge.
     */
    public interface StoredFieldsReaderSegmentInfo {
        SegmentInfo getSegmentInfo();
    }

    /**
     * Hook the fields writer to also record primary keys in index.
     * @param storedFieldsWriter normal field writer
     * @param si segment info for current writer
     * @return a wrapped writer
     * @throws IOException thrown by called methods
     */
    @Nonnull
    public StoredFieldsWriter wrapFieldsWriter(@Nonnull StoredFieldsWriter storedFieldsWriter, @Nonnull SegmentInfo si) throws IOException {
        final long segmentId = directory.primaryKeySegmentId(si.name, true);
        return new WrappedFieldsWriter(storedFieldsWriter, segmentId);
    }

    class WrappedFieldsWriter extends StoredFieldsWriter {
        @Nonnull
        private final StoredFieldsWriter inner;
        @Nonnull
        private final long segmentId;

        private int documentId;

        WrappedFieldsWriter(@Nonnull StoredFieldsWriter inner, long segmentId) {
            this.inner = inner;
            this.segmentId = segmentId;
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
                addOrDeletePrimaryKeyEntry(primaryKey, segmentId, documentId, true);
            }
        }

        @Override
        @SuppressWarnings("PMD.CloseResource")
        public int merge(MergeState mergeState) throws IOException {
            final int docCount = inner.merge(mergeState);

            final int segmentCount = mergeState.storedFieldsReaders.length;
            final PrimaryKeyVisitor visitor = new PrimaryKeyVisitor();
            for (int i = 0; i < segmentCount; i++) {
                final StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[i];
                final SegmentInfo mergedSegmentInfo = ((StoredFieldsReaderSegmentInfo)storedFieldsReader).getSegmentInfo();
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
                                addOrDeletePrimaryKeyEntry(primaryKey, segmentId, docId, true);
                            }
                        }
                        // Deleting the index entry at worst triggers a fallback to search.
                        // Ordinarily, though, transaction isolation means that the entry is there along with the pre-merge segment.
                        addOrDeletePrimaryKeyEntry(primaryKey, mergedSegmentId, j, false);
                        visitor.reset();
                    }
                }
            }

            directory.getContext().increment(LuceneEvents.Counts.LUCENE_MERGE_DOCUMENTS, docCount);
            directory.getContext().increment(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS, segmentCount);

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

    /**
     * Reconcile primary key index entries for a segment.
     * Delete those for a segment that is about to be deleted or add for a segment that has been filled without going through the wrapper.
     * The primary keys are gotten from the stored fields, so this must complete and not yet have been removed.
     * @param reader a reader for the target segment
     * @param add {@code true} to add entries; {@code false} to delete them
     * @throws IOException if the stored fields gets an error
     */
    @SuppressWarnings("PMD.CloseResource")
    public void addOrDeleteDocumentPrimaryKeys(@Nonnull SegmentReader reader, boolean add) throws IOException {
        final int maxDoc = reader.maxDoc();
        final PrimaryKeyVisitor visitor = new PrimaryKeyVisitor();
        final long segmentId = directory.primaryKeySegmentId(reader.getSegmentName(), add);
        try (StoredFieldsReader storedFields = reader.getFieldsReader()) {
            for (int documentId = 0; documentId < maxDoc; documentId++) {
                storedFields.visitDocument(documentId, visitor);
                final byte[] primaryKey = visitor.getPrimaryKey();
                if (primaryKey != null) {
                    addOrDeletePrimaryKeyEntry(primaryKey, segmentId, documentId, add);
                    visitor.reset();
                }
            }
        }
    }

    void addOrDeletePrimaryKeyEntry(@Nonnull byte[] primaryKey, long segmentId, int docId, boolean add) {
        final byte[] entryKey = ByteArrayUtil.join(subspace.getKey(), primaryKey, Tuple.from(segmentId, docId).pack());
        if (add) {
            directory.getContext().ensureActive().set(entryKey, new byte[0]);
        } else {
            directory.getContext().ensureActive().clear(entryKey);
        }
    }

    /**
     * Get the primary key byte array from a document's stored fields.
     * After calling {@link StoredFieldsReader#visitDocument}, any primary key will be in {@link #getPrimaryKey}.
     */
    static class PrimaryKeyVisitor extends StoredFieldVisitor {
        @Nullable
        private byte[] primaryKey = null;

        @Nullable
        public byte[] getPrimaryKey() {
            return primaryKey;
        }

        public void reset() {
            primaryKey = null;
        }

        @Override
        public Status needsField(final FieldInfo fieldInfo) {
            if (fieldInfo.name.equals(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME)) {
                return Status.YES;
            } else if (primaryKey != null) {
                return Status.STOP;
            } else {
                return Status.NO;
            }
        }

        @Override
        public void binaryField(final FieldInfo fieldInfo, final byte[] value) {
            if (fieldInfo.name.equals(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME)) {
                primaryKey = value;
            }
        }
    }
}
