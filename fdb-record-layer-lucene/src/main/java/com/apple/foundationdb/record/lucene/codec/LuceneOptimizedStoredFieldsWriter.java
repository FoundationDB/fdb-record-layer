/*
 * LuceneOptimizedStoredFieldsWriter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneStoredFieldsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryBase;
import com.google.protobuf.ByteString;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An implementation of {@link StoredFieldsWriter} for fields stored in the DB.
 * The data for the fields is protobuf-encoded (see lucene_stored_fields.proto) message.
 * The subspace for the range of documents is the segment name.
 * Within the subspace, each document key is then suffixed by the docId.
 */
public class LuceneOptimizedStoredFieldsWriter extends StoredFieldsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedStoredFieldsWriter.class);
    protected LuceneStoredFieldsProto.LuceneStoredFields.Builder storedFields;
    private final FDBDirectoryBase directory;
    private final String segmentName;
    private int docId;

    @SuppressWarnings("PMD.CloseResource")
    public LuceneOptimizedStoredFieldsWriter(final FDBDirectoryBase directory, final SegmentInfo si) throws IOException {
        this.directory = directory;
        this.docId = 0;
        this.segmentName = si.name;
    }

    @Override
    public void startDocument() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("startDocument");
        }
        storedFields = LuceneStoredFieldsProto.LuceneStoredFields.newBuilder();
    }

    @Override
    public void finishDocument() throws IOException {
        try {
            directory.writeStoredFields(segmentName, docId, storedFields.build().toByteArray());
            docId++;
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @Override
    public void writeField(final FieldInfo info, final IndexableField field) throws IOException {
        try {
            LuceneStoredFieldsProto.StoredField.Builder builder = LuceneStoredFieldsProto.StoredField.newBuilder();
            builder.setFieldNumber(info.number);
            Number number = field.numericValue();
            if (number != null) {
                if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
                    builder.setIntValue(number.intValue());
                } else if (number instanceof Long) {
                    builder.setLongValue(number.longValue());
                } else if (number instanceof Float) {
                    builder.setFloatValue(number.floatValue());
                } else if (number instanceof Double) {
                    builder.setDoubleValue(number.doubleValue());
                } else {
                    throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
                }
            } else {
                BytesRef bytes = field.binaryValue();
                if (bytes != null) {
                    builder.setBytesValue(ByteString.copyFrom(bytes.bytes, bytes.offset, bytes.length));
                } else {
                    String string = field.stringValue();
                    if (string == null) {
                        throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
                    }
                    builder.setStringValue(string);
                }
            }
            storedFields.addStoredFields(builder);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @Override
    public void finish(final FieldInfos fis, final int numDocs) throws IOException {
        if (docId != numDocs) {
            throw new RuntimeException("Wrote " + docId + " docs, finish called with numDocs=" + numDocs);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    @Override
    public int merge(final MergeState mergeState) throws IOException {
        try {
            List<StoredFieldsMergeSub> subs = new ArrayList<>();
            for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
                // TODO: Explore whether we should read a range to speed up the retrieval of the documents (there are more documents than we actually need
                // since some of them are tombstones)
                subs.add(new StoredFieldsMergeSub(new MergeVisitor(mergeState, i), mergeState.docMaps[i], mergeState.storedFieldsReaders[i], mergeState.maxDocs[i]));
            }

            final DocIDMerger<StoredFieldsMergeSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

            int docCount = 0;
            while (true) {
                StoredFieldsMergeSub sub = docIDMerger.next();
                if (sub == null) {
                    break;
                }
                assert sub.mappedDocID == docCount;
                startDocument();
                sub.reader.visitDocument(sub.docID, sub.visitor);
                finishDocument();
                docCount++;
            }

            finish(mergeState.mergeFieldInfos, docCount);
            return docCount;
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to do - will be committed to FDB once the transaction commits
    }

    @Override
    public long ramBytesUsed() {
        return 1; // ToDo Fix
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return super.getChildResources();
    }

    private static class StoredFieldsMergeSub extends DocIDMerger.Sub {
        private final StoredFieldsReader reader;
        private final int maxDoc;
        private final MergeVisitor visitor;
        int docID = -1;

        public StoredFieldsMergeSub(MergeVisitor visitor, MergeState.DocMap docMap, StoredFieldsReader reader, int maxDoc) {
            super(docMap);
            this.maxDoc = maxDoc;
            this.reader = reader;
            this.visitor = visitor;
        }

        @Override
        public int nextDoc() {
            docID++;
            if (docID == maxDoc) {
                return NO_MORE_DOCS;
            } else {
                return docID;
            }
        }

    }

}
