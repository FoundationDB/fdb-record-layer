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

import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedStoredFieldsWriter;
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
    private final LucenePrimaryKeySegmentIndex lucenePrimaryKeySegmentIndex;
    @Nonnull
    private final LuceneOptimizedStoredFieldsWriter inner;
    private final long segmentId;
    @Nonnull
    private final FDBDirectory directory;

    private int documentId;

    PrimaryKeyAndStoredFieldsWriter(@Nonnull LuceneOptimizedStoredFieldsWriter inner,
                                    long segmentId,
                                    @Nonnull final FDBDirectory directory) {
        this.inner = inner;
        this.segmentId = segmentId;
        this.directory = directory;
        lucenePrimaryKeySegmentIndex = directory.getPrimaryKeySegmentIndex();
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
            lucenePrimaryKeySegmentIndex.addOrDeletePrimaryKeyEntry(primaryKey, segmentId, documentId, true);
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public int merge(MergeState mergeState) throws IOException {
        return inner.merge(mergeState);
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
