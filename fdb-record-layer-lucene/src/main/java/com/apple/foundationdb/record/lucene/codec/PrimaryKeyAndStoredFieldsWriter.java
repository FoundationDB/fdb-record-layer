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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryBase;
import com.google.protobuf.ByteString;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfo;

import javax.annotation.Nonnull;
import java.io.IOException;

class PrimaryKeyAndStoredFieldsWriter extends LuceneOptimizedStoredFieldsWriter {
    private final LucenePrimaryKeySegmentIndex lucenePrimaryKeySegmentIndex;
    private final long segmentId;

    private int documentId;

    PrimaryKeyAndStoredFieldsWriter(SegmentInfo segmentInfo,
                                    @Nonnull final FDBDirectoryBase directory) throws IOException {
        super(directory, segmentInfo);
        this.segmentId = directory.primaryKeySegmentId(segmentInfo.name, true);
        this.lucenePrimaryKeySegmentIndex = directory.getPrimaryKeySegmentIndex();
    }

    @Override
    public void finishDocument() throws IOException {
        super.finishDocument();
        documentId++;
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
        super.writeField(info, field);
        try {
            if (LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME.equals(info.name)) {
                final byte[] primaryKey = field.binaryValue().bytes;
                lucenePrimaryKeySegmentIndex.addOrDeletePrimaryKeyEntry(primaryKey, segmentId, documentId, true, info.name);
                // TODO we store this twice, but we'll probably want to optimize and only store this once
                storedFields.setPrimaryKey(ByteString.copyFrom(primaryKey));
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }
}
