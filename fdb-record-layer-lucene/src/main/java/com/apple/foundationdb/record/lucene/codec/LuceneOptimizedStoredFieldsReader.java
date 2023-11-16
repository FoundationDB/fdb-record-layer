/*
 * LuceneOptimizedStoredFieldsReader.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.lucene.LuceneStoredFieldsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Accountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;


/**
 * A {@link StoredFieldsReader} implementation for Stored Fields stored in the DB.
 * The data for the fields is protobuf-encoded (see lucene_stored_fields.proto) message.
 * The subspace for the range of documents is the segment name.
 * Within the subspace, each document key is then suffixed by the docId.
 */
public class LuceneOptimizedStoredFieldsReader extends StoredFieldsReader implements LucenePrimaryKeySegmentIndex.StoredFieldsReaderSegmentInfo {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedStoredFieldsReader.class);
    private final FDBDirectory directory;
    private final SegmentInfo si;
    private final FieldInfos fieldInfos;
    private final String segmentName;
    private AsyncIterator<KeyValue> rangeIterator;

    @SuppressWarnings("PMD.CloseResource")
    public LuceneOptimizedStoredFieldsReader(final FDBDirectory directory, final SegmentInfo si, final FieldInfos fieldInfos) {
        this.si = si;
        this.fieldInfos = fieldInfos;
        this.directory = directory;
        this.segmentName = si.name;
    }

    @Override
    public void visitDocument(final int docID, final StoredFieldVisitor visitor) throws IOException {
        LuceneStoredFieldsProto.LuceneStoredFields storedFields;
        if (rangeIterator == null) {
            storedFields = LuceneStoredFieldsProto.LuceneStoredFields.parseFrom(directory.readStoredFields(segmentName, docID));
        } else {
            KeyValue keyValue = rangeIterator.next();
            if (keyValue == null) {
                throw new IOException("Range Iterator Was Exhausted, should not happen");
            }
            storedFields = LuceneStoredFieldsProto.LuceneStoredFields.parseFrom(keyValue.getValue());
        }
        List<LuceneStoredFieldsProto.StoredField> storedFieldList = storedFields.getStoredFieldsList();
        for (LuceneStoredFieldsProto.StoredField storedField: storedFieldList) {
            FieldInfo info = fieldInfos.fieldInfo(storedField.getFieldNumber());
            switch (visitor.needsField(info)) {
                case YES:
                    if (storedField.hasBytesValue()) {
                        visitor.binaryField(info, storedField.getBytesValue().toByteArray());
                    } else if (storedField.hasStringValue()) {
                        visitor.stringField(info, storedField.getStringValueBytes().toByteArray());
                    } else if (storedField.hasIntValue()) {
                        visitor.intField(info, storedField.getIntValue());
                    } else if (storedField.hasFloatValue()) {
                        visitor.floatField(info, storedField.getFloatValue());
                    } else if (storedField.hasLongValue()) {
                        visitor.longField(info, storedField.getLongValue());
                    } else if (storedField.hasDoubleValue()) {
                        visitor.doubleField(info, storedField.getDoubleValue());
                    } else {
                        throw new IOException("empty stored field, not supported: " + storedField);
                    }
                    break;
                case NO:
                    break;
                case STOP:
                    return;
                default:
                    throw new IllegalStateException("Unexpected value: " + visitor.needsField(info));
            }
        }
    }

    @Override
    @SuppressWarnings("PMD.ProperCloneImplementation")
    @SpotBugsSuppressWarnings("CN")
    public LuceneOptimizedStoredFieldsReader clone() {
        return new LuceneOptimizedStoredFieldsReader(directory, si, fieldInfos);
    }

    @Override
    public void checkIntegrity() throws IOException {
    }

    @Override
    public void close() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("close");
        }
    }

    @Override
    public long ramBytesUsed() {
        return 1024; // TODO Estimate
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return super.getChildResources();
    }

    @Override
    public String toString() {
        return si.name;
    }

    @Override
    public SegmentInfo getSegmentInfo() {
        return si;
    }
}
