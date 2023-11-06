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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.lucene.LuceneStoredFieldsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Accountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;


/**
 * This class wraps a StoreFieldsReader.
 *
 */
public class LuceneOptimizedStoredFieldsReader extends StoredFieldsReader {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedStoredFieldsReader.class);
    private final FDBDirectory directory;
    private final SegmentInfo si;
    private final FieldInfos fieldInfos;
    private final Tuple keyTuple;
    private AsyncIterator<KeyValue> rangeIterator;

    @SuppressWarnings("PMD.CloseResource")
    public LuceneOptimizedStoredFieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fieldInfos) {
        this.si = si;
        this.fieldInfos = fieldInfos;
        Directory delegate = FilterDirectory.unwrap(directory);
        if (delegate instanceof LuceneOptimizedCompoundReader) {
            delegate = ((LuceneOptimizedCompoundReader) delegate).getDirectory();
        }
        if (delegate instanceof LuceneOptimizedWrappedDirectory) {
            delegate = ((LuceneOptimizedWrappedDirectory) delegate).getFdbDirectory();
        }
        if (delegate instanceof FDBDirectory) {
            this.directory = (FDBDirectory) delegate;
        } else {
            throw new RuntimeException("Expected FDB Directory " + delegate.getClass());
        }
        this.keyTuple = Tuple.from(si.name);
    }

    public Tuple getKeyTuple() {
        return keyTuple;
    }

    public void visitDocumentViaScan() {
        // TODO: This looks wrong. It introduces a state into this class (that can then only work in this mode moving forward) and that state is lost when cloned.
        // Probably woth seeing if that can be replaced with external state that will be managed by repeated calls to visitDocument.
        this.rangeIterator = directory.scanStoredFields(keyTuple).iterator();
    }


    @Override
    public void visitDocument(final int docID, final StoredFieldVisitor visitor) throws IOException {
        LuceneStoredFieldsProto.LuceneStoredFields storedFields;
        if (rangeIterator == null) {
            storedFields = LuceneStoredFieldsProto.LuceneStoredFields.parseFrom(directory.readStoredFields(keyTuple.add(docID)));
        } else {
            KeyValue keyValue = rangeIterator.next();
            if (keyValue == null) {
                throw new IOException("Range Iterator Was Exhausted, should not happen");
            }
            storedFields = LuceneStoredFieldsProto.LuceneStoredFields.parseFrom(keyValue.getValue());
        }
        List<LuceneStoredFieldsProto.StoredField> storedFieldList = storedFields.getStoredFieldsList();
        for (LuceneStoredFieldsProto.StoredField storedField: storedFieldList) {
            // TODO: Are the numbers consistent across restarts? Isn't it safer to store the name and restore the info by name?
            // Looks like they are consistent (this is how its done in CompressingStoredFieldsReader
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
}
