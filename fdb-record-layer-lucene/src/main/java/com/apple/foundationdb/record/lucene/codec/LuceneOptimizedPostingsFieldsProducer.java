/*
 * LuceneOptimizedKVPostingsFormat.java
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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Accountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class wrpas a FieldsProducer.
 *
 */
public class LuceneOptimizedPostingsFieldsProducer extends FieldsProducer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsFieldsProducer.class);
    private final SegmentReadState state;
    private final Tuple tupleKey;

    private final PostingsReaderBase postingsReader;
    private final FDBDirectory directory;
    private final Supplier<List<String>> fields;

    public LuceneOptimizedPostingsFieldsProducer(final SegmentReadState state, final PostingsReaderBase postingsReader) {
        this.state = state;
        this.postingsReader = postingsReader;
        this.tupleKey = Tuple.from(state.segmentInfo.name);
        Directory delegate = FilterDirectory.unwrap(state.directory);
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
        fields = Suppliers.memoize(() -> {
            List<String> fieldNames = new ArrayList<>();
            Iterables.all(directory.scanTermMetadataAsync(tupleKey), (keyvalue) -> {
                Tuple key = Tuple.fromBytes(keyvalue.getKey());
                long fieldNumber = key.getLong(key.size() - 1);
                fieldNames.add(state.fieldInfos.fieldInfo((int)fieldNumber).name);
                return true;
            });
            return fieldNames;
        });
    }

    @Override
    public void close() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("close");
        }
        postingsReader.close();
    }

    @Override
    public Iterator<String> iterator() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("iterator");
        }
        return fields.get().iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("terms");
        }
        return new LuceneOptimizedTerms(state.fieldInfos.fieldInfo(field), directory, state); // TODO
    }

    @Override
    public int size() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("size");
        }
        return fields.get().size();
    }

    @Override
    public long ramBytesUsed() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("ramBytesUsed");
        }
        return postingsReader.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("getChildResources");
        }
        return postingsReader.getChildResources();
    }

    @Override
    public void checkIntegrity() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("checkIntegrity");
        }
    }

    @Override
    public String toString() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("toString");
        }
        return ""; // TODO
    }

}
