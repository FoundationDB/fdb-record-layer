/*
 * LuceneOptimizedPostingsFieldsProducer.java
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
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class LuceneOptimizedPostingsFieldsProducer extends FieldsProducer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsFieldsProducer.class);

    private final PostingsReaderBase postingsReader;
    private final FDBDirectory directory;
    private final String segmentName;
    private final FieldInfos fieldInfos;

    private LazyOpener<List<String>> fieldsSupplier;

    public LuceneOptimizedPostingsFieldsProducer(final PostingsReaderBase postingsReader, SegmentReadState state, @Nonnull Directory directory) {
        this.postingsReader = postingsReader;
        this.directory = toFdbDirectory(directory);
        this.segmentName = state.segmentInfo.name;
        this.fieldInfos = state.fieldInfos;
        fieldsSupplier = LazyOpener.supply(() ->
                // get the field numbers, convert to field names
                this.directory.getAllPostingFields(segmentName).stream()
                        .map(field -> fieldInfos.fieldInfo(field))
                        .map(field -> field.name)
                        .collect(Collectors.toList()));
    }

    @Override
    public void close() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("close");
        }
        IOUtils.close(postingsReader);
    }

    @Override
    public Iterator<String> iterator() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("iterator");
        }
        return fieldsSupplier.getUnchecked().iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("terms");
        }
        assert field != null;
        return new LuceneOptimizedTerms(segmentName, fieldInfos.fieldInfo(field), directory, postingsReader);
    }

    @Override
    public int size() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("size");
        }
        return fieldsSupplier.getUnchecked().size();
    }

    @Override
    public long ramBytesUsed() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("ramBytesUsed");
        }
        long sizeInBytes = postingsReader.ramBytesUsed();
        // TODO
        return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("getChildResources");
        }
        // TODO
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
        return getClass().getSimpleName() + "(fields=" + fieldsSupplier.getUnchecked().size() + ",delegate=" + postingsReader + ")";
    }

    /**
     * TODO: This is to be replaced by the shared utility once that PR gets merged in.
     */
    @SuppressWarnings("PMD.CloseResource")
    private FDBDirectory toFdbDirectory(Directory directory) {
        Directory delegate = FilterDirectory.unwrap(directory);
        if (delegate instanceof LuceneOptimizedCompoundReader) {
            delegate = ((LuceneOptimizedCompoundReader)delegate).getDirectory();
        }
        if (delegate instanceof FDBDirectory) {
            return (FDBDirectory)delegate;
        } else {
            throw new RuntimeException("Expected FDB Directory " + delegate.getClass());
        }
    }

}
