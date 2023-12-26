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
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class LuceneOptimizedPostingsFieldsProducer extends FieldsProducer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsFieldsProducer.class);

    private final PostingsReaderBase postingsReader;
    private final FDBDirectory directory;
    private final String segmentName;
    private final FieldInfos fieldInfos;

    // order-preserving map of field numbers to field metadata
    // This reads all the data at once (though we may need to optimize it to only read the fields we need)
    private final LazyOpener<LinkedHashMap<Long, PostingsFieldMetadata>> fieldMetadataSupplier;
    private final LazyOpener<List<String>> fieldNameSupplier;

    public LuceneOptimizedPostingsFieldsProducer(SegmentReadState state, final PostingsReaderBase postingsReader) {
        this.postingsReader = postingsReader;
        this.directory = FDBDirectoryUtils.getFDBDirectory(state.directory);
        this.segmentName = state.segmentInfo.name;
        this.fieldInfos = state.fieldInfos;

        fieldMetadataSupplier = LazyOpener.supply(() -> {
            LinkedHashMap<Long, PostingsFieldMetadata> result = new LinkedHashMap<>();
            this.directory.getAllPostingFieldMetadataStream(segmentName)
                    .forEach(pair -> result.put(pair.getKey(), new PostingsFieldMetadata(pair.getValue())));
            return result;
        });
        fieldNameSupplier = LazyOpener.supply(() ->
                // Get the field names from the field numbers
                // The iteration order is preserved from the map
                fieldMetadataSupplier.get().keySet().stream()
                        .map(fieldNumber -> fieldInfos.fieldInfo(fieldNumber.intValue()))
                        .map(fieldInfo -> fieldInfo.name)
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
        return fieldNameSupplier.getUnchecked().iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("terms");
        }
        assert field != null;
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        // Since we need to iterate over all the fields it is OK to get all the metadata here since we already brought it anyway
        PostingsFieldMetadata metadata = fieldMetadataSupplier.get().get(fieldInfo.number);
        return new LuceneOptimizedTerms(segmentName, fieldInfo, metadata, directory, postingsReader);
    }

    @Override
    public int size() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("size");
        }
        return fieldNameSupplier.getUnchecked().size();
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
        // TODO: Maybe not a good idea to fetch all fields in toString
        return getClass().getSimpleName() + "(fields=" + fieldMetadataSupplier.getUnchecked().size() + ",delegate=" + postingsReader + ")";
    }
}
