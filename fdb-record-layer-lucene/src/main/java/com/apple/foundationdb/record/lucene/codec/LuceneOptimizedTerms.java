/*
 * LuceneOptimizedTerms.java
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

import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public class LuceneOptimizedTerms extends Terms {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedTerms.class);
    private final FieldInfo fieldInfo;
    private final SegmentReadState state;

    private final Supplier<LucenePostingsProto.TermMeta> meta;
    private final Tuple tupleKey;
    private final FDBDirectory directory;

    public LuceneOptimizedTerms(final FieldInfo fieldInfo, final FDBDirectory directory, final SegmentReadState state) {
        if (LOG.isInfoEnabled()) {
            LOG.info("LuceneOptimizedTerms");
        }
        this.fieldInfo = fieldInfo;
        this.state = state;
        this.tupleKey = Tuple.from(state.segmentInfo.name).add(fieldInfo.number);
        this.directory = directory;
        meta = Suppliers.memoize( () -> {
            try {
                return LucenePostingsProto.TermMeta.parseFrom(directory.getTermMetadata(tupleKey));
            } catch (InvalidProtocolBufferException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public TermsEnum iterator() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("iterator");
        }
        return new LuceneOptimizedTermsEnum(fieldInfo, state, directory);
    }

    @Override
    public long size() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("size");
        }
        return meta.get().getNumTerms();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getSumTotalTermFreq");
        }
        return meta.get().getSumTotalFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getSumDocFreq");
        }
        return meta.get().getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getDocCount");
        }
        return state.segmentInfo.maxDoc();
    }

    @Override
    public boolean hasFreqs() {
        if (LOG.isInfoEnabled()) {
            LOG.info("hasFreqs");
        }
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
        if (LOG.isInfoEnabled()) {
            LOG.info("hasOffsets");
        }
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
        if (LOG.isInfoEnabled()) {
            LOG.info("hasPositions");
        }
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
        if (LOG.isInfoEnabled()) {
            LOG.info("hasPayloads");
        }
        return fieldInfo.hasPayloads();
    }

}
