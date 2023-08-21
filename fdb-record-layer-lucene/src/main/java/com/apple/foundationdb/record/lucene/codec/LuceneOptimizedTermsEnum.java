/*
 * LuceneOptimizedTermsEnum.java
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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LuceneOptimizedTermsEnum extends TermsEnum {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedTermsEnum.class);
    private final SegmentReadState segmentReadState;
    private final FDBDirectory directory;
    private final FieldInfo fieldInfo;
    private final Tuple tupleKey;
    private LucenePostingsProto.Term currentTerm;
    private AttributeSource atts = null;

    public LuceneOptimizedTermsEnum(final FieldInfo fieldInfo, final SegmentReadState segmentReadState, final FDBDirectory directory) {
        if (LOG.isInfoEnabled()) {
            LOG.info("LuceneOptimizedTermsEnum");
        }
        this.segmentReadState = segmentReadState;
        this.directory = directory;
        this.fieldInfo = fieldInfo;
        this.tupleKey = Tuple.from(segmentReadState.segmentInfo.name).add(fieldInfo.number);
    }

    @Override
    public AttributeSource attributes() {
        if (LOG.isInfoEnabled()) {
            LOG.info("attributes");
        }
        if (atts == null) {
            atts = new AttributeSource();
        }
        return atts;
    }

    @Override
    public boolean seekExact(final BytesRef text) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("seekExact");
        }
        byte[] data = directory.getTerm(tupleKey.add(text.bytes, text.offset, text.length));
        if (data != null) {
            currentTerm = LucenePostingsProto.Term.parseFrom(data);
        }
        return data != null;
    }

    @Override
    public SeekStatus seekCeil(final BytesRef text) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("seekCeil");
        }
        return null;
    }

    @Override
    public void seekExact(final long ord) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("seekExact by ord");
        }
    }

    @Override
    public void seekExact(final BytesRef term, final TermState state) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("seekExact by term/state");
        }
    }

    @Override
    public BytesRef term() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("term");
        }
        return null;
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException("ord() not supported");
    }

    @Override
    public int docFreq() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("docFreq");
        }
        return currentTerm.getDocFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("totalTermFreq");
        }
        return currentTerm.getTermFreq();
    }

    @Override
    public PostingsEnum postings(final PostingsEnum reuse, final int flags) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("postings");
        }
        return null;
    }

    @Override
    public ImpactsEnum impacts(final int flags) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("impacts");
        }
        return new LuceneOptimizedImpactsEnum();
    }

    @Override
    public TermState termState() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("termState");
        }
        return new LuceneOptimizedTermState();
    }

    @Override
    public BytesRef next() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("next");
        }
        return null;
    }
}
