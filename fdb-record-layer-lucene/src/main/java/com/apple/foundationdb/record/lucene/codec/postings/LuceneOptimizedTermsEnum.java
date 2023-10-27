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

package com.apple.foundationdb.record.lucene.codec.postings;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
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
    private final FDBDirectory directory;
    private final FieldInfo fieldInfo;
    private final Tuple tupleKey;
    private LuceneOptimizedBlockTermState blockTermState;
    private AsyncIterator<KeyValue> currentIterator;
    private AttributeSource atts = null;
    private final PostingsReaderBase postingsReader;

    public LuceneOptimizedTermsEnum(final FieldInfo fieldInfo, final SegmentReadState segmentReadState,
                                    final FDBDirectory directory, final PostingsReaderBase postingsReader) {
        this.directory = directory;
        this.fieldInfo = fieldInfo;
        this.postingsReader = postingsReader;
        this.tupleKey = Tuple.from(segmentReadState.segmentInfo.name).add(fieldInfo.number);
        this.blockTermState = new LuceneOptimizedBlockTermState();
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
        byte[] data = directory.getTerm(LuceneCodecUtil.BytesRefToTuple(tupleKey, text));
        if (data != null) {
            blockTermState.copyFrom(text, LucenePostingsProto.TermInfo.parseFrom(data));
        }
        return data != null;
    }

    @Override
    public SeekStatus seekCeil(final BytesRef text) throws IOException {
        currentIterator = directory.scanTermAsync(LuceneCodecUtil.BytesRefToTuple(tupleKey, text)).iterator();
        if (currentIterator.hasNext()) {
            blockTermState = new LuceneOptimizedBlockTermState(currentIterator.next());
            if (blockTermState.compareTermTo(text) == 0) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        } else {
            blockTermState.copyFrom(null, null);
            return SeekStatus.END;
        }
    }

    @Override
    public void seekExact(final long ord) throws IOException {
        throw new UnsupportedOperationException("seekExact Not Supported");
    }

    @Override
    public void seekExact(final BytesRef term, final TermState state) throws IOException {
        this.blockTermState = ((LuceneOptimizedBlockTermState) state);
    }

    @Override
    public BytesRef term() throws IOException {
        return blockTermState.getTerm();
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException("ord() not supported");
    }

    @Override
    public int docFreq() throws IOException {
        return blockTermState.getDocFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
        return blockTermState.getTotalTermFreq();
    }

    @Override
    public PostingsEnum postings(final PostingsEnum reuse, final int flags) throws IOException {
        return postingsReader.postings(fieldInfo, (BlockTermState) termState(), reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(final int flags) throws IOException {
        return postingsReader.impacts(fieldInfo, (BlockTermState) termState(), flags);
    }

    @Override
    public TermState termState() throws IOException {
        return blockTermState;
    }

    @Override
    public BytesRef next() throws IOException {
        if (currentIterator == null) {
            if (blockTermState != null && blockTermState.getTerm() != null) {
                currentIterator = directory.scanTermAsync(
                        LuceneCodecUtil.BytesRefToTuple(tupleKey, blockTermState.getTerm())).iterator();
            } else {
                currentIterator = directory.scanAllTermsAsync(tupleKey).iterator();
            }
        }
        if (!currentIterator.hasNext()) {
            blockTermState = null;
            return null;
        }
        blockTermState = new LuceneOptimizedBlockTermState(currentIterator.next());
        return blockTermState.getTerm();
    }

}
