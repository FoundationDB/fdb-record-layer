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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

// TODO: extends BaseTermsEnum???
public class LuceneOptimizedTermsEnum extends TermsEnum {
    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final FDBDirectory directory;
    private final PostingsReaderBase postingsReader;

    private LuceneOptimizedBlockTermState currentTermState;
    private AttributeSource atts;

    public LuceneOptimizedTermsEnum(final String segmentName, final FieldInfo fieldInfo, final FDBDirectory directory, final PostingsReaderBase postingsReader) {
        this.segmentName = segmentName;
        this.fieldInfo = fieldInfo;
        this.directory = directory;
        this.postingsReader = postingsReader;
    }

    @Override
    public boolean seekExact(final BytesRef text) throws IOException {
        byte[] termData = directory.getPostingsTerm(segmentName, fieldInfo.number, text);
        if (termData != null) {
            // TODO: This leaves the same instance in place - why is this necessary?
            // TODO: If null, should we reset the current state?
            // currentTermState.copyFrom(text, termData);
            currentTermState = new LuceneOptimizedBlockTermState(text, termData);
        }
        return termData != null;
    }

    @Override
    public SeekStatus seekCeil(final BytesRef text) throws IOException {
        // Scan from the given term inclusive of the first element
        Pair<byte[], byte[]> term = directory.getNextPostingsTerm(segmentName, fieldInfo.number, text, FDBDirectory.RangeType.INCLUSIVE);

        if (term != null) {
            currentTermState = new LuceneOptimizedBlockTermState(term.getKey(), term.getValue());
            if (currentTermState.compareTermTo(text) == 0) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        } else {
            // or set the state to null?
            currentTermState = new LuceneOptimizedBlockTermState();
            return SeekStatus.END;
        }
    }

    @Override
    public void seekExact(final long ord) throws IOException {
        // TODO: Should we implement the slow way?
        throw new UnsupportedOperationException("seekExact(long) Not Supported");
    }

    @Override
    public void seekExact(final BytesRef term, final TermState state) throws IOException {
         currentTermState = ((LuceneOptimizedBlockTermState) state);
    }

    @Override
    public BytesRef next() throws IOException {
        Pair<byte[], byte[]> nextTermData;
        if ((currentTermState != null) && (currentTermState.getTerm() != null)) {
            // scan for the next term following the current
            nextTermData = directory.getNextPostingsTerm(segmentName, fieldInfo.number, currentTermState.getTerm(), FDBDirectory.RangeType.EXCLUSIVE);
        } else {
            nextTermData = directory.getFirstPostingsTerm(segmentName, fieldInfo.number);
        }
        if (nextTermData == null) {
            // TODO: Is this right? Should we return to the beginning?
            currentTermState = null;
            return null;
        } else {
            currentTermState = new LuceneOptimizedBlockTermState(nextTermData.getKey(), nextTermData.getValue());
            return currentTermState.getTerm();
        }
    }

    @Override
    public BytesRef term() throws IOException {
        // TODO: ensure valid state
        return currentTermState.getTerm();
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException("ord() not supported");
    }

    @Override
    public TermState termState() throws IOException {
        // TODO: ensure valid state
        return currentTermState;
    }


    @Override
    public int docFreq() throws IOException {
        // TODO: ensure valid state
        return currentTermState.getDocFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
        // TODO: ensure valid state
        return currentTermState.getTotalTermFreq();
    }

    @Override
    public PostingsEnum postings(final PostingsEnum reuse, final int flags) throws IOException {
        // TODO: ensure valid state
        return postingsReader.postings(fieldInfo, currentTermState, reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(final int flags) throws IOException {
        // TODO: ensure valid state
        return postingsReader.impacts(fieldInfo, currentTermState, flags);
    }

    @Override
    public AttributeSource attributes() {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("attributes");
//        }
        if (atts == null) {
            atts = new AttributeSource();
        }
        return atts;
    }
}
