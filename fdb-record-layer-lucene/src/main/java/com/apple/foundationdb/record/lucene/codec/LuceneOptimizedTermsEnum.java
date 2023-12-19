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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import java.io.IOException;

// TODO: extends BaseTermsEnum???
public class LuceneOptimizedTermsEnum extends TermsEnum {
    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final FDBDirectory directory;
    private final PostingsReaderBase postingsReader;

    private LuceneOptimizedBlockTermState currentTermState;
    private AsyncIterator<KeyValue> currentIterator;
    private AttributeSource atts;

    public LuceneOptimizedTermsEnum(final String segmentName, final FieldInfo fieldInfo, final FDBDirectory directory, final PostingsReaderBase postingsReader) {
        this.segmentName = segmentName;
        this.fieldInfo = fieldInfo;
        this.directory = directory;
        this.postingsReader = postingsReader;
    }

    @Override
    public boolean seekExact(final BytesRef text) throws IOException {
        // TODO: Do we need to reset the iterator?
        Tuple termBytes = byteRefToTuple(text);
        byte[] termData = directory.getPostingsTerm(segmentName, fieldInfo.number, termBytes);
        if (termData != null) {
            // TODO: This leaves the same instance in place - why is this necessary?
            // currentTermState = new CurrentTermState(text, termData);
            currentTermState.copyFrom(text, termData);
        }
        return termData != null;
    }

    @Override
    public SeekStatus seekCeil(final BytesRef text) throws IOException {
        // TODO: Do we need to reset the iterator?
        Tuple termBytes = byteRefToTuple(text);
        // Keep the iterator around for subsequent next() calls
        currentIterator = directory.scanPostingsTerm(segmentName, fieldInfo.number, termBytes);

        if (currentIterator.hasNext()) {
            currentTermState = new LuceneOptimizedBlockTermState(currentIterator.next());
            if (currentTermState.compareTermTo(text) == 0) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        } else {
            // or set the state to null?
            currentTermState.copyFrom(null, null);
            return SeekStatus.END;
        }
    }

    @Override
    public void seekExact(final long ord) throws IOException {
        throw new UnsupportedOperationException("seekExact(long) Not Supported");
    }

    @Override
    public void seekExact(final BytesRef term, final TermState state) throws IOException {
        //TODO: Add support for advanced feature?
        throw new UnsupportedOperationException("seekExact(term, state) Not Supported");
        // currentTermState = ((LuceneOptimizedBlockTermState) state);
    }

    @Override
    public BytesRef next() throws IOException {
        if (currentIterator == null) {
            if (currentTermState != null && currentTermState.getTerm() != null) {
                currentIterator = directory.scanPostingsTerm(segmentName, fieldInfo.number, byteRefToTuple(currentTermState.getTerm()));
            } else {
                // Start at the beginning
                currentIterator = directory.scanAllPostingsTermsAsync(segmentName, fieldInfo.number);
            }
        }
        if (!currentIterator.hasNext()) {
            currentTermState = null;
            return null;
        }
        currentTermState = new LuceneOptimizedBlockTermState(currentIterator.next());
        return currentTermState.getTerm();
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

    @Nonnull
    private Tuple byteRefToTuple(final BytesRef text) throws IOException {
        return Tuple.fromBytes(term().bytes, text.offset, text.length);
    }
}
