/*
 * LuceneOptimizedBlockTermState.java
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
import com.google.common.base.Verify;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import java.io.UncheckedIOException;

/**
 * A representation of the enumeration state for a term. This state is maintained throughout the enumeration, keeping
 * track of the current term and its aggregated metrics.
 */
public class LuceneOptimizedBlockTermState extends BlockTermState {
    // The term text for the state
    private BytesRef term;
    // Caching of the documents aggregate for the term
    private LucenePostingsProto.Documents documents;

    public LuceneOptimizedBlockTermState() {
        // Empty constructor, to be used by newTermState(). This instance will be populated by data
        // later, via calls to copyFrom()
    }

    /**
     * Create a state from data read from FDB.
     * @param term the term text
     * @param termInfo a serialized {@link LucenePostingsProto.TermInfo}
     */
    public LuceneOptimizedBlockTermState(@Nonnull final BytesRef term, @Nonnull final byte[] termInfo) {
        try {
            this.term = term;
            LucenePostingsProto.TermInfo info = LucenePostingsProto.TermInfo.parseFrom(termInfo);
            this.docFreq = info.getDocFreq();
            this.totalTermFreq = info.getTotalTermFreq();
            this.ord = info.getOrd();
            this.documents = info.getDocuments();
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void copyFrom(final TermState other) {
        final LuceneOptimizedBlockTermState otherState = (LuceneOptimizedBlockTermState)other;
        Verify.verify(otherState.term != null, "Term Cannot Be Null");
        this.term = otherState.term;
        this.docFreq = otherState.getDocFreq();
        this.totalTermFreq = otherState.getTotalTermFreq();
        this.ord = otherState.getOrd();
        this.documents = otherState.documents;
    }

    public int compareTermTo(BytesRef text) {
        return text.compareTo(term);
    }

    public BytesRef getTerm() {
        return term;
    }

    public int getDocFreq() {
        return docFreq;
    }

    public long getTotalTermFreq() {
        return totalTermFreq;
    }

    public long getOrd() {
        return ord;
    }

    public LucenePostingsProto.Documents getDocuments() {
        return documents;
    }

    public void setDocuments(final LucenePostingsProto.Documents documents) {
        this.documents = documents;
    }
}
