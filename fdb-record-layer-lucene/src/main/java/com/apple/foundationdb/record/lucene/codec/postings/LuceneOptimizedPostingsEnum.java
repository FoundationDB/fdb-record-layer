/*
 * LuceneOptimizedPostingsEnum.java
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

import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LuceneOptimizedPostingsEnum extends ImpactsEnum {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsEnum.class);
    final LucenePostingsProto.Documents documents;
    private LucenePostingsProto.Positions positions;
    final FDBDirectory directory;
    private int position = -1;
    private int docPosition = -1;
    private Tuple tupleKey;
    private final boolean hasPositions;
    private final boolean hasPayloads;
    private final boolean hasOffsets;


    public LuceneOptimizedPostingsEnum(Tuple key, final LucenePostingsProto.Documents documents, FDBDirectory directory,
                                       final boolean hasPositions, final boolean hasOffsets, final boolean hasPayloads) {
        this.documents = documents;
        this.directory = directory;
        this.tupleKey = key;
        this.hasPositions = hasPositions;
        this.hasPayloads = hasPayloads;
        this.hasOffsets = hasOffsets;
    }

    @Override
    public int freq() throws IOException {
        return documents.getFreq(docPosition);
    }

    @Override
    public int nextPosition() throws IOException {
        if (!hasPositions) {
            return -1;
        }
        if (positions == null) {
            positions = LucenePostingsProto.Positions.parseFrom(directory.getTermPositions(tupleKey.add(docID())));
            if (positions.getPositionCount() != freq()) {
                throw new IOException("Index is Corrupted " + docID());
            }
        }
        return positions.getPosition(++position);
    }

    @Override
    public int startOffset() throws IOException {
        return 0;
    }

    @Override
    public int endOffset() throws IOException {
        return 0;
    }

    @Override
    public BytesRef getPayload() throws IOException {
        return null;
    }

    @Override
    public int docID() {
        if (docPosition == -1 || docPosition == NO_MORE_DOCS) {
            return docPosition;
        }
        assert docPosition < documents.getDocIdCount():"overflow with position="+docPosition;
        if (LOG.isInfoEnabled()) {
            LOG.info("docID() [docID {}, position={}]", documents.getDocId(docPosition), docPosition);
        }
        return documents.getDocId(docPosition);
    }

    @Override
    public int nextDoc() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("nextDoc called on {}", docPosition);
        }
        assert docPosition != NO_MORE_DOCS: "Should not be called";
        docPosition++;
        position = -1;
        positions = null;
        if (documents.getDocIdCount() <= docPosition) { // Exhausted
            if (LOG.isInfoEnabled()) {
                LOG.info("nextDoc exhausted on {}", docPosition);
            }
            Thread.dumpStack();
            docPosition = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        return documents.getDocId(docPosition);
    }

    @Override
    public int advance(final int target) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("advance {}", target);
        }
        while(docID() < target) {
            if (nextDoc() == NO_MORE_DOCS) {
                return NO_MORE_DOCS;
            }
        }
        return docID();
    }

    @Override
    public long cost() {
        return 0;
    }

    @Override
    public void advanceShallow(final int target) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("advanceShallow {}", target);
        }
        advance(target);
    }

    @Override
    public Impacts getImpacts() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getImpacts with position {}", docPosition);
            Thread.dumpStack();
        }
        return new Impacts() {
            private final List<Impact> impacts = Collections.singletonList(new Impact(docPosition == NO_MORE_DOCS ? 0 : documents.getFreq(docPosition), 1L));

            @Override
            public int numLevels() {
                return 1;
            }

            @Override
            public int getDocIdUpTo(int level) {
                return NO_MORE_DOCS;
            }

            @Override
            public List<Impact> getImpacts(int level) {
                return impacts;
            }

        };
    }

}
