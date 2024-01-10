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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * FDB-optimized {@link PostingsEnum} {@link ImpactsEnum}.
 */
public class LuceneOptimizedPostingsEnum extends ImpactsEnum {
    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final LuceneOptimizedBlockTermState state;
    private final LucenePostingsProto.Documents documents;
    private FDBDirectory directory;

    // current doc ordinal within the term
    private int currentDoc = -1;
    // current position ordinal within the doc
    private int currentPosition = -1;
    private LucenePostingsProto.Positions positions;
    private LucenePostingsProto.Payloads payloads;
    private boolean hasPositions;
    private boolean hasOffsets;
    private boolean hasPayloads;

    public LuceneOptimizedPostingsEnum(final String segmentName, final FieldInfo fieldInfo,
                                       final LuceneOptimizedBlockTermState state, final FDBDirectory directory,
                                       final boolean hasPositions, final boolean hasOffsets, final boolean hasPayloads) {
        this.segmentName = segmentName;
        this.fieldInfo = fieldInfo;
        this.state = state;
        this.directory = directory;
        this.hasPositions = hasPositions;
        this.hasOffsets = hasOffsets;
        this.hasPayloads = hasPayloads;
        // Documents are serialized within the term info (no need to fetch)
        this.documents = state.getDocuments();
    }

    @Override
    public int docID() {
        if (currentDoc == -1 || currentDoc == NO_MORE_DOCS) {
            return currentDoc;
        }
        assert currentDoc < documents.getDocIdCount() : "overflow with position=" + currentDoc;
        return documents.getDocId(currentDoc);
    }

    @Override
    public int nextDoc() throws IOException {
        assert currentDoc != NO_MORE_DOCS : "Should not be called";
        currentDoc++;
        currentPosition = -1;
        positions = null;
        if (documents.getDocIdCount() <= currentDoc) { // Exhausted
            currentDoc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        return documents.getDocId(currentDoc);
    }

    @Override
    public int advance(final int target) throws IOException {
        // short circuit the search (this signals jump to end)
        if (target == NO_MORE_DOCS) {
            currentDoc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        return slowAdvance(target);
    }

    @Override
    public int freq() throws IOException {
        return documents.getFreq(currentDoc);
    }

    @Override
    public int nextPosition() throws IOException {
        if (!hasPositions) {
            return -1;
        }
        ensureTermDocPositions(docID());
        return positions.getPosition(++currentPosition);
    }

    @Override
    public int startOffset() throws IOException {
        if (!hasOffsets) {
            return -1;
        }
        ensureTermDocPayloads(docID());
        return payloads.getStartOffset(currentPosition);
    }

    @Override
    public int endOffset() throws IOException {
        if (!hasOffsets) {
            return -1;
        }
        ensureTermDocPayloads(docID());
        return payloads.getEndOffset(currentPosition);
    }

    @Override
    public BytesRef getPayload() throws IOException {
        if (!hasPayloads) {
            return null;
        }
        ensureTermDocPayloads(docID());
        return new BytesRef(payloads.getPayload(currentPosition).toByteArray());
    }

    // Added methods from ImpactsEnum. There is no easy way to extend the LuceneOptimizedPostingsEnum class so this
    // class extends ImpactsEnum instead

    @Override
    public void advanceShallow(final int target) throws IOException {
        advance(target);
    }

    @Override
    public long cost() {
        // TODO
        return 0;
    }


    @Override
    public Impacts getImpacts() throws IOException {
        // TODO
        return new Impacts() {
            private final List<Impact> impacts = Collections.singletonList(
                    new Impact(currentDoc == NO_MORE_DOCS ? 0 : documents.getFreq(currentDoc), 1L));

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

    private void ensureTermDocPositions(int docId) throws IOException {
        if (positions == null) {
            byte[] posBytes = this.directory.getTermDocumentPositions(segmentName, fieldInfo.number, state.getOrd(), docId);
            final LucenePostingsProto.Positions result = LucenePostingsProto.Positions.parseFrom(posBytes);
            if (result.getPositionCount() != freq()) {
                throw new IOException("Index is Corrupted: number of positions does not match freq " +
                        segmentName + ":" + fieldInfo.number + ":" + state.getOrd() + ":" + docId);
            }
            this.positions = result;
        }
    }

    private void ensureTermDocPayloads(int docId) throws IOException {
        if (payloads == null) {
            byte[] payloadBytes = this.directory.getTermDocumentPayloads(segmentName, fieldInfo.number, state.getOrd(), docId);
            final LucenePostingsProto.Payloads result = LucenePostingsProto.Payloads.parseFrom(payloadBytes);
            if (result.getPayloadCount() != freq()) {
                throw new IOException("Index is Corrupted: number of payloads does not match freq " +
                        segmentName + ":" + fieldInfo.number + ":" + state.getOrd() + ":" + docId);
            }
            this.payloads = result;
        }
    }
}
