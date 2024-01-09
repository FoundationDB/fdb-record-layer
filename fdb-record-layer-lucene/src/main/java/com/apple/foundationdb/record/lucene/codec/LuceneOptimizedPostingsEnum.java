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
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

/**
 * FDB-optimized {@link PostingsEnum} {@link ImpactsEnum}
 */
public class LuceneOptimizedPostingsEnum extends ImpactsEnum {

    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final LuceneOptimizedBlockTermState state;
    private final TermDocuments termDocuments;
    private FDBDirectory directory;

    // current doc ordinal within the term
    private int currentDoc = -1;
    // current position ordinal within the doc
    private int currentPosition = -1;
    private DocumentPositions positions;
    private DocumentPayloads payloads;
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
        this.termDocuments = new TermDocuments(state.getTermInfo().getDocuments());
    }

    @Override
    public int docID() {
        if (currentDoc == -1 || currentDoc == NO_MORE_DOCS) {
            return currentDoc;
        }
        assert currentDoc < termDocuments.getDocIdCount() : "overflow with position=" + currentDoc;
        return termDocuments.getDocId(currentDoc);
    }

    @Override
    public int nextDoc() throws IOException {
        assert currentDoc != NO_MORE_DOCS: "Should not be called";
        currentDoc++;
        currentPosition = -1;
        positions = null;
        if (termDocuments.getDocIdCount() <= currentDoc) { // Exhausted
            currentDoc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        return termDocuments.getDocId(currentDoc);
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
        return termDocuments.getFreq(currentDoc);
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
        return payloads.getPayload(currentPosition);
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
                    new Impact(currentDoc == NO_MORE_DOCS ? 0 : termDocuments.getFreq(currentDoc), 1L));

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
            final DocumentPositions result = new DocumentPositions(posBytes);
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
            final DocumentPayloads result = new DocumentPayloads(payloadBytes);
            if (result.getPayloadCount() != freq()) {
                throw new IOException("Index is Corrupted: number of payloads does not match freq " +
                        segmentName + ":" + fieldInfo.number + ":" + state.getOrd() + ":" + docId);
            }
            this.payloads = result;
        }
    }

    private class TermDocuments {
        private final LucenePostingsProto.Documents documents;

        public TermDocuments(final LucenePostingsProto.Documents documents) {
            this.documents = documents;
        }

        public int getFreq(final int doc) {
            return documents.getFreq(doc);
        }

        public int getDocIdCount() {
            return documents.getDocIdCount();
        }

        public int getDocId(final int doc) {
            return documents.getDocId(doc);
        }
    }

    private class DocumentPositions {
        private final LucenePostingsProto.Positions positions;

        public DocumentPositions(final byte[] posBytes) {
            try {
                this.positions = LucenePostingsProto.Positions.parseFrom(posBytes);
            } catch (InvalidProtocolBufferException e) {
                throw new UncheckedIOException(e);
            }
        }

        public int getPosition(final int position) {
            return positions.getPosition(position);
        }

        public int getPositionCount() {
            return positions.getPositionCount();
        }
    }

    private class DocumentPayloads {
        private final LucenePostingsProto.Payloads payloads;

        public DocumentPayloads(final byte[] payloadBytes) {
            try {
                this.payloads = LucenePostingsProto.Payloads.parseFrom(payloadBytes);
            } catch (InvalidProtocolBufferException e) {
                throw new UncheckedIOException(e);
            }
        }

        public int getStartOffset(final int position) {
            return payloads.getStartOffset(position);
        }

        public BytesRef getPayload(final int position) {
            return new BytesRef(payloads.getPayload(position).toByteArray());
        }

        public int getEndOffset(final int position) {
            return payloads.getEndOffset(position);
        }

        public int getPayloadCount() {
            return payloads.getPayloadCount();
        }
    }
}
