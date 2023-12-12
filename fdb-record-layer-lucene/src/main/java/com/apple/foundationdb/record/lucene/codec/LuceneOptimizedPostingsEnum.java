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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LuceneOptimizedPostingsEnum extends ImpactsEnum {

    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final LuceneOptimizedBlockTermState state;
    private FDBDirectory directory;

    // current doc ordinal within the term
    private int currentDoc = -1;
    // current position ordinal within the doc
    private int currentPosition = -1;
    private final LazyOpener<TermDocuments> termDocumentsSupplier;
    private DocumentPositions positions;
    private boolean hasPositions;

    public LuceneOptimizedPostingsEnum(final String segmentName, final FieldInfo fieldInfo,
                                       final LuceneOptimizedBlockTermState state, final FDBDirectory directory,
                                       final boolean hasPositions) {
        this.segmentName = segmentName;
        this.fieldInfo = fieldInfo;
        this.state = state;
        this.directory = directory;
        this.hasPositions = hasPositions;
        termDocumentsSupplier = LazyOpener.supply(() -> {
            byte[] docBytes = this.directory.getTermDocuments(this.segmentName, this.fieldInfo.number, this.state.getOrd());
            if (docBytes == null) {
                throw new IllegalStateException("docBytes cannot be null for docs provider (TODO better message");
            }
            return new TermDocuments(docBytes);
        });
    }

    @Override
    public int freq() throws IOException {
        return termDocumentsSupplier.get().getFreq(currentDoc);
    }

    @Override
    public int nextPosition() throws IOException {
        if (!hasPositions) {
            return -1;
        }
        if (positions == null) {
            // TODO: Maybe turn into AtomicReference?
            positions = readTermDocPositions(docID());
        }
        return positions.getPosition(++currentPosition);
    }

    @Override
    public int startOffset() throws IOException {
        // TODO
        return 0;
    }

    @Override
    public int endOffset() throws IOException {
        //TODO
        return 0;
    }

    @Override
    public BytesRef getPayload() throws IOException {
        // TODO
        return null;
    }

    @Override
    public int docID() {
        if (currentDoc == -1 || currentDoc == NO_MORE_DOCS) {
            return currentDoc;
        }
        final TermDocuments documents = termDocumentsSupplier.getUnchecked();
        assert currentDoc < documents.getDocIdCount() : "overflow with position=" + currentDoc;
//        if (LOG.isInfoEnabled()) {
//            LOG.info("docID() [docID {}, position={}]", documents.getDocId(docPosition), docPosition);
//        }
        return documents.getDocId(currentDoc);
    }

    @Override
    public int nextDoc() throws IOException {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("nextDoc called on {}", docPosition);
//        }
        assert currentDoc != NO_MORE_DOCS: "Should not be called";
        currentDoc++;
        currentPosition = -1;
        positions = null;
        final TermDocuments documents = termDocumentsSupplier.get();
        if (documents.getDocIdCount() <= currentDoc) { // Exhausted
//            if (LOG.isInfoEnabled()) {
//                LOG.info("nextDoc exhausted on {}", docPosition);
//            }
//            Thread.dumpStack();
            currentDoc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        return documents.getDocId(currentDoc);
    }

    @Override
    public int advance(final int target) throws IOException {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("advance {}", target);
//        }
        // TODO: Can this be done more efficiently?
        while(docID() < target) {
            if (nextDoc() == NO_MORE_DOCS) {
                return NO_MORE_DOCS;
            }
        }
        return docID();
    }

    // Added methods from ImpactsEnum. There is no easy way to extend the LuceneOptimizedPostingsEnum class so this
    // class extends ImpactsEnum instead

    @Override
    public void advanceShallow(final int target) throws IOException {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("advanceShallow {}", target);
//        }
        advance(target);
    }

    @Override
    public long cost() {
        return 0;
    }


    @Override
    public Impacts getImpacts() throws IOException {
//        if (LOG.isInfoEnabled()) {
//            LOG.info("getImpacts with position {}", docPosition);
//            Thread.dumpStack();
//        }
        return new Impacts() {
            private final List<Impact> impacts = Collections.singletonList(
                    new Impact(currentDoc == NO_MORE_DOCS ? 0 : termDocumentsSupplier.getUnchecked().getFreq(currentDoc), 1L));

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


    private DocumentPositions readTermDocPositions(int docId) throws IOException {
        byte[] posBytes = this.directory.getTermDocumentPositions(segmentName, fieldInfo.number, state.getOrd(), docId);
        final DocumentPositions positions = new DocumentPositions(posBytes);
        if (positions.getPositionCount() != freq()) {
            // TODO: Add log infos (field/term/docid)
            throw new IOException("Index is Corrupted: number of positions does not match freq");
        }
        return positions;
    }

    private class TermDocuments {
        public TermDocuments(final byte[] docBytes) {
        }

        public int getFreq(final int currentDoc) {
            return 0;
        }

        public int getDocIdCount() {
            return 0;
        }

        public int getDocId(final int currentDoc) {
            return 0;
        }
    }

    private class DocumentPositions {
        public DocumentPositions(final byte[] posBytes) {
        }

        public int getPosition(final int i) {
            return 0;
        }

        public int getPositionCount() {
            return 0;
        }
    }
}