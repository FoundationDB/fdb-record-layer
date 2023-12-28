/*
 * LuceneOptimizedPostingsWriter.java
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
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import com.google.protobuf.ByteString;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public class LuceneOptimizedPostingsWriter extends PushPostingsWriterBase {

    private final FDBDirectory directory;
    private final String segmentName;

    // current term ordinal (globally unique incrementing number)
    private long currentTermOrd = -1;
    private BytesRef currentTermText;
    private WriterDocuments documents;
    private WriterPositions positions;
    private WriterPayloads payloads;
    private int lastDocID;
    private int docCount = 0;
    // TODO
    private NumericDocValues norms;

    public LuceneOptimizedPostingsWriter(final SegmentWriteState state) {
        this.directory = FDBDirectoryUtils.getFDBDirectory(state.directory);
        this.segmentName = state.segmentInfo.name;
    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new LuceneOptimizedBlockTermState();
    }

    @Override
    public void init(final IndexOutput termsOut, final SegmentWriteState state) throws IOException {
    }

    // Inherit setField(FieldInfo fieldInfo)

    /**
     * Delegate to the {@link PushPostingsWriterBase#writeTerm(BytesRef, TermsEnum, FixedBitSet, NormsProducer)} in
     * order to store the term text locally.
     */
    public BlockTermState writeAndSaveTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms) throws IOException {
        currentTermText = term;
        return super.writeTerm(term, termsEnum, docsSeen, norms);
    }

    @Override
    public void startTerm(NumericDocValues norms) {
        currentTermOrd++;
        documents = new WriterDocuments();
        this.norms = norms;
        lastDocID = 0;
//        competitiveFreqNormAccumulator.clear();
    }

    @Override
    public void startDoc(int docID, int termDocFreq) throws IOException {
        documents.addDocument(docID, termDocFreq);
        if (writePositions) {
            positions = new WriterPositions();
            if (writePayloads || writeOffsets) {
                payloads = new WriterPayloads();
            }
        }

        if (docID < 0 || (docCount > 0 && (docID - lastDocID) <= 0)) {
            throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )", "docCount");
        }
        lastDocID = docID;
        docCount++;

//        long norm;
//        if (fieldHasNorms) {
//            boolean found = norms.advanceExact(docID);
//            if (found == false) {
//                // This can happen if indexing hits a problem after adding a doc to the
//                // postings but before buffering the norm. Such documents are written
//                // deleted and will go away on the first merge.
//                norm = 1L;
//            } else {
//                norm = norms.longValue();
//                assert norm != 0 : docID;
//            }
//        } else {
//            norm = 1L;
//        }
//
//        competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
    }


    @Override
    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
        if (position > IndexWriter.MAX_POSITION) {
            throw new CorruptIndexException("position=" + position + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + ")", "docCount");
        }
        if (position < 0) {
            throw new CorruptIndexException("position=" + position + " is < 0", "position");
        }

        positions.addPosition(position);
        if (writePayloads) {
            if (payload == null || payload.length == 0) {
                // This adds an empty payload. TODO: Seems like necessary - or maybe not - looks like it may be ignored in finishTerm
                payloads.addEmptyPayload();
            } else {
                payloads.addPayload(payload);
            }
        }
        if (writeOffsets) {
            assert endOffset >= startOffset;
            // TODO: Check if > -1?
            payloads.addOffset(startOffset, endOffset);
        }
    }

    @Override
    public void finishDoc() throws IOException {
        if (positions != null) {
            directory.writePostingsPositions(segmentName, fieldInfo.number, currentTermOrd, documents.getLastDocId(), positions.asBytes());
        }
        if (payloads != null) {
            directory.writePostingsPayloads(segmentName, fieldInfo.number, currentTermOrd, documents.getLastDocId(), payloads.asBytes());
        }
        // reset the positions and payloads so that we are in consistent state to continue or end the iterations
        positions = null;
        payloads = null;
    }

    /**
     * Called when we are done adding docs to this term.
     * The term (text and info) will be stored once the method returns.
     */
    @Override
    public void finishTerm(BlockTermState _state) throws IOException {
        LuceneOptimizedBlockTermState state = (LuceneOptimizedBlockTermState)_state;
        assert state.docFreq > 0;
        assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

        directory.writePostingsDocuments(segmentName, fieldInfo.number, currentTermOrd, documents.asBytes());
        documents = null;

        // Complement the contents of the state
        state.ord = currentTermOrd;
        // Create a term info from the individual fields
        LucenePostingsProto.TermInfo termInfo = createTermInfo(state);
        // Copy the info back to the state (so now it has the protobufs)
        state.copyFrom(currentTermText, termInfo);
        docCount = 0;
    }

    @Override
    public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    private LucenePostingsProto.TermInfo createTermInfo(final LuceneOptimizedBlockTermState state) {
        return LucenePostingsProto.TermInfo.newBuilder()
                // Use the public variables since the state does not have the protobufs yet
                .setDocFreq(state.docFreq)
                .setTotalTermFreq(state.totalTermFreq)
                .setOrd(state.ord)
                .build();
    }

    private class WriterDocuments {
        private LucenePostingsProto.Documents.Builder builder;
        private int lastDocId = -1;

        public WriterDocuments() {
            builder = LucenePostingsProto.Documents.newBuilder();
        }

        public void addDocument(int docId, int freq) {
            builder.addDocId(docId);
            builder.addFreq(freq);
            lastDocId = docId;
        }

        public byte[] asBytes() {
            return builder.build().toByteArray();
        }

        public int getLastDocId() {
            return lastDocId;
        }
    }

    private class WriterPositions {
        LucenePostingsProto.Positions.Builder builder;

        public WriterPositions() {
            builder = LucenePostingsProto.Positions.newBuilder();
        }

        public void addPosition(int porition) {
            builder.addPosition(porition);
        }

        public byte[] asBytes() {
            return builder.build().toByteArray();
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }

    private class WriterPayloads {
        LucenePostingsProto.Payloads.Builder builder;

        public WriterPayloads() {
            builder = LucenePostingsProto.Payloads.newBuilder();
        }

        // TODO: Not sure if this is necessary
        public void addEmptyPayload() {
            builder.addPayload(ByteString.EMPTY);
        }

        public void addPayload(final BytesRef payload) {
            builder.addPayload(ByteString.copyFrom(payload.bytes, payload.offset, payload.length));
        }

        public void addOffset(int startOffset, int endOffset) {
            builder.addStartOffset(startOffset);
            builder.addEndOffset(endOffset);
        }

        public byte[] asBytes() {
            return builder.build().toByteArray();
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
