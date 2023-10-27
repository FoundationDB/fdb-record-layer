/*
 * LuceneOptimizedPushPostingsWriter.java
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
import com.google.protobuf.ByteString;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.apple.foundationdb.record.lucene.codec.postings.LuceneCodecUtil.unwrapDirectory;

public class LuceneOptimizedPushPostingsWriter extends PushPostingsWriterBase {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPushPostingsWriter.class);
    final SegmentWriteState state;
    private LucenePostingsProto.Documents.Builder documents;
    private LucenePostingsProto.Payloads.Builder payloads;
    private LucenePostingsProto.Positions.Builder positions;
    private final FDBDirectory directory;
    private Tuple tupleKey;
    private int docId;
    private int freq;
    private BytesRef currentTerm;
    private long termOrd = -1;
    public LuceneOptimizedPushPostingsWriter(final SegmentWriteState state) {
        this.state = state;
        this.directory = unwrapDirectory(state.directory);
        this.tupleKey = Tuple.from(state.segmentInfo.name);
    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new LuceneOptimizedBlockTermState(null, null);
    }

    @Override
    public void startTerm(final NumericDocValues norms) throws IOException {
        termOrd++;
        documents = LucenePostingsProto.Documents.newBuilder();
    }

    @Override
    public void finishTerm(final BlockTermState state) throws IOException {
        state.ord = termOrd;
        ((LuceneOptimizedBlockTermState) state).setTerm(currentTerm);
        ((LuceneOptimizedBlockTermState) state).setTermInfo(LucenePostingsProto.TermInfo.newBuilder()
                .setDocFreq(state.docFreq).setTotalTermFreq(state.totalTermFreq).setOrd(state.ord).build());
        directory.writeTermDocuments(tupleKey.add(fieldInfo.number).add(state.ord), documents.build().toByteArray());
    }

    @Override
    public void startDoc(final int docID, final int freq) throws IOException {
        this.docId = docID;
        this.freq = freq;
        documents.addDocId(docID).addFreq(freq);
        positions = LucenePostingsProto.Positions.newBuilder();
        payloads = LucenePostingsProto.Payloads.newBuilder();
    }

    @Override
    public void addPosition(final int position, final BytesRef payload, final int startOffset, final int endOffset) throws IOException {
        positions.addPosition(position);
        if (payload != null) {
            payloads.addPayload(ByteString.copyFrom(payload.bytes, payload.offset, payload.length));
        }
        if (startOffset != -1) {
            payloads.addStartOffset(startOffset);
            payloads.addEndOffset(endOffset);
        }
    }

    @Override
    public void finishDoc() throws IOException {
        if (freq > positions.getPositionCount()) {
            throw new UnsupportedOperationException("Error");
        }
        directory.writeTermPositions(tupleKey.add(fieldInfo.number).add(termOrd).add(docId), positions.build().toByteArray());
        directory.writeTermPayloads(tupleKey.add(fieldInfo.number).add(termOrd).add(docId), payloads.build().toByteArray());
    }

    @Override
    public void init(final IndexOutput termsOut, final SegmentWriteState state) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("init");
        }
    }

    @Override
    public void encodeTerm(final DataOutput out, final FieldInfo fieldInfo, final BlockTermState state, final boolean absolute) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("encodeTerm with state " + state);
        }
    }

    public BlockTermState writeAndRecordTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms) throws IOException {
        currentTerm = term;
        return super.writeTerm(term, termsEnum, docsSeen, norms);
    }

    @Override
    public void close() throws IOException {
    }
}
