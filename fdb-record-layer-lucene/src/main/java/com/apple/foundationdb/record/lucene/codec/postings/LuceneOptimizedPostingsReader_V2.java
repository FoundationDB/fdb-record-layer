/*
 * LuceneOptimizedPostingsReader_V2.java
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
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static com.apple.foundationdb.record.lucene.codec.postings.LuceneCodecUtil.unwrapDirectory;

public class LuceneOptimizedPostingsReader_V2 extends PostingsReaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsReader_V2.class);
    private final SegmentReadState segmentReadState;
    private final FDBDirectory directory;
    private Tuple key;

    public LuceneOptimizedPostingsReader_V2(final SegmentReadState segmentReadState) {
        this.segmentReadState = segmentReadState;
        this.key = Tuple.from(segmentReadState.segmentInfo.name);
        this.directory = unwrapDirectory(segmentReadState.directory);
    }

    @Override
    public void init(final IndexInput termsIn, final SegmentReadState segmentReadState) throws IOException {

    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new LuceneOptimizedBlockTermState();
    }

    @Override
    public void decodeTerm(final DataInput in, final FieldInfo fieldInfo, final BlockTermState state, final boolean absolute) throws IOException {
        // ?
    }

    @Override
    public PostingsEnum postings(final FieldInfo fieldInfo, final BlockTermState state, final PostingsEnum reuse, final int flags) throws IOException {
        boolean indexHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        if (indexHasPositions == false || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false) {
            return new LuceneOptimizedPostingsEnum(key.add(fieldInfo.number).add(state.ord), LucenePostingsProto.Documents.parseFrom(directory.getTermDocuments(
                    key.add(fieldInfo.number).add(state.ord))), directory, false, false, false);

        } else {
            byte[] termDocs = directory.getTermDocuments(
                    key.add(fieldInfo.number).add(state.ord));
            if (termDocs == null) {
                throw new IOException("termDocs Cannot be Null" + " -> " + state.ord + "field?" + fieldInfo.number + " segment" + segmentReadState.segmentInfo.name);
            }
            return new LuceneOptimizedPostingsEnum(key.add(fieldInfo.number).add(state.ord), LucenePostingsProto.Documents.parseFrom(directory.getTermDocuments(
                    key.add(fieldInfo.number).add(state.ord))), directory, true, false, false);
        }
    }

    @Override
    public ImpactsEnum impacts(final FieldInfo fieldInfo, final BlockTermState state, final int flags) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("impacts [segment={}, field={}, totalTermFreq={}, term={}]", segmentReadState.segmentInfo.name, fieldInfo.number, state.totalTermFreq, state.ord);
        }
        final boolean indexHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        final boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        final boolean indexHasPayloads = fieldInfo.hasPayloads();

        if (indexHasPositions == false || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false) {
            return new LuceneOptimizedPostingsEnum(key.add(fieldInfo.number).add(state.ord), LucenePostingsProto.Documents.parseFrom(directory.getTermDocuments(
                    key.add(fieldInfo.number).add(state.ord))), directory, false, false, false);
        }

        if (indexHasPositions &&
            PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) &&
            (indexHasOffsets == false || PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS) == false) &&
            (indexHasPayloads == false || PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS) == false)) {
                return new LuceneOptimizedPostingsEnum(key.add(fieldInfo.number).add(state.ord), LucenePostingsProto.Documents.parseFrom(directory.getTermDocuments(
                    key.add(fieldInfo.number).add(state.ord))), directory, true, false, false);
        }
        return new LuceneOptimizedPostingsEnum(key.add(fieldInfo.number).add(state.ord), LucenePostingsProto.Documents.parseFrom(directory.getTermDocuments(
                key.add(fieldInfo.number).add(state.ord))), directory, true, true, true);
    }

    @Override
    public void checkIntegrity() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return super.getChildResources();
    }
}
