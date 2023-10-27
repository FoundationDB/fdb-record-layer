/*
 * LuceneOptimizedPostingsFieldsConsumer.java
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
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.apple.foundationdb.record.lucene.codec.postings.LuceneCodecUtil.unwrapDirectory;

/**
 * This class wraps a FieldsConsumer.
 *
 */
public class LuceneOptimizedPostingsFieldsConsumer extends FieldsConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedPostingsFieldsConsumer.class);

    private final LuceneOptimizedPushPostingsWriter postingsWriter;
    private final SegmentWriteState state;
    private final Tuple tupleKey;
    private final FDBDirectory directory;

    public LuceneOptimizedPostingsFieldsConsumer(final SegmentWriteState state) {
        this.postingsWriter = new LuceneOptimizedPushPostingsWriter(state);
        this.state = state;
        this.directory = unwrapDirectory(state.directory);
        this.tupleKey = Tuple.from(state.segmentInfo.name);
    }

    @Override
    public void merge(final MergeState mergeState, final NormsProducer norms) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("merge");
        }
        super.merge(mergeState, norms);
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

        String lastField = null;
        for(String field : fields) {
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }
            TermsEnum termsEnum = terms.iterator();
            TermsWriter termsWriter = new TermsWriter(state.fieldInfos.fieldInfo(field));
            while (true) {
                BytesRef term = termsEnum.next();
                if (term == null) {
                    break;
                }
                termsWriter.write(term, termsEnum, norms);
            }
            termsWriter.finish();
        }
    }




    @Override
    public void close() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("close");
        }
        postingsWriter.close();
    }

    private class TermsWriter {
        private final FieldInfo fieldInfo;
        private long numTerms;
        long sumTotalTermFreq;
        long sumDocFreq;
        BytesRef minTerm;
        BytesRef maxTerm;
        private final FixedBitSet docsSeen;

        TermsWriter(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
            this.docsSeen = new FixedBitSet(state.segmentInfo.maxDoc());
            postingsWriter.setField(fieldInfo);
        }

        private void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
            LuceneOptimizedBlockTermState state = (LuceneOptimizedBlockTermState) postingsWriter.writeAndRecordTerm(text, termsEnum, docsSeen, norms);
            if (state != null) {
                assert state.docFreq != 0;
                assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.totalTermFreq >= state.docFreq:
                        "postingsWriter=" + postingsWriter;
                directory.writeTerm(tupleKey.add(fieldInfo.number).add(text.bytes, text.offset, text.length),
                        state.getTermInfoBinary());
                sumDocFreq += state.docFreq;
                sumTotalTermFreq += state.totalTermFreq;
                numTerms++;
                if (minTerm == null) {
                    minTerm = text;
                }
                maxTerm = text;
            }
        }

        public void finish() throws IOException {
            if (numTerms > 0) {
                Tuple key = tupleKey.add(fieldInfo.number);
                LucenePostingsProto.TermMeta.Builder builder = LucenePostingsProto.TermMeta.newBuilder()
                        .setMaxTerm(ByteString.copyFrom(maxTerm.bytes, maxTerm.offset, maxTerm.length))
                        .setMinTerm(ByteString.copyFrom(minTerm.bytes, minTerm.offset, minTerm.length))
                        .setNumTerms(numTerms)
                        .setSumDocFreq(sumDocFreq)
                        .setCardinality(docsSeen.cardinality());
                assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
                if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
                    builder.setSumTotalFreq(sumTotalTermFreq);
                }
                directory.writeTermMetadata(key, builder.build().toByteArray());
            } else {
                assert sumTotalTermFreq == 0 || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
                assert sumDocFreq == 0;
                assert docsSeen.cardinality() == 0;
            }
        }

    }


    }
