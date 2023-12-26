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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import com.google.protobuf.ByteString;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public class LuceneOptimizedPostingsFieldsConsumer extends FieldsConsumer {
    private final LuceneOptimizedPostingsWriter postingsWriter;
    private final FDBDirectory directory;
    private final FieldInfos fieldInfos;
    private final int maxDoc;
    private final String segmentName;

    public LuceneOptimizedPostingsFieldsConsumer(final SegmentWriteState state, final LuceneOptimizedPostingsWriter postingsWriter) {
        this.postingsWriter = postingsWriter;
        this.directory = FDBDirectoryUtils.getFDBDirectory(state.directory);
        this.fieldInfos = state.fieldInfos;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.segmentName = state.segmentInfo.name;
    }

    public void write(Fields fields, NormsProducer norms) throws IOException {

        // TODO: Create output

        String lastField = null;
        for (String field : fields) {
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;

            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            LuceneOptimizedPostingsFieldsConsumer.TermsWriter termsWriter = new LuceneOptimizedPostingsFieldsConsumer.TermsWriter(fieldInfos.fieldInfo(field));
            while (true) {
                BytesRef term = termsEnum.next();
                //if (DEBUG) System.out.println("BTTW: next term " + term);

                if (term == null) {
                    break;
                }

                //if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" + brToString(term));
                termsWriter.write(term, termsEnum, norms);
            }

            termsWriter.finish();

            //if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
        }
    }

    public void close() throws IOException {
        // close the file output, postings writer?
    }

    private class TermsWriter {
        private final FieldInfo fieldInfo;

        private long numTerms;
        final FixedBitSet docsSeen;
        long sumTotalTermFreq;
        long sumDocFreq;
        private BytesRef minTerm;
        private BytesRef maxTerm;

        public TermsWriter(final FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            docsSeen = new FixedBitSet(maxDoc);
            postingsWriter.setField(fieldInfo);
        }

        public void write(final BytesRef text, final TermsEnum termsEnum, final NormsProducer norms) throws IOException {
            // TODO: return narrow type
            LuceneOptimizedBlockTermState state = (LuceneOptimizedBlockTermState)postingsWriter.writeAndSaveTerm(text, termsEnum, docsSeen, norms);
            if (state != null) {
                assert state.getDocFreq() != 0;
                assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.getTotalTermFreq() >= state.getDocFreq() : "postingsWriter=" + postingsWriter;
                directory.writePostingsTerm(segmentName, fieldInfo.number, text, state.getTermInfo().toByteArray());

                sumDocFreq += state.getDocFreq();
                sumTotalTermFreq += state.getTotalTermFreq();
                numTerms++;
                if (minTerm == null) {
                    minTerm = text;
                }
                maxTerm = text;
            }
        }

        public void finish() {
            // Write Term metadata
            if (numTerms > 0) {
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
                directory.writePostingsTermMetadata(segmentName, fieldInfo.number, builder.build().toByteArray());
            } else {
                assert sumTotalTermFreq == 0 || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
                assert sumDocFreq == 0;
                assert docsSeen.cardinality() == 0;
            }
        }
    }
}
