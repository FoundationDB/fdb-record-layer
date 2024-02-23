/*
 * LuceneCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Continuation from scanning a Lucene index. This wraps the LuceneIndexContinuation protobuf message,
 * which contains enough information to use the Lucene
 * {@link org.apache.lucene.search.IndexSearcher#searchAfter(ScoreDoc, org.apache.lucene.search.Query, int) searchAfter}
 * feature to resume a query.
 */
class LuceneCursorContinuation implements RecordCursorContinuation {
    @Nonnull
    private final LuceneContinuationProto.LuceneIndexContinuation protoContinuation;

    @SuppressWarnings("squid:S3077") // Byte array is immutable once created, so does not need to use atomic array
    private volatile byte[] byteContinuation;

    private LuceneCursorContinuation(@Nonnull LuceneContinuationProto.LuceneIndexContinuation protoContinuation) {
        this.protoContinuation = protoContinuation;
    }

    @Nullable
    @Override
    public byte[] toBytes() {
        if (byteContinuation == null) {
            synchronized (this) {
                if (byteContinuation == null) {
                    byteContinuation = toByteString().toByteArray();
                }
            }
        }
        return byteContinuation;
    }

    @Nonnull
    @Override
    public ByteString toByteString() {
        return protoContinuation.toByteString();
    }

    @Override
    public boolean isEnd() {
        return false;
    }

    public static LuceneCursorContinuation fromScoreDoc(ScoreDoc scoreDoc,
                                                        @Nullable Integer partitionId,
                                                        @Nullable Long partitionTimestamp) {
        LuceneContinuationProto.LuceneIndexContinuation.Builder builder = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                .setDoc(scoreDoc.doc)
                .setShard(scoreDoc.shardIndex)
                .setScore(scoreDoc.score);

        if (partitionId != null) {
            builder.setPartitionId(partitionId);
        }
        if (partitionTimestamp != null) {
            builder.setPartitionTimestamp(partitionTimestamp);
        }
        if (scoreDoc instanceof FieldDoc) {
            for (Object field : ((FieldDoc)scoreDoc).fields) {
                final LuceneContinuationProto.LuceneIndexContinuation.Field.Builder value = builder.addFieldsBuilder();
                if (field instanceof BytesRef) {
                    value.setB(ZeroCopyByteString.wrap(((BytesRef)field).bytes));
                } else if (field instanceof String) {
                    value.setS((String)field);
                } else if (field instanceof Float) {
                    value.setF((Float)field);
                } else if (field instanceof Double) {
                    value.setD((Double)field);
                } else if (field instanceof Integer) {
                    value.setI((Integer)field);
                } else if (field instanceof Long) {
                    value.setL((Long)field);
                } else {
                    throw new RecordCoreException("Unknown sort field type");
                }
            }
        }
        return new LuceneCursorContinuation(builder.build());
    }

    @Nonnull
    public static ScoreDoc toScoreDoc(@Nonnull LuceneContinuationProto.LuceneIndexContinuation luceneIndexContinuation) {
        int doc = (int)luceneIndexContinuation.getDoc();
        float score = luceneIndexContinuation.getScore();
        int shard = (int)luceneIndexContinuation.getShard();
        int nfields = luceneIndexContinuation.getFieldsCount();
        if (nfields == 0) {
            return new ScoreDoc(doc, score, shard);
        }
        Object[] fields = new Object[nfields];
        for (int i = 0; i < nfields; i++) {
            Object value;
            LuceneContinuationProto.LuceneIndexContinuation.Field field = luceneIndexContinuation.getFields(i);
            switch (field.getValueCase()) {
                case I:
                    value = field.getI();
                    break;
                case L:
                    value = field.getL();
                    break;
                case F:
                    value = field.getF();
                    break;
                case D:
                    value = field.getD();
                    break;
                case S:
                    value = field.getS();
                    break;
                case B:
                    value = new BytesRef(field.getB().toByteArray());
                    break;
                case VALUE_NOT_SET:
                default:
                    value = null;
                    break;
            }
            fields[i] = value;
        }
        return new FieldDoc(doc, score, fields, shard);
    }
}
