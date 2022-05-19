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

import com.apple.foundationdb.record.RecordCursorContinuation;
import com.google.protobuf.ByteString;
import org.apache.lucene.search.ScoreDoc;

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

    public static LuceneCursorContinuation fromScoreDoc(ScoreDoc scoreDoc) {
        return new LuceneCursorContinuation(LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                .setDoc(scoreDoc.doc)
                .setShard(scoreDoc.shardIndex)
                .setScore(scoreDoc.score)
                .build()
        );
    }
}
