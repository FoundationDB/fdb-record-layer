/*
 * TerribleIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link IndexMaintainer} that fails when trying to update.
 */
public class TerribleIndexMaintainer extends IndexMaintainer {
    protected TerribleIndexMaintainer(IndexMaintainerState state) {
        super(state);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                            @Nullable byte[] continuation,
                                            @Nonnull ScanProperties scanProperties) {
        return RecordCursor.empty();
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord, @Nullable FDBIndexableRecord<M> newRecord) {
        try {
            if (newRecord != null && oldRecord != null) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new UnsupportedOperationException("TerribleIndexMaintainer cannot update"));
                return future;
            } else if (newRecord != null) {
                Key.Evaluated res = state.index.getRootExpression().evaluate(newRecord).get(0);
                if (((Number) res.toList().get(0)).intValue() % 2 == 0) {
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.completeExceptionally(new UnsupportedOperationException("TerribleIndexMaintainer does not implement update for evens on insert"));
                    return future;
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } else if (oldRecord != null) {
                Key.Evaluated res = state.index.getRootExpression().evaluate(oldRecord).get(0);
                if (((Number) res.toList().get(0)).intValue() % 2 != 0) {
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.completeExceptionally(new UnsupportedOperationException("TerribleIndexMaintainer does not implement update for odds on remove"));
                    return future;
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } else {
                return CompletableFuture.completedFuture(null);
            }
        } catch (RuntimeException e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        return AsyncUtil.DONE;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        throw new UnsupportedOperationException("Terrible index cannot scan uniqueness violations");
    }

    @Override
    public CompletableFuture<Void> clearUniquenessViolations() {
        throw new UnsupportedOperationException("Terrible index cannot clear uniqueness violations");
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                           @Nullable ScanProperties scanProperties) {
        return RecordCursor.empty();
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return false;
    }

    @Nonnull
    @Override
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("TerribleIndexMaintainer does not implement evaluateRecordFunction"));
        return future;
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        return false;
    }

    @Override
    public boolean isIdempotent() {
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        return CompletableFuture.completedFuture(false);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                               @Nonnull TupleRange range,
                                                               @Nonnull IsolationLevel isolationLevel) {
        throw new IllegalStateException("this is not an aggregate index");
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        return false;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return AsyncUtil.DONE;
    }

    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        CompletableFuture<IndexOperationResult> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("TerribleIndexMaintainer does not implement performOperation"));
        return future;
    }

    @Override
    public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record) {
        throw new UnsupportedOperationException("TerribleIndexMaintainer does not implement evaluateIndex");
    }

    @Override
    public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord) {
        throw new UnsupportedOperationException("TerribleIndexMaintainer does not implement filteredIndexEntries");
    }

    @Override
    public CompletableFuture<Void> mergeIndex() {
        return AsyncUtil.DONE;
    }
}
