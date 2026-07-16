/*
 * NoOpIndexMaintainer.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer that doesn't do anything.
 */
@API(API.Status.UNSTABLE)
public class NoOpIndexMaintainer extends IndexMaintainer {
    protected NoOpIndexMaintainer(IndexMaintainerState state) {
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
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        return AsyncUtil.DONE;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        return AsyncUtil.DONE;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return RecordCursor.empty();
    }

    @Override
    public CompletableFuture<Void> clearUniquenessViolations() {
        if (state.index.isUnique()) {
            throw new RecordCoreException(state.index.getName() + " is unique and cannot clear uniqueness violations;");
        }
        return AsyncUtil.DONE;
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation, @Nullable ScanProperties scanProperties) {
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
        return unsupportedRecordFunction(function);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                               @Nonnull TupleRange range,
                                                               @Nonnull IsolationLevel isolationLevel) {
        return unsupportedAggregateFunction(function);
    }

    @Override
    public boolean isIdempotent() {
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        return AsyncUtil.READY_FALSE;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        return true;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return AsyncUtil.DONE;
    }

    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        return CompletableFuture.completedFuture(new IndexOperationResult() {
        });
    }

    @Override
    public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record) {
        return Collections.emptyList();
    }

    @Override
    public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord) {
        return null;
    }

    @Override
    public CompletableFuture<Void> mergeIndex() {
        return AsyncUtil.DONE;
    }
}
