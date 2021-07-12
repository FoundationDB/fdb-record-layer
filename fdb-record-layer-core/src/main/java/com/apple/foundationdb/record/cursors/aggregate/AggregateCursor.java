/*
 * AggregateCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors.aggregate;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that groups incoming records by the given grouping criteria.
 *
 * @param <T> the type of elements the cursor iterates over
 * @param <M> the record type that holds the actual data
 */
@API(API.Status.EXPERIMENTAL)
public class AggregateCursor<T extends FDBRecord<M>, M extends Message> implements RecordCursor<T> {
    // Inner cursor to provide record inflow
    @Nonnull
    private final RecordCursor<T> inner;
    // group aggregator to break incoming records into groups
    @Nonnull
    private final GroupAggregator<T, M> groupAggregator;
    // Previous record processed by this cursor
    @Nullable
    private RecordCursorResult<T> previousRecord;

    public AggregateCursor(@Nonnull RecordCursor<T> inner, @Nonnull final GroupAggregator<T, M> groupAggregator) {
        this.inner = inner;
        this.groupAggregator = groupAggregator;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        // post-done termination condition: Keep returning terminal element after inner is exhausted.
        if (previousRecord != null && !previousRecord.hasNext()) {
            return CompletableFuture.completedFuture(previousRecord);
        }

        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            previousRecord = innerResult;
            boolean groupBreak = groupAggregator.apply(innerResult);
            return (!groupBreak);
        }), getExecutor()).thenApply(vignore -> {
            List<Object> result = groupAggregator.getCompletedGroupResult();
            // TODO: This should be changed once the return types are decided on
            return previousRecord;
        });
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        return null;
        //        if (nextFuture == null) {
        //            nextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        //        }
        //        return nextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public T next() {
        return null;
//        if (!hasNext()) {
//            throw new NoSuchElementException();
//        }
//        nextFuture = null;
//        mayGetContinuation = true;
//        return nextResult.get();
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    @Deprecated
    public byte[] getContinuation() {
        return null;
//        IllegalContinuationAccessChecker.check(mayGetContinuation);
//        return nextResult.getContinuation().toBytes();
    }

    @Nonnull
    @Override
    @Deprecated
    public NoNextReason getNoNextReason() {
        return null;
//        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
//        if (nextFuture != null) {
//            nextFuture.cancel(false);
//            nextFuture = null;
//        }
//        inner.close();
    }


    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

}
