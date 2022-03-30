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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that groups incoming records by the given grouping criteria.
 *
 * @param <M> the record type that holds the actual data
 */
@API(API.Status.EXPERIMENTAL)
public class AggregateCursor<M extends Message> implements RecordCursor<Object> {
    // Inner cursor to provide record inflow
    @Nonnull
    private final RecordCursor<QueryResult> inner;
    // group aggregator to break incoming records into groups
    @Nonnull
    private final StreamGrouping<M> streamGrouping;
    // Previous record processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousResult;
    // Previous non-empty record processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousValidResult;

    public AggregateCursor(@Nonnull RecordCursor<QueryResult> inner, @Nonnull final StreamGrouping<M> streamGrouping) {
        this.inner = inner;
        this.streamGrouping = streamGrouping;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<Object>> onNext() {
        if (previousResult != null && !previousResult.hasNext()) {
            // post-done termination condition: Keep returning terminal element after inner is exhausted.
            return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
        }

        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            previousResult = innerResult;
            if (!innerResult.hasNext()) {
                streamGrouping.finalizeGroup();
                return false;
            } else {
                previousValidResult = innerResult;
                final Object currentObject = Objects.requireNonNull(innerResult.get()).getObject();
                boolean groupBreak = streamGrouping.apply(currentObject);
                return (!groupBreak);
            }
        }), getExecutor()).thenApply(vignore -> {
            if ((previousValidResult == null) && (!previousResult.hasNext())) {
                // Edge case where there are no records at all
                if (streamGrouping.isResultOnEmpty()) {
                    return RecordCursorResult.withNextValue(streamGrouping.getCompletedGroupResult(), RecordCursorStartContinuation.START);
                } else {
                    return RecordCursorResult.exhausted();
                }
            }
            // Use the last valid result for the continuation as we need non-terminal one here.
            RecordCursorContinuation continuation = previousValidResult.getContinuation();
            return RecordCursorResult.withNextValue(streamGrouping.getCompletedGroupResult(), continuation);
        });
    }

    @Override
    public void close() {
        inner.close();
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
