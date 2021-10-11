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
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that groups incoming records by the given grouping criteria.
 *
 * @param <M> the record type that holds the actual data
 */
@API(API.Status.EXPERIMENTAL)
public class AggregateCursor<M extends Message> implements RecordCursor<QueryResult> {
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
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        if (previousResult != null && !previousResult.hasNext()) {
            // post-done termination condition: Keep returning terminal element after inner is exhausted.
            return CompletableFuture.completedFuture(previousResult);
        }

        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            previousResult = innerResult;
            if (!innerResult.hasNext()) {
                streamGrouping.finalizeGroup();
                return false;
            } else {
                previousValidResult = innerResult;
                FDBQueriedRecord<M> record = innerResult.get().getQueriedRecord(0);
                boolean groupBreak = streamGrouping.apply(record);
                return (!groupBreak);
            }
        }), getExecutor()).thenApply(vignore -> {
            // Done streaming inner cursor
            if ((previousValidResult == null) && (!previousResult.hasNext()) && (streamGrouping.hasGroupingCriteria())) {
                // "No records Case #1": When there are some grouping criteria, return an empty set
                return previousResult;
            }
            // Have more records, return group and continue
            // Also "No records case #2": When there are NO grouping criteria, return a single result (e.g. with count of 0)
            List<Object> groupResult = streamGrouping.getCompletedGroupResult();
            QueryResult queryResult = QueryResult.of(groupResult);
            RecordCursorContinuation continuation = calcContinuation();
            return RecordCursorResult.withNextValue(queryResult, continuation);
        });
    }

    @Nonnull
    private RecordCursorContinuation calcContinuation() {
        if (previousValidResult == null) {
            // We have no previous result, and need a non-terminal one, use empty.
            return ByteArrayContinuation.fromNullable(new byte[0]);
        } else {
            // Use the last valid result for the continuation as we need non-terminal one here.
            return previousValidResult.getContinuation();
        }
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
