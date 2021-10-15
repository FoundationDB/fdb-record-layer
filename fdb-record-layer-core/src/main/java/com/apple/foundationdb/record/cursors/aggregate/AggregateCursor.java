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
 * A cursor that groups incoming records by the given grouping keys.
 *
 * @param <M> the record type that holds the actual data
 */
@API(API.Status.EXPERIMENTAL)
public class AggregateCursor<M extends Message> implements RecordCursor<QueryResult> {
    // Inner cursor to provide record inflow
    @Nonnull
    private final RecordCursor<QueryResult> inner;
    // grouping to break incoming records into groups
    @Nonnull
    private final StreamGrouping<M> streamGrouping;
    // Previous result processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousResult;
    // Whether any records have been seen for this query (behavior is different for empty record streams)
    private boolean seenRecords;
    // Whether the current iteration ended with a group break (or else, we need to continue the group on the next request)
    private boolean groupBreak = false;

    public AggregateCursor(@Nonnull RecordCursor<QueryResult> inner, @Nonnull final StreamGrouping<M> streamGrouping, @Nonnull AggregateCursorContinuation continuation) {
        this.inner = inner;
        this.streamGrouping = streamGrouping;
        streamGrouping.setContinuationState(continuation.getGroupingKeyStates(), continuation.getAccumulatorStates());
        seenRecords = continuation.seenRecords();
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
            if (innerResult.hasNext()) {
                seenRecords = true;
                FDBQueriedRecord<M> record = innerResult.get().getQueriedRecord(0);
                groupBreak = streamGrouping.apply(record);
                return (!groupBreak);
            } else {
                if (sourceExhausted(innerResult)) {
                    // When input is exhausted, finalize the current group
                    streamGrouping.finalizeGroup();
                }
                // Reset the groupBreak. This will only be required when the same plan is reused.
                groupBreak = false;
                return false;
            }
        }), getExecutor()).thenApply(vignore -> {
            if (sourceExhausted(previousResult)) {
                if ((!seenRecords) && streamGrouping.hasGroupingCriteria()) {
                    // "No records Case #1": When there are some grouping criteria, return an empty set
                    return RecordCursorResult.exhausted();
                } else {
                    // Have seen records, return the current group
                    // Also "No records case #2": When there are NO grouping criteria, return a single result (e.g. with count of 0)
                    List<Object> groupResult = streamGrouping.getCompletedGroupResult();
                    QueryResult queryResult = QueryResult.of(groupResult);
                    return RecordCursorResult.withNextValue(queryResult, calcContinuation());
                }
            }
            if (groupBreak) {
                // Finalized a group - return with the result and a continuation for it
                List<Object> groupResult = streamGrouping.getCompletedGroupResult();
                QueryResult queryResult = QueryResult.of(groupResult);
                return RecordCursorResult.withNextValue(queryResult, calcContinuation());
            }
            // Inner cursor done but not exhausted, no group break: Have more records, return continuation only
            return RecordCursorResult.withoutNextValue(calcContinuation(), previousResult.getNoNextReason());
        });
    }

    private boolean sourceExhausted(final RecordCursorResult<QueryResult> result) {
        return (!result.hasNext()) && (result.getNoNextReason() == NoNextReason.SOURCE_EXHAUSTED);
    }

    @Nonnull
    private RecordCursorContinuation calcContinuation() {
        List<AggregateCursorContinuation.ContinuationGroupingKeyState> groupingKeyStates = streamGrouping.getGroupingKeyStates();
        List<AggregateCursorContinuation.ContinuationAccumulatorState> accumulatorStates = streamGrouping.getAccumulatorStates();
        return new AggregateCursorContinuation(seenRecords, groupingKeyStates, accumulatorStates, previousResult.getContinuation().toBytes());
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
