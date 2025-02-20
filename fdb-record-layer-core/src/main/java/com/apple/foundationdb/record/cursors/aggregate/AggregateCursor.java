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
import com.google.common.base.Verify;
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
public class AggregateCursor<M extends Message> implements RecordCursor<QueryResult> {
    // Inner cursor to provide record inflow
    @Nonnull
    private final RecordCursor<QueryResult> inner;
    // group aggregator to break incoming records into groups
    @Nonnull
    private final StreamGrouping<M> streamGrouping;
    private final boolean isCreateDefaultOnEmpty;
    // Previous record processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousResult;
    // Previous non-empty record processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousValidResult;

    public AggregateCursor(@Nonnull RecordCursor<QueryResult> inner,
                           @Nonnull final StreamGrouping<M> streamGrouping,
                           boolean isCreateDefaultOnEmpty) {
        this.inner = inner;
        this.streamGrouping = streamGrouping;
        this.isCreateDefaultOnEmpty = isCreateDefaultOnEmpty;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        if (previousResult != null && !previousResult.hasNext()) {
            // we are done
            return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
        }

        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            previousResult = innerResult;
            if (!innerResult.hasNext()) {
                if (!isNoRecords() || (isCreateDefaultOnEmpty && streamGrouping.isResultOnEmpty())) {
                    streamGrouping.finalizeGroup();
                }
                return false;
            } else {
                final QueryResult queryResult = Objects.requireNonNull(innerResult.get());
                boolean groupBreak = streamGrouping.apply(queryResult);
                if (!groupBreak) {
                    // previousValidResult is the last row before group break, it sets the continuation
                    previousValidResult = innerResult;
                }
                return (!groupBreak);
            }
        }), getExecutor()).thenApply(vignore -> {
            if (isNoRecords()) {
                // Edge case where there are no records at all
                if (isCreateDefaultOnEmpty && streamGrouping.isResultOnEmpty()) {
                    return RecordCursorResult.withNextValue(QueryResult.ofComputed(streamGrouping.getCompletedGroupResult()), RecordCursorStartContinuation.START);
                } else {
                    return RecordCursorResult.exhausted();
                }
            }
            // Use the last valid result for the continuation as we need non-terminal one here.
            RecordCursorContinuation continuation = Verify.verifyNotNull(previousValidResult).getContinuation();
            /*
            * Update the previousValidResult to the next continuation even though it hasn't been returned. This is to return the correct continuation when there are single-element groups.
            * Below is an example that shows how continuation(previousValidResult) moves:
            * Initial: previousResult = null, previousValidResult = null
            row0      groupKey0      groupBreak = False         previousValidResult = row0  previousResult = row0
            row1      groupKey0      groupBreak = False         previousValidResult = row1  previousResult = row1
            row2      groupKey1      groupBreak = True          previousValidResult = row1  previousResult = row2
            * returns result (groupKey0, continuation = row1), and set previousValidResult = row2
            *
            * Now there are 2 scenarios, 1) the current iteration continues; 2) the current iteration stops
            * In scenario 1, the iteration continues, it gets to row3:
            row3      groupKey2      groupBreak = True          previousValidResult = row2  previousResult = row3
            * returns result (groupKey1, continuation = row2), and set previousValidResult = row3
            *
            * In scenario 2, a new iteration starts from row2 (because the last returned continuation = row1), and set initial previousResult = null, previousValidResult = null:
            row2      groupKey1      groupBreak = False         previousValidResult = row2  previousResult = row2
            * (Note that because a new iteration starts, groupBreak = False for row2.)
            row3      groupKey2      groupBreak = True          previousValidResult = row2  previousResult = row3
            * returns result (groupKey1, continuation = row2), and set previousValidResult = row3
            *
            * Both scenarios returns the correct result, and continuation are both set to row3 in the end, row2 is scanned twice if a new iteration starts.
             */
            previousValidResult = previousResult;
            return RecordCursorResult.withNextValue(QueryResult.ofComputed(streamGrouping.getCompletedGroupResult()), continuation);
        });
    }
    
    private boolean isNoRecords() {
        return ((previousValidResult == null) && (!Verify.verifyNotNull(previousResult).hasNext()));
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
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
