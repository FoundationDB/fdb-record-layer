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
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
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
    @Nullable
    // when previousResult = row x, lastResult = row (x-1); when previousResult = null, lastResult = null
    private RecordCursorResult<QueryResult> lastResult;
    // Previous non-empty record processed by this cursor
    @Nullable
    private RecordCursorResult<QueryResult> previousValidResult;
    @Nullable
    private RecordCursorProto.PartialAggregationResult partialAggregationResult;
    @Nullable
    private final byte[] continuation;
    private final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode;

    public AggregateCursor(@Nonnull RecordCursor<QueryResult> inner,
                           @Nonnull final StreamGrouping<M> streamGrouping,
                           final boolean isCreateDefaultOnEmpty,
                           @Nullable byte[] continuation,
                           final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
        this.inner = inner;
        this.streamGrouping = streamGrouping;
        this.isCreateDefaultOnEmpty = isCreateDefaultOnEmpty;
        this.continuation = continuation;
        this.serializationMode = serializationMode;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
        if (previousResult != null && !previousResult.hasNext()) {
            // we are done
            return CompletableFuture.completedFuture(RecordCursorResult.withoutNextValue(new AggregateCursorContinuation(previousResult.getContinuation(), serializationMode),
                    previousResult.getNoNextReason()));
        }

        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            lastResult = previousResult;
            previousResult = innerResult;
            if (!innerResult.hasNext()) {
                if (!isNoRecords() || (isCreateDefaultOnEmpty && streamGrouping.isResultOnEmpty())) {
                    // the method streamGrouping.finalizeGroup() computes previousCompleteResult and resets the accumulator
                    partialAggregationResult = streamGrouping.finalizeGroup();
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
            // either innerResult.hasNext() = false; or groupBreak = true
            if (Verify.verifyNotNull(previousResult).hasNext()) {
                // in this case groupBreak = true, return aggregated result and continuation, partialAggregationResult = null
                // previousValidResult = null happens when 1st row of current scan != last row of last scan, results in groupBreak = true and previousValidResult = null
                RecordCursorContinuation c = previousValidResult == null ? new AggregateCursorContinuation(continuation, false, serializationMode) : new AggregateCursorContinuation(previousValidResult.getContinuation(), serializationMode);

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
                return RecordCursorResult.withNextValue(QueryResult.ofComputed(streamGrouping.getCompletedGroupResult()), c);
            } else {
                // innerResult.hasNext() = false, might stop in the middle of a group
                if (Verify.verifyNotNull(previousResult).getNoNextReason() == NoNextReason.SOURCE_EXHAUSTED) {
                    // exhausted
                    if (previousValidResult == null && partialAggregationResult == null) {
                        return RecordCursorResult.exhausted();
                    } else {
                        RecordCursorContinuation c = previousValidResult == null ? new AggregateCursorContinuation(continuation, false, serializationMode) : new AggregateCursorContinuation(previousValidResult.getContinuation(), serializationMode);
                        previousValidResult = previousResult;
                        return RecordCursorResult.withNextValue(QueryResult.ofComputed(streamGrouping.getCompletedGroupResult()), c);
                    }
                } else {
                    // stopped in the middle of a group
                    RecordCursorContinuation currentContinuation = new AggregateCursorContinuation(lastResult.getContinuation(), partialAggregationResult, serializationMode);
                    previousValidResult = previousResult;
                    return RecordCursorResult.withoutNextValue(currentContinuation, Verify.verifyNotNull(previousResult).getNoNextReason());
                }
            }
        });
    }


    private boolean isNoRecords() {
        return ((previousValidResult == null) && (!Verify.verifyNotNull(previousResult).hasNext()) && (streamGrouping.getPartialAggregationResult() == null));
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

    public static class AggregateCursorContinuation implements RecordCursorContinuation {
        @Nullable
        private final ByteString innerContinuation;

        @Nullable
        private final RecordCursorProto.PartialAggregationResult partialAggregationResult;

        @Nullable
        private RecordCursorProto.AggregateCursorContinuation cachedProto;

        private final boolean isEnd;
        private final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode;

        public AggregateCursorContinuation(@Nullable byte[] innerContinuation, boolean isEnd, @Nullable RecordCursorProto.PartialAggregationResult partialAggregationResult, final RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            this.innerContinuation = innerContinuation == null ? null : ByteString.copyFrom(innerContinuation);
            this.isEnd = isEnd;
            this.partialAggregationResult = partialAggregationResult;
            this.serializationMode = serializationMode;
        }

        public AggregateCursorContinuation(@Nullable byte[] innerContinuation, boolean isEnd, @Nullable RecordCursorProto.PartialAggregationResult partialAggregationResult) {
            this(innerContinuation, isEnd, partialAggregationResult, RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD);
        }

        public AggregateCursorContinuation(@Nonnull RecordCursorContinuation other, @Nullable RecordCursorProto.PartialAggregationResult partialAggregationResult, RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            this(other.toBytes(), other.isEnd(), partialAggregationResult, serializationMode);
        }

        public AggregateCursorContinuation(@Nullable byte[] innerContinuation, boolean isEnd, RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            this(innerContinuation, isEnd, null, serializationMode);
        }

        public AggregateCursorContinuation(@Nonnull RecordCursorContinuation other, RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            this(other, null, serializationMode);
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd() || innerContinuation == null) {
                return ByteString.EMPTY;
            }
            //if (true) {
            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD) {
                return innerContinuation;
            } else {
                return partialAggregationResult == null ? innerContinuation : toProto().toByteString();
            }
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD) {
                ByteString byteString = toByteString();
                //return byteString.isEmpty() ? null : byteString.toByteArray();
                return getInnerContinuation();
            } else {
                return partialAggregationResult == null ? getInnerContinuation() : toProto().toByteArray();
            }
        }

        @Override
        public boolean isEnd() {
            return isEnd;
        }

        @Nullable
        public byte[] getInnerContinuation() {
            return innerContinuation == null ? null : innerContinuation.toByteArray();
        }

        @Nullable
        public RecordCursorProto.PartialAggregationResult getPartialAggregationResult() {
            return partialAggregationResult;
        }

        @Nonnull
        private RecordCursorProto.AggregateCursorContinuation toProto() {
            if (cachedProto == null) {
                RecordCursorProto.AggregateCursorContinuation.Builder cachedProtoBuilder = RecordCursorProto.AggregateCursorContinuation.newBuilder();
                if (partialAggregationResult != null) {
                    cachedProtoBuilder.setPartialAggregationResults(partialAggregationResult);
                }
                if (innerContinuation != null) {
                    cachedProtoBuilder.setContinuation(innerContinuation);
                }
                cachedProto = cachedProtoBuilder.build();
            }
            return cachedProto;
        }

        public static AggregateCursorContinuation fromRawBytes(@Nullable byte[] rawBytes, RecordQueryStreamingAggregationPlan.SerializationMode serializationMode) {
            if (rawBytes == null) {
                return new AggregateCursorContinuation(null, true, serializationMode);
            }
            if (serializationMode == RecordQueryStreamingAggregationPlan.SerializationMode.TO_OLD) {
                return new AggregateCursorContinuation(rawBytes, false, serializationMode);
            }
            try {
                RecordCursorProto.AggregateCursorContinuation continuationProto = RecordCursorProto.AggregateCursorContinuation.parseFrom(rawBytes);
                if (continuationProto.hasContinuation() && continuationProto.hasPartialAggregationResults()) {
                    return new AggregateCursorContinuation(continuationProto.getContinuation().toByteArray(), false, continuationProto.getPartialAggregationResults());
                } else {
                    return new AggregateCursorContinuation(rawBytes, false, serializationMode);
                }
            } catch (InvalidProtocolBufferException ipbe) {
                return new AggregateCursorContinuation(rawBytes, false, serializationMode);
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
