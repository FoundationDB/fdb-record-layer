/*
 * SizeStatisticsGroupingCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProviderBySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that aggregates size information about a subspace, "grouped by" the sub-subspaces of that "root" space.
 * This class is an extension of {@link SizeStatisticsCollectorCursor}, in the sense that it has a generalized
 * function:
 * The {@link SizeStatisticsCollectorCursor} is returning the same result as this class would in case of group-by
 * depth of 0. When using a non-0 group-by depth, this cursor would return a collection of subspace statistics broken down by
 * the constituents of the subspace key values.
 * The cursor performs a full scan of the subspace backing whatever it is collecting statistics on. It
 * should therefore be run relatively sparingly, and it should not be expected to finish within a single
 * transaction.
 * Note that this class makes no attempt at keeping its results transactional. As a result, if this is run on an
 * actively mutating data set, there are no guarantees that the values it returns were ever actually true for any
 * version in the transaction history. However, as long as the data set is not too volatile, it should produce an
 * approximate answer for the statistics it yields.
 * <p>
 * This class could be used to replace {@link SizeStatisticsCollectorCursor} in the future but for
 * backwards-compatibility both would remain in the codebase.
 * <p>
 * A note about implementation: This cursor is implemented as a scanner across the entire subspace, adding sizes and
 * grouping as it goes along. This has the advantage of scanning all subspaces regardless of the keys enclosed within.
 * Another implementation could be considered, where specific subspaces are to be scanned, being more efficient, but less
 * flexible, as it would require figuring out which subspaces actually exist. An example of such approach can be made to
 * understand the differences between RECORD and INDEX subspaces when dealing with a Store, to provide a response that
 * knows the semantics of such subspaces.
 * <p>
 * A note about security: scanning subspaces may reveal sensitive information. Specifically, in the case of value
 * indexes, the constituents of the key may reveal the values indexed by the index. DO NOT use this tool without proper
 * authentication and authorization in place that ensures data integrity.
 */
@API(API.Status.EXPERIMENTAL)
public class SizeStatisticsGroupingCursor implements RecordCursor<SizeStatisticsGroupedResults> {

    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final ScanProperties scanProperties;
    private final int aggregationDepth;

    /** The inner cursor. Its lifecycle is the same as this cursor (lazily initialized). */
    @Nullable
    private RecordCursor<KeyValue> innerCursor;
    private byte[] kvCursorContinuation;
    /** The subspace future that will be resolved prior to the inner cursor iteration (Lazily initialized). */
    @Nullable
    private CompletableFuture<Subspace> subspaceFuture;

    /**
     * The current grouping key in progress.
     * When reaching a limit, this key would also be packed in the continuation and continued from.
     */
    private Tuple currentGroupingKey;
    /**
     * The intermediate result of the cursor.
     * Since the subspaces are ordered, aggregating will complete one sub-subspace before continuing to the next.
     * When reaching a limit, this result would also be packed in the continuation and continued from.
     */
    private SizeStatisticsResults intermediateResults;
    /**
     * The next complete result that can be returned.
     */
    @Nullable
    private RecordCursorResult<SizeStatisticsGroupedResults> nextStatsResult;

    private boolean closed;

    private SizeStatisticsGroupingCursor(@Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordContext context,
                                         @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation, final int aggregationDepth) {
        this.subspaceProvider = subspaceProvider;
        this.context = context;
        this.scanProperties = scanProperties;
        this.aggregationDepth = aggregationDepth;
        this.closed = false;

        if (continuation == null) {
            this.intermediateResults = new SizeStatisticsResults();
            kvCursorContinuation = null; // Just to emphasize that the inner cursor starts from the beginning
            // currentGroupingKey will be initialized once we get the first result
        } else {
            try {
                // if this is a continuation update stats with partial values then get the underlying cursor's continuation
                RecordCursorProto.SizeStatisticsGroupingContinuation statsContinuation = RecordCursorProto.SizeStatisticsGroupingContinuation.parseFrom(continuation);
                if (SizeStatisticsGroupingContinuation.isLastResultContinuation(statsContinuation)) {
                    // The last result has been sent, mark as done
                    this.nextStatsResult = RecordCursorResult.exhausted();
                } else {
                    // Can continue the inner cursor
                    intermediateResults = SizeStatisticsResults.fromProto(Objects.requireNonNull(statsContinuation.getPartialResults()));
                    kvCursorContinuation = Objects.requireNonNull(statsContinuation.getUnderlyingContinuation()).toByteArray(); //underlying KV cursor continues here
                    currentGroupingKey = Tuple.fromBytes(Objects.requireNonNull(statsContinuation.getCurrentGroupingKey()).toByteArray());
                }
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("Error parsing SizeStatisticsGroupingContinuation continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
            }
        }
    }

    /**
     * Create a statistics collector cursor of all keys used by a given {@link FDBRecordStore}.
     * This includes records, indexes, and other meta-data.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a cursor for collecting statistics of that store
     */
    @Nonnull
    public static SizeStatisticsGroupingCursor ofStore(@Nonnull FDBRecordStore store,
                                                       @Nonnull FDBRecordContext context,
                                                       @Nonnull ScanProperties scanProperties,
                                                       @Nullable byte[] continuation,
                                                       int aggregationDepth) {
        return new SizeStatisticsGroupingCursor(store.getSubspaceProvider(), context, scanProperties, continuation, aggregationDepth);
    }

    /**
     * Create a statistics collector cursor of all records stored within a given {@link FDBRecordStore}.
     * This only looks at the records stored by that store, not any indexes.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the records of that store
     */
    @Nonnull
    public static SizeStatisticsGroupingCursor ofRecords(@Nonnull FDBRecordStore store,
                                                         @Nonnull FDBRecordContext context,
                                                         @Nonnull ScanProperties scanProperties,
                                                         @Nullable byte[] continuation,
                                                         int aggregationDepth) {
        return new SizeStatisticsGroupingCursor(new SubspaceProviderBySubspace(store.recordsSubspace()), context, scanProperties, continuation, aggregationDepth);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param indexName the name of the index to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsGroupingCursor ofIndex(@Nonnull FDBRecordStore store,
                                                       @Nonnull String indexName,
                                                       @Nonnull FDBRecordContext context,
                                                       @Nonnull ScanProperties scanProperties,
                                                       @Nullable byte[] continuation,
                                                       int aggregationDepth) {
        final RecordMetaData metaData = store.getRecordMetaData();
        return ofIndex(store, metaData.getIndex(indexName), context, scanProperties, continuation, aggregationDepth);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param index the index to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsGroupingCursor ofIndex(@Nonnull FDBRecordStore store,
                                                       @Nonnull Index index,
                                                       @Nonnull FDBRecordContext context,
                                                       @Nonnull ScanProperties scanProperties,
                                                       @Nullable byte[] continuation,
                                                       int aggregationDepth) {
        return new SizeStatisticsGroupingCursor(new SubspaceProviderBySubspace(store.indexSubspace(index)), context, scanProperties, continuation, aggregationDepth);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link Subspace}.
     *
     * @param subspace the subspace to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given subspace
     */
    @Nonnull
    public static SizeStatisticsGroupingCursor ofSubspace(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context,
                                                          @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation, int aggregationDepth) {
        return new SizeStatisticsGroupingCursor(new SubspaceProviderBySubspace(subspace), context, scanProperties, continuation, aggregationDepth);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<SizeStatisticsGroupedResults>> onNext() {
        // Initialize the inner cursor on the first call
        if (subspaceFuture == null) {
            subspaceFuture = subspaceProvider.getSubspaceAsync(context);
            innerCursor = new LazyCursor<>(
                    subspaceFuture.thenApply(sub ->
                            KeyValueCursor.Builder
                                    .withSubspace(sub)
                                    .setContext(context)
                                    .setContinuation(kvCursorContinuation)
                                    .setScanProperties(scanProperties)
                                    .build()),
                    getExecutor());
        }

        if (nextStatsResult != null) {
            // if this cursor instance has previously hit a limit then keep returning the same result
            if (!nextStatsResult.hasNext()) {
                return CompletableFuture.completedFuture(nextStatsResult);
            }
            // Similar to the constructor, this protects when we reached the end calling onNext
            // here we can assume there is a result so the continuation is of the right type
            if (((SizeStatisticsGroupingContinuation)nextStatsResult.getContinuation()).isLastResultContinuation()) {
                nextStatsResult = RecordCursorResult.exhausted();
                return CompletableFuture.completedFuture(nextStatsResult);
            }
        }
        // iterate until next result can be returned or the cursor is done
        return subspaceFuture.thenCompose(subspace ->
                AsyncUtil.whileTrue(
                        () -> innerCursor.onNext().thenApply(nextKv ->
                                // set state of cursor, return false when done
                                handleOneItem(subspace, nextKv)),
                        getExecutor())
                .thenApply(ignore -> nextStatsResult));
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return context.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    @Override
    public void close() {
        closed = true;
        innerCursor.close();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Nonnull
    private Boolean handleOneItem(final Subspace subspace, final RecordCursorResult<KeyValue> nextKv) {
        if (nextKv.hasNext()) {
            final KeyValue keyValue = Objects.requireNonNull(nextKv.get());
            Tuple nextGroupingKey = groupingKeyFrom(subspace, keyValue.getKey());

            // Account for the edge case of the first value we ever get, where currentGroupingKey is NULL
            // Aggregate the data to the partial results and continue the iteration
            if ((currentGroupingKey == null) || !groupBreak(currentGroupingKey, nextGroupingKey)) {
                intermediateResults.updateStatistics(keyValue);
                currentGroupingKey = nextGroupingKey;
                return true;
            } else {
                // group break - finalize and return current result. Initialize new group for the next cursor.
                final SizeStatisticsGroupedResults currentResult = new SizeStatisticsGroupedResults(currentGroupingKey, intermediateResults);
                intermediateResults = new SizeStatisticsResults();
                intermediateResults.updateStatistics(keyValue);
                // return a result with the current complete group and a continuation with the next partial group
                currentGroupingKey = nextGroupingKey;
                nextStatsResult = RecordCursorResult.withNextValue(currentResult, new SizeStatisticsGroupingContinuation(nextKv, intermediateResults, currentGroupingKey));
                return false;
            }
        } else {
            if (nextKv.getNoNextReason() == NoNextReason.SOURCE_EXHAUSTED) {
                // Send the last result with a continuation that will then turn into END
                nextStatsResult = RecordCursorResult.withNextValue(new SizeStatisticsGroupedResults(currentGroupingKey, intermediateResults),
                        SizeStatisticsGroupingContinuation.LAST_RESULT_CONTINUATION);
            } else {
                // the underlying cursor did not produce a row but there are more, return a continuation and propagate the underlying no next reason
                nextStatsResult = RecordCursorResult.withoutNextValue(new SizeStatisticsGroupingContinuation(nextKv, intermediateResults, currentGroupingKey),
                        nextKv.getNoNextReason());
            }
            return false;
        }
    }

    private Tuple groupingKeyFrom(final Subspace subspace, final byte[] key) {
        // Ungrouped - no group breaks - all elements fall into the same group
        if (aggregationDepth == 0) {
            return TupleHelpers.EMPTY;
        }
        // unpack removes the subspace prefix from the key and return the remaining Tuple elements
        final Tuple unpacked = subspace.unpack(key);
        // It is possible that the unpacked Tuple is shorter than the requested depth. Not all subspaces have the same key depth either.
        // In that case, return what we have.
        int actualDepth = Math.min(aggregationDepth, unpacked.size());
        // return as any elements from the front of the remaining Tuple to match the depth
        return TupleHelpers.subTuple(unpacked, 0, actualDepth);
    }

    private boolean groupBreak(final Tuple currentGroupingKey, final Tuple nextGroupingKey) {
        return (!Objects.equals(currentGroupingKey, nextGroupingKey));
    }
}
