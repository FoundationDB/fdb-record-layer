/*
 * IndexingByRecords.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 *  This indexer scans all records in the record store.
 */
@API(API.Status.INTERNAL)
public class IndexingByRecords extends IndexingBase {
    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingByRecords.class);
    @Nonnull private static final byte[] END_BYTES = new byte[]{(byte)0xff}; // this line should be gone in a future refactoring. Give RangeSet its privacy...

    @Nonnull private final TupleRange recordsRange;
    @Nonnull private static final IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = compileIndexingTypeStamp();

    IndexingByRecords(@Nonnull IndexingCommon common, @Nonnull OnlineIndexer.IndexingPolicy policy) {
        super(common, policy);
        this.recordsRange = computeRecordsRange();
    }

    @Override
    @Nonnull
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store) {
        return myIndexingTypeStamp;
    }

    @Nonnull
    static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp() {
        return
                IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS)
                        .build();
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "by records"
        );
    }

    @Nonnull
    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return buildEndpoints().thenCompose(tupleRange -> {
            if (tupleRange != null) {
                return buildRange(Key.Evaluated.fromTuple(tupleRange.getLow()), Key.Evaluated.fromTuple(tupleRange.getHigh()));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    @Nonnull
    private TupleRange computeRecordsRange() {
        Tuple low = null;
        Tuple high = null;
        for (RecordType recordType : common.getAllRecordTypes()) {
            if (!recordType.primaryKeyHasRecordTypePrefix() || recordType.isSynthetic()) {
                // If any of the types to build for does not have a prefix, give up.
                return TupleRange.ALL;
            }
            Tuple prefix = recordType.getRecordTypeKeyTuple();
            if (low == null) {
                low = high = prefix;
            } else {
                if (low.compareTo(prefix) > 0) {
                    low = prefix;
                }
                if (high.compareTo(prefix) < 0) {
                    high = prefix;
                }
            }
        }
        if (low == null) {
            return TupleRange.ALL;
        } else {
            // Both ends inclusive.
            return new TupleRange(low, high, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
        }
    }

    /**
     * Builds (transactionally) the endpoints of an index. What this means is that builds everything from the beginning of
     * the key space to the first record and everything from the last record to the end of the key space.
     * There won't be any records within these ranges (except for the last record of the record store), but
     * it does mean that any records in the future that get added to these ranges will correctly update
     * the index. This means, e.g., that if the workload primarily adds records to the record store
     * after the current last record (because perhaps the primary key is based off of an atomic counter
     * or the current time), running this method will be highly contentious, but once it completes,
     * the rest of the index build should happen without any more conflicts.
     *
     * This will return a (possibly null) {@link TupleRange} that contains the primary keys of the
     * first and last records within the record store. This can then be used to either build the
     * range right away or to then divy-up the remaining ranges between multiple agents working
     * in parallel if one desires.
     *
     * @param store the record store in which to rebuild the index
     * @param recordsScanned continues counter
     * @return a future that will contain the range of records in the interior of the record store
     */

    @Nonnull
    public CompletableFuture<TupleRange> buildEndpoints(@Nonnull FDBRecordStore store,
                                                        @Nullable AtomicLong recordsScanned) {
        final RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(common.getIndex()));
        if (TupleRange.ALL.equals(recordsRange)) {
            return buildEndpoints(store, rangeSet, recordsScanned);
        }
        // If records do not occupy whole range, first mark outside as built.
        final Range asRange = recordsRange.toRange();
        return CompletableFuture.allOf(
                rangeSet.insertRange(store.ensureContextActive(), null, asRange.begin),
                rangeSet.insertRange(store.ensureContextActive(), asRange.end, null))
                .thenCompose(vignore -> buildEndpoints(store, rangeSet, recordsScanned));
    }

    @Nonnull
    private CompletableFuture<TupleRange> buildEndpoints(@Nonnull FDBRecordStore store, @Nonnull RangeSet rangeSet,
                                                         @Nullable AtomicLong recordsScanned) {
        boolean isIdempotent = store.getIndexMaintainer(common.getIndex()).isIdempotent();
        final IsolationLevel isolationLevel =
                isIdempotent ?
                // If idempotent: since double indexing is harmless, we can use individual records protection instead of
                // a range conflict one - which means that new records, added to the range while indexing the SNAPSHOT,
                // will not cause a conflict during the commit. At worse, few records (if added after marking WRITE_ONLY but
                // before this method's query) will be re-indexed.
                IsolationLevel.SNAPSHOT :
                IsolationLevel.SERIALIZABLE;
        final ExecuteProperties limit1 = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1)
                .setIsolationLevel(isolationLevel)
                .build();
        final ScanProperties forward = new ScanProperties(limit1);

        RecordCursor<FDBStoredRecord<Message>> beginCursor = store.scanRecords(recordsRange, null, forward);
        CompletableFuture<Tuple> begin = beginCursor.onNext().thenCompose(result -> {
            if (result.hasNext()) {
                Tuple firstTuple = result.get().getPrimaryKey();
                return buildRange(store, null, firstTuple, recordsScanned).thenApply(vignore -> firstTuple);
            } else {
                // Empty range -- add the whole thing.
                return rangeSet.insertRange(store.ensureContextActive(), null, null).thenApply(bignore -> null);
            }
        });

        final ScanProperties backward = new ScanProperties(limit1, true);
        RecordCursor<FDBStoredRecord<Message>> endCursor = store.scanRecords(recordsRange, null, backward);
        CompletableFuture<Tuple> end = endCursor.onNext().thenCompose(result -> {
            if (result.hasNext()) {
                Tuple lastTuple = result.get().getPrimaryKey();
                return buildRange(store, lastTuple, null, recordsScanned).thenApply(vignore -> lastTuple);
            } else {
                // As the range is empty, the whole range needs to be added, but that is accomplished
                // by the above future, so this has nothing to do.
                return CompletableFuture.completedFuture(null);
            }
        });

        return begin.thenCombine(end, (firstTuple, lastTuple) -> {
            if (firstTuple == null || firstTuple.equals(lastTuple)) {
                return null;
            } else {
                return new TupleRange(firstTuple, lastTuple, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE);
            }
        });
    }

    /**
     * Builds (with a retry loop) the endpoints of an index. See the
     * {@link #buildEndpoints(FDBRecordStore, AtomicLong) buildEndpoints()} method that takes
     * an {@link FDBRecordStore} as its parameter for more details. This will retry on that function
     * until it gets a non-exceptional result and return the results back.
     *
     * @return a future that will contain the range of records in the interior of the record store
     */
    @Nonnull
    public CompletableFuture<TupleRange> buildEndpoints() {
        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildEndpoints");
        return buildCommitRetryAsync(this::buildEndpoints, false, additionalLogMessageKeyValues);
    }

    // Builds a range within a single transaction. It will look for the missing ranges within the given range and build those while
    // updating the range set.
    @Nonnull
    private CompletableFuture<Void> buildRange(@Nonnull FDBRecordStore store, @Nullable Tuple start, @Nullable Tuple end,
                                               @Nullable AtomicLong recordsScanned) {
        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(common.getIndex()));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), packOrNull(start), packOrNull(end)).iterator();
        return ranges.onHasNext().thenCompose(hasAny -> {
            if (hasAny) {
                return AsyncUtil.whileTrue(() -> {
                    Range range = ranges.next();
                    Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
                    Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
                    return CompletableFuture.allOf(
                            // All of the requested range without limit.
                            // In practice, this method works because it is only called for the endpoint ranges, which are empty and
                            // one long, respectively.
                            buildRangeOnly(store, rangeStart, rangeEnd, false, recordsScanned),
                            rangeSet.insertRange(store.ensureContextActive(), range, true)
                    ).thenCompose(vignore -> ranges.onHasNext());
                }, store.getExecutor());
            } else {
                return AsyncUtil.DONE;
            }
        });
    }

    /**
     * Builds (transactionally) the index by adding records with primary keys within the given range.
     * This will look for gaps of keys within the given range that haven't yet been rebuilt and then
     * rebuild only those ranges. As a result, if this method is called twice, the first time, it will
     * build whatever needs to be built, and then the second time, it will notice that there are no ranges
     * that need to be built, so it will do nothing. In this way, it is idempotent and thus safe to
     * use in retry loops.
     *
     * This method will fail if there is too much work to be done in a single transaction. If one wants
     * to handle building a range that does not fit in a single transaction, one should use the
     * {@link #buildRange(Key.Evaluated, Key.Evaluated) buildRange()}
     * function that takes an {@link FDBDatabase} as its first parameter.
     *
     * @param store the record store in which to rebuild the range
     * @param start the (inclusive) beginning primary key of the range to build (or <code>null</code> to go to the end)
     * @param end the (exclusive) end primary key of the range to build (or <code>null</code> to go to the end)
     * @return a future that will be ready when the build has completed
     */
    @Nonnull
    public CompletableFuture<Void> buildRange(@Nonnull FDBRecordStore store, @Nullable Key.Evaluated start, @Nullable Key.Evaluated end) {
        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(common.getIndex()));
        byte[] startBytes = packOrNull(convertOrNull(start));
        byte[] endBytes = packOrNull(convertOrNull(end));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), startBytes, endBytes).iterator();
        return ranges.onHasNext().thenCompose(hasNext -> {
            if (hasNext) {
                return AsyncUtil.whileTrue(() -> {
                    Range toBuild = ranges.next();
                    Tuple startTuple = Tuple.fromBytes(toBuild.begin);
                    Tuple endTuple = RangeSet.isFinalKey(toBuild.end) ? null : Tuple.fromBytes(toBuild.end);
                    AtomicReference<Tuple> currStart = new AtomicReference<>(startTuple);
                    return AsyncUtil.whileTrue(() ->
                            // Bold claim: this will never cause a RecordBuiltRangeException because of transactions.
                            buildUnbuiltRange(store, currStart.get(), endTuple, null).thenApply(realEnd -> {
                                if (realEnd != null && !realEnd.equals(endTuple)) {
                                    currStart.set(realEnd);
                                    return true;
                                } else {
                                    return false;
                                }
                            }), store.getExecutor()).thenCompose(vignore -> ranges.onHasNext());
                }, store.getExecutor());
            } else {
                return AsyncUtil.DONE;
            }
        });
    }

    /**
     * Builds (with a retry loop) the index by adding records with primary keys within the given range.
     * This will look for gaps of keys within the given range that haven't yet been rebuilt and then rebuild
     * only those ranges. It will also limit each transaction to the number of records specified by the
     * <code>limit</code> parameter of this class's constructor. In the case that that limit is too high (i.e.,
     * it can't make any progress or errors out on a non-retriable error like <code>transaction_too_large</code>,
     * this method will actually decrease the limit so that less work is attempted each transaction. It will
     * also rate limit itself as to not make too many requests per second.
     * <p>
     * Note that it does not have the protections (synchronized sessions and index state precondition) which are imposed
     * on buildIndexAsync() (or its variations), but it does use the created synchronized session if a
     * buildIndexAsync() is running on the {@link OnlineIndexer} simultaneously or this range build is used as
     * part of buildIndexAsync internally.
     * </p>
     * @param start the (inclusive) beginning primary key of the range to build (or <code>null</code> to go from the beginning)
     * @param end the (exclusive) end primary key of the range to build (or <code>null</code> to go to the end)
     * @return a future that will be ready when the build has completed
     */
    @Nonnull
    public CompletableFuture<Void> buildRange(@Nullable Key.Evaluated start, @Nullable Key.Evaluated end) {
        return buildRange(common.getRecordStoreBuilder().getSubspaceProvider(), start, end);
    }

    @Nonnull
    private CompletableFuture<Void> buildRange(@Nonnull SubspaceProvider subspaceProvider, @Nullable Key.Evaluated start, @Nullable Key.Evaluated end) {
        return getRunner().runAsync(context -> context.getReadVersionAsync().thenCompose(vignore ->
                subspaceProvider.getSubspaceAsync(context).thenCompose(subspace -> {
                    RangeSet rangeSet = new RangeSet(subspace.subspace(Tuple.from(FDBRecordStore.INDEX_RANGE_SPACE_KEY, common.getIndex().getSubspaceKey())));
                    byte[] startBytes = packOrNull(convertOrNull(start));
                    byte[] endBytes = packOrNull(convertOrNull(end));
                    Queue<Range> rangeDeque = new ArrayDeque<>();
                    ReadTransactionContext rtc = context.ensureActive();
                    return rangeSet.missingRanges(rtc, startBytes, endBytes)
                            .thenAccept(rangeDeque::addAll)
                            .thenCompose(vignore2 -> buildRanges(subspaceProvider, subspace, rangeSet, rangeDeque));
                })
        ));
    }

    @Nonnull
    private CompletableFuture<Void> buildRanges(SubspaceProvider subspaceProvider, @Nonnull Subspace subspace,
                                                RangeSet rangeSet, Queue<Range> rangeDeque) {
        return AsyncUtil.whileTrue(() -> {
            if (rangeDeque.isEmpty()) {
                return CompletableFuture.completedFuture(false); // We're done.
            }
            Range toBuild = rangeDeque.remove();

            // This only works if the things included within the rangeSet are serialized Tuples.
            Tuple startTuple = Tuple.fromBytes(toBuild.begin);
            Tuple endTuple = RangeSet.isFinalKey(toBuild.end) ? null : Tuple.fromBytes(toBuild.end);
            return buildUnbuiltRange(startTuple, endTuple)
                    .handle((realEnd, ex) -> handleBuiltRange(subspaceProvider, subspace, rangeSet, rangeDeque, startTuple, endTuple, realEnd, ex))
                    .thenCompose(Function.identity());
        }, getRunner().getExecutor());
    }

    @Nonnull
    private CompletableFuture<Boolean> handleBuiltRange(SubspaceProvider subspaceProvider, @Nonnull Subspace subspace,
                                                        RangeSet rangeSet, Queue<Range> rangeDeque,
                                                        Tuple startTuple, Tuple endTuple, Tuple realEnd,
                                                        Throwable ex) {
        final RuntimeException unwrappedEx = ex == null ? null : getRunner().getDatabase().mapAsyncToSyncException(ex);
        if (unwrappedEx == null) {
            if (realEnd != null && !realEnd.equals(endTuple)) {
                // We didn't make it to the end. Continue on to the next item.
                if (endTuple != null) {
                    rangeDeque.add(new Range(realEnd.pack(), endTuple.pack()));
                } else {
                    rangeDeque.add(new Range(realEnd.pack(), END_BYTES));
                }
            }
            return throttleDelayAndMaybeLogProgress(subspaceProvider, Arrays.asList(
                    LogMessageKeys.START_TUPLE, startTuple,
                    LogMessageKeys.END_TUPLE, endTuple,
                    LogMessageKeys.REAL_END, realEnd));
        } else {
            Throwable cause = unwrappedEx;
            while (cause != null) {
                if (cause instanceof OnlineIndexer.RecordBuiltRangeException) {
                    return rangeSet.missingRanges(getRunner().getDatabase().database(), startTuple.pack(), endTuple.pack())
                            .thenCompose(list -> {
                                rangeDeque.addAll(list);
                                return throttleDelayAndMaybeLogProgress(subspaceProvider, Arrays.asList(
                                        LogMessageKeys.REASON, "RecordBuiltRangeException",
                                        LogMessageKeys.START_TUPLE, startTuple,
                                        LogMessageKeys.END_TUPLE, endTuple,
                                        LogMessageKeys.REAL_END, realEnd));
                            });
                } else {
                    cause = cause.getCause();
                }
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.of("possibly non-fatal error encountered building range",
                        LogMessageKeys.RANGE_START, startTuple,
                        LogMessageKeys.RANGE_END, endTuple,
                        LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack())), ex);
            }
            throw unwrappedEx; // made it to the bottom, throw original exception
        }
    }

    // Helper function that works on Tuples instead of keys.
    @Nonnull
    private CompletableFuture<Tuple> buildUnbuiltRange(@Nonnull FDBRecordStore store, @Nullable Tuple start,
                                                       @Nullable Tuple end, @Nullable AtomicLong recordsScanned) {
        CompletableFuture<Tuple> buildFuture = buildRangeOnly(store, start, end, true, recordsScanned);

        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(common.getIndex()));
        byte[] startBytes = packOrNull(start);

        AtomicReference<Tuple> toReturn = new AtomicReference<>();
        return buildFuture.thenCompose(realEnd -> {
            toReturn.set(realEnd);
            return rangeSet.insertRange(store.ensureContextActive(), startBytes, packOrNull(realEnd), true);
        }).thenApply(changed -> {
            if (changed) {
                return toReturn.get();
            } else {
                throw new OnlineIndexer.RecordBuiltRangeException(start, end);
            }
        });
    }

    /**
     * Builds (transactionally) the index by adding records with primary keys within the given range.
     * This requires that the range is initially "unbuilt", i.e., no records within the given
     * range have yet been processed by the index build job. It is acceptable if there
     * are records within that range that have already been added to the index because they were
     * added to the store after the index was added in write-only mode but have not yet been
     * processed by the index build job.
     *
     * Note that this function is not idempotent in that if the first time this function runs, if it
     * fails with <code>commit_unknown_result</code> but the transaction actually succeeds, running this
     * function again will result in a {@link OnlineIndexer.RecordBuiltRangeException} being thrown the second
     * time. Retry loops used by the <code>OnlineIndexer</code> class that call this method
     * handle this contingency. For the most part, this method should only be used by those who know
     * what they are doing. It is included because it is less expensive to make this call if one
     * already knows that the range will be unbuilt, but the caller must be ready to handle the
     * circumstance that the range might be built the second time.
     *
     * Most users should use the
     * {@link #buildRange(FDBRecordStore, Key.Evaluated, Key.Evaluated) buildRange()}
     * method with the same parameters in the case that they want to build a range of keys into the index. That
     * method <i>is</i> idempotent, but it is slightly more costly as it firsts determines what ranges are
     * have not yet been built before building them.
     *
     * @param store the record store in which to rebuild the range
     * @param start the (inclusive) beginning primary key of the range to build (or <code>null</code> to start from the beginning)
     * @param end the (exclusive) end primary key of the range to build (or <code>null</code> to go to the end)
     * @return a future with the key of the first record not processed by this range rebuild
     * @throws OnlineIndexer.RecordBuiltRangeException if the given range contains keys already processed by the index build
     */
    @Nonnull
    public CompletableFuture<Key.Evaluated> buildUnbuiltRange(@Nonnull FDBRecordStore store,
                                                              @Nullable Key.Evaluated start,
                                                              @Nullable Key.Evaluated end) {
        return buildUnbuiltRange(store, start, end, null);
    }

    // just like the overload that doesn't take a recordsScanned
    @Nonnull
    private CompletableFuture<Key.Evaluated> buildUnbuiltRange(@Nonnull FDBRecordStore store,
                                                               @Nullable Key.Evaluated start, @Nullable Key.Evaluated end,
                                                               @Nullable AtomicLong recordsScanned) {
        return buildUnbuiltRange(store, convertOrNull(start), convertOrNull(end), recordsScanned)
                .thenApply(tuple -> (tuple == null) ? null : Key.Evaluated.fromTuple(tuple));
    }

    // Helper function with the same behavior as buildUnbuiltRange, but it works on tuples instead of primary keys.
    @Nonnull
    private CompletableFuture<Tuple> buildUnbuiltRange(@Nullable Tuple start, @Nullable Tuple end) {
        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildUnbuiltRange",
                LogMessageKeys.RANGE_START, start,
                LogMessageKeys.RANGE_END, end);

        return buildCommitRetryAsync((store, recordsScanned) -> buildUnbuiltRange(store, start, end, recordsScanned),
                true,
                additionalLogMessageKeyValues);
    }

    @VisibleForTesting
    @Nonnull
    CompletableFuture<Key.Evaluated> buildUnbuiltRange(@Nullable Key.Evaluated start, @Nullable Key.Evaluated end) {
        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildUnbuiltRange",
                LogMessageKeys.RANGE_START, start,
                LogMessageKeys.RANGE_END, end);
        return buildCommitRetryAsync((store, recordsScanned) -> buildUnbuiltRange(store, start, end, recordsScanned),
                true,
                additionalLogMessageKeyValues);
    }

    // Builds the index for all of the keys within a given range. This does not update the range set
    // associated with this index, so it is really designed to be a helper for other methods.
    @Nonnull
    private CompletableFuture<Tuple> buildRangeOnly(@Nonnull FDBRecordStore store,
                                                    @Nullable Tuple start, @Nullable Tuple end,
                                                    boolean respectLimit, @Nullable AtomicLong recordsScanned) {
        return buildRangeOnly(store, TupleRange.between(start, end), respectLimit, recordsScanned);
    }

    @Nonnull
    private CompletableFuture<Tuple> buildRangeOnly(@Nonnull FDBRecordStore store, @Nonnull TupleRange range,
                                                    boolean respectLimit, @Nullable AtomicLong recordsScanned) {
        validateSameMetadataOrThrow(store);
        Index index = common.getIndex();
        int limit = getLimit();
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        final boolean isIdempotent = maintainer.isIdempotent();
        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(
                        isIdempotent ?
                        IsolationLevel.SNAPSHOT :
                        IsolationLevel.SERIALIZABLE);
        if (respectLimit) {
            executeProperties.setReturnedRowLimit(limit + 1); // +1 allows continuation item
        }
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());
        final RecordCursor<FDBStoredRecord<Message>> cursor = store.scanRecords(range, null, scanProperties);
        final AtomicBoolean hasMore = new AtomicBoolean(true);

        // Note: This runs all of the updates serially in order to avoid invoking a race condition
        // in the rank code that was causing incorrect results. If everything were thread safe,
        // a larger pipeline size would be possible.
        final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());

        return iterateRangeOnly(store, cursor,
                this::getRecordIfTypeMatch,
                lastResult,
                hasMore, recordsScanned, isIdempotent)
                .thenApply(vignore -> hasMore.get() ?
                                      lastResult.get().get().getPrimaryKey() :
                                      range.getHigh());
    }

    @Nullable
    @SuppressWarnings("unused")
    private  CompletableFuture<FDBStoredRecord<Message>> getRecordIfTypeMatch(FDBRecordStore store, @Nonnull RecordCursorResult<FDBStoredRecord<Message>> cursorResult) {
        // No need to "translate" rec, so store is unused
        FDBStoredRecord<Message> rec = cursorResult.get() ;
        return recordIfInIndexedTypes(rec);
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store) {
        AtomicReference<TupleRange> rangeToGo = new AtomicReference<>(recordsRange);
        return AsyncUtil.whileTrue(() ->
                buildRangeOnly(store, rangeToGo.get(), true, null).thenApply(nextStart -> {
                    if (nextStart == rangeToGo.get().getHigh()) {
                        return false;
                    } else {
                        rangeToGo.set(new TupleRange(nextStart, rangeToGo.get().getHigh(), EndpointType.RANGE_INCLUSIVE, rangeToGo.get().getHighEndpoint()));
                        return true;
                    }
                }), store.getExecutor());
    }

    // support splitIndexBuildRange
    @Nonnull
    public List<Pair<Tuple, Tuple>> splitIndexBuildRange(int minSplit, int maxSplit) {
        TupleRange originalRange = getRunner().asyncToSync(FDBStoreTimer.Waits.WAIT_BUILD_ENDPOINTS, buildEndpoints());

        // There is no range needing to be built.
        if (originalRange == null) {
            return Collections.emptyList();
        }

        if (minSplit < 1 || maxSplit < 1 || minSplit > maxSplit) {
            throw new RecordCoreException("splitIndexBuildRange should have 1 < minSplit <= maxSplit");
        }

        List<Tuple> boundaries = getPrimaryKeyBoundaries(originalRange);

        // The range only spans across very few FDB servers so parallelism is not necessary.
        if (boundaries.size() - 1 < minSplit) {
            return Collections.singletonList(Pair.of(originalRange.getLow(), originalRange.getHigh()));
        }

        List<Pair<Tuple, Tuple>> splitRanges = new ArrayList<>(Math.min(boundaries.size() - 1, maxSplit));

        // step size >= 1
        int stepSize = -Math.floorDiv(-(boundaries.size() - 1), maxSplit);  // Read ceilDiv(boundaries.size() - 1, maxSplit).
        int start = 0;
        while (true) {
            int next = start + stepSize;
            if (next < boundaries.size() - 1) {
                splitRanges.add(Pair.of(boundaries.get(start), boundaries.get(next)));
            } else {
                splitRanges.add(Pair.of(boundaries.get(start), boundaries.get(boundaries.size() - 1)));
                break;
            }
            start = next;
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("split index build range",
                    LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                    LogMessageKeys.ORIGINAL_RANGE, originalRange,
                    LogMessageKeys.SPLIT_RANGES, splitRanges));
        }

        return splitRanges;
    }

    private List<Tuple> getPrimaryKeyBoundaries(TupleRange tupleRange) {
        List<Tuple> boundaries = getRunner().run(context -> {
            context.getReadVersion(); // for instrumentation reasons
            RecordCursor<Tuple> cursor = common.getRecordStoreBuilder().copyBuilder().setContext(context).open()
                    .getPrimaryKeyBoundaries(tupleRange.getLow(), tupleRange.getHigh());
            return context.asyncToSync(FDBStoreTimer.Waits.WAIT_GET_BOUNDARY, cursor.asList());
        }, Lists.newArrayList(
                LogMessageKeys.TRANSACTION_NAME, "IndexingByRecords::getPrimaryKeyBoundaries",
                LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                LogMessageKeys.RANGE, tupleRange));

        // Add the two endpoints if they are not in the result
        if (boundaries.isEmpty() || tupleRange.getLow().compareTo(boundaries.get(0)) < 0) {
            boundaries.add(0, tupleRange.getLow());
        }
        if (tupleRange.getHigh().compareTo(boundaries.get(boundaries.size() - 1)) > 0) {
            boundaries.add(tupleRange.getHigh());
        }

        return boundaries;
    }

}
