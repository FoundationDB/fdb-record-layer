/*
 * IndexingMultiTargetByRecords.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This indexer scans records to build multiple indexes.
 */
@API(API.Status.INTERNAL)
public class IndexingMultiTargetByRecords extends IndexingBase {
    private IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = null;

    IndexingMultiTargetByRecords(@Nonnull IndexingCommon common,
                                 @Nonnull OnlineIndexer.IndexingPolicy policy) {
        super(common, policy);
    }

    @Override
    @Nonnull
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store) {
        if (myIndexingTypeStamp == null) {
            myIndexingTypeStamp = compileIndexingTypeStamp(common.getTargetIndexesNames());
        }
        return myIndexingTypeStamp;
    }

    @Nonnull
    private static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp(List<String> targetIndexes) {

        return IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS)
                .addAllTargetIndex(targetIndexes)
                .build();
    }

    private static boolean areTheyAllIdempotent(@Nonnull FDBRecordStore store, List<Index> targetIndexes) {
        return targetIndexes.stream()
                .allMatch(targetIndex -> store.getIndexMaintainer(targetIndex).isIdempotent());
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "multi target by records",
                LogMessageKeys.TARGET_INDEX_NAME, common.getTargetIndexesNames()
        );
    }

    @Nonnull
    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose( store -> context.getReadVersionAsync()
                            .thenCompose(vignore -> {
                                SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                                // validation checks, if any, will be performed here
                                return subspaceProvider.getSubspaceAsync(context)
                                        .thenCompose(subspace -> buildMultiTargetIndex(subspaceProvider, subspace, null, null));
                            })
                ), common.indexLogMessageKeyValues("IndexingMultiTargetByRecords::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> buildMultiTargetIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace, @Nullable byte[] start, @Nullable byte[] end) {
        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildMultiTargetIndex",
                LogMessageKeys.RANGE_START, start,
                LogMessageKeys.RANGE_END, end);

        return iterateAllRanges(additionalLogMessageKeyValues,
                (store, recordsScanned) -> buildRangeOnly(store, start, end , recordsScanned),
                subspaceProvider, subspace);
    }

    @Nonnull
    private CompletableFuture<Boolean> buildRangeOnly(@Nonnull FDBRecordStore store, byte[] startBytes, byte[] endBytes, @Nonnull AtomicLong recordsScanned) {
        // return false when done
        /* Multi target consistency:
         * 1. Identify missing ranges from only the first index
         * 2. Update all indexes' range sets as the indexes are built - each inserted range is validated as empty.
         * 3. While each index as readable, we validate that its range is completely built.
         */
        validateSameMetadataOrThrow(store);
        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(common.getPrimaryIndex()));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), startBytes, endBytes).iterator();
        final List<Index> targetIndexes = common.getTargetIndexes();
        final List<RangeSet> targetRangeSets = targetIndexes.stream().map(targetIndex -> new RangeSet(store.indexRangeSubspace(targetIndex))).collect(Collectors.toList());
        final boolean isIdempotent = areTheyAllIdempotent(store, targetIndexes);

        final IsolationLevel isolationLevel =
                isIdempotent ?
                IsolationLevel.SNAPSHOT :
                IsolationLevel.SERIALIZABLE;

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(getLimit() + 1); // always respect limit in this path; +1 allows a continuation item
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        return ranges.onHasNext().thenCompose(hasNext -> {
            if (Boolean.FALSE.equals(hasNext)) {
                return AsyncUtil.READY_FALSE; // no more missing ranges - all done
            }
            final Range range = ranges.next();
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            RecordCursor<FDBStoredRecord<Message>> cursor =
                    store.scanRecords(tupleRange, null, scanProperties);

            final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final AtomicBoolean hasMore = new AtomicBoolean(true);

            return iterateRangeOnly(store, cursor,
                    this::getRecordIfTypeMatch,
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getPrimaryKey() :
                                          rangeEnd)
                    .thenCompose(cont -> insertRanges(store.ensureContextActive(), targetRangeSets, packOrNull(rangeStart), packOrNull(cont))
                                .thenApply(ignore -> !Objects.equals(cont, rangeEnd)));

        });
    }

    private static CompletableFuture<Void> insertRanges(Transaction transaction,
                                                        List<RangeSet> rangeSets,
                                                        byte[] start, byte[] end) {
        return AsyncUtil.whenAll(rangeSets.stream().map(set -> set.insertRange(transaction, start, end, true)).collect(Collectors.toList()));
    }

    @Nullable
    @SuppressWarnings("unused")
    private  CompletableFuture<FDBStoredRecord<Message>> getRecordIfTypeMatch(FDBRecordStore store, @Nonnull RecordCursorResult<FDBStoredRecord<Message>> cursorResult) {
        // No need to "translate" rec, so store is unused
        FDBStoredRecord<Message> rec = cursorResult.get();
        return recordIfInIndexedTypes(rec);
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store) {
        AtomicReference<Tuple> nextResultCont = new AtomicReference<>(null);
        AtomicLong recordScanned = new AtomicLong();
        return AsyncUtil.whileTrue(() ->
                rebuildRangeOnly(store, nextResultCont.get(), recordScanned).thenApply(cont -> {
                    if (cont == null) {
                        return false;
                    }
                    nextResultCont.set(cont);
                    return true;
                }), store.getExecutor());
    }

    @Nonnull
    private CompletableFuture<Tuple> rebuildRangeOnly(@Nonnull FDBRecordStore store, Tuple cont, @Nonnull AtomicLong recordsScanned) {

        validateSameMetadataOrThrow(store);
        final boolean isIdempotent = areTheyAllIdempotent(store, common.getTargetIndexes());

        final IsolationLevel isolationLevel =
                isIdempotent ?
                IsolationLevel.SNAPSHOT :
                IsolationLevel.SERIALIZABLE;

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel);
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());
        final TupleRange tupleRange = TupleRange.between(cont, null);

        RecordCursor<FDBStoredRecord<Message>> cursor =
                store.scanRecords(tupleRange, null, scanProperties);

        final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
        final AtomicBoolean hasMore = new AtomicBoolean(true);

        return iterateRangeOnly(store, cursor,
                this::getRecordIfTypeMatch,
                lastResult, hasMore, recordsScanned, isIdempotent
        ).thenApply(vignore -> hasMore.get() ?
                               lastResult.get().get().getPrimaryKey() :
                               null );
    }
}
