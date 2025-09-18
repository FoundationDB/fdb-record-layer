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
import com.apple.foundationdb.annotation.API;
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
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
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

        if (targetIndexes.isEmpty()) {
            throw new ValidationException("No target index was set");
        }
        if (targetIndexes.size() == 1) {
            // backward compatibility
            return compileSingleTargetLegacyIndexingTypeStamp();
        }
        return IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS)
                .addAllTargetIndex(targetIndexes)
                .build();
    }

    @Nonnull
    protected static IndexBuildProto.IndexBuildIndexingStamp compileSingleTargetLegacyIndexingTypeStamp() {
        return
                IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS)
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
                                        .thenCompose(this::buildMultiTargetIndex);
                            })
                ), common.indexLogMessageKeyValues("IndexingMultiTargetByRecords::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> buildMultiTargetIndex(@Nonnull Subspace subspace) {
        final TupleRange tupleRange = common.computeRecordsRange();
        final byte[] rangeStart;
        final byte[] rangeEnd;
        if (tupleRange == null) {
            rangeStart = rangeEnd = null;
        } else {
            final Range range = tupleRange.toRange();
            rangeStart = range.begin;
            // tupleRange has an inclusive high endpoint, so end isn't a valid tuple.
            // But buildRangeOnly needs to convert missing Ranges back to TupleRanges, so round up.
            rangeEnd = ByteArrayUtil.strinc(range.end);
        }

        final CompletableFuture<FDBRecordStore> maybePresetRangeFuture =
                rangeStart == null ?
                CompletableFuture.completedFuture(null) :
                buildCommitRetryAsync((store, recordsScanned) -> {
                    // Here: only records inside the defined records-range are relevant to the index. Hence, the completing range
                    // can be preemptively marked as indexed.
                    final List<Index> targetIndexes = common.getTargetIndexes();
                    final List<IndexingRangeSet> targetRangeSets = targetIndexes.stream()
                            .map(targetIndex -> IndexingRangeSet.forIndexBuild(store, targetIndex))
                            .collect(Collectors.toList());
                    return CompletableFuture.allOf(
                            insertRanges(targetRangeSets, null, rangeStart),
                                    insertRanges(targetRangeSets, rangeEnd, null))
                            .thenApply(ignore -> null);
                }, null);

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildMultiTargetIndex",
                LogMessageKeys.RANGE_START, rangeStart,
                LogMessageKeys.RANGE_END, rangeEnd);

        return maybePresetRangeFuture.thenCompose(ignore ->
                        iterateAllRanges(additionalLogMessageKeyValues, this::buildRangeOnly, subspace));
    }

    @Nonnull
    private CompletableFuture<Boolean> buildRangeOnly(@Nonnull FDBRecordStore store, @Nonnull AtomicLong recordsScanned) {
        // return false when done
        /* Multi target consistency:
         * 1. Identify missing ranges from only the first index
         * 2. Update all indexes' range sets as the indexes are built - each inserted range is validated as empty.
         * 3. While marking each index as readable, we validate that it is completely built.
         */
        validateSameMetadataOrThrow(store);
        final List<Index> targetIndexes = common.getTargetIndexes();
        final boolean isIdempotent = areTheyAllIdempotent(store, targetIndexes);
        final ScanProperties scanProperties = scanPropertiesWithLimits(isIdempotent);
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, common.getPrimaryIndex());

        return rangeSet.firstMissingRangeAsync().thenCompose(range -> {
            if (range == null) {
                return AsyncUtil.READY_FALSE; // no more missing ranges - all done
            }
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            RecordCursor<FDBStoredRecord<Message>> cursor =
                    store.scanRecords(tupleRange, null, scanProperties);

            final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final AtomicBoolean hasMore = new AtomicBoolean(true);

            final List<IndexingRangeSet> targetRangeSets = targetIndexes.stream()
                    .map(targetIndex -> IndexingRangeSet.forIndexBuild(store, targetIndex))
                    .collect(Collectors.toList());
            return iterateRangeOnly(store, cursor,
                    this::getRecordIfTypeMatch,
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenCompose(ignore -> postIterateRangeOnly(targetRangeSets, hasMore.get(), lastResult,
                            rangeStart, rangeEnd, scanProperties.isReverse()));
        });
    }

    private CompletableFuture<Boolean> postIterateRangeOnly(List<IndexingRangeSet> targetRangeSets, boolean hasMore,
                                                            AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult,
                                                            Tuple rangeStart, Tuple rangeEnd, boolean isReverse) {
        if (isReverse) {
            Tuple continuation = hasMore ? lastResult.get().get().getPrimaryKey() : rangeStart;
            return insertRanges(targetRangeSets, packOrNull(continuation), packOrNull(rangeEnd))
                    .thenApply(ignore -> hasMore || rangeStart != null);
        } else {
            Tuple continuation = hasMore ? lastResult.get().get().getPrimaryKey() : rangeEnd;
            return insertRanges(targetRangeSets, packOrNull(rangeStart), packOrNull(continuation))
                    .thenApply(ignore -> hasMore || rangeEnd != null);
        }
    }

    private static CompletableFuture<Void> insertRanges(List<IndexingRangeSet> rangeSets,
                                                        byte[] start, byte[] end) {
        return AsyncUtil.whenAll(rangeSets.stream().map(set -> set.insertRangeAsync(start, end, true)).collect(Collectors.toList()));
    }

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
        final TupleRange tupleRange = common.computeRecordsRange();
        // if non-null, the tupleRange contains the range of all the records need indexing (in practice,
        // the type keys containing the lexicographically first to last types). Hence, we can skip indexing records
        // that are outside this range.
        final Tuple rangeStart = tupleRange == null ? null : tupleRange.getLow();
        final Tuple rangeEndInclusive = tupleRange == null ? null : tupleRange.getHigh();

        AtomicReference<Tuple> nextResultCont = new AtomicReference<>(rangeStart);
        AtomicLong recordScanned = new AtomicLong();
        return AsyncUtil.whileTrue(() ->
                rebuildRangeOnly(store, nextResultCont.get(), recordScanned, rangeEndInclusive).thenApply(cont -> {
                    if (cont == null) {
                        return false;
                    }
                    nextResultCont.set(cont);
                    return true;
                }), store.getExecutor());
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Tuple> rebuildRangeOnly(@Nonnull FDBRecordStore store, Tuple cont, @Nonnull AtomicLong recordsScanned, @Nullable Tuple rangeEndInclusive) {
        validateSameMetadataOrThrow(store);
        final boolean isIdempotent = areTheyAllIdempotent(store, common.getTargetIndexes());

        final IsolationLevel isolationLevel =
                isIdempotent ?
                IsolationLevel.SNAPSHOT :
                IsolationLevel.SERIALIZABLE;

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel);
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());
        final TupleRange tupleRange = TupleRange.betweenInclusive(cont, rangeEndInclusive);

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
