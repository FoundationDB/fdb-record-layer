/*
 * IndexingByIndex.java
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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This indexer scans records by a source index.
 */
@API(API.Status.INTERNAL)
public class IndexingByIndex extends IndexingBase {
    // LOGGER here?
    private IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = null;

    IndexingByIndex(@Nonnull IndexingCommon common,
                    @Nonnull OnlineIndexer.IndexingPolicy policy) {
        super(common, policy);
    }

    @Override
    @Nonnull
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store) {
        if ( myIndexingTypeStamp == null) {
            Index srcIndex = getSourceIndex(store.getRecordMetaData());
            myIndexingTypeStamp = compileIndexingTypeStamp(srcIndex);
        }
        return myIndexingTypeStamp;
    }

    @Nonnull
    private static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp(Index srcIndex) {
        return IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_INDEX)
                .setSourceIndexSubspaceKey(ByteString.copyFrom(Tuple.from(srcIndex.getSubspaceKey()).pack()))
                .setSourceIndexLastModifiedVersion(srcIndex.getLastModifiedVersion())
                .build();
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "by index",
                LogMessageKeys.SOURCE_INDEX, policy.getSourceIndex(),
                LogMessageKeys.SUBSPACE_KEY, policy.getSourceIndexSubspaceKey()
        );
    }


    @Nonnull
    private Index getSourceIndex(RecordMetaData metaData) {
        if (policy.getSourceIndexSubspaceKey() != null) {
            return metaData.getIndexFromSubspaceKey(policy.getSourceIndexSubspaceKey());
        }
        if (policy.getSourceIndex() != null) {
            return metaData.getIndex(policy.getSourceIndex());
        }
        throw new ValidationException("no source index",
                LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                LogMessageKeys.SOURCE_INDEX, policy.getSourceIndex(),
                LogMessageKeys.INDEXER_ID, common.getUuid());
    }

    @Nonnull
    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose( store -> {
                    // first validate that both src and tgt are of a single, similar, type
                    final RecordMetaData metaData = store.getRecordMetaData();
                    final Index srcIndex = getSourceIndex(metaData);
                    final Collection<RecordType> srcRecordTypes = metaData.recordTypesForIndex(srcIndex);

                    validateOrThrowEx(common.getAllRecordTypes().size() == 1, "target index has multiple types");
                    validateOrThrowEx(srcRecordTypes.size() == 1, "source index has multiple types");
                    validateOrThrowEx(!srcIndex.getRootExpression().createsDuplicates(), "source index creates duplicates");
                    validateOrThrowEx(srcIndex.getType().equals(IndexTypes.VALUE), "source index is not a VALUE index");
                    validateOrThrowEx(common.getAllRecordTypes().containsAll(srcRecordTypes), "source index's type is not equal to target index's");

                    // all valid; back to the future. Note that for practical reasons, readability and idempotency will be validated later
                    return context.getReadVersionAsync()
                            .thenCompose(vignore -> {
                                SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                                return subspaceProvider.getSubspaceAsync(context)
                                        .thenCompose(subspace -> buildIndexFromIndex(subspaceProvider, subspace, null, null));
                            });
                }), common.indexLogMessageKeyValues("IndexingByIndex::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> buildIndexFromIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace, @Nullable byte[] start, @Nullable byte[] end) {
        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildIndexFromIndex",
                LogMessageKeys.RANGE_START, start,
                LogMessageKeys.RANGE_END, end);

        return iterateAllRanges(additionalLogMessageKeyValues,
                (store, recordsScanned) -> buildRangeOnly(store, start, end , recordsScanned),
                subspaceProvider, subspace);
    }

    @Nonnull
    private CompletableFuture<Boolean> buildRangeOnly(@Nonnull FDBRecordStore store, byte[] startBytes, byte[] endBytes, @Nonnull AtomicLong recordsScanned) {
        // return false when done

        validateSameMetadataOrThrow(store);
        final Index index = common.getIndex();
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);

        // idempotence - We could have verified it at the first iteration only, but the repeating checks seem harmless
        validateOrThrowEx(maintainer.isIdempotent(), "target index is not idempotent");
        // readability - This method shouldn't block if one has already opened the record store (as we did)
        Index srcIndex = getSourceIndex(store.getRecordMetaData());
        validateOrThrowEx(store.isIndexReadable(srcIndex), "source index is not readable");

        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), startBytes, endBytes).iterator();

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(getLimit() + 1); // respect limit in this path; +1 allows a continuation item
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        return ranges.onHasNext().thenCompose(hasNext -> {
            if (Boolean.FALSE.equals(hasNext)) {
                return AsyncUtil.READY_FALSE; // no more missing ranges - all done
            }
            final Range range = ranges.next();
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            RecordCursor<FDBIndexedRecord<Message>> cursor =
                    store.scanIndexRecords(srcIndex.getName(), IndexScanType.BY_VALUE, tupleRange, null, scanProperties);

            final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final AtomicBoolean hasMore = new AtomicBoolean(true);

            final boolean isIdempotent = true ; // Note that currently indexing by index is online implemented for idempotent indexes
            return iterateRangeOnly(store, cursor,
                    this::getRecordIfTypeMatch,
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getIndexEntry().getKey() :
                                          rangeEnd)
                    .thenCompose(cont -> rangeSet.insertRange(store.ensureContextActive(), packOrNull(rangeStart), packOrNull(cont), true)
                                .thenApply(ignore -> !Objects.equals(cont, rangeEnd)));

        });
    }

    @Nonnull
    @SuppressWarnings("unused")
    private  CompletableFuture<FDBStoredRecord<Message>> getRecordIfTypeMatch(FDBRecordStore store, @Nonnull RecordCursorResult<FDBIndexedRecord<Message>> cursorResult) {
        FDBIndexedRecord<Message> indexResult = cursorResult.get();
        FDBStoredRecord<Message> rec =  indexResult == null ? null : indexResult.getStoredRecord();
        return recordIfInIndexedTypes(rec);
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store) {
        AtomicReference<Tuple> nextResultCont = new AtomicReference<>();
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
        final Index index = common.getIndex();
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);

        // idempotence - We could have verified it at the first iteration only, but the repeating checks seem harmless
        validateOrThrowEx(maintainer.isIdempotent(), "target index is not idempotent");

        // readability - This method shouldn't block if one has already opened the record store (as we did)
        final Index srcIndex = getSourceIndex(store.getRecordMetaData());
        validateOrThrowEx(store.isIndexReadable(srcIndex), "source index is not readable");

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT);

        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());
        final TupleRange tupleRange = TupleRange.between(cont, null);

        RecordCursor<FDBIndexedRecord<Message>> cursor =
                store.scanIndexRecords(srcIndex.getName(), IndexScanType.BY_VALUE, tupleRange, null, scanProperties);

        final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
        final AtomicBoolean hasMore = new AtomicBoolean(true);

        final boolean isIdempotent = true ; // Note that currently indexing by index is online implemented for idempotent indexes
        return iterateRangeOnly(store, cursor,
                this::getRecordIfTypeMatch,
                lastResult, hasMore, recordsScanned, isIdempotent
        ).thenApply(vignore -> hasMore.get() ?
                               lastResult.get().get().getIndexEntry().getKey() :
                               null );
    }
}
