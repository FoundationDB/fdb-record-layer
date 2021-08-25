/*
 * IndexingRepair.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manifesto:
 *  Scrub a readable index to validate its consistency. Repair when needed.
 *  Keep the type stamp simple, to allow parallel scrubbing.
 *  Support two scrubbers - detect missing and detect dangling index entries. Set by the policy.
 */
@API(API.Status.INTERNAL)
public class IndexingScrubDangling extends IndexingBase {
    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingScrubMissing.class);
    @Nonnull private static final IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = compileIndexingTypeStamp();

    @Nonnull private final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy;
    private long scanCounter = 0;
    private int logWarningCounter;

    public IndexingScrubDangling(@Nonnull final IndexingCommon common,
                                 @Nonnull final OnlineIndexer.IndexingPolicy policy,
                                 @Nonnull final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy) {
        super(common, policy, true);
        this.scrubbingPolicy = scrubbingPolicy;
        this.logWarningCounter = scrubbingPolicy.getLogWarningsLimit();
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "scrub dangling index entries",
                LogMessageKeys.ALLOW_REPAIR, scrubbingPolicy.allowRepair(),
                LogMessageKeys.LIMIT, scrubbingPolicy.getEntriesScanLimit()
        );
    }

    @Nonnull
    @Override
    IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(final FDBRecordStore store) {
        return myIndexingTypeStamp;
    }

    @Nonnull
    static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp() {
        return
                IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR)
                        .build();
    }

    @Override
    CompletableFuture<Void> buildIndexInternalAsync() {
        return getRunner().runAsync(context ->
                        context.getReadVersionAsync()
                                .thenCompose(vignore -> {
                                    SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                                    return subspaceProvider.getSubspaceAsync(context)
                                            .thenCompose(subspace ->
                                                    scrubIndex(subspaceProvider, subspace, null, null)
                                            );
                                }),
                common.indexLogMessageKeyValues("IndexingScrubber::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> scrubIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace,
                                               @Nullable byte[] start, @Nullable byte[] end) {

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "scrubRecords",
                LogMessageKeys.RANGE_START, start,
                LogMessageKeys.RANGE_END, end);

        return iterateAllRanges(additionalLogMessageKeyValues,
                (store, recordsScanned) -> scrubIndexRangeOnly(store, start, end, recordsScanned),
                subspaceProvider, subspace);
    }

    @Nonnull
    private CompletableFuture<Boolean> scrubIndexRangeOnly(@Nonnull FDBRecordStore store, byte[] startBytes, byte[] endBytes, @Nonnull AtomicLong recordsScanned) {
        // return false when done
        Index index = common.getIndex();
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if (recordMetaDataProvider == null || !metaData.equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);

        // scrubbing only readable, VALUE, idempotence indexes (at least for now)
        validateOrThrowEx(maintainer.isIdempotent(), "scrubbed index is not idempotent");
        validateOrThrowEx(index.getType().equals(IndexTypes.VALUE), "scrubbed index is not a VALUE index");
        validateOrThrowEx(store.getIndexState(index) == IndexState.READABLE, "scrubbed index is not readable");

        RangeSet rangeSet = new RangeSet(indexScrubIndexRangeSubspace(store, index));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), startBytes, endBytes).iterator();

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(getLimit() + 1); // always respectLimit in this path; +1 allows a continuation item
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        return ranges.onHasNext().thenCompose(hasNext -> {
            if (Boolean.FALSE.equals(hasNext)) {
                // Here: no more missing ranges - all done
                // To avoid stale metadata, we'll keep the scrubbed-ranges indicator empty until the next scrub call.
                Transaction tr = store.getContext().ensureActive();
                tr.clear(indexScrubIndexRangeSubspace(store, index).range());
                return AsyncUtil.READY_FALSE;
            }
            final Range range = ranges.next();
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            RecordCursor<FDBIndexedRecord<Message>> cursor =
                    store.scanIndexRecords(index.getName(), IndexScanType.BY_VALUE, tupleRange, null, IndexOrphanBehavior.RETURN, scanProperties);

            final AtomicBoolean hasMore = new AtomicBoolean(true);
            final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final long scanLimit = scrubbingPolicy.getEntriesScanLimit();

            return iterateRangeOnly(store, cursor,
                    this::deleteIndexIfDangling,
                    lastResult, hasMore, recordsScanned)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getIndexEntry().getKey() :
                                          rangeEnd)
                    .thenCompose(cont -> rangeSet.insertRange(store.ensureContextActive(), packOrNull(rangeStart), packOrNull(cont), true)
                            .thenApply(ignore -> {
                                if ( scanLimit > 0 ) {
                                    scanCounter += recordsScanned.get();
                                    if (scanLimit <= scanCounter) {
                                        return false;
                                    }
                                }
                                return !Objects.equals(cont, rangeEnd);
                            }));
        });
    }

    @Nullable
    private CompletableFuture<FDBStoredRecord<Message>> deleteIndexIfDangling(FDBRecordStore store, final RecordCursorResult<FDBIndexedRecord<Message>> cursorResult) {
        // This will always return null (!) - but sometimes will delete a dangling index
        FDBIndexedRecord<Message> indexResult = cursorResult.get();

        if (! indexResult.hasStoredRecord() ) {
            // Here: Oh, No! this index is dangling!
            final FDBStoreTimer timer = getRunner().getTimer();
            timerIncrement(timer, FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES);
            final IndexEntry indexEntry = indexResult.getIndexEntry();
            final Tuple valueKey = indexEntry.getKey();
            final byte[] keyBytes = store.indexSubspace(common.getIndex()).pack(valueKey);

            if (LOGGER.isWarnEnabled() && logWarningCounter > 0) {
                logWarningCounter --;
                LOGGER.warn(KeyValueLogMessage.build("Scrubber: dangling index entry",
                        LogMessageKeys.KEY, valueKey.toString())
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
            if (scrubbingPolicy.allowRepair()) {
                // remove this index entry
                // Note that there no record can be added to the conflict list, so we'll add the index entry itself.
                store.addRecordReadConflict(indexEntry.getPrimaryKey());
                store.getContext().ensureActive().clear(keyBytes);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(final FDBRecordStore store) {
        throw new UnsupportedOperationException();
    }
}
