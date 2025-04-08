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


import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PipelineOperation;
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
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
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
import java.util.stream.Collectors;

/**
 * Manifesto:
 *  Scrub a readable index to validate its consistency. Repair when needed.
 *  This scrubber will scan the records and look for missing indexes.
 */
@API(API.Status.INTERNAL)
public class IndexingScrubMissing extends IndexingBase {
    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingScrubMissing.class);
    @Nonnull private static final IndexBuildProto.IndexBuildIndexingStamp myIndexingTypeStamp = compileIndexingTypeStamp();

    @Nonnull private final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy;
    @Nonnull private final AtomicLong missingCount;
    private long scanCounter = 0;
    private int logWarningCounter;

    public IndexingScrubMissing(@Nonnull final IndexingCommon common,
                                @Nonnull final OnlineIndexer.IndexingPolicy policy,
                                @Nonnull final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy,
                                @Nonnull final AtomicLong missingCount) {
        super(common, policy, true);
        this.scrubbingPolicy = scrubbingPolicy;
        this.logWarningCounter = scrubbingPolicy.getLogWarningsLimit();
        this.missingCount = missingCount;
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "scrub missing index entries",
                LogMessageKeys.ALLOW_REPAIR, scrubbingPolicy.allowRepair(),
                LogMessageKeys.RANGE_ID, scrubbingPolicy.getRangeId(),
                LogMessageKeys.RANGE_RESET, scrubbingPolicy.isRangeReset(),
                LogMessageKeys.SCAN_LIMIT, scrubbingPolicy.getEntriesScanLimit()
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
                                                    scrubRecords(subspaceProvider, subspace));
                                }),
                common.indexLogMessageKeyValues("IndexingScrubMissing::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> scrubRecords(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "scrubRecords");

        return iterateAllRanges(additionalLogMessageKeyValues,
                (store, recordsScanned) -> scrubRecordsRangeOnly(store, recordsScanned),
                subspaceProvider, subspace);
    }

    @Nonnull
    private CompletableFuture<Boolean> scrubRecordsRangeOnly(@Nonnull FDBRecordStore store, @Nonnull AtomicLong recordsScanned) {
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
        validateOrThrowEx(IndexTypes.VALUE.equals(index.getType()) || scrubbingPolicy.ignoreIndexTypeCheck(), "scrubbed index is not a VALUE index");
        validateOrThrowEx(store.getIndexState(index).isScannable(), "scrubbed index is not readable");

        final ScanProperties scanProperties = scanPropertiesWithLimits(true);
        final IndexingRangeSet rangeSet = IndexingRangeSet.forScrubbingRecords(store, index, scrubbingPolicy.getRangeId());
        return rangeSet.firstMissingRangeAsync().thenCompose(range -> {
            if (range == null) {
                // Here: no more missing ranges - all done
                // To avoid stale metadata, we'll keep the scrubbed-ranges indicator empty until the next scrub call.
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .addKeyAndValue(LogMessageKeys.REASON, "range exhausted")
                            .toString());
                }
                rangeSet.clear();
                return AsyncUtil.READY_FALSE;
            }
            final Tuple rangeStart = RangeSet.isFirstKey(range.begin) ? null : Tuple.fromBytes(range.begin);
            final Tuple rangeEnd = RangeSet.isFinalKey(range.end) ? null : Tuple.fromBytes(range.end);
            final TupleRange tupleRange = TupleRange.between(rangeStart, rangeEnd);

            final RecordCursor<FDBStoredRecord<Message>> cursor = store.scanRecords(tupleRange, null, scanProperties);
            final AtomicBoolean hasMore = new AtomicBoolean(true);
            final AtomicReference<RecordCursorResult<FDBStoredRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final long scanLimit = scrubbingPolicy.getEntriesScanLimit();

            final boolean isIdempotent = true ; // Note that currently we only scrub idempotent indexes
            return iterateRangeOnly(store, cursor, this::getRecordIfMissingIndex,
                    lastResult, hasMore, recordsScanned, isIdempotent)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getPrimaryKey() :
                                          rangeEnd)
                    .thenCompose(cont -> rangeSet.insertRangeAsync(packOrNull(rangeStart), packOrNull(cont), true)
                            .thenApply(ignore -> {
                                if ( scanLimit > 0 ) {
                                    scanCounter += recordsScanned.get();
                                    if (scanLimit <= scanCounter) {
                                        return false;
                                    }
                                }
                                return notAllRangesExhausted(cont, rangeEnd);
                            }));
        });
    }

    @Nullable
    private CompletableFuture<FDBStoredRecord<Message>> getRecordIfMissingIndex(FDBRecordStore store, final RecordCursorResult<FDBStoredRecord<Message>> currResult) {
        final FDBStoredRecord<Message> rec = currResult.get();
        // return true if an index is missing and updated
        if (!common.getAllRecordTypes().contains(rec.getRecordType())) {
            return CompletableFuture.completedFuture(null);
        }
        return getMissingIndexKeys(store, rec)
                .thenApply(list -> {
                    List<Tuple> missingIndexesKeys = list.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    if (missingIndexesKeys.isEmpty()) {
                        return null;
                    }
                    // Here: Oh, No! the index is missing!!
                    // (Maybe) report an error and (maybe) return this record to be index
                    if (LOGGER.isWarnEnabled() && logWarningCounter > 0) {
                        logWarningCounter --;
                        LOGGER.warn(KeyValueLogMessage.build("Scrubber: missing index entry",
                                        LogMessageKeys.KEY, rec.getPrimaryKey().toString(),
                                        LogMessageKeys.INDEX_KEY, missingIndexesKeys.toString())
                                .addKeysAndValues(common.indexLogMessageKeyValues())
                                .toString());
                    }
                    missingCount.incrementAndGet();
                    timerIncrement(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES);
                    if (scrubbingPolicy.allowRepair()) {
                        // record to be indexed
                        return rec;
                    }
                    // report only mode
                    return null;
                });
    }

    @Nonnull
    private CompletableFuture<List<Tuple>> getMissingIndexKeys(@Nonnull FDBRecordStore store, @Nonnull FDBStoredRecord<Message> rec) {
        final Index index = common.getIndex();
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        return indexEntriesForRecord(store, rec)
                .mapPipelined(indexEntry -> {
                    final Tuple valueKey = indexEntry.getKey();
                    final byte[] keyBytes = maintainer.getIndexSubspace().pack(valueKey);
                    return maintainer.state.transaction.get(keyBytes).thenApply(indexVal -> indexVal == null ? valueKey : null);
                }, store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                .filter(Objects::nonNull)
                .asList();
    }

    @Nonnull
    private RecordCursor<IndexEntry> indexEntriesForRecord(@Nonnull FDBRecordStore store, @Nonnull FDBStoredRecord<Message> rec) {
        final IndexingCommon.IndexContext indexContext = common.getIndexContext();
        final Index index = indexContext.index;
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        if (indexContext.isSynthetic) {
            final SyntheticRecordFromStoredRecordPlan syntheticPlan = syntheticPlanForIndex(store, indexContext);
            return RecordCursor.flatMapPipelined(
                    outerContinuation -> syntheticPlan.execute(store, rec),
                    (syntheticRecord, innerContinuation) -> {
                        final List<IndexEntry> entriesForSyntheticRecord = maintainer.filteredIndexEntries(syntheticRecord);
                        if (entriesForSyntheticRecord == null) {
                            return RecordCursor.empty();
                        } else {
                            return RecordCursor.fromList(store.getExecutor(), entriesForSyntheticRecord, innerContinuation)
                                    .map(entryNoPK -> rewriteWithPrimaryKey(entryNoPK, syntheticRecord));
                        }
                    },
                    null,
                    store.getPipelineSize(PipelineOperation.SYNTHETIC_RECORD_JOIN)
            );
        } else {
            List<IndexEntry> indexEntryNoPKs = maintainer.filteredIndexEntries(rec);
            if (indexEntryNoPKs == null) {
                return RecordCursor.empty();
            }
            return RecordCursor.fromList(store.getExecutor(), indexEntryNoPKs)
                    .map(entryNoPK -> rewriteWithPrimaryKey(entryNoPK, rec));
        }
    }

    @Nonnull
    private IndexEntry rewriteWithPrimaryKey(@Nonnull IndexEntry indexEntry, @Nonnull FDBRecord<? extends Message> rec) {
        return new IndexEntry(indexEntry.getIndex(), FDBRecordStoreBase.indexEntryKey(indexEntry.getIndex(), indexEntry.getKey(), rec.getPrimaryKey()), indexEntry.getValue(), rec.getPrimaryKey());
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    @Override
    protected CompletableFuture<Void> setScrubberTypeOrThrow(FDBRecordStore store) {
        // Note: this duplicated function should be eliminated with this obsolete module (for legacy mode) is deleted
        // HERE: The index must be readable, checked by the caller
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);
        validateOrThrowEx(indexingTypeStamp.getMethod().equals(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR),
                "Not a scrubber type-stamp");

        final Index index = common.getIndex(); // Note: the scrubbers do not support multi target (yet)
        IndexingRangeSet recordsRangeSet = IndexingRangeSet.forScrubbingRecords(store, index, scrubbingPolicy.getRangeId());
        if (scrubbingPolicy.isRangeReset()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .addKeyAndValue(LogMessageKeys.REASON, "forced reset")
                        .toString());
            }
            recordsRangeSet.clear();
            return AsyncUtil.DONE;
        }
        return recordsRangeSet.firstMissingRangeAsync()
                .thenAccept(recordRange -> {
                    if (recordRange == null) {
                        // Here: no un-scrubbed records range was left for this call. We will
                        // erase the 'ranges' data to allow a fresh records re-scrubbing.
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                                    .addKeysAndValues(common.indexLogMessageKeyValues())
                                    .addKeyAndValue(LogMessageKeys.REASON, "range exhausted detected")
                                    .toString());
                        }
                        recordsRangeSet.clear();
                    }
                });
    }

    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(final FDBRecordStore store) {
        throw new UnsupportedOperationException();
    }
}
