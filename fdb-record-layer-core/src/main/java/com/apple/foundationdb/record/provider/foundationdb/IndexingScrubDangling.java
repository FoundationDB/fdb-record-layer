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
import com.apple.foundationdb.record.IndexScanType;
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
import com.apple.foundationdb.record.metadata.NestedRecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    @Nonnull private final AtomicLong danglingCount;
    private long scanCounter = 0;
    private int logWarningCounter;

    public IndexingScrubDangling(@Nonnull final IndexingCommon common,
                                 @Nonnull final OnlineIndexer.IndexingPolicy policy,
                                 @Nonnull final OnlineIndexScrubber.ScrubbingPolicy scrubbingPolicy,
                                 @Nonnull final AtomicLong danglingCount) {
        super(common, policy, true);
        this.scrubbingPolicy = scrubbingPolicy;
        this.logWarningCounter = scrubbingPolicy.getLogWarningsLimit();
        this.danglingCount = danglingCount;
    }

    @Override
    List<Object> indexingLogMessageKeyValues() {
        return Arrays.asList(
                LogMessageKeys.INDEXING_METHOD, "scrub dangling index entries",
                LogMessageKeys.ALLOW_REPAIR, scrubbingPolicy.allowRepair(),
                LogMessageKeys.RANGE_ID, scrubbingPolicy.getScrubbingRangeId(),
                LogMessageKeys.RANGE_RESET, scrubbingPolicy.isScrubbingRangeReset(),
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
                                                    scrubIndex(subspaceProvider, subspace)
                                            );
                                }),
                common.indexLogMessageKeyValues("IndexingScrubber::buildIndexInternalAsync"));
    }

    @Nonnull
    private CompletableFuture<Void> scrubIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {

        final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "scrubRecords");

        return iterateAllRanges(additionalLogMessageKeyValues,
                (store, recordsScanned) -> scrubIndexRangeOnly(store, recordsScanned),
                subspaceProvider, subspace);
    }

    @Nonnull
    private CompletableFuture<Boolean> scrubIndexRangeOnly(@Nonnull FDBRecordStore store, @Nonnull AtomicLong recordsScanned) {
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
        final IndexingRangeSet rangeSet = IndexingRangeSet.forScrubbingIndex(store, index, scrubbingPolicy.getScrubbingRangeId());
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

            RecordCursor<IndexEntry> cursor = store.scanIndex(index, IndexScanType.BY_VALUE, tupleRange, null, scanProperties);

            final AtomicBoolean hasMore = new AtomicBoolean(true);
            final AtomicReference<RecordCursorResult<IndexEntry>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final long scanLimit = scrubbingPolicy.getEntriesScanLimit();

            return iterateRangeOnly(store, cursor,
                    this::deleteIndexIfDangling,
                    lastResult, hasMore, recordsScanned, true)
                    .thenApply(vignore -> hasMore.get() ?
                                          lastResult.get().get().getKey() :
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
    private CompletableFuture<FDBStoredRecord<Message>> deleteIndexIfDangling(FDBRecordStore store, final RecordCursorResult<IndexEntry> indexEntryResult) {
        // This will always return null (!) - but sometimes will delete a dangling index
        final IndexingCommon.IndexContext indexContext = common.getIndexContext();
        final IndexEntry indexEntry = indexEntryResult.get();
        if (indexContext.isSynthetic) {
            return store.loadSyntheticRecord(indexEntry.getPrimaryKey()).thenApply(syntheticRecord -> {
                if (syntheticRecord.getConstituents().isEmpty()) {
                    // None of the constituents of this synthetic type are present, so it must be dangling
                    List<Tuple> primaryKeysForConflict = new ArrayList<>(indexEntry.getPrimaryKey().size() - 1);
                    SyntheticRecordType<?> syntheticRecordType = store.getRecordMetaData().getSyntheticRecordTypeFromRecordTypeKey(indexEntry.getPrimaryKey().get(0));
                    for (int i = 0; i < syntheticRecordType.getConstituents().size(); i++) {
                        if (!(syntheticRecordType.getConstituents().get(i).getRecordType() instanceof NestedRecordType) && indexEntry.getPrimaryKey().get(i + 1) != null) {
                            primaryKeysForConflict.add(indexEntry.getPrimaryKey().getNestedTuple(i + 1));
                        }
                    }
                    scrubDanglingEntry(store, indexEntry, primaryKeysForConflict);
                }
                return null;
            });
        } else {
            return store.loadIndexEntryRecord(indexEntry, IndexOrphanBehavior.RETURN).thenApply(indexedRecord -> {
                if (!indexedRecord.hasStoredRecord()) {
                    // Here: Oh, No! this index is dangling!
                    scrubDanglingEntry(store, indexEntry, List.of(indexEntry.getPrimaryKey()));
                }
                return null;
            });
        }
    }

    private void scrubDanglingEntry(@Nonnull FDBRecordStore store, @Nonnull IndexEntry indexEntry, @Nonnull List<Tuple> conflictPrimaryKeys) {
        danglingCount.incrementAndGet();
        timerIncrement(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES);
        final Tuple valueKey = indexEntry.getKey();
        final byte[] keyBytes = store.indexSubspace(common.getIndex()).pack(valueKey);

        if (LOGGER.isWarnEnabled() && logWarningCounter > 0) {
            logWarningCounter--;
            LOGGER.warn(KeyValueLogMessage.build("Scrubber: dangling index entry",
                            LogMessageKeys.KEY, valueKey,
                            LogMessageKeys.PRIMARY_KEY, indexEntry.getPrimaryKey())
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .toString());
        }
        if (scrubbingPolicy.allowRepair()) {
            // remove this index entry
            // Note that there no record can be added to the conflict list, so we'll add the index entry itself.
            for (Tuple primaryKey : conflictPrimaryKeys) {
                store.addRecordReadConflict(primaryKey);
            }
            store.getContext().ensureActive().clear(keyBytes);
        }
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
        IndexingRangeSet indexRangeSet = IndexingRangeSet.forScrubbingIndex(store, index, scrubbingPolicy.getScrubbingRangeId());
        if (scrubbingPolicy.isScrubbingRangeReset()) {
            indexRangeSet.clear();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .addKeyAndValue(LogMessageKeys.REASON, "forced reset")
                        .toString());
            }
            return AsyncUtil.DONE;
        }
        return indexRangeSet.firstMissingRangeAsync()
                .thenAccept(indexRange -> {
                    if (indexRange == null) {
                        // Here: no un-scrubbed records range was left for this call. We will
                        // erase the 'ranges' data to allow a fresh records re-scrubbing.
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(KeyValueLogMessage.build("Reset index scrubbing range")
                                    .addKeysAndValues(common.indexLogMessageKeyValues())
                                    .addKeyAndValue(LogMessageKeys.REASON, "range exhausted detected")
                                    .toString());
                        }
                        indexRangeSet.clear();
                    }
                });
    }

    @Override
    CompletableFuture<Void> rebuildIndexInternalAsync(final FDBRecordStore store) {
        throw new UnsupportedOperationException();
    }
}
