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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexMetaDataProto;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.function.Function;

/**
 * This indexer scans records by a source index.
 */
@API(API.Status.INTERNAL)
public class IndexingByIndex extends IndexingBase {
    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingByIndex.class);
    @Nonnull private static final byte[] START_BYTES = new byte[]{0x00};
    @Nonnull private static final byte[] END_BYTES = new byte[]{(byte)0xff};

    @Nonnull private final OnlineIndexer.IndexFromIndexPolicy policy;
    @Nonnull private final byte[] myTypeStamp;

    IndexingByIndex(@Nonnull IndexingCommon common,
                    @Nonnull OnlineIndexer.IndexFromIndexPolicy policy) {
        super(common);
        this.policy = policy;
        this.myTypeStamp = compileTypeStamp();
    }

    @Override
    @Nonnull
    byte[] getTypeStamp() {
        return myTypeStamp;
    }

    @Override
    boolean matchingTypeStamp(final byte[] stamp) {
        return Arrays.equals(stamp, myTypeStamp);
    }

    @Nonnull
    private byte[] compileTypeStamp() {
        return IndexMetaDataProto.IndexMetaDataIndexingStamp.newBuilder()
                .setMethod(IndexMetaDataProto.IndexMetaDataIndexingStamp.Method.BY_INDEX)
                .setSourceIndex(policy.getSourceIndex())
                .build().toByteArray();
    }

    @Nonnull
    @Override
    CompletableFuture<Void> buildIndexByEntityAsync() {
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose( store -> {
                    // first validate that both src and tgt are of a single, similar, type
                    final RecordMetaData metaData = store.getRecordMetaData();
                    final Index srcIndex = metaData.getIndex(Objects.requireNonNull(policy.getSourceIndex()));
                    final Collection<RecordType> srcRecordTypes = metaData.recordTypesForIndex(srcIndex);

                    validateOrThrowEx(!common.isSyntheticIndex(), "target index is synthetic");
                    validateOrThrowEx(common.recordTypes.size() == 1, "target index has multiple types");
                    validateOrThrowEx(srcRecordTypes.size() == 1, "source index has multiple types");
                    validateOrThrowEx(!srcIndex.getRootExpression().createsDuplicates(), "source index creates duplicates");
                    validateOrThrowEx(srcIndex.getType().equals(IndexTypes.VALUE), "source index is not a VALUE index");
                    validateOrThrowEx(common.recordTypes.equals(srcRecordTypes), "source index's type is not equal to target index's");

                    // all valid; back to the future. Note that for practical reasons, readability and idempotency will be validated later
                    return context.getReadVersionAsync()
                            .thenCompose(vignore -> {
                                SubspaceProvider subspaceProvider = common.getRecordStoreBuilder().getSubspaceProvider();
                                return subspaceProvider.getSubspaceAsync(context)
                                        .thenCompose(subspace -> buildIndexFromIndex(subspaceProvider, subspace, null, null));
                            });
                }));
    }

    private void maybeLogBuildProgress(SubspaceProvider subspaceProvider) {
        if (LOGGER.isInfoEnabled() && shouldLogBuildProgress()) {
            // todo: add progress
            final Index index = common.getIndex();
            LOGGER.info(KeyValueLogMessage.of("Built Range",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.RECORDS_SCANNED, common.getTotalRecordsScanned().get()),
                    LogMessageKeys.INDEXER_ID, common.getUuid());
        }
    }

    @Nonnull
    private CompletableFuture<Void> buildIndexFromIndex(@Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace, @Nullable byte[] start, @Nullable byte[] end) {
        return AsyncUtil.whileTrue(() -> {
            final List<Object> additionalLogMessageKeyValues = Arrays.asList(LogMessageKeys.CALLING_METHOD, "buildIndexFromIndex",
                    LogMessageKeys.RANGE_START, start,
                    LogMessageKeys.RANGE_END, end);

            return buildCommitRetryAsync( (store, recordsScanned) -> buildRangeOnly(store, start, end , recordsScanned),
                    true,
                    additionalLogMessageKeyValues)
                    .handle((hasMore, ex) -> {
                        if (ex == null) {
                            maybeLogBuildProgress(subspaceProvider);
                            if (Boolean.FALSE.equals(hasMore)) {
                                // all done
                                return AsyncUtil.READY_FALSE;
                            }
                            return throttleDelay(); // returns true after an appropriate delay (to avoid an overload)
                        }
                        final RuntimeException unwrappedEx = getRunner().getDatabase().mapAsyncToSyncException(ex);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(KeyValueLogMessage.of("possibly non-fatal error encountered building range",
                                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack())), ex);
                        }
                        throw unwrappedEx;
                    })
                    .thenCompose(Function.identity());
        }, getRunner().getExecutor());
    }

    @Nullable
    private static FDBStoredRecord<Message> castCursorResultToStoreRecord(@Nonnull RecordCursorResult<FDBIndexedRecord<Message>> cursorResult) {
        FDBIndexedRecord<Message> indexResult = cursorResult.get();
        return indexResult == null ? null : indexResult.getStoredRecord();
    }

    @Nonnull
    private CompletableFuture<Boolean> buildRangeOnly(@Nonnull FDBRecordStore store, byte[] startBytes, byte[] endBytes, @Nonnull AtomicLong recordsScanned) {
        // return false when done
        Index index = common.getIndex();
        final Subspace scannedRecordsSubspace = indexBuildScannedRecordsSubspace(store, index);
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if ( recordMetaDataProvider == null ||
                !store.getRecordMetaData().equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
        final String srcIndex = policy.getSourceIndex();

        final IndexMaintainer maintainer = store.getIndexMaintainer(index);

        // this should never happen. But it makes the compiler happy
        validateOrThrowEx(srcIndex != null, "source index is null");
        // idempotence - We could have verified it at the first iteration only, but the repeating checks seem harmless
        validateOrThrowEx(maintainer.isIdempotent(), "target index is not idempotent");
        // readability - This method shouldn't block if one has already opened the record store (as we did)
        validateOrThrowEx(store.isIndexReadable(srcIndex), "source index is not readable");

        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive(), startBytes, endBytes).iterator();

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(getLimit()); // always respectLimit in this path
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        return ranges.onHasNext().thenCompose(hasNext -> {
            if (Boolean.FALSE.equals(hasNext)) {
                return AsyncUtil.READY_FALSE; // no more missing ranges - all done
            }
            Range range = ranges.next();
            Tuple rangeStart = Arrays.equals(range.begin, START_BYTES) ? null : Tuple.fromBytes(range.begin);
            Tuple rangeEnd = Arrays.equals(range.end, END_BYTES) ? null : Tuple.fromBytes(range.end);

            RecordCursor<FDBIndexedRecord<Message>> cursor =
                    store.scanIndexRecords(srcIndex, IndexScanType.BY_VALUE, TupleRange.between(rangeStart, rangeEnd), range.begin, scanProperties);

            final AtomicLong recordsScannedCounter = new AtomicLong();
            final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
            final AtomicBoolean isEmpty = new AtomicBoolean(true);

            return iterateRangeOnly(store, cursor,
                    IndexingByIndex::castCursorResultToStoreRecord,
                    lastResult::set,
                    isEmpty, recordsScannedCounter
            ).thenCompose(vignore -> {
                long recordsScannedInTransaction = recordsScannedCounter.get();
                recordsScanned.addAndGet(recordsScannedInTransaction);
                if (common.isTrackProgress()) {
                    store.context.ensureActive().mutate(MutationType.ADD, scannedRecordsSubspace.getKey(),
                            FDBRecordStore.encodeRecordCount(recordsScannedInTransaction));
                }
                final byte[] realEnd =
                        isEmpty.get() ? null : lastResult.get().getContinuation().toBytes();

                return rangeSet.insertRange(store.ensureContextActive(), packOrNull(rangeStart), realEnd, true)
                        .thenApply(ignore -> !isEmpty.get());
            });
        });
    }

    // support rebuildIndexAsync
    @Nonnull
    @Override
    CompletableFuture<Void> rebuildIndexByEntityAsync(FDBRecordStore store) {
        AtomicReference<byte[]> nextCont = new AtomicReference<>();
        AtomicLong recordScanned = new AtomicLong();
        return AsyncUtil.whileTrue(() ->
                rebuildRangeOnly(store, nextCont.get(), recordScanned).thenApply(cont -> {
                    if (cont == null) {
                        return false;
                    }
                    nextCont.set(cont);
                    return true;
                }), store.getExecutor());
    }

    @Nonnull
    private CompletableFuture<byte[]> rebuildRangeOnly(@Nonnull FDBRecordStore store, byte[] cont, @Nonnull AtomicLong recordsScanned) {

        Index index = common.getIndex();
        final Subspace scannedRecordsSubspace = indexBuildScannedRecordsSubspace(store, index);
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if ( recordMetaDataProvider == null ||
                !store.getRecordMetaData().equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
        final String srcIndex = policy.getSourceIndex();

        final IndexMaintainer maintainer = store.getIndexMaintainer(index);

        // idempotence - We could have verified it at the first iteration only, but the repeating checks seem harmless
        validateOrThrowEx(maintainer.isIdempotent(), "target index is not idempotent");

        // readability - This method shouldn't block if one has already opened the record store (as we did)
        validateOrThrowEx(srcIndex != null && store.isIndexReadable(srcIndex), "source index is not readable");

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(getLimit()); // always respectLimit in this path
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        assert srcIndex != null ; // this can never happen; suppressing spotBug MS_PKGPROTECT (srcIndex mightbe null) compilation error
        RecordCursor<FDBIndexedRecord<Message>> cursor =
                store.scanIndexRecords(srcIndex, IndexScanType.BY_VALUE, TupleRange.ALL, cont, scanProperties);

        final AtomicLong recordsScannedCounter = new AtomicLong();
        final AtomicReference<RecordCursorResult<FDBIndexedRecord<Message>>> lastResult = new AtomicReference<>(RecordCursorResult.exhausted());
        final AtomicBoolean isEmpty = new AtomicBoolean(true);

        return iterateRangeOnly(store, cursor,
                IndexingByIndex::castCursorResultToStoreRecord,
                lastResult::set,
                isEmpty, recordsScannedCounter
        ).thenCompose(vignore -> {
            long recordsScannedInTransaction = recordsScannedCounter.get();
            recordsScanned.addAndGet(recordsScannedInTransaction);
            if (common.isTrackProgress()) {
                store.context.ensureActive().mutate(MutationType.ADD, scannedRecordsSubspace.getKey(),
                        FDBRecordStore.encodeRecordCount(recordsScannedInTransaction));
            }
            final byte[] retCont = isEmpty.get() ? null : lastResult.get().getContinuation().toBytes();
            return CompletableFuture.completedFuture( retCont );
        });
    }

    private void validateOrThrowEx(boolean isValid, @Nonnull String msg) {
        if (!isValid) {
            throw new ValidationException(msg,
                    LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                    LogMessageKeys.SOURCE_INDEX, policy.getSourceIndex(),
                    LogMessageKeys.INDEXER_ID, common.getUuid());
        }
    }

    /**
     * thrown when IndexFromIndex validation fails.
     */
    @SuppressWarnings("serial")
    public static class ValidationException extends RecordCoreException {
        public ValidationException(@Nonnull String msg, @Nullable Object ... keyValues) {
            super(msg, keyValues);
        }
    }
}
