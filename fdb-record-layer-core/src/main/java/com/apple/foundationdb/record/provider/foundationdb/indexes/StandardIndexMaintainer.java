/*
 * StandardIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexPrefetchRangeKeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.provider.foundationdb.SplitHelper.unpackKey;

/**
 * Base class for {@link IndexMaintainer} implementation.
 *
 */
// TODO: Need more practical examples to confirm what goes into what base class(es).
@API(API.Status.MAINTAINED)
public abstract class StandardIndexMaintainer extends IndexMaintainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardIndexMaintainer.class);
    protected static final int TOO_LARGE_VALUE_MESSAGE_LIMIT = 100;

    protected StandardIndexMaintainer(IndexMaintainerState state) {
        super(state);
    }

    @Nullable
    protected FDBStoreTimer getTimer() {
        return state.context.getTimer();
    }

    @Nonnull
    protected Executor getExecutor() {
        return state.context.getExecutor();
    }

    /**
     * Scan the primary index tree for the given range.
     * @param range range of index keys to scan
     * @param continuation any continuation from previous scan
     * @param scanProperties any limits on the scan
     * @return a cursor of index entries within the given range
     */
    @SuppressWarnings("PMD.CloseResource")
    protected RecordCursor<IndexEntry> scan(@Nonnull final TupleRange range,
                                            @Nullable byte[] continuation,
                                            @Nonnull ScanProperties scanProperties) {
        final RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(state.indexSubspace)
                .setContext(state.context)
                .setRange(range)
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return keyValues.map(kv -> {
            state.store.countKeyValue(FDBStoreTimer.Counts.LOAD_INDEX_KEY, FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES,
                    kv);
            return unpackKeyValue(kv);
        });
    }

    @Nonnull
    /**
     * An implementation of the {@link #scanRemoteFetch} method for the {@link IndexScanType.BY_VALUE} case.
     * Index Maintainers that support the {@link #scanRemoteFetch} method can use this implementation. Note that this
     * method is not supported by default by an index maintainer.
     */
    @SuppressWarnings("PMD.CloseResource")
    protected RecordCursor<FDBIndexedRawRecord> scanRemoteFetchByValue(@Nonnull final IndexScanBounds scanBounds,
                                                                       @Nullable final byte[] continuation,
                                                                       @Nonnull final ScanProperties scanProperties,
                                                                       int commonPrimaryKeyLength) {
        if (commonPrimaryKeyLength <= 0) {
            throw new RecordCoreArgumentException("scanRemoteFetch requires a positive commonPrimaryKeyLength");
        }
        if (!scanBounds.getScanType().equals(IndexScanType.BY_VALUE) || (!(scanBounds instanceof IndexScanRange))) {
            throw new RecordCoreArgumentException("scanRemoteFetch can only be used with VALUE index scan type and Range Scan");
        }
        IndexScanRange scanRange = (IndexScanRange)scanBounds;
        Tuple mapper = createRemoteFetchMapper(commonPrimaryKeyLength);
        final RecordCursor<MappedKeyValue> keyValues = IndexPrefetchRangeKeyValueCursor.Builder.newBuilder(state.indexSubspace, mapper.pack())
                .setContext(state.context)
                .setRange(scanRange.getScanRange())
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return keyValues.map(this::unpackRemoteFetchRecord);
    }

    /**
     * Convert stored key value pair into an index entry.
     * @param kv a raw key-value from the database
     * @return an index entry
     */
    @Nonnull
    protected IndexEntry unpackKeyValue(@Nonnull final KeyValue kv) {
        return unpackKeyValue(state.indexSubspace, kv);
    }

    /**
     * Convert stored key value pair located in the given subspace into an index entry.
     * @param subspace the database subspace for the index
     * @param kv a raw key-value within {@code subspace}
     * @return an index entry
     */
    @Nonnull
    protected IndexEntry unpackKeyValue(@Nonnull final Subspace subspace, @Nonnull final KeyValue kv) {
        return new IndexEntry(state.index, unpackKey(subspace, kv), decodeValue(kv.getValue()));
    }

    @Nonnull
    protected FDBIndexedRawRecord unpackRemoteFetchRecord(@Nonnull MappedKeyValue indexKeyValue) {
        IndexEntry indexEntry = new IndexEntry(state.index, unpackKey(state.indexSubspace, indexKeyValue), decodeValue(indexKeyValue.getValue()));
        return new FDBIndexedRawRecord(indexEntry, indexKeyValue);
    }

    /**
     * Decode value portion of key value pair.
     * @param value the raw value portion of a key value pair
     * @return a decoded tuple of any values stored in the value side of the index, which is
     * usually empty
     */
    @Nonnull
    protected Tuple decodeValue(@Nonnull final byte[] value) {
        return value.length == 0 ? TupleHelpers.EMPTY : Tuple.fromBytes(value);
    }

    public boolean skipUpdateForUnchangedKeys() {
        return true;
    }

    @Override
    @Nonnull
    public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                              @Nullable final FDBIndexableRecord<M> newRecord) {
        List<IndexEntry> oldIndexEntries = filteredIndexEntries(oldRecord);
        List<IndexEntry> newIndexEntries = filteredIndexEntries(newRecord);
        if (oldIndexEntries != null && newIndexEntries != null && skipUpdateForUnchangedKeys()) {
            // Remove unchanged keys from list of keys to update.
            List<IndexEntry> commonKeys = commonKeys(oldIndexEntries, newIndexEntries);
            if (!commonKeys.isEmpty()) {
                oldIndexEntries = makeMutable(oldIndexEntries);
                oldIndexEntries.removeAll(commonKeys);
                newIndexEntries = makeMutable(newIndexEntries);
                newIndexEntries.removeAll(commonKeys);
            }
        }
        // If updateIndexKeys has any async work, allow it to complete before starting the next step.
        // This guarantees consistent state for any persistent data structures that are modified in this transaction.
        CompletableFuture<Void> future = AsyncUtil.DONE;
        if (oldIndexEntries != null && !oldIndexEntries.isEmpty()) {
            final Function<Void, CompletableFuture<Void>> oldUpdate =
                    updateIndexKeysFunction(oldRecord, true, oldIndexEntries);
            if (MoreAsyncUtil.isCompletedNormally(future)) {
                future = oldUpdate.apply(null);
            } else {
                future = future.thenCompose(oldUpdate);
            }
        }
        if (newIndexEntries != null && !newIndexEntries.isEmpty()) {
            final Function<Void, CompletableFuture<Void>> newUpdate =
                    updateIndexKeysFunction(newRecord, false, newIndexEntries);
            if (MoreAsyncUtil.isCompletedNormally(future)) {
                future = newUpdate.apply(null);
            } else {
                future = future.thenCompose(newUpdate);
            }
        }
        return future;
    }

    @Override
    @Nonnull
    public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        if (isIdempotent()) {
            // Idempotent indexes can just update the index data structures directly
            return update(oldRecord, newRecord);
        }
        return state.store.loadIndexingTypeStampAsync(state.index).thenCompose(stamp -> {
            if (stamp == null) {
                // Either the index build has not started (in which case the record should not be
                // indexed) or this is a by-records build that did not write the stamp, which can
                // happen with certain kinds of segmented index builds.
                return updateWriteOnlyByRecords(oldRecord, newRecord);
            }
            switch (stamp.getMethod()) {
                case BY_RECORDS:
                case MULTI_TARGET_BY_RECORDS:
                case MUTUAL_BY_RECORDS:
                    return updateWriteOnlyByRecords(oldRecord, newRecord);
                case BY_INDEX:
                    Object sourceIndexKey = Tuple.fromBytes(stamp.getSourceIndexSubspaceKey().toByteArray()).get(0);
                    Index sourceIndex = state.store.getRecordMetaData().getIndexFromSubspaceKey(sourceIndexKey);
                    return updateWriteOnlyByIndex(sourceIndex, oldRecord, newRecord);
                default:
                    throw new RecordCoreException("unable to update write-only index with current type stamp")
                            .addLogInfo("stamp", stamp);
            }
        });
    }

    private <M extends Message> CompletableFuture<Void> updateWriteOnlyByRecords(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        // Check if the record has been built by checking its primary key in the range set. Update the index
        // if it is a built range
        Tuple primaryKey = oldRecord == null ? Verify.verifyNotNull(newRecord).getPrimaryKey() : oldRecord.getPrimaryKey();
        return addedRangeWithKey(primaryKey).thenCompose(inRange ->
                inRange ? update(oldRecord, newRecord) : AsyncUtil.DONE);
    }

    @SuppressWarnings("java:S3776") // Trying to simplify this method cognitive complexity seems to make it harder to follow
    private <M extends Message> CompletableFuture<Void> updateWriteOnlyByIndex(@Nonnull Index sourceIndex, @Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        IndexMaintainer sourceIndexMaintainer = state.store.getIndexMaintainer(sourceIndex);
        Tuple oldEntryKey = evaluateSingletonIndexKey(sourceIndex, sourceIndexMaintainer, oldRecord);
        Tuple newEntryKey = evaluateSingletonIndexKey(sourceIndex, sourceIndexMaintainer, newRecord);
        if (oldEntryKey != null && newEntryKey != null) {
            if (oldEntryKey.equals(newEntryKey)) {
                // The old and new record use the same key in the source index, so check if it has been
                // built, and update the index if so
                return addedRangeWithKey(oldEntryKey).thenCompose(inRange ->
                        inRange ? update(oldRecord, newRecord) : AsyncUtil.DONE);
            } else {
                // The old and new record use different keys in the source index. Check each one individually,
                // and then simulate deleting the old record (if needed) and adding the new record (if needed)
                // Note: index maintainers on the same index are not thread safe, so the index updates need to
                // be serialized here, but the range set checks can be executed concurrently
                CompletableFuture<Boolean> oldInRangeFuture = addedRangeWithKey(oldEntryKey);
                CompletableFuture<Boolean> newInRangeFuture = addedRangeWithKey(newEntryKey);
                return oldInRangeFuture
                        .thenCompose(oldInRange -> oldInRange ? update(oldRecord, null) : AsyncUtil.DONE)
                        .thenCompose(ignore -> newInRangeFuture)
                        .thenCompose(newInRange -> newInRange ? update(null, newRecord) : AsyncUtil.DONE);
            }
        } else {
            Tuple entryKey = oldEntryKey == null ? newEntryKey : oldEntryKey;
            if (entryKey == null) {
                // Both the old and new entries are null. This can happen if there is an index maintenance filter
                // results in one (or both) of the records being excluded from indexing. If the record(s) is/are
                // excluded by the filter from the source index, then they must also be excluded from this index,
                // so it is safe to just do nothing
                return AsyncUtil.DONE;
            } else {
                // One of the entry keys is not null. Check if that one has been built, and then update if it
                // is in the built range
                return addedRangeWithKey(entryKey).thenCompose(inRange ->
                        inRange ? update(oldRecord, newRecord) : AsyncUtil.DONE);
            }
        }
    }

    @Nullable
    private static <M extends Message> Tuple evaluateSingletonIndexKey(Index index, IndexMaintainer maintainer, @Nullable FDBIndexableRecord<M> record) {
        if (record == null) {
            return null;
        }
        List<IndexEntry> entries = maintainer.filteredIndexEntries(record);
        if (entries == null || entries.isEmpty()) {
            // If there is an IndexMaintenanceFilter on the index, this can return null/an empty list even if the
            // index key expression always returns a single value. In this case, the record is excluded from the index
            return null;
        } else if (entries.size() != 1) {
            throw new RecordCoreException("index produced incorrect number of entries for use as source index");
        }
        IndexEntry entry = entries.get(0);
        // Make sure the primary key is included in the key
        return FDBRecordStoreBase.indexEntryKey(index, entry.getKey(), record.getPrimaryKey());
    }

    /**
     * Filter out index keys according to {@link IndexMaintenanceFilter}.
     * Keys that do not pass the filter will not be stored / removed from the index.
     * @param <M> the message type of the record
     * @param savedRecord record for key evaluation
     * @return filtered list of index keys for the given record
     */
    @Override
    @Nullable
    public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord) {
        if (savedRecord == null) {
            return null;
        }
        // Apply both filters:
        // 1. Index predicates (if exist)
        // 2. IndexMaintenanceFilter
        // In the longer term, we will probably think about deprecating the index maintenance filter.
        final FDBStoreTimer timer = state.store.getTimer();
        final IndexPredicate predicate = state.index.getPredicate();
        if (predicate != null) {
            final long startTime = System.nanoTime();
            final boolean useMe = predicate.shouldIndexThisRecord(state.store, savedRecord);
            // Note: for now, IndexPredicate will not support filtering of certain index entries
            if (timer != null) {
                final FDBStoreTimer.Events event =
                        useMe ?
                        FDBStoreTimer.Events.USE_INDEX_RECORD_BY_PREDICATE :
                        FDBStoreTimer.Events.SKIP_INDEX_RECORD_BY_PREDICATE;
                timer.recordSinceNanoTime(event, startTime);
            }
            if (!useMe) {
                // Here: index predicate filters out this record
                return null;
            }
        }
        final Message record = savedRecord.getRecord();
        long startTime = System.nanoTime();
        boolean filterIndexKeys = false;
        switch (state.filter.maintainIndex(state.index, record)) {
            case NONE:
                if (timer != null) {
                    timer.recordSinceNanoTime(FDBStoreTimer.Events.SKIP_INDEX_RECORD, startTime);
                }
                return null;
            case SOME:
                filterIndexKeys = true;
                break;
            case ALL:
            default:
                break;
        }
        List<IndexEntry> indexEntries = evaluateIndex(savedRecord);
        if (!filterIndexKeys) {
            return indexEntries;
        }
        int i = 0;
        while (i < indexEntries.size()) {
            if (state.filter.maintainIndexValue(state.index, record, indexEntries.get(i))) {
                i++;
            } else {
                indexEntries = makeMutable(indexEntries);
                indexEntries.remove(i);
                long endTime = System.nanoTime();
                if (state.store.getTimer() != null) {
                    state.store.getTimer().record(FDBStoreTimer.Events.SKIP_INDEX_ENTRY, endTime - startTime);
                }
                startTime = endTime;
            }
        }
        return indexEntries;
    }

    @Nonnull
    protected List<IndexEntry> commonKeys(@Nonnull List<IndexEntry> oldIndexEntries,
                                          @Nonnull List<IndexEntry> newIndexEntries) {
        List<IndexEntry> commonKeys = new ArrayList<>();
        for (IndexEntry oldEntry : oldIndexEntries) {
            if (newIndexEntries.contains(oldEntry)) {
                commonKeys.add(oldEntry);
            }
        }
        return commonKeys;
    }

    @Nonnull
    protected static <T> List<T> makeMutable(@Nonnull List<T> list) {
        if (list instanceof ArrayList) {
            return list;
        } else {
            return new ArrayList<>(list);
        }
    }

    @Nonnull
    protected <M extends Message> Function<Void, CompletableFuture<Void>> updateIndexKeysFunction(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                                                  final boolean remove,
                                                                                                  @Nonnull final List<IndexEntry> indexEntries) {
        return vignore -> updateIndexKeys(savedRecord, remove, indexEntries);
    }

    /**
     * Update index according to record keys.
     * Often this operation returns an already completed future because there is no asynchronous work to be done.
     * @param <M> the message type of the record
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index
     * @param indexEntries the result of {@link #evaluateIndex(FDBRecord)}
     * @return a future completed when update is done
     */
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        return CompletableFuture.allOf(indexEntries.stream()
                .map(entry -> updateOneKeyAsync(savedRecord, remove, entry))
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Store a single key in the index.
     * @param <M> the message type of the record
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index
     * @param indexEntry the entry for the index to be updated
     * @return a future completed when the key is updated
     */
    protected <M extends Message> CompletableFuture<Void> updateOneKeyAsync(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                            final boolean remove,
                                                                            @Nonnull final IndexEntry indexEntry) {
        final Tuple valueKey = indexEntry.getKey();
        final Tuple value = indexEntry.getValue();
        final long startTime = System.nanoTime();
        final Tuple entryKey = indexEntryKey(valueKey, savedRecord.getPrimaryKey());
        final byte[] keyBytes = state.indexSubspace.pack(entryKey);
        final byte[] valueBytes = value.pack();
        if (remove) {
            state.transaction.clear(keyBytes);
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.DELETE_INDEX_ENTRY, startTime);
                state.store.countKeyValue(FDBStoreTimer.Counts.DELETE_INDEX_KEY, FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES, FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES,
                        keyBytes, valueBytes);
            }
            if (isWriteOnlyOrUniquePending() && state.index.isUnique()) {
                return removeUniquenessViolationsAsync(valueKey, savedRecord.getPrimaryKey());
            } else {
                return AsyncUtil.DONE;
            }
        } else {
            checkKeyValueSizes(savedRecord, valueKey, value, keyBytes, valueBytes);
            if (state.index.isUnique()) {
                // This part needs to be synchronous so that if two records within the same transaction
                // are writing the same field with a unique index that one of them will see the write
                // from the other one and trigger an error.
                synchronized (state.context) {
                    if (!indexEntry.keyContainsNonUniqueNull()) {
                        checkUniqueness(savedRecord, indexEntry);
                    }
                    state.transaction.set(keyBytes, valueBytes);
                }
            } else {
                state.transaction.set(keyBytes, valueBytes);
            }
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.SAVE_INDEX_ENTRY, startTime);
                state.store.countKeyValue(FDBStoreTimer.Counts.SAVE_INDEX_KEY, FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES, FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES,
                        keyBytes, valueBytes);
            }
            return AsyncUtil.DONE;
        }
    }

    protected <M extends Message> void checkUniqueness(@Nonnull FDBIndexableRecord<M> savedRecord, @Nonnull IndexEntry indexEntry) {
        Tuple valueKey = indexEntry.getKey();
        AsyncIterable<KeyValue> kvs = state.transaction.getRange(state.indexSubspace.range(valueKey));
        Tuple primaryKey = savedRecord.getPrimaryKey();
        final CompletableFuture<Void> checker = state.store.getContext().instrument(FDBStoreTimer.Events.CHECK_INDEX_UNIQUENESS,
                AsyncUtil.forEach(kvs, kv -> {
                    Tuple existingEntry = unpackKey(getIndexSubspace(), kv);
                    Tuple existingKey = state.index.getEntryPrimaryKey(existingEntry);
                    if (!TupleHelpers.equals(primaryKey, existingKey)) {
                        if (state.store.isIndexWriteOnly(state.index)) {
                            addUniquenessViolation(valueKey, primaryKey, existingKey);
                            addUniquenessViolation(valueKey, existingKey, primaryKey);
                        } else {
                            throw new RecordIndexUniquenessViolation(state.index, indexEntry, primaryKey, existingKey);
                        }
                    }
                }, getExecutor()));
        // Add a pre-commit check to prevent accidentally committing and getting into an invalid state.
        state.store.addIndexUniquenessCommitCheck(state.index, checker);
    }

    private boolean isWriteOnlyOrUniquePending() {
        return state.store.isIndexWriteOnly(state.index) || state.store.isIndexReadableUniquePending(state.index);
    }

    /**
     * Add a uniqueness violation within the database. This is used to keep track of
     * uniqueness violations that occur when an index is in write-only mode, both during
     * the built itself and by other writes. This means that the writes will succeed, but
     * it will cause a later attempt to make the index readable to fail.
     * @param valueKey the indexed key that is (apparently) not unique
     * @param primaryKey the primary key of one record that is causing a violation
     * @param existingKey the primary key of another record that is causing a violation (or <code>null</code> if none specified)
     */
    protected void addUniquenessViolation(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey, @Nullable Tuple existingKey) {
        byte[] uniquenessKeyBytes = state.store.indexUniquenessViolationsSubspace(state.index).pack(FDBRecordStoreBase.uniquenessViolationKey(valueKey, primaryKey));
        state.transaction.set(uniquenessKeyBytes, (existingKey == null) ? new byte[0] : existingKey.pack());
    }

    /**
     * Remove a uniqueness violation within the database. This is used to keep track of
     * uniqueness violations that occur when an index is in write-only mode, both during
     * the built itself and by other writes. This means that the writes will succeed, but
     * it will cause a later attempt to make the index readable to fail.
     *
     * <p>This will remove the last uniqueness violation entry when removing the second
     * last entry that contains the value key.</p>
     * @param valueKey the indexed key that is (apparently) not unique
     * @param primaryKey the primary key of one record that is causing a violation
     * @return a future that is complete when the uniqueness violation is removed
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    protected CompletableFuture<Void> removeUniquenessViolationsAsync(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        Subspace uniqueValueSubspace = state.store.indexUniquenessViolationsSubspace(state.index).subspace(valueKey);
        state.transaction.clear(uniqueValueSubspace.pack(primaryKey));
        // Remove the last entry if it was the second last entry in the unique value subspace.
        RecordCursor<KeyValue> uniquenessViolationEntries = KeyValueCursor.Builder.withSubspace(uniqueValueSubspace)
                .setContext(state.context)
                .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(2)
                        .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                        .setDefaultCursorStreamingMode(CursorStreamingMode.WANT_ALL)
                        .build()))
                .build();
        return uniquenessViolationEntries.getCount().thenAccept(count -> {
            if (count == 1) {
                state.context.clear(Range.startsWith(uniqueValueSubspace.pack()));
            }
        });
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        final Subspace uniquenessViolationsSubspace = state.store.indexUniquenessViolationsSubspace(state.index);
        RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(uniquenessViolationsSubspace)
                .setContext(state.context)
                .setRange(range)
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return keyValues.map(kv -> unpackKeyValue(uniquenessViolationsSubspace, kv));
    }

    /**
     * Validate the integrity of the index (such as identifying index entries that do not point to records or
     * identifying records that do not point to valid index entries). The default implementation provided by the
     * <code>StandardIndexMaintainer</code> class is a no-op (performs no validation) and should be overridden by
     * implementing classes.
     * @param continuation any continuation from a previous validation invocation
     * @param scanProperties skip, limit and other properties of the validation (use default values if <code>null</code>)
     * @return a cursor over invalid index entries including reasons (the default is an empty cursor)
     */
    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                           @Nullable ScanProperties scanProperties) {
        return RecordCursor.empty();
    }

    /**
     * Validate entries in the index. It scans the index and checks if the record associated with each index entry exists.
     * @param continuation any continuation from a previous validation invocation
     * @param scanProperties skip, limit and other properties of the validation
     * @return a cursor over index entries that have no associated records including the reason
     */
    @Nonnull
    protected RecordCursor<InvalidIndexEntry> validateOrphanEntries(@Nullable byte[] continuation,
                                                                    @Nonnull ScanProperties scanProperties) {
        return scan(IndexScanType.BY_VALUE, TupleRange.ALL, continuation, scanProperties)
                .filterAsync(
                        indexEntry -> state.store
                                .hasIndexEntryRecord(indexEntry, IsolationLevel.SNAPSHOT)
                                .thenApply(has -> !has),
                        state.store.getPipelineSizer().getPipelineSize(PipelineOperation.INDEX_ASYNC_FILTER))
                .map(InvalidIndexEntry::newOrphan);
    }

    /**
     * Validate entries in the index. It scans the records and checks if the index entries associated with each record
     * exist. Note that it may not work for indexes on synthetic record types (e.g., join indexes).
     * @param continuation any continuation from a previous validation invocation
     * @param scanProperties skip, limit and other properties of the validation
     * @return a cursor over records that have no associated index entries including the reason
     */
    @Nonnull
    protected RecordCursor<InvalidIndexEntry> validateMissingEntries(@Nullable byte[] continuation,
                                                                     @Nonnull ScanProperties scanProperties) {
        final Collection<RecordType> recordTypes = state.store.getRecordMetaData().recordTypesForIndex(state.index);
        final FDBRecordStoreBase.PipelineSizer pipelineSizer = state.store.getPipelineSizer();
        return RecordCursor.flatMapPipelined(
                cont -> state.store.scanRecords(TupleRange.ALL, cont, scanProperties)
                        .filter(rec -> recordTypes.contains(rec.getRecordType())),
                (record, cont) -> {
                    List<IndexEntry> filteredIndexEntries = filteredIndexEntries(record);
                    return RecordCursor.fromList(filteredIndexEntries == null ? Collections.emptyList() :
                            filteredIndexEntries.stream()
                                    .map(indexEntryWithoutPrimaryKey -> new IndexEntry(
                                            indexEntryWithoutPrimaryKey.getIndex(),
                                            indexEntryKey(indexEntryWithoutPrimaryKey.getKey(), record.getPrimaryKey()),
                                            indexEntryWithoutPrimaryKey.getValue()
                                    ))
                                    .map(indexEntry -> InvalidIndexEntry.newMissing(indexEntry, record))
                                    .collect(Collectors.toList()),
                            cont);
                },
                continuation, pipelineSizer.getPipelineSize(PipelineOperation.RECORD_FUNCTION))
        .filterAsync(missingEntryCandidate -> {
            final byte[] keyBytes = state.indexSubspace.pack(missingEntryCandidate.getEntry().getKey());
            return state.transaction.get(keyBytes).thenApply(Objects::isNull);
        }, pipelineSizer.getPipelineSize(PipelineOperation.INDEX_ASYNC_FILTER));
    }

    protected <M extends Message> void checkKeyValueSizes(@Nonnull FDBIndexableRecord<M> savedRecord,
                                                          @Nonnull Tuple valueKey, @Nonnull Tuple value,
                                                          @Nonnull byte[] keyBytes, @Nonnull byte[] valueBytes) {
        if (keyBytes.length > state.store.getKeySizeLimit()) {
            throw new FDBExceptions.FDBStoreKeySizeException("index entry is too large to be stored in FDB key",
                        LogMessageKeys.PRIMARY_KEY, savedRecord.getPrimaryKey(),
                        LogMessageKeys.VALUE_KEY, trimTooLargeTuple(valueKey),
                        LogMessageKeys.INDEX_NAME, state.index.getName());
        }
        if (valueBytes.length > state.store.getValueSizeLimit()) {
            throw new FDBExceptions.FDBStoreValueSizeException("index entry is too large to be stored in FDB value",
                        LogMessageKeys.PRIMARY_KEY, savedRecord.getPrimaryKey(),
                        LogMessageKeys.VALUE, trimTooLargeTuple(value),
                        LogMessageKeys.INDEX_NAME, state.index.getName());
        }
    }

    protected static String trimTooLargeTuple(@Nonnull Tuple tuple) {
        final String fullString = tuple.toString();
        if (fullString.length() > TOO_LARGE_VALUE_MESSAGE_LIMIT) {
            return fullString.substring(0, TOO_LARGE_VALUE_MESSAGE_LIMIT) + "...";
        } else {
            return fullString;
        }
    }

    /**
     * The entire index key to be used, including both the indexed value(s) and the primary key(s), with redundancy
     * removed.
     * @param valueKey the indexed value(s) for the entry
     * @param primaryKey the primary key for the record
     * @return the key to use for an index entry
     */
    @Nonnull
    protected Tuple indexEntryKey(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
        return FDBRecordStoreBase.indexEntryKey(state.index, valueKey, primaryKey);
    }

    /**
     * Manually save an index entry, for example when rebuilding in place with a different storage format.
     * Does not check uniqueness or maintain any secondary subspaces.
     * @param keyValue the entry to save
     */
    protected void saveIndexEntryAsKeyValue(IndexEntry keyValue) {
        state.transaction.set(state.indexSubspace.pack(keyValue.getKey()), keyValue.getValue().pack());
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return false;
    }

    @Override
    @Nonnull
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        return unsupportedRecordFunction(function);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        return false;
    }

    @Override
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull final IsolationLevel isolationLevel) {
        return unsupportedAggregateFunction(function);
    }

    protected int getGroupingCount() {
        return ((GroupingKeyExpression)state.index.getRootExpression()).getGroupingCount();
    }

    protected int getGroupedCount() {
        return ((GroupingKeyExpression)state.index.getRootExpression()).getGroupedCount();
    }

    @Override
    public boolean isIdempotent() {
        return true;
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(state.store, state.index);
        return rangeSet.containsAsync(primaryKey.pack());
    }

    protected static boolean canDeleteWhere(@Nonnull IndexMaintainerState state, @Nonnull QueryToKeyMatcher.Match match, @Nonnull Key.Evaluated evaluated) {
        if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
            return false;
        }
        if (evaluated.equals(match.getEquality(state.store, EvaluationContext.EMPTY))) {
            return true;
        }
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(KeyValueLogMessage.of("IndexPrefixes don't align on deleteRecordsWhere",
                    LogMessageKeys.INITIAL_PREFIX, evaluated,
                    LogMessageKeys.SECOND_PREFIX, match.getEquality(state.store, EvaluationContext.EMPTY),
                    LogMessageKeys.INDEX_NAME, state.index.getName()));
        }
        return false;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(state.index.getRootExpression());
        return canDeleteWhere(state, match, evaluated);
    }

    protected boolean canDeleteGroup(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        KeyExpression rootExpression = state.index.getRootExpression();
        if (!(rootExpression instanceof GroupingKeyExpression)) {
            return false;
        }
        final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(rootExpression);
        return canDeleteWhere(state, match, evaluated);
    }

    // Update index for deleting records where primary key starts with prefix. Prefix must be a prefix of the index grouping.
    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we sometimes
        // store data at the prefix key itself.
        final byte[] key = state.indexSubspace.pack(prefix);
        Range indexRange = new Range(key, ByteArrayUtil.strinc(key));
        state.context.clear(indexRange);
        return AsyncUtil.DONE;
    }

    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        throw new RecordCoreException("Unsupported index operation",
                    LogMessageKeys.INDEX_NAME, state.index.getName(),
                    LogMessageKeys.INDEX_OPERATION, operation.getClass().getSimpleName());
    }

    /**
     * Apply the key and value expressions to a <code>record</code>.
     * @param <M> the message type of the record
     * @param record the record from which the index will extract its key and value
     * @return a list of index keys and values
     */
    @Override
    public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record) {
        final KeyExpression rootExpression = state.index.getRootExpression();
        final List<Key.Evaluated> indexKeys = rootExpression.evaluate(record);

        // A KeyWithValue expression returns a value that is both the key and the value of the index,
        // so we have to tease them apart.
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression) rootExpression;
            return indexKeys.stream()
                    .map(key -> new IndexEntry(state.index, keyWithValueExpression.getKey(key), keyWithValueExpression.getValue(key)) )
                    .collect(Collectors.toList());
        }

        return indexKeys.stream().map(key -> new IndexEntry(state.index, key)).collect(Collectors.toList());
    }

    /**
     * Create the list of index key-references that would allow the DB to prefetch the records pointed to by the index entries.
     * The commonPrimaryKey is the representation of the primary key for the referenced records.
     * The index structure is: [P1...Pn, I1...In, K1...Kn] where Px are the prefix elements, Ix are the index fields and Kx are the primary keys of the indexed record.
     * Since {@link Index#trimPrimaryKey(List)} removes redundant key entries, we need to construct the list of locations of the primary key
     * elements by using the keyLocations (if there are any), followed by the remaining key elements.
     * @param commonPrimaryKeyLength the length (# of elements) of the primary key (used to construct the PK locations in the de-referenced record)
     * @return A Tuple representing the Mapper structure required by the FDB getMappedRange call
     */
    @Nonnull
    private Tuple createRemoteFetchMapper(int commonPrimaryKeyLength) {
        int prefixLength = Tuple.fromBytes(state.indexSubspace.pack()).size();
        List<Integer> keyLocations = state.index.getEntryPrimaryKeyPositions(commonPrimaryKeyLength);

        Tuple result =  Tuple.fromBytes(state.store.recordsSubspace().pack());
        for (int i: keyLocations) {
            result = result.add("{K[" + (i + prefixLength) + "]}");
        }
        result = result.add("{...}");
        return result;
    }

    @Override
    public CompletableFuture<Void> mergeIndex() {
        return AsyncUtil.DONE;
    }
}
