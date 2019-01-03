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

import com.apple.foundationdb.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.provider.foundationdb.SplitHelper.unpackKey;

/**
 * Base class for {@link IndexMaintainer} implementation.
 *
 * @param <M> type used to represent stored records
 */
// TODO: Need more practical examples to confirm what goes into what base class(es).
@API(API.Status.MAINTAINED)
public abstract class StandardIndexMaintainer<M extends Message> extends IndexMaintainer<M> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardIndexMaintainer.class);

    protected StandardIndexMaintainer(IndexMaintainerState<M> state) {
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
        return new IndexEntry(unpackKey(subspace, kv), decodeValue(kv.getValue()));
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
    public CompletableFuture<Void> update(@Nullable final FDBStoredRecord<M> oldRecord, @Nullable final FDBStoredRecord<M> newRecord) {
        final FDBEvaluationContext<M> context = state.store.emptyEvaluationContext();
        List<IndexEntry> oldIndexEntries = filteredIndexEntries(context, oldRecord);
        List<IndexEntry> newIndexEntries = filteredIndexEntries(context, newRecord);
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

    /**
     * Filter out index keys according to {@link IndexMaintenanceFilter}.
     * Keys that do not pass the filter will not be stored / removed from the index.
     * @param context context for key evaluation
     * @param savedRecord record for key evaluation
     * @return filtered list of index keys for the given record
     */
    @Nullable
    protected List<IndexEntry> filteredIndexEntries(@Nonnull final FDBEvaluationContext<M> context,
                                                    @Nullable final FDBStoredRecord<M> savedRecord) {
        if (savedRecord == null) {
            return null;
        }
        final Message record = savedRecord.getRecord();
        long startTime = System.nanoTime();
        boolean filterIndexKeys = false;
        switch (state.filter.maintainIndex(state.index, record)) {
            case NONE:
                if (state.store.getTimer() != null) {
                    state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.SKIP_INDEX_RECORD, startTime);
                }
                return null;
            case SOME:
                filterIndexKeys = true;
                break;
            case ALL:
            default:
                break;
        }
        List<IndexEntry> indexEntries = evaluateIndex(context, savedRecord);
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
    protected Function<Void, CompletableFuture<Void>> updateIndexKeysFunction(@Nonnull final FDBStoredRecord<M> savedRecord,
                                                                              final boolean remove,
                                                                              @Nonnull final List<IndexEntry> indexEntries) {
        return vignore -> updateIndexKeys(savedRecord, remove, indexEntries);
    }
                                                                 
    /**
     * Update index according to record keys.
     * Often this operation returns an already completed future because there is no asynchronous work to be done.
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index
     * @param indexEntries the result of {@link #evaluateIndex(FDBEvaluationContext, FDBRecord)}
     * @return a future completed when update is done
     */
    protected CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBStoredRecord<M> savedRecord,
                                                      final boolean remove,
                                                      @Nonnull final List<IndexEntry> indexEntries) {
        for (IndexEntry entry : indexEntries) {
            updateOneKey(savedRecord, remove, entry);
        }
        return AsyncUtil.DONE;
    }

    /**
     * Store a single key in the index.
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index
     * @param indexEntry the entry for the index to be updated
     */
    protected void updateOneKey(@Nonnull final FDBStoredRecord<M> savedRecord,
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
            if (state.store.isIndexWriteOnly(state.index) && state.index.isUnique()) {
                updateUniquenessViolations(valueKey, savedRecord.getPrimaryKey(), null, true);
            }
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.DELETE_INDEX_ENTRY, startTime);
                state.store.countKeyValue(FDBStoreTimer.Counts.DELETE_INDEX_KEY, FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES, FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES,
                        keyBytes, valueBytes);
            }
        } else {
            checkKeyValueSizes(savedRecord, valueKey, value, keyBytes, valueBytes);
            if (state.index.isUnique()) {
                // This part needs to be synchronous so that if two records within the same transaction
                // are writing the same field with a unique index that one of them will see the write
                // from the other one and trigger an error.
                synchronized (state.context) {
                    if (!indexEntry.keyContainsNonUniqueNull()) {
                        AsyncIterable<KeyValue> kvs = state.transaction.getRange(state.indexSubspace.range(valueKey));
                        state.store.addUniquenessCheck(kvs, state.index, indexEntry, savedRecord.getPrimaryKey());
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
        }
    }

    @Override
    @Nonnull
    public void updateUniquenessViolations(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey, @Nullable Tuple existingKey, boolean remove) {
        byte[] uniquenessKeyBytes = state.store.indexUniquenessViolationsSubspace(state.index).pack(FDBRecordStore.uniquenessViolationKey(valueKey, primaryKey));
        if (remove) {
            state.transaction.clear(uniquenessKeyBytes);
        } else {
            state.transaction.set(uniquenessKeyBytes, (existingKey == null) ? new byte[0] : existingKey.pack());
        }
    }

    @Override
    @Nonnull
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

    protected void checkKeyValueSizes(@Nonnull FDBStoredRecord<M> savedRecord,
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

    protected static final int TOO_LARGE_VALUE_MESSAGE_LIMIT = 100;

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
        return FDBRecordStore.indexEntryKey(state.index, valueKey, primaryKey);
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
    public <T> CompletableFuture<T> evaluateRecordFunction(@Nonnull FDBEvaluationContext<M> context,
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
        RangeSet rangeSet = new RangeSet(state.store.indexRangeSubspace(state.index));
        return rangeSet.contains(state.transaction, primaryKey.pack());
    }

    protected static <M extends Message> boolean canDeleteWhere(@Nonnull IndexMaintainerState<M> state, @Nonnull QueryToKeyMatcher.Match match, @Nonnull Key.Evaluated evaluated) {
        if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
            return false;
        }
        if (evaluated.equals(match.getEquality(state.store.emptyEvaluationContext()))) {
            return true;
        }
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(KeyValueLogMessage.of("IndexPrefixes don't align on deleteRecordsWhere",
                    LogMessageKeys.INITIAL_PREFIX, evaluated,
                    LogMessageKeys.SECOND_PREFIX, match.getEquality(),
                    LogMessageKeys.INDEX_NAME, state.index.getName()));
        }
        return false;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(state.index.getRootExpression());
        return canDeleteWhere(state, match, evaluated);
    }

    // Update index for deleting records where primary key starts with prefix. Prefix must be a prefix of the index grouping.
    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we sometimes
        // store data at the prefix key itself.
        final byte[] key = state.indexSubspace.pack(prefix);
        tr.clear(key, ByteArrayUtil.strinc(key));
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
     * @param context context in which the operation is performed
     * @param record the record from which the index will extract its key and value
     * @return a list of index keys and values
     */
    @Nonnull
    protected List<IndexEntry> evaluateIndex(@Nonnull FDBEvaluationContext<M> context,
                                             @Nonnull FDBRecord<M> record) {
        final KeyExpression rootExpression = state.index.getRootExpression();
        final List<Key.Evaluated> indexKeys = rootExpression.evaluate(context, record);

        // A KeyWithValue expression returns a value that is both the key and the value of the index,
        // so we have to tease them apart.
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression) rootExpression;
            return indexKeys.stream()
                    .map(key -> new IndexEntry(keyWithValueExpression.getKey(key), keyWithValueExpression.getValue(key)) )
                    .collect(Collectors.toList());
        }

        return indexKeys.stream().map(IndexEntry::new).collect(Collectors.toList());
    }
}
