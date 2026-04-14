/*
 * SlidingWindowIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer decorator that keeps only the top-N records based on a globally-scoped window key.
 * It wraps a delegate {@link IndexMaintainer} (which can be any index type, e.g. value, vector, etc.)
 * and applies sliding window semantics for mutations before delegating the actual index operations.
 *
 * <p>The sliding window subspace contains two logical regions:</p>
 * <ul>
 *     <li>Entries: {@code [windowKey..., primaryKey...] → primaryKey packed} — all tracked records, sorted by window key</li>
 *     <li>Meta: {@code ["count"] → long}, {@code ["boundary"] → packed entry key} — window count and boundary pointer</li>
 * </ul>
 *
 * <p>All entries live in a single sorted subspace. The boundary pointer marks the worst entry
 * currently in the window. Entries on the "good" side of the boundary are in the window (present
 * in the delegate index); entries on the "bad" side are overflow (not in the delegate).
 * Eviction and re-election shift the boundary pointer without moving data.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class SlidingWindowIndexMaintainer extends IndexMaintainer {

    protected enum Type {
        MIN(Comparator.naturalOrder()),
        MAX(Comparator.reverseOrder()),
        ;

        @Nonnull
        private final Comparator<Tuple> valueComparator;

        Type(@Nonnull Comparator<Tuple> valueComparator) {
            this.valueComparator = valueComparator;
        }

        /**
         * Returns true if candidate is strictly better than worst.
         */
        public boolean isBetter(@Nonnull Tuple candidate, @Nonnull Tuple worst) {
            return valueComparator.compare(candidate, worst) < 0;
        }

        /**
         * Returns true if entry is in the window (on the "good" side of the boundary, inclusive).
         * ASC/MIN: window = entries ≤ boundary. DESC/MAX: window = entries ≥ boundary.
         */
        public boolean isInWindow(@Nonnull Tuple entryKey, @Nonnull Tuple boundaryKey) {
            // isBetter means entryKey is on the good side; equals means it IS the boundary
            return isBetter(entryKey, boundaryKey) || entryKey.equals(boundaryKey);
        }

        /**
         * Returns true if the new entry is worse than (or equal to) the current boundary.
         * Used to determine if the boundary should be updated when the window is not yet full.
         */
        public boolean isWorseOrEqual(@Nonnull Tuple candidate, @Nonnull Tuple boundary) {
            return !isBetter(candidate, boundary);
        }

        /**
         * Get the best overflow entry (first entry on the overflow side of the boundary).
         * ASC/MIN: overflow is after boundary → forward scan from boundary (exclusive).
         * DESC/MAX: overflow is before boundary → reverse scan up to boundary (exclusive).
         */
        @Nonnull
        public CompletableFuture<KeyValue> getBestInOverflow(@Nonnull Subspace entriesSubspace,
                                                             @Nonnull Transaction tr,
                                                             @Nonnull byte[] boundaryPackedKey) {
            if (this == MIN) {
                // Overflow is after boundary: scan forward starting just after boundary
                final byte[] begin = afterKey(boundaryPackedKey);
                final byte[] end = entriesSubspace.range().end;
                return tr.getRange(begin, end, 1, false)
                        .asList()
                        .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
            } else {
                // Overflow is before boundary: scan reverse ending just before boundary
                final byte[] begin = entriesSubspace.range().begin;
                return tr.getRange(begin, boundaryPackedKey, 1, true)
                        .asList()
                        .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
            }
        }

        /**
         * After evicting the boundary entry, find the new boundary (the next entry inward).
         * ASC/MIN: new boundary = entry just before old boundary (reverse scan).
         * DESC/MAX: new boundary = entry just after old boundary (forward scan).
         */
        @Nonnull
        public CompletableFuture<KeyValue> getNewBoundaryAfterEviction(@Nonnull Subspace entriesSubspace,
                                                                       @Nonnull Transaction tr,
                                                                       @Nonnull byte[] oldBoundaryPackedKey) {
            if (this == MIN) {
                // Window is on the left; new boundary = entry just before old boundary
                final byte[] begin = entriesSubspace.range().begin;
                return tr.getRange(begin, oldBoundaryPackedKey, 1, true)
                        .asList()
                        .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
            } else {
                // Window is on the right; new boundary = entry just after old boundary
                final byte[] begin = afterKey(oldBoundaryPackedKey);
                final byte[] end = entriesSubspace.range().end;
                return tr.getRange(begin, end, 1, false)
                        .asList()
                        .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
            }
        }
    }

    private static final Tuple ENTRIES_SUBSPACE_KEY = Tuple.from(0);
    private static final Tuple META_SUBSPACE_KEY = Tuple.from(1);
    private static final Tuple COUNT_KEY = Tuple.from("count");
    private static final Tuple BOUNDARY_KEY = Tuple.from("boundary");

    @Nonnull
    private final IndexMaintainer delegate;
    private final Type extremumType;
    private final int windowSize;
    @Nonnull
    private final KeyExpression windowKey;
    private final int windowKeyColumnSize;
    @Nullable
    private final KeyExpression partitionKey;
    private final int partitionKeyColumnSize;

    public SlidingWindowIndexMaintainer(@Nonnull IndexMaintainerState state, @Nonnull IndexMaintainer delegate) {
        super(state);
        this.delegate = delegate;
        final IndexPredicate.RowNumberWindowPredicate predicate = getQualifyPredicate(state.index);
        this.windowKey = predicate.getWindowKey();
        this.windowKeyColumnSize = windowKey.getColumnSize();
        this.windowSize = predicate.getSize();
        this.extremumType = predicate.getDirection() == IndexPredicate.RowNumberWindowPredicate.Direction.ASC
                            ? Type.MIN : Type.MAX;
        this.partitionKey = predicate.getPartitionKey();
        this.partitionKeyColumnSize = predicate.getPartitionKeyColumnSize();
    }

    @Nonnull
    protected static IndexPredicate.RowNumberWindowPredicate getQualifyPredicate(@Nonnull Index index) {
        IndexPredicate predicate = index.getPredicate();
        if (predicate != null) {
            IndexPredicate.validateRowNumberWindowPlacement(predicate);
        }
        if (predicate instanceof IndexPredicate.RowNumberWindowPredicate) {
            return (IndexPredicate.RowNumberWindowPredicate)predicate;
        }
        if (predicate instanceof IndexPredicate.AndPredicate) {
            for (IndexPredicate child : ((IndexPredicate.AndPredicate)predicate).getChildren()) {
                if (child instanceof IndexPredicate.RowNumberWindowPredicate) {
                    return (IndexPredicate.RowNumberWindowPredicate)child;
                }
            }
        }
        throw new MetaDataException("sliding window index requires a RowNumberWindowPredicate",
                LogMessageKeys.INDEX_NAME, index.getName());
    }

    // ===== Delegate all read/query operations to the inner maintainer =====

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        return delegate.scan(scanType, range, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanBounds scanBounds,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        return delegate.scan(scanBounds, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range,
                                                             @Nullable byte[] continuation,
                                                             @Nonnull ScanProperties scanProperties) {
        return delegate.scanUniquenessViolations(range, continuation, scanProperties);
    }

    @Override
    public CompletableFuture<Void> clearUniquenessViolations() {
        return delegate.clearUniquenessViolations();
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                           @Nullable ScanProperties scanProperties) {
        return delegate.validateEntries(continuation, scanProperties);
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return delegate.canEvaluateRecordFunction(function);
    }

    @Nullable
    @Override
    public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull FDBRecord<M> record) {
        return delegate.evaluateIndex(record);
    }

    @Nullable
    @Override
    public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable FDBIndexableRecord<M> savedRecord) {
        return delegate.filteredIndexEntries(savedRecord);
    }

    @Nonnull
    @Override
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        return delegate.evaluateRecordFunction(context, function, record);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        return delegate.canEvaluateAggregateFunction(function);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return delegate.evaluateAggregateFunction(function, range, isolationLevel);
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        if (!delegate.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        // Only support deleteWhere if the sliding window is partitioned and the prefix
        // covers the partition columns.
        return partitionKeyColumnSize > 0 && evaluated.size() <= partitionKeyColumnSize;
    }

    @Nonnull
    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        return delegate.performOperation(operation);
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public RecordCursor<FDBIndexedRawRecord> scanRemoteFetch(@Nonnull final IndexScanBounds scanBounds,
                                                             @Nullable final byte[] continuation,
                                                             @Nonnull final ScanProperties scanProperties,
                                                             int commonPrimaryKeyLength) {
        return delegate.scanRemoteFetch(scanBounds, continuation, scanProperties, commonPrimaryKeyLength);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        return delegate.addedRangeWithKey(primaryKey);
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public CompletableFuture<Void> mergeIndex() {
        return delegate.mergeIndex();
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    @Nullable
    public IndexScrubbingTools<?> getIndexScrubbingTools(IndexScrubbingTools.ScrubbingType type) {
        return delegate.getIndexScrubbingTools(type);
    }

    // ===== Sliding window mutation logic =====

    @Override
    public boolean isIdempotent() {
        return false;
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        CompletableFuture<Void> future = AsyncUtil.DONE;
        if (oldRecord != null && passesBaseFilter(oldRecord)) {
            future = future.thenCompose(vignore -> handleDelete(oldRecord));
        }
        if (newRecord != null && passesBaseFilter(newRecord)) {
            final CompletableFuture<Void> prev = future;
            future = prev.thenCompose(vignore -> handleInsert(newRecord));
        }
        return future;
    }

    private <M extends Message> boolean passesBaseFilter(@Nonnull FDBIndexableRecord<M> record) {
        final IndexPredicate predicate = state.index.getPredicate();
        if (predicate == null) {
            return true;
        }
        return predicate.shouldIndexThisRecord(state.store, record);
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable FDBIndexableRecord<M> oldRecord,
                                                                            @Nullable FDBIndexableRecord<M> newRecord) {
        return update(oldRecord, newRecord);
    }

    /**
     * Evaluates the partition key from a record. Returns an empty tuple if there is no partition.
     */
    @Nonnull
    private <M extends Message> Tuple evaluatePartition(@Nonnull FDBIndexableRecord<M> record) {
        if (partitionKey == null) {
            return Tuple.from();
        }
        return partitionKey.evaluateSingleton(record).toTuple();
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleInsert(@Nonnull final FDBIndexableRecord<M> savedRecord) {
        final Subspace swSubspace = getSlidingWindowSubspace();
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();
        final Tuple partitionTuple = evaluatePartition(savedRecord);

        // Scope by partition first, then by entries/meta
        final Subspace partitionSubspace = swSubspace.subspace(partitionTuple);
        final Subspace entriesSubspace = partitionSubspace.subspace(ENTRIES_SUBSPACE_KEY);
        final Subspace metaSubspace = partitionSubspace.subspace(META_SUBSPACE_KEY);

        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();
        final Tuple entryKey = windowValue.addAll(primaryKey);

        // Always write the entry to the entries subspace
        tr.set(entriesSubspace.pack(entryKey), primaryKey.pack());

        final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
        final byte[] boundaryMetaKey = metaSubspace.pack(BOUNDARY_KEY);

        return tr.get(counterKey).thenCompose(counterBytes -> {
            final long count = counterBytes == null ? 0L : decodeLong(counterBytes);

            if (count < windowSize) {
                // Window not full: add to delegate, update count, maybe update boundary
                return delegate.update(null, savedRecord).thenCompose(vignore ->
                        tr.get(boundaryMetaKey).thenApply(boundaryBytes -> {
                            tr.set(counterKey, encodeLong(count + 1));
                            if (boundaryBytes == null || extremumType.isWorseOrEqual(entryKey,
                                    Tuple.fromBytes(boundaryBytes))) {
                                tr.set(boundaryMetaKey, entryKey.pack());
                            }
                            return null;
                        })
                );
            } else {
                // Window full: read boundary to compare
                return tr.get(boundaryMetaKey).thenCompose(boundaryBytes -> {
                    if (boundaryBytes == null) {
                        // Shouldn't happen if count >= windowSize, but handle gracefully
                        return AsyncUtil.DONE;
                    }
                    final Tuple boundaryEntryKey = Tuple.fromBytes(boundaryBytes);
                    final Tuple boundaryWindowTuple = TupleHelpers.subTuple(boundaryEntryKey, 0, windowKeyColumnSize);

                    if (!extremumType.isBetter(windowValue, boundaryWindowTuple)) {
                        // New entry is not better than boundary: it's already written to entries
                        // subspace on the overflow side. Nothing more to do.
                        return AsyncUtil.DONE;
                    }

                    // New entry is better: evict boundary from delegate, add new to delegate
                    final Tuple boundaryPrimaryKey = TupleHelpers.subTuple(boundaryEntryKey,
                            windowKeyColumnSize, boundaryEntryKey.size());
                    final byte[] oldBoundaryPackedKey = entriesSubspace.pack(boundaryEntryKey);

                    return state.store.loadRecordAsync(boundaryPrimaryKey).thenCompose(evictedRecord -> {
                        CompletableFuture<Void> removeFuture = (evictedRecord != null)
                                                               ? delegate.update(evictedRecord, null) : AsyncUtil.DONE;

                        return removeFuture.thenCompose(v2 ->
                                delegate.update(null, savedRecord).thenCompose(v3 ->
                                        // Find new boundary: the entry just inside the old boundary
                                        extremumType.getNewBoundaryAfterEviction(entriesSubspace, tr,
                                                oldBoundaryPackedKey).thenApply(newBoundaryKV -> {
                                            if (newBoundaryKV != null) {
                                                final Tuple newBoundaryKey = entriesSubspace.unpack(
                                                        newBoundaryKV.getKey());
                                                tr.set(boundaryMetaKey, newBoundaryKey.pack());
                                            }
                                            return null;
                                        })
                                )
                        );
                    });
                });
            }
        });
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleDelete(@Nonnull final FDBIndexableRecord<M> savedRecord) {
        final Subspace swSubspace = getSlidingWindowSubspace();
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();
        final Tuple partitionTuple = evaluatePartition(savedRecord);

        // Scope by partition first, then by entries/meta
        final Subspace partitionSubspace = swSubspace.subspace(partitionTuple);
        final Subspace entriesSubspace = partitionSubspace.subspace(ENTRIES_SUBSPACE_KEY);
        final Subspace metaSubspace = partitionSubspace.subspace(META_SUBSPACE_KEY);

        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();
        final Tuple entryKey = windowValue.addAll(primaryKey);
        final byte[] packedEntryKey = entriesSubspace.pack(entryKey);

        // Check if this entry exists in the entries subspace
        return tr.get(packedEntryKey).thenCompose(entryValue -> {
            if (entryValue == null) {
                // Not tracked, no-op
                return AsyncUtil.DONE;
            }

            // Remove from entries subspace
            tr.clear(packedEntryKey);

            final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
            final byte[] boundaryMetaKey = metaSubspace.pack(BOUNDARY_KEY);

            return tr.get(boundaryMetaKey).thenCompose(boundaryBytes -> {
                if (boundaryBytes == null) {
                    // No boundary → window was empty or corrupted, just clean up
                    return AsyncUtil.DONE;
                }
                final Tuple boundaryEntryKey = Tuple.fromBytes(boundaryBytes);

                if (!extremumType.isInWindow(entryKey, boundaryEntryKey)) {
                    // Entry was in overflow: already removed from entries, nothing else to do
                    return AsyncUtil.DONE;
                }

                // Entry was in the window: remove from delegate, update count, re-elect
                return tr.get(counterKey).thenCompose(counterBytes -> {
                    final long count = counterBytes == null ? 0L : decodeLong(counterBytes);
                    final long newCount = Math.max(0, count - 1);
                    tr.set(counterKey, encodeLong(newCount));

                    return delegate.update(savedRecord, null).thenCompose(vignore -> {
                        // If we deleted the boundary itself, find the new boundary (entry just inside)
                        CompletableFuture<byte[]> updatedBoundaryFuture;
                        if (entryKey.equals(boundaryEntryKey)) {
                            updatedBoundaryFuture = extremumType.getNewBoundaryAfterEviction(
                                    entriesSubspace, tr, packedEntryKey).thenApply(newBoundaryKV -> {
                                if (newBoundaryKV != null) {
                                    final Tuple newBKey = entriesSubspace.unpack(newBoundaryKV.getKey());
                                    tr.set(boundaryMetaKey, newBKey.pack());
                                    return newBoundaryKV.getKey();
                                } else {
                                    tr.clear(boundaryMetaKey);
                                    return null;
                                }
                            });
                        } else {
                            updatedBoundaryFuture = CompletableFuture.completedFuture(
                                    entriesSubspace.pack(boundaryEntryKey));
                        }

                        // Re-elect: find best overflow entry
                        return updatedBoundaryFuture.thenCompose(currentBoundaryPacked -> {
                            if (currentBoundaryPacked == null) {
                                // No boundary → no entries left
                                return AsyncUtil.DONE;
                            }
                            return extremumType.getBestInOverflow(entriesSubspace, tr,
                                    currentBoundaryPacked).thenCompose(bestKV -> {
                                if (bestKV == null) {
                                    return AsyncUtil.DONE;
                                }
                                // Promote: update boundary to include overflow entry, add to delegate
                                final Tuple bestEntryKey = entriesSubspace.unpack(bestKV.getKey());
                                final Tuple bestPrimaryKey = Tuple.fromBytes(bestKV.getValue());
                                tr.set(boundaryMetaKey, bestEntryKey.pack());
                                tr.set(counterKey, encodeLong(newCount + 1));

                                return state.store.loadRecordAsync(bestPrimaryKey).thenCompose(
                                        promotedRecord -> {
                                            if (promotedRecord != null) {
                                                return delegate.update(null, promotedRecord);
                                            }
                                            return AsyncUtil.DONE;
                                        });
                            });
                        });
                    });
                });
            });
        });
    }

    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull Transaction tr, @Nonnull Tuple prefix) {
        // deleteWhere is only supported when the sliding window has a partition prefix
        // (validated by canDeleteWhere). The given prefix must fit within the partition columns.
        Verify.verify(partitionKeyColumnSize >= prefix.size(),
                "deleteWhere prefix size %s exceeds partition key column size %s",
                prefix.size(), partitionKeyColumnSize);
        final byte[] key = getSlidingWindowSubspace().pack(prefix);
        Range indexRange = new Range(key, ByteArrayUtil.strinc(key));
        state.context.clear(indexRange);
        return delegate.deleteWhere(tr, prefix);
    }

    private static byte[] encodeLong(long value) {
        return Tuple.from(value).pack();
    }

    private static long decodeLong(byte[] bytes) {
        return Tuple.fromBytes(bytes).getLong(0);
    }

    /**
     * Returns a byte array that is the first key strictly greater than the given key.
     * Used for exclusive range starts in FDB's getRange(begin, end) API.
     */
    private static byte[] afterKey(byte[] key) {
        final byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        result[key.length] = 0x00;
        return result;
    }
}
