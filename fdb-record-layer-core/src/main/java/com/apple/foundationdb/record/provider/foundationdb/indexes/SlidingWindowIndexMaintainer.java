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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.locking.LockIdentifier;
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
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
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
 * An index maintainer decorator that keeps only the top-N records based on a window key,
 * optionally grouped by a partition key. It wraps a vector (HNSW)
 * {@link IndexMaintainer} and applies sliding window semantics for mutations before delegating
 * the actual index operations. This bounds the size of the HNSW graph while maintaining search
 * quality over a curated subset of vectors.
 *
 * <p>Currently, only vector indexes are supported as the delegate. This restriction is enforced
 * by {@link SlidingWindowIndexMaintainerFactory#isSlidingWindowIndex(Index)} and validated by
 * {@link SlidingWindowIndexMaintainerFactory}.</p>
 *
 * <p>The sliding window subspace is organized as {@code <partition...> / <region> / ...} where
 * the partition prefix comes from the {@code PARTITION BY} clause in the
 * {@link IndexPredicate.RowNumberWindowPredicate}. When there is no partition, the prefix is empty.</p>
 *
 * <p>Within each partition, there are two logical regions:</p>
 * <ul>
 *     <li>Entries (key 0): {@code [windowKey..., primaryKey...] → primaryKey packed} — all tracked records, sorted by window key</li>
 *     <li>Meta (key 1): {@code [3] → long} (window count), {@code [4] → packed entry key} (boundary pointer)</li>
 * </ul>
 *
 * <p>All entries live in a single sorted subspace per partition. The boundary pointer marks the
 * worst entry currently in the window. Entries on the "good" side of the boundary are in the
 * window (present in the delegate index); entries on the "bad" side are overflow (not in the
 * delegate). Eviction and re-election shift the boundary pointer without moving data.</p>
 *
 * <p>{@code deleteWhere} is only supported when the window is partitioned. It clears the entire
 * partition group from the sliding window subspace and delegates to the inner index.</p>
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
                final byte[] begin = ByteArrayUtil.keyAfter(boundaryPackedKey);
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
                final byte[] begin = ByteArrayUtil.keyAfter(oldBoundaryPackedKey);
                final byte[] end = entriesSubspace.range().end;
                return tr.getRange(begin, end, 1, false)
                        .asList()
                        .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
            }
        }
    }

    /** Subspace key for the entries region within a partition. */
    private static final Tuple ENTRIES_SUBSPACE_KEY = Tuple.from(0);
    /** Subspace key for the metadata region within a partition. */
    private static final Tuple META_SUBSPACE_KEY = Tuple.from(1);
    /** Meta key for the window count (long). */
    private static final Tuple COUNT_KEY = Tuple.from(3);
    /** Meta key for the boundary pointer (packed tuple of windowValue + primaryKey). */
    private static final Tuple BOUNDARY_KEY = Tuple.from(4);

    @Nonnull
    private final IndexMaintainer delegate;
    @Nonnull
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
        this.windowKey = predicate.getOrderingKey();
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

    @Nonnull
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
        if (partitionKey == null) {
            return false;
        }
        final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(partitionKey);
        return StandardIndexMaintainer.canDeleteWhere(state, match, evaluated);
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
    public boolean isIdempotent() {
        return delegate.isIdempotent();
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        final Subspace swSubspace = getSlidingWindowSubspace();
        return state.context.doWithWriteLock(new LockIdentifier(swSubspace), () -> {
            CompletableFuture<Void> future = AsyncUtil.DONE;

            if (oldRecord != null) {
                final var filteringType = IndexMaintenanceUtils.getFilterTypeForRecord(state, oldRecord);
                if (filteringType == IndexMaintenanceFilter.IndexValues.SOME) {
                    throw new RecordCoreException("filtering type SOME is not supported")
                            .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
                } else if (filteringType == IndexMaintenanceFilter.IndexValues.ALL) {
                    future = future.thenCompose(vignore -> handleDelete(oldRecord));
                }
            }
            if (newRecord != null) {
                final var filteringType = IndexMaintenanceUtils.getFilterTypeForRecord(state, newRecord);
                if (filteringType == IndexMaintenanceFilter.IndexValues.SOME) {
                    throw new RecordCoreException("filtering type SOME is not supported")
                            .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
                } else if (filteringType == IndexMaintenanceFilter.IndexValues.ALL) {
                    future = future.thenCompose(vignore -> handleInsert(newRecord));
                }
            }
            return future;
        });
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable FDBIndexableRecord<M> oldRecord,
                                                                            @Nullable FDBIndexableRecord<M> newRecord) {
        // During a write-only index build, the sliding window cannot rely on the normal
        // update(old, new) contract because the indexer may have already processed newRecord
        // in an earlier range scan. If we blindly call update(null, newRecord), the window
        // counter would be incremented a second time, leading to an inflated count and
        // incorrect eviction/re-election behavior.
        //
        // The standard index maintainer (StandardIndexMaintainer.updateWriteOnlyByRecords)
        // handles this by checking the range set to see if the record's primary key has
        // already been built. The sliding window takes a simpler approach: preemptively
        // delete newRecord from the window (if it exists) before applying the full
        // update(old, new). This is safe because:
        //  - If newRecord was NOT previously indexed, the delete is a no-op (the entry
        //    simply isn't found in the entries subspace).
        //  - If newRecord WAS previously indexed, the delete removes it from the window
        //    and decrements the counter, so the subsequent insert does not double-count.
        //
        // The net effect is that after this method completes, newRecord is indexed exactly
        // once with its current values, and the counter accurately reflects the window size.
        update(newRecord, null);
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
                        tr.get(boundaryMetaKey).thenAccept(boundaryBytes -> {
                            tr.set(counterKey, encodeLong(count + 1));
                            if (boundaryBytes == null || extremumType.isWorseOrEqual(entryKey,
                                    Tuple.fromBytes(boundaryBytes))) {
                                tr.set(boundaryMetaKey, entryKey.pack());
                            }
                        })
                );
            } else {
                // Window full: read boundary to compare
                return tr.get(boundaryMetaKey).thenCompose(boundaryBytes -> {
                    if (boundaryBytes == null) {
                        throw new RecordCoreException("sliding window boundary is missing but count >= windowSize, possible corruption")
                                .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
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
                                                oldBoundaryPackedKey).thenAccept(newBoundaryKV -> {
                                                    if (newBoundaryKV != null) {
                                                        final Tuple newBoundaryKey = entriesSubspace.unpack(
                                                                newBoundaryKV.getKey());
                                                        tr.set(boundaryMetaKey, newBoundaryKey.pack());
                                                    }
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
                    throw new RecordCoreException("sliding window boundary is missing but entry exists, possible corruption")
                            .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
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
        // (validated by canDeleteWhere). The given prefix must be an actual prefix of the partitioning key.
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
}
