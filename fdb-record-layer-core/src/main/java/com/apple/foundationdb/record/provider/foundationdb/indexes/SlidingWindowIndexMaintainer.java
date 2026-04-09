/*
 * SlidingWindowIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
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
 * <p>The secondary subspace is partitioned into three logical sub-subspaces:</p>
 * <ul>
 *     <li>Partition 0 (WINDOW): {@code [windowKey..., primaryKey...] → primaryKey packed} — records IN the window</li>
 *     <li>Partition 1 (OVERFLOW): {@code [windowKey..., primaryKey...] → primaryKey packed} — records NOT in the window</li>
 *     <li>Partition 2 (META): {@code ["count"] → long} — count of records in the window</li>
 * </ul>
 *
 * <p>On insert, if the window is full, the new record is compared against the worst entry in the window.
 * If better, it replaces the worst (which moves to overflow). If not better, it goes directly to overflow.</p>
 *
 * <p>On delete, if the deleted record was in the window, the best candidate from overflow is promoted
 * (re-election).</p>
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

        public boolean isBetter(@Nonnull Tuple candidate, @Nonnull Tuple worst) {
            return valueComparator.compare(candidate, worst) < 0;
        }

        /**
         * Read the worst entry in the window: a single entry scan.
         * MAX keeps highest values, so the worst is the lowest (forward scan).
         * MIN keeps lowest values, so the worst is the highest (reverse scan).
         */
        @Nonnull
        public CompletableFuture<KeyValue> getWorstInWindow(@Nonnull Subspace windowSubspace,
                                                             @Nonnull Transaction tr) {
            final boolean reverse = this == MIN;
            return tr.getRange(windowSubspace.range(), 1, reverse)
                    .asList()
                    .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
        }

        /**
         * Read the best candidate from overflow for re-election: a single entry scan.
         * MAX: best overflow = highest = reverse scan.
         * MIN: best overflow = lowest = forward scan.
         */
        @Nonnull
        public CompletableFuture<KeyValue> getBestInOverflow(@Nonnull Subspace overflowSubspace,
                                                              @Nonnull Transaction tr) {
            final boolean reverse = this == MAX;
            return tr.getRange(overflowSubspace.range(), 1, reverse)
                    .asList()
                    .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
        }
    }


    private static final Tuple WINDOW_SUBSPACE_KEY = Tuple.from(0);
    private static final Tuple OVERFLOW_SUBSPACE_KEY = Tuple.from(1);
    private static final Tuple META_SUBSPACE_KEY = Tuple.from(2);
    private static final Tuple COUNT_KEY = Tuple.from("count");

    @Nonnull
    private final IndexMaintainer delegate;
    private final Type extremumType;
    private final int windowSize;
    @Nonnull
    private final KeyExpression windowKey;
    private final int windowKeyColumnSize;

    public SlidingWindowIndexMaintainer(@Nonnull IndexMaintainerState state, @Nonnull IndexMaintainer delegate) {
        super(state);
        this.delegate = delegate;
        final IndexPredicate.RowNumberWindowPredicate predicate = getQualifyPredicate(state.index);
        this.windowKey = predicate.getWindowKey();
        this.windowKeyColumnSize = windowKey.getColumnSize();
        this.windowSize = predicate.getSize();
        this.extremumType = predicate.getDirection() == IndexPredicate.RowNumberWindowPredicate.Direction.ASC
                ? Type.MIN : Type.MAX;
    }

    @Nonnull
    protected static IndexPredicate.RowNumberWindowPredicate getQualifyPredicate(@Nonnull Index index) {
        IndexPredicate predicate = index.getPredicate();
        if (predicate != null) {
            IndexPredicate.validateRowNumberWindowPlacement(predicate);
        }
        if (predicate instanceof IndexPredicate.RowNumberWindowPredicate) {
            return (IndexPredicate.RowNumberWindowPredicate) predicate;
        }
        if (predicate instanceof IndexPredicate.AndPredicate) {
            for (IndexPredicate child : ((IndexPredicate.AndPredicate) predicate).getChildren()) {
                if (child instanceof IndexPredicate.RowNumberWindowPredicate) {
                    return (IndexPredicate.RowNumberWindowPredicate) child;
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
        return delegate.canDeleteWhere(matcher, evaluated);
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

    /**
     * Checks whether a record passes the non-window portion of the index predicate.
     * The full predicate is an AND of value predicates and the window predicate; the window
     * predicate always evaluates to {@code true}, so this effectively checks the value predicates.
     */
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
        // For non-idempotent indexes during write-only builds, use the same logic as update.
        // The sliding window state is authoritative.
        return update(oldRecord, newRecord);
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleInsert(@Nonnull final FDBIndexableRecord<M> savedRecord) {
        final Subspace secondarySubspace = getSecondarySubspace();
        final Subspace windowSubspace = secondarySubspace.subspace(WINDOW_SUBSPACE_KEY);
        final Subspace overflowSubspace = secondarySubspace.subspace(OVERFLOW_SUBSPACE_KEY);
        final Subspace metaSubspace = secondarySubspace.subspace(META_SUBSPACE_KEY);
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();

        // Evaluate window key
        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();

        // Read counter
        final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
        return tr.get(counterKey).thenCompose(counterBytes -> {
            final long count = counterBytes == null ? 0L : decodeLong(counterBytes);

            if (count < windowSize) {
                // Window not full yet: add to delegate index and window partition
                return delegate.update(null, savedRecord).thenApply(vignore -> {
                    writePartitionEntry(tr, windowSubspace, windowValue, primaryKey);
                    tr.set(counterKey, encodeLong(count + 1));
                    return null;
                });
            } else {
                // Window is full: read the worst entry in the window (single entry, O(1))
                return extremumType.getWorstInWindow(windowSubspace, tr).thenCompose(worstKV -> {
                    if (worstKV == null) {
                        // Should not happen if count >= windowSize, but handle gracefully
                        return addToOverflow(tr, overflowSubspace, windowValue, primaryKey);
                    }

                    final Tuple worstEntryKey = windowSubspace.unpack(worstKV.getKey());
                    final Tuple worstWindowTuple = TupleHelpers.subTuple(worstEntryKey, 0, windowKeyColumnSize);

                    if (!extremumType.isBetter(windowValue, worstWindowTuple)) {
                        // New record is not better than the worst in the window: add to overflow
                        return addToOverflow(tr, overflowSubspace, windowValue, primaryKey);
                    }

                    // New record IS better: evict worst from window to overflow, insert new into window
                    final Tuple worstPrimaryKey = Tuple.fromBytes(worstKV.getValue());
                    tr.clear(worstKV.getKey());

                    // Move evicted record to overflow
                    writePartitionEntry(tr, overflowSubspace, worstWindowTuple,
                            TupleHelpers.subTuple(worstEntryKey, windowKeyColumnSize, worstEntryKey.size()));

                    // Remove evicted record from delegate index
                    return state.store.loadRecordAsync(worstPrimaryKey).thenCompose(oldRecord -> {
                        CompletableFuture<Void> removeFuture;
                        if (oldRecord != null) {
                            removeFuture = delegate.update(oldRecord, null);
                        } else {
                            removeFuture = AsyncUtil.DONE;
                        }

                        // Add new record to delegate index and window partition
                        return removeFuture.thenCompose(vignore ->
                            delegate.update(null, savedRecord).thenApply(vignore2 -> {
                                writePartitionEntry(tr, windowSubspace, windowValue, primaryKey);
                                return null;
                            })
                        );
                    });
                });
            }
        });
    }

    @Nonnull
    private CompletableFuture<Void> addToOverflow(@Nonnull Transaction tr, @Nonnull Subspace overflowSubspace,
                                                   @Nonnull Tuple windowValue, @Nonnull Tuple primaryKey) {
        writePartitionEntry(tr, overflowSubspace, windowValue, primaryKey);
        return AsyncUtil.DONE;
    }

    private void writePartitionEntry(@Nonnull Transaction tr, @Nonnull Subspace partitionSubspace,
                                     @Nonnull Tuple windowValue, @Nonnull Tuple primaryKey) {
        final Tuple entryKey = windowValue.addAll(primaryKey);
        tr.set(partitionSubspace.pack(entryKey), primaryKey.pack());
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleDelete(@Nonnull final FDBIndexableRecord<M> savedRecord) {
        final Subspace secondarySubspace = getSecondarySubspace();
        final Subspace windowSubspace = secondarySubspace.subspace(WINDOW_SUBSPACE_KEY);
        final Subspace overflowSubspace = secondarySubspace.subspace(OVERFLOW_SUBSPACE_KEY);
        final Subspace metaSubspace = secondarySubspace.subspace(META_SUBSPACE_KEY);
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();

        // Evaluate window key
        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();

        final Tuple entryKey = windowValue.addAll(primaryKey);
        final byte[] packedWindowKey = windowSubspace.pack(entryKey);
        final byte[] packedOverflowKey = overflowSubspace.pack(entryKey);

        // Check if this record is in the window partition
        return tr.get(packedWindowKey).thenCompose(windowEntry -> {
            if (windowEntry != null) {
                // Record is in the window: remove it, update count, remove from delegate index
                tr.clear(packedWindowKey);
                final byte[] counterKey = metaSubspace.pack(COUNT_KEY);

                return tr.get(counterKey).thenCompose(counterBytes -> {
                    final long count = counterBytes == null ? 0L : decodeLong(counterBytes);
                    tr.set(counterKey, encodeLong(Math.max(0, count - 1)));

                    // Remove from delegate index
                    return delegate.update(savedRecord, null).thenCompose(vignore -> {
                        // Re-election: find best candidate from overflow
                        return extremumType.getBestInOverflow(overflowSubspace, tr).thenCompose(bestKV -> {
                            if (bestKV == null) {
                                // No overflow candidates
                                return AsyncUtil.DONE;
                            }
                            // Promote: move from overflow to window, add to delegate index
                            final Tuple bestEntryKey = overflowSubspace.unpack(bestKV.getKey());
                            final Tuple bestPrimaryKey = Tuple.fromBytes(bestKV.getValue());
                            final Tuple bestWindowTuple = TupleHelpers.subTuple(bestEntryKey, 0, windowKeyColumnSize);
                            final Tuple bestPKTuple = TupleHelpers.subTuple(bestEntryKey, windowKeyColumnSize, bestEntryKey.size());

                            // Remove from overflow
                            tr.clear(bestKV.getKey());
                            // Add to window
                            writePartitionEntry(tr, windowSubspace, bestWindowTuple, bestPKTuple);
                            // Restore count
                            tr.set(counterKey, encodeLong(Math.max(0, count - 1) + 1));

                            // Add promoted record to delegate index
                            return state.store.loadRecordAsync(bestPrimaryKey).thenCompose(promotedRecord -> {
                                if (promotedRecord != null) {
                                    return delegate.update(null, promotedRecord);
                                }
                                return AsyncUtil.DONE;
                            });
                        });
                    });
                });
            }

            // Check if it's in the overflow partition
            return tr.get(packedOverflowKey).thenCompose(overflowEntry -> {
                if (overflowEntry != null) {
                    // Record is in overflow: just remove it
                    tr.clear(packedOverflowKey);
                }
                // If in neither, no-op
                return AsyncUtil.DONE;
            });
        });
    }

    @SuppressWarnings("PMD.CloseResource")
    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull Transaction tr, @Nonnull Tuple prefix) {
        final Subspace secondarySubspace = getSecondarySubspace();
        final Subspace windowSubspace = secondarySubspace.subspace(WINDOW_SUBSPACE_KEY);
        final Subspace overflowSubspace = secondarySubspace.subspace(OVERFLOW_SUBSPACE_KEY);
        final Subspace metaSubspace = secondarySubspace.subspace(META_SUBSPACE_KEY);
        final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
        final KeyExpression rootExpression = state.index.getRootExpression();

        // The prefix is defined over the index root expression, but the secondary subspace
        // is keyed by (windowValue, primaryKey). We must load each record and evaluate the
        // index root expression to determine which entries match the prefix.
        return scanAndRemoveMatching(tr, windowSubspace, rootExpression, prefix)
                .thenCompose(removedFromWindow ->
                        scanAndRemoveMatching(tr, overflowSubspace, rootExpression, prefix)
                                .thenCompose(removedFromOverflow ->
                                        delegate.deleteWhere(tr, prefix).thenCompose(v -> {
                                            if (removedFromWindow == 0) {
                                                return AsyncUtil.DONE;
                                            }
                                            return tr.get(counterKey).thenCompose(counterBytes -> {
                                                final long count = counterBytes == null ? 0L : decodeLong(counterBytes);
                                                final long newCount = Math.max(0, count - removedFromWindow);
                                                tr.set(counterKey, encodeLong(newCount));

                                                return reelectFromOverflow(tr, windowSubspace, overflowSubspace,
                                                        counterKey, newCount, removedFromWindow);
                                            });
                                        })
                                )
                );
    }

    /**
     * Scan all entries in a partition subspace, load each record, evaluate the index root
     * expression, and clear entries whose index key starts with the given prefix.
     * @return the number of entries removed
     */
    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private CompletableFuture<Integer> scanAndRemoveMatching(@Nonnull Transaction tr,
                                                              @Nonnull Subspace partitionSubspace,
                                                              @Nonnull KeyExpression rootExpression,
                                                              @Nonnull Tuple prefix) {
        return tr.getRange(partitionSubspace.range())
                .asList()
                .thenCompose(entries -> {
                    CompletableFuture<Integer> result = CompletableFuture.completedFuture(0);
                    for (KeyValue kv : entries) {
                        final byte[] keyBytes = kv.getKey();
                        final Tuple primaryKey = Tuple.fromBytes(kv.getValue());
                        result = result.thenCompose(count ->
                                state.store.loadRecordAsync(primaryKey).thenApply(record -> {
                                    if (record != null) {
                                        final Key.Evaluated evaluated = rootExpression.evaluateSingleton(record);
                                        final Tuple indexKey = evaluated.toTuple();
                                        if (TupleHelpers.isPrefix(prefix, indexKey)) {
                                            tr.clear(keyBytes);
                                            return count + 1;
                                        }
                                    }
                                    return count;
                                })
                        );
                    }
                    return result;
                });
    }

    /**
     * Promote up to {@code slotsToFill} best candidates from overflow into the window,
     * adding each promoted record to the delegate index.
     */
    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private CompletableFuture<Void> reelectFromOverflow(@Nonnull Transaction tr,
                                                         @Nonnull Subspace windowSubspace,
                                                         @Nonnull Subspace overflowSubspace,
                                                         @Nonnull byte[] counterKey,
                                                         long currentCount,
                                                         int slotsToFill) {
        if (slotsToFill <= 0) {
            return AsyncUtil.DONE;
        }

        return extremumType.getBestInOverflow(overflowSubspace, tr).thenCompose(bestKV -> {
            if (bestKV == null) {
                // No more overflow candidates.
                return AsyncUtil.DONE;
            }

            final Tuple bestEntryKey = overflowSubspace.unpack(bestKV.getKey());
            final Tuple bestPrimaryKey = Tuple.fromBytes(bestKV.getValue());
            final Tuple bestWindowTuple = TupleHelpers.subTuple(bestEntryKey, 0, windowKeyColumnSize);
            final Tuple bestPKTuple = TupleHelpers.subTuple(bestEntryKey, windowKeyColumnSize, bestEntryKey.size());

            // Move from overflow to window.
            tr.clear(bestKV.getKey());
            writePartitionEntry(tr, windowSubspace, bestWindowTuple, bestPKTuple);
            final long newCount = currentCount + 1;
            tr.set(counterKey, encodeLong(newCount));

            // Add promoted record to delegate index.
            return state.store.loadRecordAsync(bestPrimaryKey).thenCompose(promotedRecord -> {
                CompletableFuture<Void> addFuture = AsyncUtil.DONE;
                if (promotedRecord != null) {
                    addFuture = delegate.update(null, promotedRecord);
                }
                return addFuture.thenCompose(vignore ->
                        reelectFromOverflow(tr, windowSubspace, overflowSubspace,
                                counterKey, newCount, slotsToFill - 1));
            });
        });
    }

    private static byte[] encodeLong(long value) {
        return Tuple.from(value).pack();
    }

    private static long decodeLong(byte[] bytes) {
        return Tuple.fromBytes(bytes).getLong(0);
    }
}
