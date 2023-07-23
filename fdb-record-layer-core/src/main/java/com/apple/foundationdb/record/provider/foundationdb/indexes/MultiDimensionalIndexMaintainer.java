/*
 * RankIndexMaintainer.java
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

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RTree;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AsyncIteratorCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An index maintainer for keeping a {@link com.apple.foundationdb.async.RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class MultiDimensionalIndexMaintainer extends StandardIndexMaintainer {
    private final RTree.Config config;

    public MultiDimensionalIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = RTreeIndexHelper.getConfig(state.index);
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange scanRange,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreException("Can only scan multidimensional index by value.");
        }

        final DimensionsKeyExpression dimensionsKeyExpression = getDimensionsKeyExpression(state.index.getRootExpression());
        final int prefixCount = dimensionsKeyExpression.getPrefixCount();
        final int dimensionsCount = dimensionsKeyExpression.getDimensionsCount();

        final CursorLimitManager cursorLimitManager = new CursorLimitManager(state.context, scanProperties);

        final Function<byte[], RecordCursor<IndexEntry>> outerFunction;
        if (prefixCount > 0) {
            outerFunction = outerContinuation -> scan(scanRange.prefix(prefixCount), outerContinuation, scanProperties);
        } else {
            outerFunction = outerContinuation -> RecordCursor.fromFuture(CompletableFuture.completedFuture(null));
        }

        RecordCursor.flatMapPipelined(outerFunction,
                (outer, innerContinuation) -> {
                    Subspace extraSubspace = getSecondarySubspace();
                    if (outer != null) {
                        extraSubspace = extraSubspace.subspace(outer.getKey());
                    }
                    final int limit = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
                    final FDBStoreTimer timer = Objects.requireNonNull(state.context.getTimer());
                    final RTree rTree = new RTree(extraSubspace, getExecutor(), config, RTree::newRandomNodeId,
                            new OnReadLimiter(cursorLimitManager, timer));
                    final ReadTransaction rT = state.context.readTransaction(true);
                    final ItemSlotCursor itemSlotCursor = new ItemSlotCursor(getExecutor(),
                            rTree.scan(rT, overlapsWithMbr(scanRange, prefixCount, dimensionsCount)),
                            cursorLimitManager, timer, limit);
                    itemSlotCursor.filter(itemSlot -> {
                                //TODO filter out false-positives frome the same leaf node
                                return true;
                            })
                            .map(itemSlot -> {
                                //TODO create new IndexEntry
                            })

                },
                continuation,
                state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD);

        final CompletableFuture<TupleRange> scoreRangeFuture = RankedSetIndexHelper.rankRangeToScoreRange(state,
                getGroupingCount(), extraSubspace, config, scanRange);
        return RecordCursor.mapFuture(getExecutor(), scoreRangeFuture, continuation,
                (scoreRange, scoreContinuation) -> {
                    if (scoreRange == null) {
                        return RecordCursor.empty(getExecutor());
                    } else {
                        return scan(scoreRange, scoreContinuation, scanProperties);
                    }
                });
    }

    @Nonnull
    private DimensionsKeyExpression getDimensionsKeyExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            return (DimensionsKeyExpression)((KeyWithValueExpression)root).getInnerKey();
        }
        return (DimensionsKeyExpression)root;
    }


//    @Nonnull
//    @Override
//    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
//        return super.scan(scanBounds, continuation, scanProperties);
//    }


    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final int groupPrefixSize = getGroupingCount();
        final Subspace extraSubspace = getSecondarySubspace();
        final List<CompletableFuture<Void>> ordinaryIndexFutures = new ArrayList<>(indexEntries.size());
        final Map<Subspace, CompletableFuture<Void>> rankFutures = Maps.newHashMapWithExpectedSize(indexEntries.size());
        for (IndexEntry indexEntry : indexEntries) {
            // Maintain an ordinary B-tree index by score.
            CompletableFuture<Void> updateOrdinaryIndex = updateOneKeyAsync(savedRecord, remove, indexEntry);
            if (!MoreAsyncUtil.isCompletedNormally(updateOrdinaryIndex)) {
                ordinaryIndexFutures.add(updateOrdinaryIndex);
            }

            final Subspace rankSubspace;
            final Tuple scoreKey;
            if (groupPrefixSize > 0) {
                final List<Object> keyValues = indexEntry.getKey().getItems();
                rankSubspace = extraSubspace.subspace(Tuple.fromList(keyValues.subList(0, groupPrefixSize)));
                scoreKey = Tuple.fromList(keyValues.subList(groupPrefixSize, keyValues.size()));
            } else {
                rankSubspace = extraSubspace;
                scoreKey = indexEntry.getKey();
            }
            // It is unsafe to have two concurrent updates to the same ranked set, so ensure that at most
            // one update per grouping key is ongoing at any given time
            final Function<Void, CompletableFuture<Void>> futureSupplier = vignore -> RankedSetIndexHelper.updateRankedSet(
                    state, rankSubspace, config, indexEntry.getKey(), scoreKey, remove
            );
            CompletableFuture<Void> existingFuture = rankFutures.get(rankSubspace);
            if (existingFuture == null) {
                rankFutures.put(rankSubspace, futureSupplier.apply(null));
            } else {
                rankFutures.put(rankSubspace, existingFuture.thenCompose(futureSupplier));
            }
        }
        return CompletableFuture.allOf(AsyncUtil.whenAll(ordinaryIndexFutures), AsyncUtil.whenAll(rankFutures.values()));
    }

    public <M extends Message> CompletableFuture<Long> rank(@Nonnull FDBRecord<M> record) {
        return rank(EvaluationContext.empty(), null, record);
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we sometimes
            // store data at the prefix key itself.
            final Subspace rankSubspace = getSecondarySubspace();
            final byte[] key = rankSubspace.pack(prefix);
            tr.clear(key, ByteArrayUtil.strinc(key));
            return v;
        });
    }

    private interface EvaluateEqualRange {
        @Nonnull
        CompletableFuture<Tuple> apply(@Nonnull RankedSet rankedSet, @Nonnull Tuple values);
    }

    private CompletableFuture<Tuple> evaluateEqualRange(@Nonnull TupleRange range,
                                                        @Nonnull EvaluateEqualRange function) {
        Subspace rankSubspace = getSecondarySubspace();
        Tuple values = range.getLow();
        final int groupingCount = getGroupingCount();
        if (groupingCount > 0) {
            rankSubspace = rankSubspace.subspace(TupleHelpers.subTuple(values, 0, groupingCount));
            values = TupleHelpers.subTuple(values, groupingCount, values.size());
        }
        final RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, config);
        return function.apply(rankedSet, values);
    }

    private static Predicate<RTree.Rectangle> overlapsWithMbr(@Nonnull final TupleRange tupleRange,
                                                              final int prefixCount,
                                                              final int dimensionsCount) {
        return r -> true;
    }

    static class OnReadLimiter implements RTree.OnReadListener {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;

        public OnReadLimiter(@Nonnull final CursorLimitManager cursorLimitManager, @Nonnull final FDBStoreTimer timer) {
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
        }

        @Override
        public void onRead(@Nonnull final byte[] nodeId, @Nonnull final RTree.Kind nodeKind,
                           @Nonnull final List<KeyValue> keyValues) {
            final int accumulatedKeysSize =
                    keyValues.stream()
                            .mapToInt(keyValue -> keyValue.getKey().length)
                            .sum();

            final int accumulatedValuesSize =
                    keyValues.stream()
                            .mapToInt(keyValue -> keyValue.getValue().length)
                            .sum();
            cursorLimitManager.reportScannedBytes(accumulatedKeysSize + accumulatedValuesSize);
            cursorLimitManager.tryRecordScan();

            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, accumulatedKeysSize);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, accumulatedValuesSize);
        }
    }

    static class ItemSlotCursor extends AsyncIteratorCursor<RTree.ItemSlot> {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;
        private final int limit;

        public ItemSlotCursor(@Nonnull final Executor executor, @Nonnull final AsyncIterator<RTree.ItemSlot> iterator,
                              @Nonnull final CursorLimitManager cursorLimitManager, @Nonnull final FDBStoreTimer timer,
                              final int limit) {
            super(executor, iterator);
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
            this.limit = limit;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<RTree.ItemSlot>> onNext() {
            if (nextResult != null && !nextResult.hasNext()) {
                // This guard is needed to guarantee that if onNext is called multiple times after the cursor has
                // returned a result without a value, then the same NoNextReason is returned each time. Without this guard,
                // one might return SCAN_LIMIT_REACHED (for example) after returning a result with SOURCE_EXHAUSTED because
                // of the tryRecordScan check.
                return CompletableFuture.completedFuture(nextResult);
            } else if (cursorLimitManager.tryRecordScan()) {
                return iterator.onHasNext().thenApply(hasNext -> {
                    if (hasNext) {
                        final RTree.ItemSlot itemSlot = iterator.next();
                        timer.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                        timer.increment(FDBStoreTimer.Counts.LOAD_KEY_VALUE);
                        valuesSeen++;
                        nextResult = RecordCursorResult.withNextValue(itemSlot, continuationHelper());
                    } else if (valuesSeen >= limit) {
                        // Source iterator hit limit that we passed down.
                        nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), NoNextReason.RETURN_LIMIT_REACHED);
                    } else {
                        // Source iterator is exhausted.
                        nextResult = RecordCursorResult.exhausted();
                    }
                    return nextResult;
                });
            } else { // a limit must have been exceeded
                final Optional<NoNextReason> stoppedReason = cursorLimitManager.getStoppedReason();
                if (stoppedReason.isEmpty()) {
                    throw new RecordCoreException("limit manager stopped cursor but did not report a reason");
                }
                nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), stoppedReason.get());
                return CompletableFuture.completedFuture(nextResult);
            }
        }
    }
}
