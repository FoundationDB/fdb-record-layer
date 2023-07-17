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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An index maintainer for keeping a {@link com.apple.foundationdb.async.RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class MultiDimensionalIndexMaintainer extends StandardIndexMaintainer {
    private final RankedSet.Config config;

    public MultiDimensionalIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = RankedSetIndexHelper.getConfig(state.index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange rankRange,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        // TODO throw exception
        if (scanType.equals(IndexScanType.BY_VALUE)) {
            return scan(rankRange, continuation, scanProperties);
        } else if (!scanType.equals(IndexScanType.BY_RANK)) {
            throw new RecordCoreException("Can only scan rank index by rank or by value.");
        }
        final Subspace extraSubspace = getSecondarySubspace();
        final CompletableFuture<TupleRange> scoreRangeFuture = RankedSetIndexHelper.rankRangeToScoreRange(state,
                getGroupingCount(), extraSubspace, config, rankRange);
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
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        return super.scan(scanBounds, continuation, scanProperties);
    }


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

    protected <M extends Message> CompletableFuture<Long> rank(@Nonnull EvaluationContext context,
                                                               @Nullable IndexRecordFunction<Long> function,
                                                               @Nonnull FDBRecord<M> record) {
        final int groupPrefixSize = getGroupingCount();
        Key.Evaluated indexKey = IndexFunctionHelper.recordFunctionIndexEntry(state.store, state.index, context, function, record, groupPrefixSize);
        if (indexKey == null) {
            return CompletableFuture.completedFuture(null);
        }
        Tuple scoreValue = indexKey.toTuple();
        Subspace rankSubspace = getSecondarySubspace();
        if (groupPrefixSize > 0) {
            Tuple prefix = Tuple.fromList(scoreValue.getItems().subList(0, groupPrefixSize));
            rankSubspace = rankSubspace.subspace(prefix);
            scoreValue = Tuple.fromList(scoreValue.getItems().subList(groupPrefixSize, scoreValue.size()));
        }
        RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, config);
        return RankedSetIndexHelper.rankForScore(state, rankedSet, scoreValue, true);
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

}
