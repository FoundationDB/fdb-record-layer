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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer for keeping a {@link RankedSet} of record field values.
 *
 * A rank index is used to implement these operations in queries:
 * <ul>
 * <li><b>rank</b>: Given a record, where would its field value be in an ordered enumeration of all record's values.</li>
 * <li><b>select</b>: Given a range of ranks, return (the primary keys for) the records with values in that range.</li>
 * </ul>
 *
 * Any number of fields in the index can optionally separate records into non-overlapping <i>groups</i>.
 * Each group, determined by the values of those fields, has separate ranking.
 */
@API(API.Status.MAINTAINED)
public class RankIndexMaintainer extends StandardIndexMaintainer {
    private final int nlevels;

    static int getNLevels(@Nonnull Index index) {
        String nlevelsOption = index.getOption(IndexOptions.RANK_NLEVELS);
        return nlevelsOption == null ? RankedSet.DEFAULT_LEVELS : Integer.parseInt(nlevelsOption);
    }

    public RankIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.nlevels = getNLevels(state.index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange rankRange,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (scanType == IndexScanType.BY_VALUE) {
            return scan(rankRange, continuation, scanProperties);
        } else if (scanType != IndexScanType.BY_RANK) {
            throw new RecordCoreException("Can only scan rank index by rank or by value.");
        }
        final Subspace extraSubspace = getSecondarySubspace();
        final CompletableFuture<TupleRange> scoreRangeFuture = RankedSetIndexHelper.rankRangeToScoreRange(state,
                getGroupingCount(), extraSubspace, nlevels, rankRange);
        return RecordCursor.mapFuture(getExecutor(), scoreRangeFuture, continuation,
                (scoreRange, scoreContinuation) -> {
                    if (scoreRange == null) {
                        return RecordCursor.empty(getExecutor());
                    } else {
                        return scan(scoreRange, scoreContinuation, scanProperties);
                    }
                });
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final int groupPrefixSize = getGroupingCount();
        final Subspace extraSubspace = getSecondarySubspace();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (IndexEntry indexEntry : indexEntries) {
            // First maintain an ordinary B-tree index by score.
            updateOneKey(savedRecord, remove, indexEntry);
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
            futures.add(RankedSetIndexHelper.updateRankedSet(state, rankSubspace, nlevels, indexEntry.getKey(),
                    scoreKey, remove));
        }
        return AsyncUtil.whenAll(futures);
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return function.getName().equals(FunctionNames.RANK) &&
                state.index.getRootExpression().equals(function.getOperand());
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        if (function.getName().equals(FunctionNames.RANK)) {
            return (CompletableFuture<T>)rank(record);
        } else {
            return unsupportedRecordFunction(function);
        }
    }

    public <M extends Message> CompletableFuture<Long> rank(@Nonnull FDBRecord<M> record) {
        final int groupPrefixSize = getGroupingCount();
        KeyExpression indexExpr = state.index.getRootExpression();
        Key.Evaluated indexKey = indexExpr.evaluateSingleton(record);
        Tuple scoreValue = indexKey.toTuple();
        Subspace rankSubspace = getSecondarySubspace();
        if (groupPrefixSize > 0) {
            Tuple prefix = Tuple.fromList(scoreValue.getItems().subList(0, groupPrefixSize));
            rankSubspace = rankSubspace.subspace(prefix);
            scoreValue = Tuple.fromList(scoreValue.getItems().subList(groupPrefixSize, scoreValue.size()));
        }
        RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, nlevels);
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

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        // Can do COUNT_DISTINCT(score BY group) by sizing the ranked set.
        if (FunctionNames.COUNT_DISTINCT.equals(function.getName()) &&
                function.getOperand().equals(state.index.getRootExpression())) {
            return true;
        }
        // Likewise COUNT(* BY group), if don't allow ties.
        if (FunctionNames.COUNT.equals(function.getName()) &&
                state.index.isUnique() &&
                function.getOperand().getColumnSize() == getGroupingCount() &&
                function.getOperand().isPrefixKey(state.index.getRootExpression())) {
            return true;
        }
        if ((FunctionNames.SCORE_FOR_RANK.equals(function.getName()) ||
                FunctionNames.SCORE_FOR_RANK_ELSE_SKIP.equals(function.getName()) ||
                FunctionNames.RANK_FOR_SCORE.equals(function.getName())) &&
                function.getOperand().equals(state.index.getRootExpression())) {
            return true;
        }
        return super.canEvaluateAggregateFunction(function);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull final IsolationLevel isolationLevel) {
        if ((FunctionNames.COUNT.equals(function.getName()) ||
                FunctionNames.COUNT_DISTINCT.equals(function.getName())) &&
                range.isEquals()) {
            return evaluateEqualRange(range, (rankedSet, values) ->
                    rankedSet.size(state.context.readTransaction(isolationLevel.isSnapshot())).thenApply(Tuple::from));
        }
        if ((FunctionNames.SCORE_FOR_RANK.equals(function.getName()) ||
                 FunctionNames.SCORE_FOR_RANK_ELSE_SKIP.equals(function.getName())) &&
                 range.isEquals()) {
            final Tuple outOfRange = FunctionNames.SCORE_FOR_RANK_ELSE_SKIP.equals(function.getName()) ?
                                     RankedSetIndexHelper.COMPARISON_SKIPPED_SCORE : null;
            return evaluateEqualRange(range, (rankedSet, values) ->
                    RankedSetIndexHelper.scoreForRank(state, rankedSet, (Number)values.get(0), outOfRange));
        }
        if (FunctionNames.RANK_FOR_SCORE.equals(function.getName()) && range.isEquals()) {
            return evaluateEqualRange(range, (rankedSet, values) ->
                    RankedSetIndexHelper.rankForScore(state, rankedSet, values, false).thenApply(Tuple::from));
        }
        return unsupportedAggregateFunction(function);
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
        final RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, nlevels);
        return function.apply(rankedSet, values);
    }

}
