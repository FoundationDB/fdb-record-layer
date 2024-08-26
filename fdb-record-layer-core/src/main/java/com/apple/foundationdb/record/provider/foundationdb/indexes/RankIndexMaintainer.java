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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
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
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An index maintainer for keeping a {@link RankedSet} of record field values.
 *
 * <p>
 * A <i>ranked-set</i> is a persistent skip-list that efficiently implements two complementary functions:
 * </p>
 * <ul>
 * <li><b>rank</b>: Given a value, where would this value be in an ordered enumeration of all values in the set?</li>
 * <li><b>select</b>: Given an ordinal position in the ordered enumeration of values, what is the specific value at that position?</li>
 * </ul>
 *
 * <p>
 * Any number of fields in a {@code RANK} index can optionally separate records into non-overlapping <i>groups</i>.
 * Each group, determined by the values of those fields, has separate ranking and therefore a separate ranked-set.
 * </p>
 *
 * <p>
 * <b>Physical layout</b>: a {@code RANK} index maintains two subspaces in the database:
 * </p>
 * <ul>
 * <li><b>primary subspace</b>: an ordinary B-tree index on <code>[<i>group</i>, ..., <i>score</i>, ...]</code>.</li>
 * <li><b>secondary subspace</b>: a ranked-set per group, that is, with any group key as a prefix.</li>
 * </ul>
 * <p>
 * The <b>overhead</b> of the secondary subspace is one key-value pair for each value at the finest level (0) of the ranked-set skip-list,
 * plus additional key-value pairs at coarser levels, with a probability of {@code 1/16ⁿ} for level <i>n</i>.
 * </p>
 *
 * <p>
 * <b>Store operations</b>: The basic <b>rank</b> and <b>select</b> functions are exposed by the index as <i>aggregate</i> functions,
 * that is, as operations on the whole record store rather than specific records.
 * </p>
 * <ul>
 * <li>{@link FunctionNames#RANK_FOR_SCORE}: the <b>rank</b> operation; given a tuple of group keys and score values, return the ordinal position of that score</li>
 * <li>{@link FunctionNames#SCORE_FOR_RANK}: the <b>select</b> operation; given a tuple of group keys and a rank ordinal, return the score at that position or an error if out of range</li>
 * <li>{@link FunctionNames#SCORE_FOR_RANK_ELSE_SKIP}: similarly, given a tuple of group keys and a rank ordinal, return the score at that position, or a special value if out of range</li>
 * </ul>
 * <p>
 * Because the ranked-set skip-list keeps a count of entries, the index can also perform the {@link FunctionNames#COUNT_DISTINCT} aggregate function,
 * and, equivalently when the index does not permit ties, that is, when the index is declared <i>unique</i>, the more basic {@link FunctionNames#COUNT} aggregate function.
 * </p>
 *
 * <p>
 * <b>Record operations</b>: Given a record, {@link FunctionNames#RANK} record function returns the rank of that record according to the index.
 * This is done by evaluating the same key expressions used by index maintenance against the record to determine any group keys
 * and score values. These are then given to the ranked-set's <b>rank</b> function.
 * </p>
 *
 * <p>
 * <b>Scan operations</b>: The index can be used to return a range of records within a group in rank order.
 * </p>
 * <ul>
 * <li>{@link IndexScanType#BY_VALUE}: The primary B-tree is used just like a {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE} index.</li>
 * <li>{@link IndexScanType#BY_RANK}: Given a range of ranks, return those records.</li>
 * </ul>
 * <p>
 * This is done as follows:
 * </p>
 * <ol>
 * <li>Take the group keys from the common prefix of the range endpoints to determine which ranked-set to use.</li>
 * <li>Convert each of the rank endpoints of the range into score endpoints using <b>select</b>.</li>
 * <li>Add back the group prefixes to those scores.</li>
 * <li>Return a {@code BY_VALUE} scan between those grouped score endpoints.</li>
 * </ol>
 */
@API(API.Status.MAINTAINED)
public class RankIndexMaintainer extends StandardIndexMaintainer {
    private final RankedSet.Config config;

    public RankIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = RankedSetIndexHelper.getConfig(state.index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange rankRange,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
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

    @Override
    public boolean isIdempotent() {
        // In the not counting case, updateRankedSet only does remove from ranked set for the last occurrence,
        // since it doesn't track duplicates itself. In the counting case, we just decrement, which has the possibility
        // of removing someone else's entry if the record being removed hasn't been indexed yet.
        return !config.isCountDuplicates();
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return FunctionNames.RANK.equals(function.getName()) &&
                state.index.getRootExpression().equals(function.getOperand());
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        if (FunctionNames.RANK.equals(function.getName())) {
            return (CompletableFuture<T>)rank(context, (IndexRecordFunction<Long>)function, record);
        } else {
            return unsupportedRecordFunction(function);
        }
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
            state.context.clear(new Range(key, ByteArrayUtil.strinc(key)));
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
        final RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, config);
        return function.apply(rankedSet, values);
    }
}
