/*
 * RecordQueryScoreForRankPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.EvaluationContextBuilder;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.RankedSetIndexHelper;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A query plan that converts ranks to scores and executes a child plan with the conversion results bound in named parameters.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryScoreForRankPlan implements RecordQueryPlanWithChild {
    @Nonnull
    private final ExpressionRef<RecordQueryPlan> plan;
    @Nonnull
    private final List<ScoreForRank> ranks;

    public RecordQueryScoreForRankPlan(RecordQueryPlan plan, List<ScoreForRank> ranks) {
        this.plan = SingleExpressionRef.of(plan);
        this.ranks = ranks;
    }

    private <M extends Message> CompletableFuture<Tuple> bindScore(@Nonnull FDBRecordStoreBase<M> store,
                                                                   @Nonnull EvaluationContext context,
                                                                   ScoreForRank scoreForRank,
                                                                   @Nonnull IsolationLevel isolationLevel) {
        final Tuple operand = Tuple.fromList(scoreForRank.comparisons.stream().map(c -> c.getComparand(store, context)).collect(Collectors.toList()));
        return store.evaluateAggregateFunction(context, Collections.emptyList(), scoreForRank.function, TupleRange.allOf(operand), isolationLevel);
    }
    
    private <M extends Message> CompletableFuture<EvaluationContext> bindScores(@Nonnull FDBRecordStoreBase<M> store,
                                                                                @Nonnull EvaluationContext context,
                                                                                @Nonnull IsolationLevel isolationLevel) {
        final List<CompletableFuture<Tuple>> scores = ranks.stream().map(r -> bindScore(store, context, r, isolationLevel)).collect(Collectors.toList());
        return AsyncUtil.whenAll(scores).thenApply(vignore -> {
            EvaluationContextBuilder builder = context.childBuilder();
            for (int i = 0; i < scores.size(); i++) {
                final ScoreForRank rank = ranks.get(i);
                final Tuple score = scores.get(i).join();
                final Object binding;
                if (score == null) {
                    binding = null;
                } else if (score == RankedSetIndexHelper.COMPARISON_SKIPPED_SCORE) {
                    binding = Comparisons.COMPARISON_SKIPPED_BINDING;
                } else {
                    binding = rank.bindingFunction.apply(score);
                }
                builder.setBinding(rank.bindingName, binding);
            }
            return builder.build();
        });
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        return RecordCursor.mapFuture(store.getExecutor(), bindScores(store, context, executeProperties.getIsolationLevel()), continuation,
                (innerContext, innerContinuation) -> getChild().execute(store, innerContext, innerContinuation, executeProperties));
    }

    @Override
    public boolean hasRecordScan() {
        return getChild().hasRecordScan();
    }

    @Override
    public boolean hasFullRecordScan() {
        return getChild().hasFullRecordScan();
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getChild().hasIndexScan(indexName);
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return plan.get();
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return getChild().getUsedIndexes();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.plan);
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public String toString() {
        return getChild() + " WHERE " + ranks.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryScoreForRankPlan that = (RecordQueryScoreForRankPlan) o;
        return Objects.equals(getChild(), that.getChild()) &&
                Objects.equals(ranks, that.ranks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChild(), ranks);
    }

    @Override
    public int planHash() {
        return getChild().planHash() + PlanHashable.planHash(ranks);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_SCORE_FOR_RANK);
        getChild().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getChild().getComplexity();
    }

    /**
     * A single conversion of a rank to a score to be bound to some name.
     */
    public static class ScoreForRank implements PlanHashable {
        @Nonnull
        private final String bindingName;
        @Nonnull
        private final Function<Tuple, Object> bindingFunction;
        @Nonnull
        private final IndexAggregateFunction function;
        @Nonnull
        private final List<Comparisons.Comparison> comparisons;

        public ScoreForRank(@Nonnull String bindingName, @Nonnull Function<Tuple, Object> bindingFunction,
                            @Nonnull IndexAggregateFunction function, @Nonnull List<Comparisons.Comparison> comparisons) {
            this.bindingName = bindingName;
            this.bindingFunction = bindingFunction;
            this.function = function;
            this.comparisons = comparisons;
        }

        @Nonnull
        public String getBindingName() {
            return bindingName;
        }

        @Override
        public String toString() {
            return bindingName + " = " + function.getIndex() + "." + function.getName() + comparisons.stream().map(Comparisons.Comparison::typelessString).collect(Collectors.joining(", ", "(", ")"));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScoreForRank that = (ScoreForRank) o;
            return Objects.equals(bindingName, that.bindingName) &&
                   Objects.equals(bindingFunction, that.bindingFunction) &&
                   Objects.equals(function, that.function) &&
                   Objects.equals(comparisons, that.comparisons);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bindingName, bindingFunction, function, comparisons);
        }

        @Override
        public int planHash() {
            return bindingName.hashCode() + function.getName().hashCode() + PlanHashable.planHash(comparisons);
        }
    }
}
