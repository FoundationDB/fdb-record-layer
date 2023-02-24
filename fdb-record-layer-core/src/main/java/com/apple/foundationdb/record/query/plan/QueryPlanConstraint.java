/*
 * QueryPlanConstraint.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRangePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * Represents a query plan constraint.
 */
@FunctionalInterface
public interface QueryPlanConstraint {
    @Nonnull
    QueryPlanConstraint TAUTOLOGY = ignored -> true;

    boolean satisfiesConstraint(@Nonnull final EvaluationContext context);

    @Nonnull
    static QueryPlanConstraint tautology() {
        return TAUTOLOGY;
    }

    @Nonnull
    static QueryPlanConstraint collectConstraints(@Nonnull final RecordQueryPlan plan) {
        final var collector = new QueryPlanConstraintsCollector();
        return collector.getConstraint(plan);
    }

    /**
     * Visits a plan and collects all the {@link QueryPlanConstraint}s from it.
     */
    class QueryPlanConstraintsCollector implements RecordQueryPlanVisitor<Void> {

        @Nonnull
        private final ImmutableList.Builder<QueryPlanConstraint> builder = ImmutableList.builder();

        @Nonnull
        @Override
        public Void visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan element) {
            builder.add(element.getConstraint());
            return null;
        }

        @Nonnull
        @Override
        public Void visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitDeletePlan(@Nonnull final RecordQueryDeletePlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitMapPlan(@Nonnull final RecordQueryMapPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitRangePlan(@Nonnull final RecordQueryRangePlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInsertPlan(@Nonnull final RecordQueryInsertPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitIndexPlan(@Nonnull final RecordQueryIndexPlan element) {
            builder.add(element.getConstraint());
            return null;
        }

        @Nonnull
        @Override
        public Void visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitFilterPlan(@Nonnull final RecordQueryFilterPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitScanPlan(@Nonnull final RecordQueryScanPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitSortPlan(@Nonnull final RecordQuerySortPlan element) {
            return null;
        }

        @Nonnull
        @Override
        public Void visitDefault(@Nonnull final RecordQueryPlan element) {
            return null;
        }

        public QueryPlanConstraint getConstraint(@Nonnull final RecordQueryPlan plan) {
            visit(plan);
            final var constraints = builder.build();
            if (constraints.isEmpty()) {
                return tautology();
            } else if (constraints.size() == 1) {
                return constraints.get(0);
            } else {
                return evaluationContext -> constraints.stream().allMatch(constraint -> constraint.satisfiesConstraint(evaluationContext));
            }
        }
    }
}
