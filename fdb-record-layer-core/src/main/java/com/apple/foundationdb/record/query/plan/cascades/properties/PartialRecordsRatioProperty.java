/*
 * PartialRecordsRatioProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryDamPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An attribute used to communicate to the planner that a plan flows instances of
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord} (and its subclasses) which can only
 * represent records that reside stored on disk and were retrieved by this query. This is opposite of truly computed
 * records which do not such data associated with them (such as primary key information and/or similar).
 */
public class PartialRecordsRatioProperty implements PlanProperty<Double> {
    public static final PartialRecordsRatioProperty PARTIAL_RECORDS_RATIO = new PartialRecordsRatioProperty();

    @Nonnull
    @Override
    public RecordQueryPlanVisitor<Double> createVisitor() {
        return new PartialRecordsRatioRecordVisitor();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
    
    /**
     * Planner property which indicates the ratio of partial records being flowed. This ratio is usually computed using
     * a best effort method which at times may be pessimistically biased.
     */
    public static class PartialRecordsRatioRecordVisitor implements RecordQueryPlanVisitor<Double> {
        @Nonnull
        @Override
        public Double visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan element) {
            return 0.d;
        }

        @Nonnull
        @Override
        public Double visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return partialRecordsRatioFromSingleChild(predicatesFilterPlan);
        }

        @Nonnull
        @Override
        public Double visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Double visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Nonnull
        @Override
        public Double visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan element) {
            return 1.0d;
        }

        @Nonnull
        @Override
        public Double visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
            return 1.0d;
        }

        @Nonnull
        @Override
        public Double visitDeletePlan(@Nonnull final RecordQueryDeletePlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionPlan) {
            return visitIntersectionPlan(intersectionPlan);
        }

        @Nonnull
        @Override
        public Double visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            return partialRecordsRatioFromSingleChild(mapPlan);
        }

        @Nonnull
        @Override
        public Double visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan distinctPlan) {
            return partialRecordsRatioFromSingleChild(distinctPlan);
        }

        @Nonnull
        @Override
        public Double visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitRangePlan(@Nonnull final RecordQueryRangePlan element) {
            return 1.0d;
        }

        @Nonnull
        @Override
        public Double visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return 1.0d;
        }

        @Nonnull
        @Override
        public Double visitInsertPlan(@Nonnull final RecordQueryInsertPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
            return visitIntersectionPlan(intersectionOnValuesPlan);
        }

        private double visitIntersectionPlan(final @Nonnull RecordQueryIntersectionPlan intersectionPlan) {
            return partialRecordsRatiosFromChildren(intersectionPlan)
                    .stream()
                    .mapToDouble(r -> r)
                    .min()
                    .orElseThrow(() -> new RecordCoreException("intersection plan needs to have at least two legs"));
        }

        @Nonnull
        @Override
        public Double visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
            return partialRecordsRatioFromSingleChild(scoreForRankPlan);
        }

        @Nonnull
        @Override
        public Double visitIndexPlan(@Nonnull final RecordQueryIndexPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            return 0.0d;
        }

        @Nonnull
        public Double visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            return partialRecordsRatioFromSingleChild(inJoinPlan);
        }

        @Nonnull
        @Override
        public Double visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            return partialRecordsRatioFromSingleChild(filterPlan);
        }

        @Nonnull
        @Override
        public Double visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan distinctPlan) {
            return partialRecordsRatioFromSingleChild(distinctPlan);
        }

        @Nonnull
        @Override
        public Double visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
            return visitUnionPlanBase(unionOnKeyExpressionPlan);
        }

        private double visitUnionPlanBase(@Nonnull final RecordQueryUnionPlanBase unionPlanBase) {
            return partialRecordsRatiosFromChildren(unionPlanBase)
                    .stream()
                    .mapToDouble(r -> r)
                    .average()
                    .orElseThrow(() -> new RecordCoreException("union plan needs to have at least two legs"));
        }

        @Nonnull
        @Override
        public Double visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            return partialRecordsRatioFromSingleChild(typeFilterPlan);
        }

        @Nonnull
        @Override
        public Double visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            return partialRecordsRatioFromSingleChild(inUnionOnKeyExpressionPlan);
        }

        @Nonnull
        @Override
        public Double visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Double visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
            return partialRecordsRatioFromSingleQuantifier(flatMapPlan.getOuterQuantifier()) *
                   partialRecordsRatioFromSingleQuantifier(flatMapPlan.getInnerQuantifier());
        }

        @Nonnull
        @Override
        public Double visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan aggregationPlan) {
            return partialRecordsRatioFromSingleChild(aggregationPlan);
        }

        @Nonnull
        @Override
        public Double visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return visitUnionPlanBase(unionOnValuesPlan);
        }

        @Nonnull
        @Override
        public Double visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return visitUnionPlanBase(unorderedUnionPlan);
        }

        @Nonnull
        @Override
        public Double visitScanPlan(@Nonnull final RecordQueryScanPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
            return partialRecordsRatioFromSingleChild(inUnionOnValuesPlan);
        }

        @Nonnull
        @Override
        public Double visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            return 0.0d;
        }

        @Nonnull
        @Override
        public Double visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
            return partialRecordsRatioFromSingleChild(damPlan);
        }

        @Nonnull
        @Override
        public Double visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
            return partialRecordsRatioFromSingleChild(sortPlan);
        }

        @Nonnull
        @Override
        public Double visitDefault(@Nonnull final RecordQueryPlan element) {
            return 1.0d;
        }

        private double partialRecordsRatioFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return partialRecordsRatioFromSingleQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private double partialRecordsRatioFromSingleQuantifier(@Nonnull final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        @Nonnull
        private List<Double> partialRecordsRatiosFromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> evaluateForReference(quantifier.getRangesOver()))
                    .collect(ImmutableList.toImmutableList());
        }

        private double evaluateForReference(@Nonnull ExpressionRef<? extends RelationalExpression> reference) {
            return reference.getMembers()
                    .stream()
                    .mapToDouble(member -> {
                        Verify.verify(member instanceof RecordQueryPlan);
                        return visit((RecordQueryPlan)member);
                    })
                    .min()
                    .orElseThrow(() -> new RecordCoreException("cannot have a reference without members"));
        }

        public static double evaluate(@Nonnull RecordQueryPlan recordQueryPlan) {
            // Won't actually be null for relational planner expressions.
            return new PartialRecordsRatioRecordVisitor().visit(recordQueryPlan);
        }
    }

    public double evaluate(@Nonnull RelationalExpression expression) {
        Verify.verify(expression instanceof RecordQueryPlan);
        // Won't actually be null for relational planner expressions.
        return createVisitor().visit((RecordQueryPlan)expression);
    }
}
