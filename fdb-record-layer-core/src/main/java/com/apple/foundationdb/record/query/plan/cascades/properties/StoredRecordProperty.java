/*
 * StoredRecordProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMultiIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveDfsJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveLevelUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTableFunctionPlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;
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
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryDamPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
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
public class StoredRecordProperty implements ExpressionProperty<Boolean> {
    private static final StoredRecordProperty STORED_RECORD = new StoredRecordProperty();

    private StoredRecordProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<Boolean> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new StoredRecordVisitor());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public boolean evaluate(@Nonnull final Reference reference) {
        return evaluate(reference.getOnlyElementAsPlan());
    }

    public boolean evaluate(@Nonnull final RecordQueryPlan recordQueryPlan) {
        return createVisitor().visit(recordQueryPlan);
    }

    @Nonnull
    public static StoredRecordProperty storedRecord() {
        return STORED_RECORD;
    }

    /**
     * Planner property which indicates if the record flowed as the result of a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} flows things stored records flow,
     * e.g. primary keys, or if the result does not flow them.
     */
    public static class StoredRecordVisitor implements RecordQueryPlanVisitor<Boolean> {
        @Nonnull
        @Override
        public Boolean visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return storedRecordsFromSingleChild(predicatesFilterPlan);
        }

        @Nonnull
        @Override
        public Boolean visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitDeletePlan(@Nonnull final RecordQueryDeletePlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            return storedRecordsFromSingleChild(mapPlan);
        }

        @Nonnull
        @Override
        public Boolean visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
            return storedRecordsFromChildren(comparatorPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
            return storedRecordsFromChildren(selectorPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitRangePlan(@Nonnull final RecordQueryRangePlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitTempTableScanPlan(@Nonnull final TempTableScanPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitInsertPlan(@Nonnull final RecordQueryInsertPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitTableFunctionPlan(@Nonnull final RecordQueryTableFunctionPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitTempTableInsertPlan(@Nonnull final TempTableInsertPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
            return storedRecordsFromChildren(intersectionOnValuesPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
            return storedRecordsFromSingleChild(scoreForRankPlan);
        }

        @Nonnull
        @Override
        public Boolean visitIndexPlan(@Nonnull final RecordQueryIndexPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitRecursiveLevelUnionPlan(@Nonnull final RecordQueryRecursiveLevelUnionPlan recursiveUnionPlan) {
            return storedRecordsFromChildren(recursiveUnionPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitDefaultOnEmptyPlan(@Nonnull final RecordQueryDefaultOnEmptyPlan element) {
            return false;
        }

        @Nonnull
        public Boolean visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            return storedRecordsFromSingleChild(inJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            return storedRecordsFromSingleChild(filterPlan);
        }

        @Nonnull
        @Override
        public Boolean visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            return storedRecordsFromSingleChild(typeFilterPlan);
        }

        @Nonnull
        @Override
        public Boolean visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitMultiIntersectionOnValuesPlan(@Nonnull final RecordQueryMultiIntersectionOnValuesPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
            if (flatMapPlan.isInheritOuterRecordProperties()) {
                return storedRecordsFromSingleQuantifier(flatMapPlan.getOuterQuantifier());
            }
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return storedRecordsFromChildren(unionOnValuesPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return storedRecordsFromChildren(unorderedUnionPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitScanPlan(@Nonnull final RecordQueryScanPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
            return storedRecordsFromSingleChild(inUnionOnValuesPlan);
        }

        @Nonnull
        @Override
        public Boolean visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
            return storedRecordsFromSingleChild(damPlan);
        }

        @Nonnull
        @Override
        public Boolean visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
            return storedRecordsFromSingleChild(sortPlan);
        }

        @Nonnull
        @Override
        public Boolean visitRecursiveDfsJoinPlan(@Nonnull final RecordQueryRecursiveDfsJoinPlan recursiveDfsJoinPlan) {
            return storedRecordsFromChildren(recursiveDfsJoinPlan).stream().allMatch(s -> s);
        }

        @Nonnull
        @Override
        public Boolean visitDefault(@Nonnull final RecordQueryPlan element) {
            return true;
        }

        private boolean storedRecordsFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return storedRecordsFromSingleQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private boolean storedRecordsFromSingleQuantifier(@Nonnull final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        @Nonnull
        private List<Boolean> storedRecordsFromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> {
                        if (quantifier instanceof Quantifier.Existential) {
                            return true;
                        }
                        return evaluateForReference(quantifier.getRangesOver());
                    })
                    .collect(ImmutableList.toImmutableList());
        }

        private boolean evaluateForReference(@Nonnull Reference reference) {
            final var memberStoredRecordsCollection =
                    reference.getPropertyForPlans(STORED_RECORD).values();

            return memberStoredRecordsCollection
                    .stream()
                    .allMatch(d -> d);
        }
    }
}
