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

    @Override
    public RelationalExpressionVisitor<Boolean> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new StoredRecordVisitor());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public boolean evaluate(final Reference reference) {
        return evaluate(reference.getOnlyElementAsPlan());
    }

    public boolean evaluate(final RecordQueryPlan recordQueryPlan) {
        return createVisitor().visit(recordQueryPlan);
    }

    public static StoredRecordProperty storedRecord() {
        return STORED_RECORD;
    }

    /**
     * Planner property which indicates if the record flowed as the result of a
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} flows things stored records flow,
     * e.g. primary keys, or if the result does not flow them.
     */
    public static class StoredRecordVisitor implements RecordQueryPlanVisitor<Boolean> {
        @Override
        public Boolean visitUpdatePlan(final RecordQueryUpdatePlan element) {
            return true;
        }

        @Override
        public Boolean visitPredicatesFilterPlan(final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return storedRecordsFromSingleChild(predicatesFilterPlan);
        }

        @Override
        public Boolean visitLoadByKeysPlan(final RecordQueryLoadByKeysPlan element) {
            return true;
        }

        @Override
        public Boolean visitInValuesJoinPlan(final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Override
        public Boolean visitInComparandJoinPlan(final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Override
        public Boolean visitAggregateIndexPlan(final RecordQueryAggregateIndexPlan element) {
            return false;
        }

        @Override
        public Boolean visitCoveringIndexPlan(final RecordQueryCoveringIndexPlan element) {
            return true;
        }

        @Override
        public Boolean visitDeletePlan(final RecordQueryDeletePlan element) {
            return true;
        }

        @Override
        public Boolean visitIntersectionOnKeyExpressionPlan(final RecordQueryIntersectionOnKeyExpressionPlan element) {
            return true;
        }

        @Override
        public Boolean visitMapPlan(final RecordQueryMapPlan mapPlan) {
            return storedRecordsFromSingleChild(mapPlan);
        }

        @Override
        public Boolean visitComparatorPlan(final RecordQueryComparatorPlan comparatorPlan) {
            return storedRecordsFromChildren(comparatorPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitUnorderedDistinctPlan(final RecordQueryUnorderedDistinctPlan element) {
            return true;
        }

        @Override
        public Boolean visitSelectorPlan(final RecordQuerySelectorPlan selectorPlan) {
            return storedRecordsFromChildren(selectorPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitRangePlan(final RecordQueryRangePlan element) {
            return false;
        }

        @Override
        public Boolean visitTempTableScanPlan(final TempTableScanPlan element) {
            return false;
        }

        @Override
        public Boolean visitExplodePlan(final RecordQueryExplodePlan element) {
            return false;
        }

        @Override
        public Boolean visitInsertPlan(final RecordQueryInsertPlan element) {
            return true;
        }

        @Override
        public Boolean visitTableFunctionPlan(final RecordQueryTableFunctionPlan element) {
            return false;
        }

        @Override
        public Boolean visitTempTableInsertPlan(final TempTableInsertPlan element) {
            return false;
        }

        @Override
        public Boolean visitIntersectionOnValuesPlan(final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
            return storedRecordsFromChildren(intersectionOnValuesPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitScoreForRankPlan(final RecordQueryScoreForRankPlan scoreForRankPlan) {
            return storedRecordsFromSingleChild(scoreForRankPlan);
        }

        @Override
        public Boolean visitIndexPlan(final RecordQueryIndexPlan element) {
            return true;
        }

        @Override
        public Boolean visitRecursiveLevelUnionPlan(final RecordQueryRecursiveLevelUnionPlan recursiveUnionPlan) {
            return storedRecordsFromChildren(recursiveUnionPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitFirstOrDefaultPlan(final RecordQueryFirstOrDefaultPlan element) {
            return false;
        }

        @Override
        public Boolean visitDefaultOnEmptyPlan(final RecordQueryDefaultOnEmptyPlan element) {
            return false;
        }

        public Boolean visitInJoinPlan(final RecordQueryInJoinPlan inJoinPlan) {
            return storedRecordsFromSingleChild(inJoinPlan);
        }

        @Override
        public Boolean visitFilterPlan(final RecordQueryFilterPlan filterPlan) {
            return storedRecordsFromSingleChild(filterPlan);
        }

        @Override
        public Boolean visitUnorderedPrimaryKeyDistinctPlan(final RecordQueryUnorderedPrimaryKeyDistinctPlan element) {
            return true;
        }

        @Override
        public Boolean visitUnionOnKeyExpressionPlan(final RecordQueryUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Override
        public Boolean visitTextIndexPlan(final RecordQueryTextIndexPlan element) {
            return true;
        }

        @Override
        public Boolean visitFetchFromPartialRecordPlan(final RecordQueryFetchFromPartialRecordPlan element) {
            return true;
        }

        @Override
        public Boolean visitTypeFilterPlan(final RecordQueryTypeFilterPlan typeFilterPlan) {
            return storedRecordsFromSingleChild(typeFilterPlan);
        }

        @Override
        public Boolean visitInUnionOnKeyExpressionPlan(final RecordQueryInUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Override
        public Boolean visitMultiIntersectionOnValuesPlan(final RecordQueryMultiIntersectionOnValuesPlan element) {
            return false;
        }

        @Override
        public Boolean visitInParameterJoinPlan(final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Override
        public Boolean visitFlatMapPlan(final RecordQueryFlatMapPlan flatMapPlan) {
            if (flatMapPlan.isInheritOuterRecordProperties()) {
                return storedRecordsFromSingleQuantifier(flatMapPlan.getOuterQuantifier());
            }
            return false;
        }

        @Override
        public Boolean visitStreamingAggregationPlan(final RecordQueryStreamingAggregationPlan element) {
            return false;
        }

        @Override
        public Boolean visitUnionOnValuesPlan(final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return storedRecordsFromChildren(unionOnValuesPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitUnorderedUnionPlan(final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return storedRecordsFromChildren(unorderedUnionPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitScanPlan(final RecordQueryScanPlan element) {
            return true;
        }

        @Override
        public Boolean visitInUnionOnValuesPlan(final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
            return storedRecordsFromSingleChild(inUnionOnValuesPlan);
        }

        @Override
        public Boolean visitComposedBitmapIndexQueryPlan(final ComposedBitmapIndexQueryPlan element) {
            return true;
        }

        @Override
        public Boolean visitDamPlan(final RecordQueryDamPlan damPlan) {
            return storedRecordsFromSingleChild(damPlan);
        }

        @Override
        public Boolean visitSortPlan(final RecordQuerySortPlan sortPlan) {
            return storedRecordsFromSingleChild(sortPlan);
        }

        @Override
        public Boolean visitRecursiveDfsJoinPlan(final RecordQueryRecursiveDfsJoinPlan recursiveDfsJoinPlan) {
            return storedRecordsFromChildren(recursiveDfsJoinPlan).stream().allMatch(s -> s);
        }

        @Override
        public Boolean visitDefault(final RecordQueryPlan element) {
            return true;
        }

        private boolean storedRecordsFromSingleChild(final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return storedRecordsFromSingleQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private boolean storedRecordsFromSingleQuantifier(final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        private List<Boolean> storedRecordsFromChildren(final RelationalExpression expression) {
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

        private boolean evaluateForReference(Reference reference) {
            final var memberStoredRecordsCollection =
                    reference.getPropertyForPlans(STORED_RECORD).values();

            return memberStoredRecordsCollection
                    .stream()
                    .allMatch(d -> d);
        }
    }
}
