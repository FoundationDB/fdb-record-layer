/*
 * DistinctRecordsProperty.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
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
 * An attribute used to indicate if a plan produces distinct records.
 */
public class DistinctRecordsProperty implements ExpressionProperty<Boolean> {
    private static final DistinctRecordsProperty DISTINCT_RECORDS = new DistinctRecordsProperty();

    private DistinctRecordsProperty() {
        // prevent outside instantiation
    }

    @Override
    public RelationalExpressionVisitor<Boolean> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new DistinctRecordsVisitor());
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

    public static DistinctRecordsProperty distinctRecords() {
        return DISTINCT_RECORDS;
    }

    /**
     * A visitor that determines whether the expression may produce distinct records.
     */
    @API(API.Status.EXPERIMENTAL)
    public static class DistinctRecordsVisitor implements RecordQueryPlanVisitor<Boolean> {
        @Override
        public Boolean visitUpdatePlan(final RecordQueryUpdatePlan updatePlan) {
            return distinctRecordsFromSingleChild(updatePlan);
        }

        @Override
        public Boolean visitPredicatesFilterPlan(final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return distinctRecordsFromSingleChild(predicatesFilterPlan);
        }

        @Override
        public Boolean visitLoadByKeysPlan(final RecordQueryLoadByKeysPlan element) {
            // TODO this could be wrong -- but it is the way it was previously encoded
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
        public Boolean visitAggregateIndexPlan(final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
            return true;
        }

        @Override
        public Boolean visitCoveringIndexPlan(final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            final var indexPlan = coveringIndexPlan.getIndexPlan();
            if (!(indexPlan instanceof RecordQueryIndexPlan)) {
                return false;
            }

            return visitIndexPlan((RecordQueryIndexPlan)indexPlan);
        }

        @Override
        public Boolean visitDeletePlan(final RecordQueryDeletePlan deletePlan) {
            return distinctRecordsFromSingleChild(deletePlan);
        }

        @Override
        public Boolean visitIntersectionOnKeyExpressionPlan(final RecordQueryIntersectionOnKeyExpressionPlan element) {
            return true;
        }

        @Override
        public Boolean visitMapPlan(final RecordQueryMapPlan mapPlan) {
            final var resultValue = mapPlan.getResultValue();

            if (resultValue instanceof QuantifiedObjectValue) {
                if (((QuantifiedObjectValue)resultValue).getAlias().equals(mapPlan.getInner().getAlias())) {
                    return distinctRecordsFromSingleChild(mapPlan);
                }
            }
            return false;
        }

        @Override
        public Boolean visitComparatorPlan(final RecordQueryComparatorPlan comparatorPlan) {
            return distinctRecordsFromChildren(comparatorPlan).stream().allMatch(d -> d);
        }

        @Override
        public Boolean visitUnorderedDistinctPlan(final RecordQueryUnorderedDistinctPlan element) {
            return true;
        }

        @Override
        public Boolean visitSelectorPlan(final RecordQuerySelectorPlan selectorPlan) {
            return distinctRecordsFromChildren(selectorPlan).stream().allMatch(d -> d);
        }

        @Override
        public Boolean visitRangePlan(final RecordQueryRangePlan element) {
            return true;
        }

        @Override
        public Boolean visitTempTableScanPlan(final TempTableScanPlan element) {
            return false;
        }

        @Override
        public Boolean visitExplodePlan(final RecordQueryExplodePlan element) {
            return element.isWithOrdinality();
        }

        @Override
        public Boolean visitInsertPlan(final RecordQueryInsertPlan insertPlan) {
            return distinctRecordsFromSingleChild(insertPlan);
        }

        @Override
        public Boolean visitTableFunctionPlan(final RecordQueryTableFunctionPlan element) {
            return  false;
        }

        @Override
        public Boolean visitTempTableInsertPlan(final TempTableInsertPlan tempTableInsertPlan) {
            return distinctRecordsFromSingleChild(tempTableInsertPlan);
        }

        @Override
        public Boolean visitIntersectionOnValuesPlan(final RecordQueryIntersectionOnValuesPlan element) {
            return true;
        }

        @Override
        public Boolean visitScoreForRankPlan(final RecordQueryScoreForRankPlan element) {
            // TODO this could be wrong -- but it is the way it was previously encoded
            return true;
        }

        @Override
        public Boolean visitIndexPlan(final RecordQueryIndexPlan indexPlan) {
            final var matchCandidateOptional = indexPlan.getMatchCandidateMaybe();
            if (matchCandidateOptional.isEmpty()) {
                return false;
            }

            final var matchCandidate = matchCandidateOptional.get();
            return !matchCandidate.createsDuplicates();
        }

        @Override
        public Boolean visitRecursiveLevelUnionPlan(final RecordQueryRecursiveLevelUnionPlan element) {
            return false;
        }

        @Override
        public Boolean visitFirstOrDefaultPlan(final RecordQueryFirstOrDefaultPlan element) {
            return true;
        }

        @Override
        public Boolean visitDefaultOnEmptyPlan(final RecordQueryDefaultOnEmptyPlan element) {
            return distinctRecordsFromSingleChild(element);
        }

        public Boolean visitInJoinPlan(final RecordQueryInJoinPlan inJoinPlan) {
            return distinctRecordsFromSingleChild(inJoinPlan);
        }

        @Override
        public Boolean visitFilterPlan(final RecordQueryFilterPlan filterPlan) {
            return distinctRecordsFromSingleChild(filterPlan);
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
            return false;
        }

        @Override
        public Boolean visitFetchFromPartialRecordPlan(final RecordQueryFetchFromPartialRecordPlan fetchFromPartialRecordPlan) {
            return distinctRecordsFromSingleChild(fetchFromPartialRecordPlan);
        }

        @Override
        public Boolean visitTypeFilterPlan(final RecordQueryTypeFilterPlan typeFilterPlan) {
            return distinctRecordsFromSingleChild(typeFilterPlan);
        }

        @Override
        public Boolean visitInUnionOnKeyExpressionPlan(final RecordQueryInUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Override
        public Boolean visitMultiIntersectionOnValuesPlan(final RecordQueryMultiIntersectionOnValuesPlan element) {
            return true;
        }

        @Override
        public Boolean visitInParameterJoinPlan(final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Override
        public Boolean visitFlatMapPlan(final RecordQueryFlatMapPlan element) {
            return false;
        }

        @Override
        public Boolean visitStreamingAggregationPlan(final RecordQueryStreamingAggregationPlan element) {
            return false;
        }

        @Override
        public Boolean visitUnionOnValuesPlan(final RecordQueryUnionOnValuesPlan element) {
            return true;
        }

        @Override
        public Boolean visitUnorderedUnionPlan(final RecordQueryUnorderedUnionPlan element) {
            return false;
        }

        @Override
        public Boolean visitScanPlan(final RecordQueryScanPlan element) {
            return true;
        }

        @Override
        public Boolean visitInUnionOnValuesPlan(final RecordQueryInUnionOnValuesPlan element) {
            return true;
        }

        @Override
        public Boolean visitComposedBitmapIndexQueryPlan(final ComposedBitmapIndexQueryPlan element) {
            return false;
        }

        @Override
        public Boolean visitDamPlan(final RecordQueryDamPlan damPlan) {
            return distinctRecordsFromSingleChild(damPlan);
        }

        @Override
        public Boolean visitSortPlan(final RecordQuerySortPlan sortPlan) {
            return distinctRecordsFromSingleChild(sortPlan);
        }

        @Override
        public Boolean visitRecursiveDfsJoinPlan(final RecordQueryRecursiveDfsJoinPlan recursiveDfsJoinPlan) {
            return false;
        }

        @Override
        public Boolean visitDefault(final RecordQueryPlan element) {
            return false;
        }

        private boolean distinctRecordsFromSingleChild(final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return evaluateForReference(Iterables.getOnlyElement(quantifiers).getRangesOver());
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private List<Boolean> distinctRecordsFromChildren(final RelationalExpression expression) {
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
            final var memberDistinctRecordsCollection =
                    reference.getPropertyForPlans(DISTINCT_RECORDS).values();

            return memberDistinctRecordsCollection
                    .stream()
                    .allMatch(d -> d);
        }
    }
}
