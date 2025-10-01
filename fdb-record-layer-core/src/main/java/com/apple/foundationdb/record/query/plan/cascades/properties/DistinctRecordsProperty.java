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

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An attribute used to indicate if a plan produces distinct records.
 */
public class DistinctRecordsProperty implements ExpressionProperty<Boolean> {
    private static final DistinctRecordsProperty DISTINCT_RECORDS = new DistinctRecordsProperty();

    private DistinctRecordsProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<Boolean> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new DistinctRecordsVisitor());
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
    public static DistinctRecordsProperty distinctRecords() {
        return DISTINCT_RECORDS;
    }

    /**
     * A visitor that determines whether the expression may produce distinct records.
     */
    @API(API.Status.EXPERIMENTAL)
    public static class DistinctRecordsVisitor implements RecordQueryPlanVisitor<Boolean> {
        @Nonnull
        @Override
        public Boolean visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
            return distinctRecordsFromSingleChild(updatePlan);
        }

        @Nonnull
        @Override
        public Boolean visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return distinctRecordsFromSingleChild(predicatesFilterPlan);
        }

        @Nonnull
        @Override
        public Boolean visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            // TODO this could be wrong -- but it is the way it was previously encoded
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
        public Boolean visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            final var indexPlan = coveringIndexPlan.getIndexPlan();
            if (!(indexPlan instanceof RecordQueryIndexPlan)) {
                return false;
            }

            return visitIndexPlan((RecordQueryIndexPlan)indexPlan);
        }

        @Nonnull
        @Override
        public Boolean visitDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
            return distinctRecordsFromSingleChild(deletePlan);
        }

        @Nonnull
        @Override
        public Boolean visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            final var resultValue = mapPlan.getResultValue();

            if (resultValue instanceof QuantifiedObjectValue) {
                if (((QuantifiedObjectValue)resultValue).getAlias().equals(mapPlan.getInner().getAlias())) {
                    return distinctRecordsFromSingleChild(mapPlan);
                }
            }
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
            return distinctRecordsFromChildren(comparatorPlan).stream().allMatch(d -> d);
        }

        @Nonnull
        @Override
        public Boolean visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
            return distinctRecordsFromChildren(selectorPlan).stream().allMatch(d -> d);
        }

        @Nonnull
        @Override
        public Boolean visitRangePlan(@Nonnull final RecordQueryRangePlan element) {
            return true;
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
        public Boolean visitInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
            return distinctRecordsFromSingleChild(insertPlan);
        }

        @Nonnull
        @Override
        public Boolean visitTableFunctionPlan(@Nonnull final RecordQueryTableFunctionPlan element) {
            return  false;
        }

        @Nonnull
        @Override
        public Boolean visitTempTableInsertPlan(@Nonnull final TempTableInsertPlan tempTableInsertPlan) {
            return distinctRecordsFromSingleChild(tempTableInsertPlan);
        }

        @Nonnull
        @Override
        public Boolean visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan element) {
            // TODO this could be wrong -- but it is the way it was previously encoded
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
            final var matchCandidateOptional = indexPlan.getMatchCandidateMaybe();
            if (matchCandidateOptional.isEmpty()) {
                return false;
            }

            final var matchCandidate = matchCandidateOptional.get();
            return !matchCandidate.createsDuplicates();
        }

        @Nonnull
        @Override
        public Boolean visitRecursiveLevelUnionPlan(@Nonnull final RecordQueryRecursiveLevelUnionPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitDefaultOnEmptyPlan(@Nonnull final RecordQueryDefaultOnEmptyPlan element) {
            return distinctRecordsFromSingleChild(element);
        }

        @Nonnull
        public Boolean visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            return distinctRecordsFromSingleChild(inJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            return distinctRecordsFromSingleChild(filterPlan);
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
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan fetchFromPartialRecordPlan) {
            return distinctRecordsFromSingleChild(fetchFromPartialRecordPlan);
        }

        @Nonnull
        @Override
        public Boolean visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            return distinctRecordsFromSingleChild(typeFilterPlan);
        }

        @Nonnull
        @Override
        public Boolean visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitMultiIntersectionOnValuesPlan(@Nonnull final RecordQueryMultiIntersectionOnValuesPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Boolean visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitScanPlan(@Nonnull final RecordQueryScanPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan element) {
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
            return distinctRecordsFromSingleChild(damPlan);
        }

        @Nonnull
        @Override
        public Boolean visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
            return distinctRecordsFromSingleChild(sortPlan);
        }

        @Nonnull
        @Override
        public Boolean visitRecursiveDfsJoinPlan(@Nonnull final RecordQueryRecursiveDfsJoinPlan recursiveDfsJoinPlan) {
            return false;
        }

        @Nonnull
        @Override
        public Boolean visitDefault(@Nonnull final RecordQueryPlan element) {
            return false;
        }

        private boolean distinctRecordsFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return evaluateForReference(Iterables.getOnlyElement(quantifiers).getRangesOver());
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        @Nonnull
        private List<Boolean> distinctRecordsFromChildren(@Nonnull final RelationalExpression expression) {
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
            final var memberDistinctRecordsCollection =
                    reference.getPropertyForPlans(DISTINCT_RECORDS).values();

            return memberDistinctRecordsCollection
                    .stream()
                    .allMatch(d -> d);
        }
    }
}
