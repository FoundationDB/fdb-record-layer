/*
 * PrimaryKeyProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;
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
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The primary key property that computes the primary key of a sub graph if it exists. A primary key may not exist,
 * if the {@link RecordQueryPlan}s on the way from the source to the top of the graph modifies the flowing data in
 * a way that a primary key cannot be derived anymore.
 * This property is used by e.g. the implementation of set plans (e.g. distinct unions, intersections) to understand
 * if a stream of records originates from the same source (i.e. table) or not.
 */
public class PrimaryKeyProperty implements ExpressionProperty<Optional<List<Value>>> {
    private static final PrimaryKeyProperty PRIMARY_KEY = new PrimaryKeyProperty();

    private PrimaryKeyProperty() {
        // prevent outside instantiation
    }

    @Override
    public RelationalExpressionVisitor<Optional<List<Value>>> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new PrimaryKeyVisitor());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public Optional<List<Value>> evaluate(final Reference reference) {
        return evaluate(reference.getOnlyElementAsPlan());
    }

    public Optional<List<Value>> evaluate(final RecordQueryPlan recordQueryPlan) {
        return createVisitor().visit(recordQueryPlan);
    }

    public static PrimaryKeyProperty primaryKey() {
        return PRIMARY_KEY;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public static Optional<List<Value>> commonPrimaryKeyValuesMaybeFromOptionals(Iterable<Optional<List<Value>>> primaryKeyOptionals) {
        if (Streams.stream(primaryKeyOptionals).anyMatch(Optional::isEmpty)) {
            return Optional.empty();
        }
        return commonPrimaryKeyMaybe(Streams.stream(primaryKeyOptionals).map(Optional::get).collect(ImmutableList.toImmutableList()));
    }

    private static Optional<List<Value>> commonPrimaryKeyMaybe(Iterable<List<Value>> primaryKeys) {
        List<Value> common = null;
        var first = true;
        for (final var primaryKey : primaryKeys) {
            if (first) {
                common = primaryKey;
                first = false;
            } else if (!common.equals(primaryKey)) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(common);
    }

    /**
     * Planner property which indicates if the record flowed as the result of a
     * {@link RecordQueryPlan} flows things stored records flow,
     * e.g. primary keys, or if the result does not flow them.
     */
    public static class PrimaryKeyVisitor implements RecordQueryPlanVisitor<Optional<List<Value>>> {
        @Override
        public Optional<List<Value>> visitUpdatePlan(final RecordQueryUpdatePlan updatePlan) {
            // TODO make better
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitPredicatesFilterPlan(final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return primaryKeyFromSingleChild(predicatesFilterPlan);
        }

        @Override
        public Optional<List<Value>> visitLoadByKeysPlan(final RecordQueryLoadByKeysPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitInValuesJoinPlan(final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Override
        public Optional<List<Value>> visitInComparandJoinPlan(final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Override
        public Optional<List<Value>> visitAggregateIndexPlan(final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitCoveringIndexPlan(final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            final var indexPlan = coveringIndexPlan.getIndexPlan();
            if (indexPlan instanceof RecordQueryIndexPlan) {
                return visitIndexPlan((RecordQueryIndexPlan)indexPlan);
            }
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitDeletePlan(final RecordQueryDeletePlan deletePlan) {
            return primaryKeyFromSingleChild(deletePlan);
        }

        @Override
        public Optional<List<Value>> visitIntersectionOnKeyExpressionPlan(final RecordQueryIntersectionOnKeyExpressionPlan intersectionOnKeyExpressionPlan) {
            return commonPrimaryKeyFromChildren(intersectionOnKeyExpressionPlan);
        }

        @Override
        public Optional<List<Value>> visitMapPlan(final RecordQueryMapPlan mapPlan) {
            return primaryKeyFromSingleChild(mapPlan);
        }

        @Override
        public Optional<List<Value>> visitComparatorPlan(final RecordQueryComparatorPlan comparatorPlan) {
            return commonPrimaryKeyFromChildren(comparatorPlan);
        }

        @Override
        public Optional<List<Value>> visitUnorderedDistinctPlan(final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return primaryKeyFromSingleChild(unorderedDistinctPlan);
        }

        @Override
        public Optional<List<Value>> visitSelectorPlan(final RecordQuerySelectorPlan selectorPlan) {
            return commonPrimaryKeyFromChildren(selectorPlan);
        }

        @Override
        public Optional<List<Value>> visitRangePlan(final RecordQueryRangePlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitTempTableScanPlan(final TempTableScanPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitExplodePlan(final RecordQueryExplodePlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitInsertPlan(final RecordQueryInsertPlan insertPlan) {
            // TODO make better
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitTableFunctionPlan(final RecordQueryTableFunctionPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitTempTableInsertPlan(final TempTableInsertPlan tempTableInsertPlan) {
            // table queues do not support primary key currently.
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitIntersectionOnValuesPlan(final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
            return commonPrimaryKeyFromChildren(intersectionOnValuesPlan);
        }

        @Override
        public Optional<List<Value>> visitScoreForRankPlan(final RecordQueryScoreForRankPlan scoreForRankPlan) {
            return primaryKeyFromSingleChild(scoreForRankPlan);
        }

        @Override
        public Optional<List<Value>> visitIndexPlan(final RecordQueryIndexPlan indexPlan) {
            return Optional.of(ScalarTranslationVisitor.translateKeyExpression(indexPlan.getCommonPrimaryKey(), Objects.requireNonNull(indexPlan.getResultType().getInnerType())));
        }

        @Override
        public Optional<List<Value>> visitRecursiveLevelUnionPlan(final RecordQueryRecursiveLevelUnionPlan recursiveUnionPlan) {
            return commonPrimaryKeyFromChildren(recursiveUnionPlan);
        }

        @Override
        public Optional<List<Value>> visitFirstOrDefaultPlan(final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan) {
            return primaryKeyFromSingleChild(firstOrDefaultPlan);
        }

        @Override
        public Optional<List<Value>> visitDefaultOnEmptyPlan(final RecordQueryDefaultOnEmptyPlan defaultOnEmptyPlan) {
            return Optional.empty();
        }

        public Optional<List<Value>> visitInJoinPlan(final RecordQueryInJoinPlan inJoinPlan) {
            return primaryKeyFromSingleChild(inJoinPlan);
        }

        @Override
        public Optional<List<Value>> visitFilterPlan(final RecordQueryFilterPlan filterPlan) {
            return primaryKeyFromSingleChild(filterPlan);
        }

        @Override
        public Optional<List<Value>> visitUnorderedPrimaryKeyDistinctPlan(final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
            return primaryKeyFromSingleChild(unorderedPrimaryKeyDistinctPlan);
        }

        @Override
        public Optional<List<Value>> visitUnionOnKeyExpressionPlan(final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
            return commonPrimaryKeyFromChildren(unionOnKeyExpressionPlan);
        }

        @Override
        public Optional<List<Value>> visitTextIndexPlan(final RecordQueryTextIndexPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitFetchFromPartialRecordPlan(final RecordQueryFetchFromPartialRecordPlan fetchFromPartialRecordPlan) {
            return primaryKeyFromSingleChild(fetchFromPartialRecordPlan);
        }

        @Override
        public Optional<List<Value>> visitTypeFilterPlan(final RecordQueryTypeFilterPlan typeFilterPlan) {
            return primaryKeyFromSingleChild(typeFilterPlan);
        }

        @Override
        public Optional<List<Value>> visitInUnionOnKeyExpressionPlan(final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            return primaryKeyFromSingleChild(inUnionOnKeyExpressionPlan);
        }

        @Override
        public Optional<List<Value>> visitMultiIntersectionOnValuesPlan(final RecordQueryMultiIntersectionOnValuesPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitInParameterJoinPlan(final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Override
        public Optional<List<Value>> visitFlatMapPlan(final RecordQueryFlatMapPlan flatMapPlan) {
            if (flatMapPlan.isInheritOuterRecordProperties()) {
                return primaryKeyFromSingleQuantifier(flatMapPlan.getOuterQuantifier());
            }
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitStreamingAggregationPlan(final RecordQueryStreamingAggregationPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitUnionOnValuesPlan(final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return commonPrimaryKeyFromChildren(unionOnValuesPlan);
        }

        @Override
        public Optional<List<Value>> visitUnorderedUnionPlan(final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return commonPrimaryKeyFromChildren(unorderedUnionPlan);
        }

        @Override
        public Optional<List<Value>> visitScanPlan(final RecordQueryScanPlan scanPlan) {
            return scanPlan.getMatchCandidateMaybe().flatMap(WithPrimaryKeyMatchCandidate::getPrimaryKeyValuesMaybe);
        }

        @Override
        public Optional<List<Value>> visitInUnionOnValuesPlan(final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
            return primaryKeyFromSingleChild(inUnionOnValuesPlan);
        }

        @Override
        public Optional<List<Value>> visitComposedBitmapIndexQueryPlan(final ComposedBitmapIndexQueryPlan element) {
            return Optional.empty();
        }

        @Override
        public Optional<List<Value>> visitDamPlan(final RecordQueryDamPlan damPlan) {
            return primaryKeyFromSingleChild(damPlan);
        }

        @Override
        public Optional<List<Value>> visitSortPlan(final RecordQuerySortPlan sortPlan) {
            return primaryKeyFromSingleChild(sortPlan);
        }

        @Override
        public Optional<List<Value>> visitRecursiveDfsJoinPlan(final RecordQueryRecursiveDfsJoinPlan recursiveDfsJoinPlan) {
            return commonPrimaryKeyFromChildren(recursiveDfsJoinPlan);
        }

        @Override
        public Optional<List<Value>> visitDefault(final RecordQueryPlan element) {
            return Optional.empty();
        }

        private Optional<List<Value>> primaryKeyFromSingleChild(final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return primaryKeyFromSingleQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private Optional<List<Value>> primaryKeyFromSingleQuantifier(final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        private List<Optional<List<Value>>> primaryKeysFromChildren(final RecordQueryPlan recordQueryPlan) {
            return recordQueryPlan.getQuantifiers()
                    .stream()
                    .filter(quantifier -> quantifier instanceof Quantifier.ForEach || quantifier instanceof Quantifier.Physical)
                    .map(quantifier -> evaluateForReference(quantifier.getRangesOver()))
                    .collect(ImmutableList.toImmutableList());
        }

        private Optional<List<Value>> commonPrimaryKeyFromChildren(final RecordQueryPlan recordQueryPlan) {
            final var primaryKeysFromChildren = primaryKeysFromChildren(recordQueryPlan);

            return commonPrimaryKeyValuesMaybeFromOptionals(primaryKeysFromChildren);
        }

        private Optional<List<Value>> evaluateForReference(Reference reference) {
            final var memberPrimaryKeysCollection =
                    reference.getPropertyForPlans(PRIMARY_KEY).values();

            return commonPrimaryKeyValuesMaybeFromOptionals(memberPrimaryKeysCollection);
        }
    }
}
