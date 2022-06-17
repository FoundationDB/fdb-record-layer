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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * TODO
 */
public class PrimaryKeyProperty implements PlanProperty<Optional<KeyExpression>> {
    public static final PlanProperty<Optional<KeyExpression>> PRIMARY_KEY = new PrimaryKeyProperty();

    @Nonnull
    @Override
    public RecordQueryPlanVisitor<Optional<KeyExpression>> createVisitor() {
        return new PrimaryKeyVisitor();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Planner property which indicates if the record flowed as the result of a
     * {@link RecordQueryPlan} flows things stored records flow,
     * e.g. primary keys, or if the result does not flow them.
     */
    public static class PrimaryKeyVisitor implements RecordQueryPlanVisitor<Optional<KeyExpression>> {
        @Nonnull
        @Override
        public Optional<KeyExpression> visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            return primaryKeyFromSingleChild(predicatesFilterPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            final var indexPlan = coveringIndexPlan.getIndexPlan();
            if (indexPlan instanceof RecordQueryIndexPlan) {
                return visitIndexPlan((RecordQueryIndexPlan)indexPlan);
            }
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionOnKeyExpressionPlan) {
            return commonPrimaryKeyFromChildren(intersectionOnKeyExpressionPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            return primaryKeyFromSingleChild(mapPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
            return commonPrimaryKeyFromChildren(comparatorPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return primaryKeyFromSingleChild(unorderedDistinctPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
            return commonPrimaryKeyFromChildren(selectorPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitIntersectionOnValuePlan(@Nonnull final RecordQueryIntersectionOnValuePlan intersectionOnValuePlan) {
            return commonPrimaryKeyFromChildren(intersectionOnValuePlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
            return primaryKeyFromSingleChild(scoreForRankPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
            return Optional.ofNullable(indexPlan.getCommonPrimaryKey());
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan) {
            return primaryKeyFromSingleChild(firstOrDefaultPlan);
        }

        @Nonnull
        public Optional<KeyExpression> visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            return primaryKeyFromSingleChild(inJoinPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            return primaryKeyFromSingleChild(filterPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
            return primaryKeyFromSingleChild(unorderedPrimaryKeyDistinctPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
            return commonPrimaryKeyFromChildren(unionOnKeyExpressionPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan fetchFromPartialRecordPlan) {
            return primaryKeyFromSingleChild(fetchFromPartialRecordPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            return primaryKeyFromSingleChild(typeFilterPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            return primaryKeyFromSingleChild(inUnionOnKeyExpressionPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
            if (flatMapPlan.isInheritOuterRecordProperties()) {
                return primaryKeyFromSingleQuantifier(flatMapPlan.getOuterQuantifier());
            }
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitUnionOnValuePlan(@Nonnull final RecordQueryUnionOnValuePlan unionOnValuePlan) {
            return commonPrimaryKeyFromChildren(unionOnValuePlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return commonPrimaryKeyFromChildren(unorderedUnionPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
            return Optional.ofNullable(scanPlan.getCommonPrimaryKey());
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitInUnionOnValuePlan(@Nonnull final RecordQueryInUnionOnValuePlan inUnionOnValuePlan) {
            return primaryKeyFromSingleChild(inUnionOnValuePlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
            return primaryKeyFromSingleChild(sortPlan);
        }

        @Nonnull
        @Override
        public Optional<KeyExpression> visitDefault(@Nonnull final RecordQueryPlan element) {
            return Optional.empty();
        }

        private Optional<KeyExpression> primaryKeyFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return primaryKeyFromSingleQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot compute property for expression");
        }

        private Optional<KeyExpression> primaryKeyFromSingleQuantifier(@Nonnull final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        @Nonnull
        private List<Optional<KeyExpression>> primaryKeysFromChildren(@Nonnull final RecordQueryPlan recordQueryPlan) {
            return recordQueryPlan.getQuantifiers()
                    .stream()
                    .filter(quantifier -> quantifier instanceof Quantifier.ForEach || quantifier instanceof Quantifier.Physical)
                    .map(quantifier -> evaluateForReference(quantifier.getRangesOver()))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        private Optional<KeyExpression> commonPrimaryKeyFromChildren(@Nonnull final RecordQueryPlan recordQueryPlan) {
            final var primaryKeysFromChildren = primaryKeysFromChildren(recordQueryPlan);

            return commonPrimaryKeyFromOptionals(primaryKeysFromChildren);
        }

        @Nonnull
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Optional<KeyExpression> commonPrimaryKeyFromOptionals(@Nonnull Iterable<Optional<KeyExpression>> primaryKeyOptionals) {
            if (Streams.stream(primaryKeyOptionals).anyMatch(Optional::isEmpty)) {
                return Optional.empty();
            }
            return commonPrimaryKeyMaybe(Streams.stream(primaryKeyOptionals).map(Optional::get).collect(ImmutableList.toImmutableList()));
        }

        @Nonnull
        private static Optional<KeyExpression> commonPrimaryKeyMaybe(@Nonnull Iterable<KeyExpression> primaryKeys) {
            KeyExpression common = null;
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

        private Optional<KeyExpression> evaluateForReference(@Nonnull ExpressionRef<? extends RelationalExpression> reference) {
            final var memberPrimaryKeysCollection =
                    reference.getPlannerAttributeForMembers(PRIMARY_KEY).values();

            return commonPrimaryKeyFromOptionals(memberPrimaryKeysCollection);
        }

        public static Optional<KeyExpression> evaluate(@Nonnull RecordQueryPlan recordQueryPlan) {
            // Won't actually be null for relational planner expressions.
            return new PrimaryKeyVisitor().visit(recordQueryPlan);
        }
    }
}
