/*
 * CardinalitiesProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * This property establishes a partial order over the expressions contained in a subgraph.
 */
public class CardinalitiesProperty implements ExpressionProperty<CardinalitiesProperty.Cardinalities> {
    @Nonnull
    @Override
    public Cardinalities visitRecordQueryPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        return fromChild(predicatesFilterPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
        final var keysSource = element.getKeysSource();
        if (keysSource.maxCardinality() == QueryPlan.UNKNOWN_MAX_CARDINALITY) {
            return Cardinalities.unknownCardinalities;
        }

        return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(keysSource.maxCardinality()));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
        final var childCardinalities = fromChild(inValuesJoinPlan);
        final var valuesSize = inValuesJoinPlan.getInListValues().size();
        return  new Cardinalities(
                Cardinality.ofCardinality(valuesSize).times(childCardinalities.getMinCardinality()),
                Cardinality.ofCardinality(valuesSize).times(childCardinalities.getMaxCardinality()));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
        // TODO be better
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
        return fromChild(mapPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
        return unionCardinalities(fromChildren(comparatorPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
        return fromChild(unorderedDistinctPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionOnKeyExpressionPlan) {
        return intersectCardinalities(fromChildren(intersectionOnKeyExpressionPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQuerySelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
        return unionCardinalities(fromChildren(selectorPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryIntersectionOnValuePlan(@Nonnull final RecordQueryIntersectionOnValuePlan intersectionOnValuePlan) {
        return unionCardinalities(fromChildren(intersectionOnValuePlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
        return fromChild(scoreForRankPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryIndexPlan(@Nonnull final RecordQueryIndexPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
        return new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.ofCardinality(1L));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
        return unionCardinalities(fromChildren(unionOnKeyExpressionPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
        return fromChild(filterPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
        return fromChild(unorderedPrimaryKeyDistinctPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan fetchFromPartialRecordPlan) {
        return fromChild(fetchFromPartialRecordPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
        return fromChild(typeFilterPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
        return visitRecordQueryInUnionPlan(inUnionOnKeyExpressionPlan);
    }

    @Nonnull
    public Cardinalities visitRecordQueryInUnionPlan(@Nonnull final RecordQueryInUnionPlan inUnionPlan) {
        final var inSources = inUnionPlan.getInSources();

        final var inSourcesCardinalitiesOptional =
                inSources.stream()
                        .map(inSource -> {
                            if (inSource instanceof InParameterSource) {
                                return Cardinalities.unknownCardinalities();
                            } else {
                                Verify.verify(inSource instanceof InValuesSource);
                                final var inValuesSource = (InValuesSource)inSource;
                                final var size = inValuesSource.getValues().size();
                                return new Cardinalities(Cardinality.ofCardinality(size), Cardinality.ofCardinality(size));
                            }
                        })
                        .reduce(Cardinalities::times);

        Verify.verify(inSourcesCardinalitiesOptional.isPresent());
        final var inSourcesCardinalities = inSourcesCardinalitiesOptional.get();
        final var childCardinalities = fromChild(inUnionPlan);

        return inSourcesCardinalities.times(childCardinalities);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
        final var fromChildren = fromChildren(flatMapPlan);
        final var outerCardinalities = fromChildren.get(0);
        final var innerCardinalities = fromChildren.get(1);

        return outerCardinalities.times(innerCardinalities);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnionOnValuePlan(@Nonnull final RecordQueryUnionOnValuePlan unionOnValuePlan) {
        return unionCardinalities(fromChildren(unionOnValuePlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
        return unionCardinalities(fromChildren(unorderedUnionPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryScanPlan(@Nonnull final RecordQueryScanPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInUnionOnValuePlan(@Nonnull final RecordQueryInUnionOnValuePlan inUnionOnValuePlan) {
        return visitRecordQueryInUnionPlan(inUnionOnValuePlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitMatchableSortExpression(@Nonnull final MatchableSortExpression matchableSortExpression) {
        return fromChild(matchableSortExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitIndexScanExpression(@Nonnull final IndexScanExpression element) {
        // TODO do better
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitPrimaryScanExpression(@Nonnull final PrimaryScanExpression element) {
        // TODO do better
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalSortExpression(@Nonnull final LogicalSortExpression logicalSortExpression) {
        return fromChild(logicalSortExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression logicalTypeFilterExpression) {
        return fromChild(logicalTypeFilterExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalUnionExpression(@Nonnull final LogicalUnionExpression logicalUnionExpression) {
        return unionCardinalities(fromChildren(logicalUnionExpression));
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalIntersectionExpression(@Nonnull final LogicalIntersectionExpression logicalIntersectionExpression) {
        return intersectCardinalities(fromChildren(logicalIntersectionExpression));
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalProjectionExpression(@Nonnull final LogicalProjectionExpression logicalProjectionExpression) {
        return fromChild(logicalProjectionExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitSelectExpression(@Nonnull final SelectExpression selectExpression) {
        return fromChildren(selectExpression)
                .stream()
                .reduce(Cardinalities::times)
                .orElseThrow(() -> new RecordCoreException("must have at least one quantifier"));
    }

    @Nonnull
    @Override
    public Cardinalities visitExplodeExpression(@Nonnull final ExplodeExpression element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitFullUnorderedScanExpression(@Nonnull final FullUnorderedScanExpression element) {
        return Cardinalities.unknownCardinalities();
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalDistinctExpression(@Nonnull final LogicalDistinctExpression logicalDistinctExpression) {
        return fromChild(logicalDistinctExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitLogicalFilterExpression(@Nonnull final LogicalFilterExpression logicalFilterExpression) {
        return fromChild(logicalFilterExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQuerySortPlan(@Nonnull final RecordQuerySortPlan querySortPlan) {
        return fromChild(querySortPlan);
    }

    @Nonnull
    @Override
    public Cardinalities evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Cardinalities> childResults) {
        throw new RecordCoreException("unsupported method call. property should instead override visit for expression");
    }

    @Nonnull
    @Override
    public Cardinalities evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Cardinalities> memberResults) {
        return intersectCardinalities(memberResults);
    }

    @Nonnull
    private Cardinalities intersectCardinalities(@Nonnull Iterable<Cardinalities> cardinalitiesIterable) {
        //
        // Merge all cardinalities in the iterable.
        // If the cardinality in one member is unknown but not in the other, we'll take the more constraining one
        //
        var minCardinality = Cardinality.unknownCardinality();
        var maxCardinality = Cardinality.unknownCardinality();
        for (final var cardinalities : cardinalitiesIterable) {
            if (minCardinality.isUnknown()) {
                minCardinality = cardinalities.getMinCardinality();
            } else {
                if (!cardinalities.getMinCardinality().isUnknown()) {
                    minCardinality = Cardinality.ofCardinality(Math.max(minCardinality.getCardinality(), cardinalities.getMinCardinality().getCardinality()));
                }
            }

            if (maxCardinality.isUnknown()) {
                maxCardinality = cardinalities.getMaxCardinality();
            } else {
                if (!cardinalities.getMaxCardinality().isUnknown()) {
                    maxCardinality = Cardinality.ofCardinality(Math.min(maxCardinality.getCardinality(), cardinalities.getMaxCardinality().getCardinality()));
                }
            }
        }

        return new Cardinalities(minCardinality, maxCardinality);
    }

    @Nonnull
    private Cardinalities unionCardinalities(@Nonnull Iterable<Cardinalities> cardinalitiesIterable) {
        //
        // Merge all cardinalities in the iterable.
        // If the cardinality in one member is unknown but not in the other, we'll take the less constraining one
        //
        final var iterator = cardinalitiesIterable.iterator();

        if (!iterator.hasNext()) {
            return Cardinalities.unknownCardinalities;
        }

        var cardinalities = iterator.next();

        var minCardinality = cardinalities.getMinCardinality();
        var maxCardinality = cardinalities.getMaxCardinality();
        while (iterator.hasNext()) {
            cardinalities = iterator.next();

            if (!minCardinality.isUnknown()) {
                final var currentMinCardinality = cardinalities.getMinCardinality();
                if (currentMinCardinality.isUnknown() ||
                    minCardinality.getCardinality() > currentMinCardinality.getCardinality()) {
                    minCardinality = currentMinCardinality;
                }
            }

            if (!maxCardinality.isUnknown()) {
                final var currentMaxCardinality = cardinalities.getMaxCardinality();
                if (currentMaxCardinality.isUnknown() ||
                    minCardinality.getCardinality() < currentMaxCardinality.getCardinality()) {
                    maxCardinality = currentMaxCardinality;
                }
            }
        }

        return new Cardinalities(minCardinality, maxCardinality);
    }

    @Nonnull
    private  Cardinalities fromChild(@Nonnull final RelationalExpression relationalExpression) {
        Verify.verify(relationalExpression.getQuantifiers().size() == 1);
        return Iterables.getOnlyElement(fromChildren(relationalExpression));
    }

    @Nonnull
    private List<Cardinalities> fromChildren(@Nonnull final RelationalExpression relationalExpression) {
        return fromQuantifiers(relationalExpression.getQuantifiers());
    }

    @Nonnull
    private List<Cardinalities> fromQuantifiers(@Nonnull final List<? extends Quantifier> quantifiers) {
        final var quantifierResults = Lists.<Cardinalities>newArrayListWithCapacity(quantifiers.size());
        for (final Quantifier quantifier : quantifiers) {
            quantifierResults.add(fromQuantifier(quantifier));
        }

        return quantifierResults;
    }

    @Nonnull
    private Cardinalities fromQuantifier(@Nonnull final Quantifier quantifier) {
        if (quantifier instanceof Quantifier.Existential) {
            return new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.ofCardinality(1L));
        }

        @Nullable final var nullableResult =
                quantifier.acceptPropertyVisitor(this);
        return Objects.requireNonNull(nullableResult);
    }

    @Nonnull
    public static Cardinalities evaluate(@Nonnull Quantifier quantifier) {
        return new CardinalitiesProperty().fromQuantifier(quantifier);
    }

    @Nonnull
    public static Cardinalities evaluate(@Nonnull ExpressionRef<? extends RelationalExpression> ref) {
        @Nullable final var nullableResult =
                ref.acceptPropertyVisitor(new CardinalitiesProperty());
        return Objects.requireNonNull(nullableResult);
    }

    public static class Cardinalities {
        private static final Cardinalities unknownCardinalities = new Cardinalities(Cardinality.unknownCardinality(), Cardinality.unknownCardinality());

        @Nonnull
        private final Cardinality minCardinality;
        @Nonnull
        private final Cardinality maxCardinality;

        public Cardinalities(@Nonnull final Cardinality minCardinality, @Nonnull final Cardinality maxCardinality) {
            this.minCardinality = minCardinality;
            this.maxCardinality = maxCardinality;
        }

        @Nonnull
        public Cardinality getMinCardinality() {
            return minCardinality;
        }

        @Nonnull
        public Cardinality getMaxCardinality() {
            return maxCardinality;
        }

        public Cardinalities times(@Nonnull final Cardinalities otherCardinalities) {
            return new Cardinalities(getMinCardinality().times(otherCardinalities.getMinCardinality()),
                    getMaxCardinality().times(otherCardinalities.getMaxCardinality()));
        }

        public static Cardinalities unknownCardinalities() {
            return unknownCardinalities;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Cardinality {
        private static final Cardinality unknownCardinality = new Cardinality(OptionalLong.empty());

        @Nonnull
        private final OptionalLong cardinalityOptional;

        private Cardinality(@Nonnull final OptionalLong cardinalityOptional) {
            this.cardinalityOptional = cardinalityOptional;
        }

        public boolean isUnknown() {
            return cardinalityOptional.isEmpty();
        }

        public long getCardinality() {
            Verify.verify(cardinalityOptional.isPresent());
            return cardinalityOptional.getAsLong();
        }

        public Cardinality times(@Nonnull final Cardinality otherCardinality) {
            if (isUnknown() || otherCardinality.isUnknown()) {
                return unknownCardinality();
            }
            return Cardinality.ofCardinality(getCardinality() * otherCardinality.getCardinality());
        }

        public static Cardinality ofCardinality(final long cardinality) {
            Preconditions.checkArgument(cardinality >= 0L);
            return new Cardinality(OptionalLong.of(cardinality));
        }

        public static Cardinality unknownCardinality() {
            return unknownCardinality;
        }
    }
}
