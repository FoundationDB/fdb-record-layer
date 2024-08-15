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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.InComparandSource;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * This property attempts to derive the minimum and the maximum cardinality of the conceptual result of a
 * {@link RelationalExpression}. If the expression happens to be a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} the minimum and maximum cardinalities derived
 * by this property represent the cardinality bounds of the plan. This property is implemented in a defensive way,
 * using best-effort, e.g. the result may indicate {@link Cardinality#unknownCardinality()} if a cardinality cannot be
 * constrained from the data flow graph given to the property.
 */
public class CardinalitiesProperty implements ExpressionProperty<CardinalitiesProperty.Cardinalities> {

    private static final CardinalitiesProperty INSTANCE = new CardinalitiesProperty();

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
        return fromChild(updatePlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        final var cardinalitiesFromChild = fromChild(predicatesFilterPlan);
        return new Cardinalities(Cardinality.ofCardinality(0L), cardinalitiesFromChild.getMaxCardinality());
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
    public Cardinalities visitRecordQueryAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
        final var groupingValueMaybe = aggregateIndexPlan.getGroupingValueMaybe();
        if (groupingValueMaybe.isEmpty()) {
            return new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.ofCardinality(1L));
        }
        final var groupingValue = groupingValueMaybe.get();
        final var indexScanPlan = aggregateIndexPlan.getIndexPlan();
        final var matchCandidateOptional = indexScanPlan.getMatchCandidateMaybe();
        if (matchCandidateOptional.isEmpty()) {
            return Cardinalities.unknownMaxCardinality();
        }
        final var ordering = matchCandidateOptional.get()
                .computeOrderingFromScanComparisons(
                        indexScanPlan.getScanComparisons(),
                        indexScanPlan.isReverse(),
                        false);
        if (ordering.getEqualityBoundValues().contains(groupingValue)) {
            return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
        } else {
            return Cardinalities.unknownMaxCardinality();
        }
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
        if (!(coveringIndexPlan.getIndexPlan() instanceof RecordQueryIndexPlan)) {
            return Cardinalities.unknownMaxCardinality();
        }
        return visitRecordQueryIndexPlan((RecordQueryIndexPlan)coveringIndexPlan.getIndexPlan());
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
        return fromChild(deletePlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
        return fromChild(mapPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
        return weakenCardinalities(fromChildren(comparatorPlan));
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
        return weakenCardinalities(fromChildren(selectorPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryRangePlan(@Nonnull final RecordQueryRangePlan rangePlan) {
        final var limitValue = rangePlan.getExclusiveLimitValue();
        if (limitValue instanceof LiteralValue) {
            final var limit = (int)Verify.verifyNotNull(limitValue.compileTimeEval(EvaluationContext.EMPTY));
            return new Cardinalities(Cardinality.ofCardinality(limit), Cardinality.ofCardinality(limit));
        }
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
        return fromChild(insertPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
        return weakenCardinalities(fromChildren(intersectionOnValuesPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
        return fromChild(scoreForRankPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
        final var matchCandidateOptional = indexPlan.getMatchCandidateMaybe();
        if (matchCandidateOptional.isEmpty()) {
            return Cardinalities.unknownMaxCardinality();
        }
        final var matchCandidate = matchCandidateOptional.get();

        final var ordering =
                matchCandidate.computeOrderingFromScanComparisons(indexPlan.getScanComparisons(),
                        indexPlan.isReverse(),
                        false);
        final var equalityBoundValues = ordering.getEqualityBoundValues();

        // try to see if the primary key is bound by equalities
        if (matchCandidate instanceof WithPrimaryKeyMatchCandidate) {
            final var primaryKeyValuesOptional = ((WithPrimaryKeyMatchCandidate)matchCandidate).getPrimaryKeyValuesMaybe();
            if (primaryKeyValuesOptional.isPresent()) {
                final var primaryKeyValues = primaryKeyValuesOptional.get();
                if (equalityBoundValues.containsAll(primaryKeyValues)) {
                    return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
                }
            }
        }

        if (matchCandidate.isUnique() && matchCandidate instanceof ValueIndexScanMatchCandidate) {
            // unique index
            final var valueIndexScanMatchCandidate = (ValueIndexScanMatchCandidate)matchCandidate;
            final var translationMap = AliasMap.ofAliases(valueIndexScanMatchCandidate.getBaseAlias(), Quantifier.current());
            final var keyValues =
                    valueIndexScanMatchCandidate.getIndexKeyValues()
                            .stream()
                            .limit(matchCandidate.getColumnSize())
                            .map(keyValue -> keyValue.rebase(translationMap))
                            .collect(ImmutableList.toImmutableList());
            if (equalityBoundValues.containsAll(keyValues)) {
                return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
            }
        }

        return Cardinalities.unknownMaxCardinality();
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
        final var cardinalitiesFromChild = fromChild(filterPlan);
        return new Cardinalities(Cardinality.ofCardinality(0L), cardinalitiesFromChild.getMaxCardinality());
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
        final var cardinalitiesFromChild = fromChild(unorderedPrimaryKeyDistinctPlan);
        if (!cardinalitiesFromChild.getMinCardinality().isUnknown()) {
            if (cardinalitiesFromChild.getMinCardinality().getCardinality() >= 1L) {
                return new Cardinalities(Cardinality.ofCardinality(1L), cardinalitiesFromChild.getMaxCardinality());
            }
            // the case where min-cardinality == 0L is to fall through
        }
        return cardinalitiesFromChild;
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
        return Cardinalities.unknownMaxCardinality();
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
                            if (inSource instanceof InParameterSource || inSource instanceof InComparandSource) {
                                return Cardinalities.unknownMaxCardinality();
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
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan element) {
        return Cardinalities.unknownMaxCardinality();
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
        // if we do not have any grouping value, we will apply the aggregation(s) over the entire child result set
        // and return a single row comprising the aggregation(s) result
        if (element.getGroupingValue() == null) {
            return new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.ofCardinality(1L));
        }
        // if the grouping value is constant, the cardinality ranges between 0 and 1.
        if (element.getGroupingValue().isConstant()) {
            return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
        }
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
        return unionCardinalities(fromChildren(unionOnValuesPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
        return unionCardinalities(fromChildren(unorderedUnionPlan));
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
        final var matchCandidateOptional = scanPlan.getMatchCandidateMaybe();
        if (matchCandidateOptional.isEmpty()) {
            return Cardinalities.unknownMaxCardinality();
        }

        final var matchCandidate = matchCandidateOptional.get();
        final var primaryKeyValuesOptional = matchCandidate.getPrimaryKeyValuesMaybe();
        if (primaryKeyValuesOptional.isEmpty()) {
            return Cardinalities.unknownMaxCardinality();
        }
        final var primaryKeyValues = primaryKeyValuesOptional.get();

        final var ordering =
                matchCandidate.computeOrderingFromScanComparisons(scanPlan.getScanComparisons(),
                        scanPlan.isReverse(),
                        false);

        final var equalityBoundValues = ordering.getEqualityBoundValues();
        if (equalityBoundValues.containsAll(primaryKeyValues)) {
            return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
        }

        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
        return visitRecordQueryInUnionPlan(inUnionOnValuesPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitRecordQueryDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
        return fromChild(damPlan);
    }

    @Nonnull
    @Override
    public Cardinalities visitMatchableSortExpression(@Nonnull final MatchableSortExpression matchableSortExpression) {
        return fromChild(matchableSortExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitInsertExpression(@Nonnull final InsertExpression insertExpression) {
        return fromChild(insertExpression);
    }

    @Nonnull
    @Override
    public Cardinalities visitPrimaryScanExpression(@Nonnull final PrimaryScanExpression element) {
        // TODO do better
        return Cardinalities.unknownMaxCardinality();
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
    public Cardinalities visitLogicalUniqueExpression(@Nonnull final LogicalUniqueExpression logicalUniqueExpression) {
        return fromChild(logicalUniqueExpression);
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
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitFullUnorderedScanExpression(@Nonnull final FullUnorderedScanExpression element) {
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitGroupByExpression(@Nonnull final GroupByExpression element) {
        // if we do not have any grouping value, we will apply the aggregation(s) over the entire child result set
        // and return a single row comprising the aggregation(s) result
        if (element.getGroupingValue() == null) {
            return new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.ofCardinality(1L));
        }
        // if the grouping value is constant, the cardinality ranges between 0 and 1.
        if (element.getGroupingValue().isConstant()) {
            return new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(1L));
        }
        return Cardinalities.unknownMaxCardinality();
    }

    @Nonnull
    @Override
    public Cardinalities visitUpdateExpression(@Nonnull final UpdateExpression updateExpression) {
        return fromChild(updateExpression);
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
    public Cardinalities visitDeleteExpression(@Nonnull final DeleteExpression deleteExpression) {
        return fromChild(deleteExpression);
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
    public Cardinalities evaluateAtRef(@Nonnull Reference ref, @Nonnull List<Cardinalities> memberResults) {
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
                    minCardinality = Cardinality.ofCardinality(0);
                } else {
                    minCardinality = Cardinality.unknownCardinality();
                }
            }

            if (maxCardinality.isUnknown()) {
                maxCardinality = cardinalities.getMaxCardinality();
            } else {
                if (!cardinalities.getMaxCardinality().isUnknown()) {
                    maxCardinality = Cardinality.ofCardinality(Math.min(maxCardinality.getCardinality(), cardinalities.getMaxCardinality().getCardinality()));
                } else {
                    maxCardinality = Cardinality.unknownCardinality();
                }
            }
        }

        return new Cardinalities(minCardinality, maxCardinality);
    }

    @Nonnull
    private Cardinalities unionCardinalities(@Nonnull Iterable<Cardinalities> cardinalitiesIterable) {
        //
        // Merge all cardinalities in the iterable.
        //
        final var iterator = cardinalitiesIterable.iterator();

        if (!iterator.hasNext()) {
            return Cardinalities.unknownMaxCardinality();
        }

        var cardinalities = iterator.next();

        var minCardinality = cardinalities.getMinCardinality();
        var maxCardinality = cardinalities.getMaxCardinality();
        while (iterator.hasNext()) {
            cardinalities = iterator.next();

            if (!minCardinality.isUnknown()) {
                final var currentMinCardinality = cardinalities.getMinCardinality();
                if (currentMinCardinality.isUnknown()) {
                    minCardinality = Cardinality.unknownCardinality();
                } else {
                    minCardinality = Cardinality.ofCardinality(minCardinality.getCardinality() + currentMinCardinality.getCardinality());
                }
            }

            if (!maxCardinality.isUnknown()) {
                final var currentMaxCardinality = cardinalities.getMaxCardinality();
                if (currentMaxCardinality.isUnknown()) {
                    maxCardinality = Cardinality.unknownCardinality();
                } else {
                    maxCardinality = Cardinality.ofCardinality(maxCardinality.getCardinality() + currentMaxCardinality.getCardinality());
                }
            }
        }

        return new Cardinalities(minCardinality, maxCardinality);
    }

    @Nonnull
    private Cardinalities weakenCardinalities(@Nonnull Iterable<Cardinalities> cardinalitiesIterable) {
        //
        // Merge all cardinalities in the iterable.
        // If the cardinality in one member is unknown but not in the other, we'll take the less constraining one
        //
        final var iterator = cardinalitiesIterable.iterator();

        if (!iterator.hasNext()) {
            return Cardinalities.unknownMaxCardinality();
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
                        maxCardinality.getCardinality() < currentMaxCardinality.getCardinality()) {
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
    public static Cardinalities evaluate(@Nonnull RelationalExpression expression) {
        Cardinalities result = expression.acceptPropertyVisitor(INSTANCE);
        if (result == null) {
            return Cardinalities.unknownMaxCardinality();
        }
        return result;
    }

    @Nonnull
    public static Cardinalities evaluate(@Nonnull Quantifier quantifier) {
        return new CardinalitiesProperty().fromQuantifier(quantifier);
    }

    @Nonnull
    public static Cardinalities evaluate(@Nonnull Reference ref) {
        @Nullable final var nullableResult =
                ref.acceptPropertyVisitor(new CardinalitiesProperty());
        return Objects.requireNonNull(nullableResult);
    }


    /**
     *  Class to capture both minimum and maximum cardinality of an expression. Also contains some helpers to combine
     *  cardinality information.
     */
    @SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR", justification = "False positive as this is not a singleton class")
    public static class Cardinalities {
        private static final Cardinalities unknownCardinalities = new Cardinalities(Cardinality.unknownCardinality(), Cardinality.unknownCardinality());

        private static final Cardinalities unknownMaxCardinality = new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.unknownCardinality());
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

        @Nonnull
        public static Cardinalities unknownCardinalities() {
            return unknownCardinalities;
        }

        @Nonnull
        public static Cardinalities unknownMaxCardinality() {
            return unknownMaxCardinality;
        }
    }

    /**
     * Class to encapsulate the minimum or maximum cardinality of an expression. Also contains helpers to combine
     * cardinality information which each other.
     */
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
