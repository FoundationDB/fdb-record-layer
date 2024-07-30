/*
 * DerivationsProperty.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithComparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FirstOrDefaultValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisonKeyValues;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRangePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A property used to create and collect the derivations of data flowing in a plan. A derivation is a {@link Value}-tree
 * that is not correlated to anything. This is achieved by exhaustively inlining the producer of a correlation into the
 * consumer of a correlation. Executing these value trees that are obtained by doing this are not really producing
 * anything useful. A derivation helps the planner to statically infer useful properties by understanding what
 * operations happen in what order starting from a base table access.
 * <br>
 * In particular, a {@link Value}-tree that somewhere appears in a plan as part of a {@link QueryPredicate}, or other
 * expression, is guaranteed to be part of a derivation which is not correlated.
 */
public class DerivationsProperty implements PlanProperty<DerivationsProperty.Derivations> {
    public static final DerivationsProperty DERIVATIONS = new DerivationsProperty();

    @Nonnull
    @Override
    public RecordQueryPlanVisitor<Derivations> createVisitor() {
        return new DerivationsVisitor();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * A visitor that determines the derivations of a {@link RecordQueryPlan}.
     */
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("java:S3776")
    public static class DerivationsVisitor implements RecordQueryPlanVisitor<Derivations> {
        @Nonnull
        @Override
        public Derivations visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
            final Quantifier rangesOver = Iterables.getOnlyElement(updatePlan.getQuantifiers());
            final var childDerivations = derivationsFromQuantifier(rangesOver);
            final var childResultValues = childDerivations.getResultValues();
            final var computationValue = updatePlan.getComputationValue();
            final var transformationsTrie = updatePlan.getTransformationsTrie();

            final var resultValuesBuilder = ImmutableList.<Value>builder();
            final var localValuesBuilder = ImmutableList.<Value>builder();
            localValuesBuilder.addAll(childDerivations.getLocalValues());
            for (final var childResultValue : childResultValues) {
                if (transformationsTrie != null) {
                    final var translationMap = TranslationMap.builder()
                            .when(rangesOver.getAlias()).then(((sourceAlias, leafValue) -> childResultValue))
                            .build();
                    transformationsTrie.values()
                            .forEach(updateValue -> localValuesBuilder.add(updateValue.translateCorrelationsAndSimplify(translationMap)));
                }

                final var resultsTranslationMap = TranslationMap.builder()
                        .when(rangesOver.getAlias()).then(((sourceAlias, leafValue) -> childResultValue))
                        .when(Quantifier.current()).then((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType(), ImmutableList.of(updatePlan.getTargetRecordType())))
                        .build();
                resultValuesBuilder.add(computationValue.translateCorrelationsAndSimplify(resultsTranslationMap));
            }
            return new Derivations(resultValuesBuilder.build(), localValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            final var childDerivations = derivationsFromSingleChild(predicatesFilterPlan);
            final var childResultValues = childDerivations.getResultValues();

            final var localValuesBuilder = ImmutableList.<Value>builder();
            localValuesBuilder.addAll(childDerivations.getLocalValues());
            final Quantifier rangesOver = Iterables.getOnlyElement(predicatesFilterPlan.getQuantifiers());
            for (final var predicate : predicatesFilterPlan.getPredicates()) {
                final var valuesFromPredicate = predicate.fold(valuesInPredicate(), combineValuesInChildren());

                for (final var childResultValue : childResultValues) {
                    final var translationMap = TranslationMap.builder()
                            .when(rangesOver.getAlias())
                            .then(((sourceAlias, leafValue) -> childResultValue))
                            .build();
                    // need to translate the value trees to directly use the value trees from the plan below
                    valuesFromPredicate.stream()
                            .map(value ->
                                    value.translateCorrelationsAndSimplify(translationMap))
                            .forEach(localValuesBuilder::add);
                }
            }
            return new Derivations(childDerivations.getResultValues(), localValuesBuilder.build());
        }

        @Nonnull
        private static TreeLike.NonnullBiFunction<List<Value>, Iterable<? extends List<Value>>, List<Value>> combineValuesInChildren() {
            return (values, childrenValuesLists) -> {
                final ImmutableList.Builder<Value> valuesBuilder = ImmutableList.builder();
                valuesBuilder.addAll(values);
                childrenValuesLists.forEach(valuesBuilder::addAll);
                return valuesBuilder.build();
            };
        }

        @Nonnull
        private static TreeLike.NonnullFunction<QueryPredicate, List<Value>> valuesInPredicate() {
            return p -> {
                final ImmutableList.Builder<Value> valuesBuilder = ImmutableList.builder();
                if (p instanceof PredicateWithValue) {
                    valuesBuilder.add(Objects.requireNonNull(((PredicateWithValue)p).getValue()));
                }
                if (p instanceof PredicateWithComparisons) {
                    ((PredicateWithComparisons)p).getComparisons()
                            .stream()
                            .map(Comparisons.Comparison::getValue)
                            .filter(Objects::nonNull)
                            .forEach(valuesBuilder::add);
                }
                return valuesBuilder.build();
            };
        }

        @Nonnull
        @Override
        public Derivations visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return derivationsFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Derivations visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Derivations visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Nonnull
        @Override
        public Derivations visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
            final var matchCandidate = aggregateIndexPlan.getMatchCandidate();
            return visitPlanWithComparisons(aggregateIndexPlan, matchCandidate.getQueriedRecordTypeNames());
        }

        @Nonnull
        @Override
        public Derivations visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            return visit(coveringIndexPlan.getIndexPlan());
        }

        @Nonnull
        @Override
        public Derivations visitDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
            return derivationsFromSingleChild(deletePlan);
        }

        @Nonnull
        @Override
        public Derivations visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionPlan) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            final Quantifier rangesOver = Iterables.getOnlyElement(mapPlan.getQuantifiers());
            final var childDerivations = derivationsFromQuantifier(rangesOver);
            final var childResultValues = childDerivations.getResultValues();
            final var resultValue = mapPlan.getResultValue();
            final var resultValuesBuilder = ImmutableList.<Value>builder();
            final var localValuesBuilder = ImmutableList.<Value>builder();
            localValuesBuilder.addAll(childDerivations.getLocalValues());
            for (final var childResultValue : childResultValues) {
                final var resultsTranslationMap = TranslationMap.builder()
                        .when(rangesOver.getAlias()).then(((sourceAlias, leafValue) -> childResultValue))
                        .build();
                resultValuesBuilder.add(resultValue.translateCorrelationsAndSimplify(resultsTranslationMap));
            }
            return new Derivations(resultValuesBuilder.build(), localValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return derivationsFromSingleChild(unorderedDistinctPlan);
        }

        @Nonnull
        @Override
        public Derivations visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitRangePlan(@Nonnull final RecordQueryRangePlan rangePlan) {
            final var values = ImmutableList.of(rangePlan.getResultValue());
            return new Derivations(values, values);
        }

        @Nonnull
        @Override
        public Derivations visitExplodePlan(@Nonnull final RecordQueryExplodePlan explodePlan) {
            final var collectionValue = explodePlan.getCollectionValue();
            final var resultType = collectionValue.getResultType();
            Verify.verify(resultType.isArray());
            final var elementType = Objects.requireNonNull(((Type.Array)resultType).getElementType());
            final var values = ImmutableList.<Value>of(new FirstOrDefaultValue(collectionValue, new ThrowsValue(elementType)));
            return new Derivations(values, values);
        }

        @Nonnull
        @Override
        public Derivations visitInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
            final Quantifier rangesOver = Iterables.getOnlyElement(insertPlan.getQuantifiers());
            final var childDerivations = derivationsFromQuantifier(rangesOver);
            final var childResultValues = childDerivations.getResultValues();
            final var computationValue = insertPlan.getComputationValue();

            final var resultValuesBuilder = ImmutableList.<Value>builder();
            final var localValuesBuilder = ImmutableList.<Value>builder();
            localValuesBuilder.addAll(childDerivations.getLocalValues());
            for (final var childResultValue : childResultValues) {
                final var resultsTranslationMap = TranslationMap.builder()
                        .when(rangesOver.getAlias()).then(((sourceAlias, leafValue) -> childResultValue))
                        .when(Quantifier.current()).then((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType(), ImmutableList.of(insertPlan.getTargetRecordType())))
                        .build();
                resultValuesBuilder.add(computationValue.translateCorrelationsAndSimplify(resultsTranslationMap));
            }
            return new Derivations(resultValuesBuilder.build(), localValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuePlan) {
            return visitSetPlan(intersectionOnValuePlan);
        }

        @Nonnull
        @Override
        public Derivations visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
            final var matchCandidate = indexPlan.getMatchCandidate();
            return visitPlanWithComparisons(indexPlan, matchCandidate.getQueriedRecordTypeNames());
        }

        @Nonnull
        private Derivations visitPlanWithComparisons(@Nonnull final RecordQueryPlanWithComparisons planWithComparisons,
                                                     @Nonnull final Iterable<String> recordTypeNames) {
            final var comparisons = planWithComparisons.getComparisons();
            final var comparisonValues = comparisons.stream()
                    .map(Comparisons.Comparison::getValue)
                    .filter(Objects::nonNull)
                    .collect(ImmutableList.toImmutableList());
            final var resultValueFromPlan = planWithComparisons.getResultValue();
            final var resultValue = new QueriedValue(resultValueFromPlan.getResultType(), recordTypeNames);

            return new Derivations(ImmutableList.of(resultValue), comparisonValues);
        }

        @Nonnull
        @Override
        public Derivations visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan) {
            final Quantifier rangesOver = Iterables.getOnlyElement(firstOrDefaultPlan.getQuantifiers());
            final var childDerivations = derivationsFromSingleChild(firstOrDefaultPlan);
            final var childResultValues = childDerivations.getResultValues();
            final var onEmptyResultValue = firstOrDefaultPlan.getOnEmptyResultValue();
            final var localValuesBuilder = ImmutableList.<Value>builder();
            localValuesBuilder.addAll(childDerivations.getLocalValues());
            for (final var childResultValue : childResultValues) {
                final var resultsTranslationMap = TranslationMap.builder()
                        .when(rangesOver.getAlias()).then(((sourceAlias, leafValue) -> childResultValue))
                        .build();
                localValuesBuilder.add(onEmptyResultValue.translateCorrelationsAndSimplify(resultsTranslationMap));
            }
            return new Derivations(childDerivations.getResultValues(), localValuesBuilder.build());
        }

        @Nonnull
        @SuppressWarnings("java:S135")
        public Derivations visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            final var outerAlias = inJoinPlan.getInAlias();
            final var innerQuantifier = inJoinPlan.getInner();
            final var innerDerivations = derivationsFromQuantifier(innerQuantifier);

            //
            // De-correlate inner against in-source. Since the in-source is not really a value, we can only
            // fake it. We just need to remove the correlation.
            //
            final var innerDecorrelatedLocalValuesBuilder = ImmutableList.<Value>builder();
            for (final var innerValue : innerDerivations.getLocalValues()) {
                if (innerValue.isCorrelatedTo(outerAlias)) {
                    final var translationMap = TranslationMap.builder()
                            .when(outerAlias)
                            .then(((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType())))
                            .build();
                    innerDecorrelatedLocalValuesBuilder.add(innerValue.translateCorrelationsAndSimplify(translationMap));
                } else {
                    innerDecorrelatedLocalValuesBuilder.add(innerValue);
                }
            }

            final var innerDecorrelatedResultValuesBuilder = ImmutableList.<Value>builder();
            for (final var innerValue : innerDerivations.getResultValues()) {
                if (innerValue.isCorrelatedTo(outerAlias)) {
                    final var translationMap = TranslationMap.builder()
                            .when(outerAlias)
                            .then(((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType())))
                            .build();
                    innerDecorrelatedResultValuesBuilder.add(innerValue.translateCorrelationsAndSimplify(translationMap));
                } else {
                    innerDecorrelatedResultValuesBuilder.add(innerValue);
                }
            }

            return new Derivations(innerDecorrelatedResultValuesBuilder.build(), innerDecorrelatedLocalValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
            return derivationsFromSingleChild(unorderedPrimaryKeyDistinctPlan);
        }

        @Nonnull
        @Override
        public Derivations visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return derivationsFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Derivations visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            final var childDerivations = derivationsFromSingleChild(typeFilterPlan);
            final var childResultValues = childDerivations.getResultValues();

            // change all QueriedValues by restricting their record type names
            final var resultValuesBuilder = ImmutableList.<Value>builder();
            final var filteredRecordTypeNames = ImmutableSet.copyOf(typeFilterPlan.getRecordTypes());
            for (final Value childResultValue : childResultValues) {
                final var replacedChildResultValueOptional =
                        childResultValue.replaceLeavesMaybe(value -> {
                            if (value instanceof QueriedValue) {
                                final var queriedValue = (QueriedValue)value;
                                final var childRecordTypeNames = queriedValue.getRecordTypeNames();
                                if (childRecordTypeNames == null) {
                                    return value;
                                }
                                final var intersectedRecordTypeNames =
                                        childRecordTypeNames.stream()
                                                .filter(filteredRecordTypeNames::contains)
                                                .collect(ImmutableList.toImmutableList());
                                return new QueriedValue(typeFilterPlan.getResultValue().getResultType(), intersectedRecordTypeNames);
                            }
                            return value;
                        });
                Verify.verify(replacedChildResultValueOptional.isPresent());
                resultValuesBuilder.add(replacedChildResultValueOptional.get());
            }
            return new Derivations(resultValuesBuilder.build(), childDerivations.getLocalValues());
        }

        @Nonnull
        @Override
        public Derivations visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Derivations visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
            final var outerQuantifier = flatMapPlan.getOuterQuantifier();
            final var innerQuantifier = flatMapPlan.getInnerQuantifier();
            final var outerDerivations = derivationsFromQuantifier(outerQuantifier);
            final var innerDerivations = derivationsFromQuantifier(innerQuantifier);

            //
            // De-correlate inner against outer.
            //
            final var innerDecorrelatedLocalValuesBuilder = ImmutableList.<Value>builder();
            for (final var innerValue : innerDerivations.getLocalValues()) {
                if (innerValue.isCorrelatedTo(outerQuantifier.getAlias())) {
                    for (final var outerResultValue : outerDerivations.getResultValues()) {
                        final var translationMap = TranslationMap.builder()
                                .when(outerQuantifier.getAlias())
                                .then(((sourceAlias, leafValue) -> outerResultValue))
                                .build();
                        innerDecorrelatedLocalValuesBuilder.add(innerValue.translateCorrelationsAndSimplify(translationMap));
                    }
                } else {
                    innerDecorrelatedLocalValuesBuilder.add(innerValue);
                }
            }

            final var resultValue = flatMapPlan.getResultValue();
            final var decorrelatedResultValuesBuilder = ImmutableList.<Value>builder();
            for (final var outerResultValue : outerDerivations.getResultValues()) {
                for (final var innerResultValue : innerDerivations.getResultValues()) {
                    final var translationMap = TranslationMap.builder()
                            .when(outerQuantifier.getAlias()).then((sourceAlias, leafValue) -> outerResultValue)
                            .build();
                    final var innerDecorrelatedValue = innerResultValue.translateCorrelationsAndSimplify(translationMap);

                    final var resultsTranslationMap = TranslationMap.builder()
                            .when(outerQuantifier.getAlias()).then((sourceAlias, leafValue) -> outerResultValue)
                            .when(innerQuantifier.getAlias()).then((sourceAlias, leafValue) -> innerDecorrelatedValue)
                            .build();
                    final var decorrelatedResultsValue = resultValue.translateCorrelationsAndSimplify(resultsTranslationMap);
                    decorrelatedResultValuesBuilder.add(decorrelatedResultsValue);

                    if (!resultValue.isCorrelatedTo(innerQuantifier.getAlias())) {
                        // would just lead to duplicated Values
                        break;
                    }
                }

                if (!resultValue.isCorrelatedTo(outerQuantifier.getAlias())) {
                    // would just lead to duplicated Values
                    break;
                }
            }

            final var decorrelatedResultValues = decorrelatedResultValuesBuilder.build();

            return new Derivations(decorrelatedResultValues,
                    ImmutableList.<Value>builder()
                            .addAll(outerDerivations.getLocalValues())
                            .addAll(innerDecorrelatedLocalValuesBuilder.build())
                            .addAll(decorrelatedResultValues)
                            .build());
        }

        @Nonnull
        @Override
        public Derivations visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan streamingAggregationPlan) {
            //
            // get the result value and translate the groupings and aggregations into it
            //
            final var resultValue = streamingAggregationPlan.getResultValue();
            final var resultTranslationMap = TranslationMap.builder();

            final var groupingValue = streamingAggregationPlan.getGroupingValue();
            if (groupingValue != null) {
                resultTranslationMap
                        .when(streamingAggregationPlan.getGroupingKeyAlias())
                        .then((sourceAlias, leafValue) -> streamingAggregationPlan.getGroupingValue());
            }

            resultTranslationMap.when(streamingAggregationPlan.getAggregateAlias())
                    .then((sourceAlias, leafValue) -> streamingAggregationPlan.getAggregateValue());

            final var expandedResultValue = resultValue.translateCorrelationsAndSimplify(resultTranslationMap.build());
            final var childDerivations = derivationsFromSingleChild(streamingAggregationPlan);
            final var innerQuantifier = streamingAggregationPlan.getInner();
            final var decorrelatedResultValuesBuilder = ImmutableList.<Value>builder();
            for (final var childResultValue : childDerivations.getResultValues()) {
                final var translationMap = TranslationMap.builder()
                        .when(innerQuantifier.getAlias()).then((sourceAlias, leafValue) -> childResultValue)
                        .build();
                final var decorrelatedExpandedResultValue = expandedResultValue.translateCorrelationsAndSimplify(translationMap);
                decorrelatedResultValuesBuilder.add(decorrelatedExpandedResultValue);
            }

            final var decorrelatedResultValues = decorrelatedResultValuesBuilder.build();

            return new Derivations(decorrelatedResultValues,
                    ImmutableList.<Value>builder()
                            .addAll(decorrelatedResultValues)
                            .addAll(childDerivations.getLocalValues())
                            .build());
        }

        @Nonnull
        @Override
        public Derivations visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return visitSetPlan(unionOnValuesPlan);
        }

        @Nonnull
        private Derivations visitSetPlan(@Nonnull final RecordQuerySetPlan setPlan) {
            Verify.verify(!(setPlan instanceof RecordQueryInUnionPlan)); // dealt with by specific logic
            final var resultValuesBuilder = ImmutableList.<Value>builder();
            final var localValuesBuilder = ImmutableList.<Value>builder();

            for (final var quantifier : setPlan.getQuantifiers()) {
                final var childDerivations = derivationsFromQuantifier(quantifier);
                resultValuesBuilder.addAll(childDerivations.getResultValues());
                localValuesBuilder.addAll(childDerivations.getLocalValues());
            }

            final var resultValues = resultValuesBuilder.build();

            if (setPlan instanceof RecordQueryPlanWithComparisonKeyValues) {
                for (final var comparisonKeyValue : ((RecordQueryPlanWithComparisonKeyValues)setPlan).getComparisonKeyValues()) {
                    for (final var resultValue : resultValues) {
                        final var translationMap = TranslationMap.builder()
                                .when(Quantifier.current()).then((sourceAlias, leafValue) -> resultValue)
                                .build();
                        localValuesBuilder.add(comparisonKeyValue.translateCorrelationsAndSimplify(translationMap));
                    }
                }
            }

            return new Derivations(resultValues, localValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return visitSetPlan(unorderedUnionPlan);
        }

        @Nonnull
        @Override
        public Derivations visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
            return visitPlanWithComparisons(scanPlan, Objects.requireNonNull(scanPlan.getRecordTypes()));
        }

        @Nonnull
        @Override
        public Derivations visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuePlan) {
            final var outerAliases = inUnionOnValuePlan.getInSources()
                    .stream()
                    .map(inUnionOnValuePlan::getInAlias)
                    .collect(ImmutableList.toImmutableList());
            final var innerQuantifier = inUnionOnValuePlan.getInner();
            final var innerDerivations = derivationsFromQuantifier(innerQuantifier);

            //
            // De-correlate inner against in-source. Since the in-source is not really a value, we can only
            // fake it. We just need to remove the correlation.
            //
            final var innerDecorrelatedLocalValuesBuilder = ImmutableList.<Value>builder();
            for (final var innerValue : innerDerivations.getLocalValues()) {
                final var innerCorrelatedTo = innerValue.getCorrelatedTo();
                if (outerAliases.stream().anyMatch(innerCorrelatedTo::contains)) {
                    var translationMap = TranslationMap.builder()
                            .whenAny(outerAliases)
                            .then(((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType())))
                            .build();
                    innerDecorrelatedLocalValuesBuilder.add(innerValue.translateCorrelationsAndSimplify(translationMap));
                } else {
                    innerDecorrelatedLocalValuesBuilder.add(innerValue);
                }
            }

            final var innerDecorrelatedResultValuesBuilder = ImmutableList.<Value>builder();
            for (final var innerValue : innerDerivations.getResultValues()) {
                final var innerCorrelatedTo = innerValue.getCorrelatedTo();
                if (outerAliases.stream().anyMatch(innerCorrelatedTo::contains)) {
                    final var translationMap = TranslationMap.builder()
                            .whenAny(outerAliases)
                            .then(((sourceAlias, leafValue) -> new QueriedValue(leafValue.getResultType())))
                            .build();
                    innerDecorrelatedResultValuesBuilder.add(innerValue.translateCorrelationsAndSimplify(translationMap));
                } else {
                    innerDecorrelatedResultValuesBuilder.add(innerValue);
                }
            }

            final var innerDecorrelatedResultValues = innerDecorrelatedResultValuesBuilder.build();

            for (final var comparisonKeyValue : inUnionOnValuePlan.getComparisonKeyValues()) {
                for (final var resultValue : innerDecorrelatedResultValues) {
                    final var translationMap = TranslationMap.builder()
                            .when(Quantifier.current()).then((sourceAlias, leafValue) -> resultValue)
                            .build();
                    innerDecorrelatedLocalValuesBuilder.add(comparisonKeyValue.translateCorrelationsAndSimplify(translationMap));
                }
            }

            return new Derivations(innerDecorrelatedResultValues, innerDecorrelatedLocalValuesBuilder.build());
        }

        @Nonnull
        @Override
        public Derivations visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        @Override
        public Derivations visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
            return derivationsFromSingleChild(damPlan);
        }

        @Nonnull
        @Override
        public Derivations visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
            return derivationsFromSingleChild(sortPlan);
        }

        @Nonnull
        @Override
        public Derivations visitDefault(@Nonnull final RecordQueryPlan element) {
            throw new RecordCoreException("unsupported plan operator");
        }

        @Nonnull
        private Derivations derivationsFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return derivationsFromQuantifier(Iterables.getOnlyElement(quantifiers));
            }
            throw new RecordCoreException("cannot derive derivations for more than one quantifier");
        }

        @Nonnull
        private Derivations derivationsFromQuantifier(@Nonnull final Quantifier quantifier) {
            return evaluateForReference(quantifier.getRangesOver());
        }

        @Nonnull
        private Derivations evaluateForReference(@Nonnull Reference reference) {
            final RelationalExpression expression = reference.get();
            return visit((RecordQueryPlan)expression);
        }
    }

    @Nonnull
    public static Derivations evaluateDerivations(@Nonnull RecordQueryPlan recordQueryPlan) {
        return DERIVATIONS.createVisitor().visit(recordQueryPlan);
    }

    /**
     * Cases class to capture the derivations that are being collected by the visitor.
     */
    @SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR", justification = "False positive as this is not a singleton class")
    public static class Derivations {
        private static final Derivations EMPTY = new Derivations(ImmutableList.of(), ImmutableList.of());

        @Nonnull
        private final List<Value> resultValues;
        @Nonnull
        private final List<Value> localValues;

        public Derivations(final List<Value> resultValues, final List<Value> localValues) {
            this.resultValues = ImmutableList.copyOf(resultValues);
            this.localValues = ImmutableList.copyOf(localValues);
        }

        @Nonnull
        public List<Value> getResultValues() {
            return resultValues;
        }

        @Nonnull
        public List<Value> getLocalValues() {
            return localValues;
        }

        @Nonnull
        public List<Value> simplifyLocalValues() {
            final var simplifiedLocalValuesBuilder = ImmutableList.<Value>builder();
            for (final var localValue : getLocalValues()) {
                final var aliasMap = AliasMap.emptyMap();
                simplifiedLocalValuesBuilder.add(localValue.simplify(aliasMap, ImmutableSet.of()));
            }
            return simplifiedLocalValuesBuilder.build();
        }

        @Nonnull
        public static Derivations empty() {
            return EMPTY;
        }
    }
}
