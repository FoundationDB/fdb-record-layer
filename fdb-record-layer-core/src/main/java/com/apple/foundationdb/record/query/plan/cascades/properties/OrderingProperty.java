/*
 * OrderingProperty.java
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
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
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
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;

/**
 * A property used for the ordering(s) of a plan.
 */
public class OrderingProperty implements PlanProperty<Ordering> {
    public static final PlanProperty<Ordering> ORDERING = new OrderingProperty();

    @Nonnull
    @Override
    public RecordQueryPlanVisitor<Ordering> createVisitor() {
        return new OrderingVisitor();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * A property that determines the ordering of a {@link RecordQueryPlan}.
     */
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("java:S3776")
    public static class OrderingVisitor implements RecordQueryPlanVisitor<Ordering> {
        @Nonnull
        @Override
        public Ordering visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            final var childOrdering = orderingFromSingleChild(predicatesFilterPlan);

            final SetMultimap<Value, Comparisons.Comparison> equalityBoundValuesMap =
                    predicatesFilterPlan.getPredicates()
                            .stream()
                            .flatMap(queryPredicate -> {
                                if (!(queryPredicate instanceof ValuePredicate)) {
                                    return Stream.empty();
                                }
                                final var valuePredicate = (ValuePredicate)queryPredicate;
                                if (!valuePredicate.getComparison().getType().isEquality()) {
                                    return Stream.empty();
                                }

                                if (!(valuePredicate.getValue() instanceof FieldValue)) {
                                    return Stream.empty();
                                }

                                final var fieldValue = (FieldValue)valuePredicate.getValue();
                                if (fieldValue.getFieldPathNamesMaybe()
                                        .stream()
                                        .anyMatch(Optional::isEmpty)) {
                                    return Stream.empty();
                                }

                                // filter out field values that are correlated to some other alias as well
                                final var fieldValueCorrelatedTo = fieldValue.getCorrelatedTo();
                                final var innerAlias = predicatesFilterPlan.getInner().getAlias();
                                if (fieldValueCorrelatedTo.size() != 1 ||
                                        !Iterables.getOnlyElement(fieldValueCorrelatedTo).equals(innerAlias)) {
                                    return Stream.empty();
                                }

                                // filter out comparisons that are not really equality binding like x = f(x)
                                if (valuePredicate.getComparison().getCorrelatedTo().contains(innerAlias)) {
                                    return Stream.empty();
                                }

                                final var translationMap = AliasMap.ofAliases(innerAlias, Quantifier.current());
                                return Stream.of(Pair.of(fieldValue.rebase(translationMap), valuePredicate.getComparison()));
                            })
                            .collect(ImmutableSetMultimap.toImmutableSetMultimap(Pair::getLeft, Pair::getRight));

            // We can create a new ordering set by adding the equality-bound values to the ordering set domain no matter what.
            final var childOrderingSet = childOrdering.getOrderingSet();
            final var childBindingMap = childOrdering.getBindingMap();
            final var resultOrderingSetDomain =
                    Sets.union(childOrderingSet.getSet(), equalityBoundValuesMap.keySet());

            final var resultBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
            for (final var value : resultOrderingSetDomain) {
                if (equalityBoundValuesMap.containsKey(value)) {
                    equalityBoundValuesMap.get(value)
                            .stream()
                            .map(Binding::fixed)
                            .forEach(binding -> resultBindingMapBuilder.put(value, binding));
                } else {
                    resultBindingMapBuilder.putAll(value, childBindingMap.get(value));
                }
            }
            final var resultBindingMap = resultBindingMapBuilder.build();

            //
            // Create ordering set based on information compiled above; note that the ordering set needs to
            // be normalized as there are dependencies in the original dependency map that need to be removed due to
            // newly-introduced equalities.
            //
            final var orderingSet =
                    Ordering.normalizeOrderingSet(resultBindingMap,
                            PartiallyOrderedSet.of(resultOrderingSetDomain, childOrderingSet.getDependencyMap()));
            return Ordering.ofOrderingSet(resultBindingMap, orderingSet, childOrdering.isDistinct());
        }

        @Nonnull
        @Override
        public Ordering visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Ordering visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
            return visitInJoinPlan(inComparandJoinPlan);
        }

        @Nonnull
        @Override
        public Ordering visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan aggregateIndexPlan) {
            return visit(aggregateIndexPlan.getIndexPlan());
        }

        @Nonnull
        @Override
        public Ordering visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            return visit(coveringIndexPlan.getIndexPlan());
        }

        @Nonnull
        @Override
        public Ordering visitDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
            return orderingFromSingleChild(deletePlan);
        }

        @Nonnull
        @Override
        public Ordering visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionPlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
            final var childOrdering = orderingFromSingleChild(mapPlan);
            final var resultValue = mapPlan.getResultValue();

            return childOrdering.pullUp(resultValue, AliasMap.ofAliases(mapPlan.getInner().getAlias(), Quantifier.current()), mapPlan.getCorrelatedTo());
        }

        @Nonnull
        @Override
        public Ordering visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return orderingFromSingleChild(unorderedDistinctPlan);
        }

        @Nonnull
        @Override
        public Ordering visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitRangePlan(@Nonnull final RecordQueryRangePlan element) {
            final var resultValue = ObjectValue.of(Quantifier.current(), Type.primitiveType(Type.TypeCode.INT));
            return Ordering.ofOrderingSet(ImmutableSetMultimap.of(resultValue, Binding.sorted(ProvidedSortOrder.ASCENDING)),
                    PartiallyOrderedSet.of(
                            ImmutableSet.of(resultValue),
                            ImmutableSetMultimap.of()), true);
        }

        @Nonnull
        @Override
        public Ordering visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuePlan) {
            final var orderings = orderingsFromChildren(intersectionOnValuePlan);
            return deriveForDistinctSetOperationFromOrderings(orderings, intersectionOnValuePlan.getComparisonKeyValues(),
                    intersectionOnValuePlan.isReverse(), Ordering.INTERSECTION);
        }

        @Nonnull
        @Override
        public Ordering visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
            final var scanComparisons = indexPlan.getScanComparisons();
            return indexPlan.getMatchCandidateMaybe()
                    .map(matchCandidate -> matchCandidate.computeOrderingFromScanComparisons(scanComparisons, indexPlan.isReverse(), indexPlan.isStrictlySorted()))
                    .orElse(Ordering.empty());
        }

        @Nonnull
        @Override
        public Ordering visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            // TODO This plan is sorted by anything it's flowing as its max cardinality is one.
            //      We cannot express that as of yet.
            return Ordering.empty();
        }

        @Nonnull
        @SuppressWarnings("java:S135")
        public Ordering visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            final var innerOrdering = orderingFromSingleChild(inJoinPlan);
            final var bindingMap = innerOrdering.getBindingMap();
            final var inSource = inJoinPlan.getInSource();
            final var inAlias = inJoinPlan.getInAlias();

            final Value inValue = findValueForIn(bindingMap, inAlias);

            final SetMultimap<Value, Binding> resultBindingMap;
            if (inValue != null && inSource.isSorted()) {
                final var resultBindingMapBuilder =
                        ImmutableSetMultimap.<Value, Binding>builder();
                for (final var entry : bindingMap.entries()) {
                    final var value = entry.getKey();
                    final var binding = entry.getValue();

                    if (!binding.isFixed() || !value.equals(inValue)) {
                        resultBindingMapBuilder.put(value, binding);
                    } else {
                        resultBindingMapBuilder.put(value, Binding.sorted(inSource.isReverse()));
                    }
                }
                resultBindingMap = resultBindingMapBuilder.build();
            } else {
                // inherit fixed bindings
                final var resultBindingMapBuilder =
                        ImmutableSetMultimap.<Value, Binding>builder();
                for (final var entry : bindingMap.entries()) {
                    final var value = entry.getKey();
                    final var binding = entry.getValue();

                    if (binding.isFixed() && !value.equals(inValue)) {
                        resultBindingMapBuilder.put(value, binding);
                    }
                }
                resultBindingMap = resultBindingMapBuilder.build();
            }

            if (inValue == null || !inSource.isSorted()) {
                //
                // This can only really happen if the inSource is not sorted.
                // We can only propagate equality-bound information. Everything related to order and
                // distinctness is lost.
                //
                return Ordering.ofOrderingSet(resultBindingMap,
                        PartiallyOrderedSet.of(resultBindingMap.keySet(), ImmutableSetMultimap.of()), false);
            }

            final var outerOrderingSet =
                    PartiallyOrderedSet.<Value>builder()
                            .add(inValue)
                            .build();
            final var outerOrdering =
                    Ordering.ofOrderingSet(ImmutableSetMultimap.of(inValue, Binding.sorted(inSource.isReverse())), outerOrderingSet, true);

            final var filteredInnerOrderingSet =
                    innerOrdering.getOrderingSet()
                            .filterElements(value -> innerOrdering.isSingularDirectionalValue(value) || !inValue.equals(value));
            final var filteredInnerOrdering =
                    Ordering.ofOrderingSet(resultBindingMap, filteredInnerOrderingSet, innerOrdering.isDistinct());

            //
            // Note, that while we could potentially pull up the concatenated ordering along the result value of the
            // in-join plan, the ordering would stay identical as we only pull up along a simple QOV over the
            // inner quantifier.
            //
            return Ordering.concatOrderings(outerOrdering, filteredInnerOrdering);
        }

        @Nullable
        private static Value findValueForIn(final SetMultimap<Value, Binding> bindingMap, final CorrelationIdentifier inAlias) {
            Value inValue = null;
            for (final var entry : bindingMap.entries()) {
                final var binding = entry.getValue();
                if (binding.isFixed()) {
                    final var comparison = binding.getComparison();
                    final var correlatedTo = comparison.getCorrelatedTo();
                    if (correlatedTo.size() != 1) {
                        continue;
                    }

                    if (inAlias.equals(Iterables.getOnlyElement(correlatedTo))) {
                        inValue = entry.getKey();
                        break;
                    }
                }
            }
            return inValue;
        }

        @Nonnull
        @Override
        public Ordering visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
            return orderingFromSingleChild(filterPlan);
        }

        @Nonnull
        @Override
        public Ordering visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
            return orderingFromSingleChild(unorderedPrimaryKeyDistinctPlan);
        }

        @Nonnull
        @Override
        public Ordering visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return orderingFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Ordering visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
            return orderingFromSingleChild(typeFilterPlan);
        }

        @Nonnull
        @Override
        public Ordering visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
            return visitInJoinPlan(inParameterJoinPlan);
        }

        @Nonnull
        @Override
        public Ordering visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
            //
            // For now, we special-case where we can find exactly one _regular_ quantifier and only
            // another quantifier with a max cardinality of 1.
            //
            final var orderingsFromChildren = orderingsFromChildren(flatMapPlan);
            final var outerOrdering = orderingsFromChildren.get(0);
            final var innerOrdering = orderingsFromChildren.get(1);

            final var correlatedTo = flatMapPlan.getCorrelatedTo();
            final var resultValue = flatMapPlan.getResultValue();

            final var outerCardinalities = CardinalitiesProperty.evaluate(flatMapPlan.getOuterQuantifier());
            final var outerMaxCardinality = outerCardinalities.getMaxCardinality();
            if (!outerMaxCardinality.isUnknown() && outerMaxCardinality.getCardinality() == 1L) {
                // outer max cardinality is proven to be 1 row
                return innerOrdering.pullUp(resultValue, AliasMap.ofAliases(flatMapPlan.getInnerQuantifier().getAlias(), Quantifier.current()), correlatedTo);
            }

            if (!outerOrdering.isDistinct()) {
                // outer ordering is not distinct
                return outerOrdering.pullUp(resultValue, AliasMap.ofAliases(flatMapPlan.getInnerQuantifier().getAlias(), Quantifier.current()), correlatedTo);
            }

            //
            // Outer ordering is distinct and the inner max cardinality is not proven to be 1L.
            //
            return Ordering.concatOrderings(outerOrdering, innerOrdering);
        }

        @Nonnull
        @Override
        public Ordering visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan streamingAggregationPlan) {
            final var childOrdering = orderingFromSingleChild(streamingAggregationPlan);

            //
            // Note that the ordering we see here is satisfying the ordering required by the aggregation plan. In fact,
            // the ordering is a permutation of the group by columns among many. In order to find out the output ordering,
            // we plug in the childOrdering and pull it through the complete result value of the streaming aggregation plan.
            //
            final var groupingValue = streamingAggregationPlan.getGroupingValue();

            if (groupingValue == null) {
                // TODO To be reconsidered. It should be an ordering that is ordered by anything as the result
                //      has a maximum cardinality of 1L.
                return Ordering.empty();
            }

            final var groupingKeyAlias = streamingAggregationPlan.getGroupingKeyAlias();

            final var completeResultValue = streamingAggregationPlan.getCompleteResultValue();

            //
            // Substitute the grouping key value everywhere the ObjectValue of the grouping key alias is used.
            //
            final var composedCompleteResultValueOptional = completeResultValue.replaceLeavesMaybe(value -> {
                if (value instanceof ObjectValue && ((ObjectValue)value).getAlias().equals(groupingKeyAlias)) {
                    return groupingValue;
                }
                return value;
            });

            if (composedCompleteResultValueOptional.isEmpty()) {
                return Ordering.empty();
            }

            final var composedCompleteResultValue = composedCompleteResultValueOptional.get();

            return childOrdering.pullUp(composedCompleteResultValue, AliasMap.ofAliases(streamingAggregationPlan.getInner().getAlias(), Quantifier.current()), streamingAggregationPlan.getCorrelatedTo());
        }

        @Nonnull
        @Override
        public Ordering visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
            return deriveForDistinctSetOperationFromOrderings(
                    orderingsFromChildren(unionOnValuesPlan),
                    unionOnValuesPlan.getComparisonKeyValues(),
                    unionOnValuesPlan.isReverse(),
                    Ordering.UNION);
        }

        @Nonnull
        @Override
        public Ordering visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
            final var primaryMatchCandidate = scanPlan.getMatchCandidateMaybe();
            return primaryMatchCandidate.map(withPrimaryKeyMatchCandidate ->
                            withPrimaryKeyMatchCandidate
                                    .computeOrderingFromScanComparisons(
                                            scanPlan.getScanComparisons(),
                                            scanPlan.isReverse(),
                                            false))
                    .orElseGet(Ordering::empty);
        }

        @Nonnull
        @Override
        public Ordering visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuePlan) {
            final var childOrdering = orderingFromSingleChild(inUnionOnValuePlan);
            final var bindingMap = childOrdering.getBindingMap();
            final var comparisonKeyValues = inUnionOnValuePlan.getComparisonKeyValues();
            final var resultBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();

            final var sourceAliases =
                    inUnionOnValuePlan.getInSources()
                            .stream()
                            .map(inSource -> CorrelationIdentifier.of(CORRELATION.identifier(inSource.getBindingName())))
                            .collect(ImmutableSet.toImmutableSet());

            for (final var entry : bindingMap.entries()) {
                final var value = entry.getKey();
                final var binding = entry.getValue();
                if (binding.isFixed()) {
                    final var correlatedTo = value.getCorrelatedTo();

                    if (correlatedTo.stream().anyMatch(sourceAliases::contains)) {
                        resultBindingMapBuilder.putAll(value, Binding.ascending());
                    } else {
                        resultBindingMapBuilder.putAll(value, binding);
                    }
                } else {
                    resultBindingMapBuilder.putAll(value, binding);
                }
            }

            return Ordering.ofOrderingSequence(resultBindingMapBuilder.build(), comparisonKeyValues, childOrdering.isDistinct());
        }

        @Nonnull
        @Override
        public Ordering visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            // TODO
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
            return orderingFromSingleChild(damPlan);
        }

        @Nonnull
        @Override
        public Ordering visitSortPlan(@Nonnull final RecordQuerySortPlan element) {
            // TODO
            return Ordering.empty();
        }

        @Nonnull
        @Override
        public Ordering visitDefault(@Nonnull final RecordQueryPlan element) {
            return Ordering.empty();
        }

        @Nonnull
        private Ordering orderingFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return evaluateForReference(Iterables.getOnlyElement(quantifiers).getRangesOver());
            }
            return Ordering.empty();
        }

        @Nonnull
        private List<Ordering> orderingsFromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> {
                        if (quantifier instanceof Quantifier.Existential) {
                            return Ordering.empty();
                        }
                        return evaluateForReference(quantifier.getRangesOver());
                    })
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        private Ordering evaluateForReference(@Nonnull Reference reference) {
            final var memberOrderings =
                    reference.getPlannerAttributeForMembers(ORDERING).values();
            final var allAreDistinct =
                    memberOrderings
                            .stream()
                            .allMatch(Ordering::isDistinct);

            return Ordering.merge(memberOrderings, Ordering.UNION, (left, right) -> allAreDistinct);
        }

        public static <O extends Ordering.SetOperationsOrdering> Ordering deriveForDistinctSetOperationFromOrderings(@Nonnull final List<Ordering> orderings,
                                                                                                                     @Nonnull final List<? extends Value> comparisonKeyValues,
                                                                                                                     final boolean isReverse,
                                                                                                                     @Nonnull final Ordering.MergeOperator<O> mergeOperator) {
            final var mergedOrdering = Ordering.merge(orderings, mergeOperator, (left, right) -> true);
            return mergedOrdering.applyComparisonKey(comparisonKeyValues,
                    Ordering.sortedBindingsForValues(comparisonKeyValues, ProvidedSortOrder.fromIsReverse(isReverse)));
        }

        public static Ordering evaluate(@Nonnull RecordQueryPlan recordQueryPlan) {
            return new OrderingVisitor().visit(recordQueryPlan);
        }
    }
}
