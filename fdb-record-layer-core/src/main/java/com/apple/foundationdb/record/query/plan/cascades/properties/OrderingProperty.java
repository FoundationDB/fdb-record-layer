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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.KeyPart;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexLikeMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
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
        public Ordering visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
            final var childOrdering = orderingFromSingleChild(predicatesFilterPlan);

            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundFileKeyExpressions =
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
                                    return Stream.of();
                                }
                                return Stream.of(Pair.of((FieldValue)valuePredicate.getValue(), valuePredicate.getComparison()));
                            })
                            .map(valueComparisonPair -> {

                                final var fieldValue = valueComparisonPair.getLeft();
                                final String fieldName = fieldValue.getFieldName();
                                KeyExpression keyExpression =
                                        Key.Expressions.field(fieldName);
                                final List<String> fieldPrefix = fieldValue.getFieldPrefix();
                                for (int i = fieldPrefix.size() - 1; i >= 0; i --) {
                                    keyExpression = Key.Expressions.field(fieldPrefix.get(i)).nest(keyExpression);
                                }
                                return Pair.of(keyExpression, valueComparisonPair.getRight());
                            })
                            .collect(ImmutableSetMultimap.toImmutableSetMultimap(Pair::getLeft, Pair::getRight));

            final var resultOrderingKeyParts =
                    childOrdering.getOrderingKeyParts()
                            .stream()
                            .filter(keyPart -> !equalityBoundFileKeyExpressions.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap =
                    HashMultimap.create(childOrdering.getEqualityBoundKeyMap());

            equalityBoundFileKeyExpressions.forEach(resultEqualityBoundKeyMap::put);

            return new Ordering(resultEqualityBoundKeyMap, resultOrderingKeyParts, childOrdering.isDistinct());
        }

        @Nonnull
        @Override
        public Ordering visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
            return visitInJoinPlan(inValuesJoinPlan);
        }

        @Nonnull
        @Override
        public Ordering visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
            return visit(coveringIndexPlan.getIndexPlan());
        }

        @Nonnull
        @Override
        public Ordering visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionPlan) {
            final var orderings = orderingsFromChildren(intersectionPlan);
            final var requestedOrdering = requestedOrderingFromComparisonKey(intersectionPlan.getComparisonKeyExpression(), intersectionPlan.isReverse());
            final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                    Ordering.combineEqualityBoundKeys(orderings, Ordering::unionEqualityBoundKeys);
            if (commonEqualityBoundKeysMapOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }
            final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

            final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderings, requestedOrdering);
            if (commonOrderingKeysOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }

            final var commonOrderingKeys =
                    commonOrderingKeysOptional.get()
                            .stream()
                            .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            final boolean allAreDistinct =
                    orderings.stream()
                            .anyMatch(Ordering::isDistinct);

            return new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, allAreDistinct);
        }

        @Nonnull
        @Override
        public Ordering visitMapPlan(@Nonnull final RecordQueryMapPlan element) {
            return orderingFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Ordering visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return orderingFromSingleChild(unorderedDistinctPlan);
        }

        @Nonnull
        @Override
        public Ordering visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitExplodePlan(@Nonnull final RecordQueryExplodePlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitIntersectionOnValuePlan(@Nonnull final RecordQueryIntersectionOnValuePlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
            final var scanComparisons = indexPlan.getComparisons();
            return indexPlan.getMatchCandidateOptional()
                    .map(matchCandidate -> matchCandidate.computeOrderingFromScanComparisons(scanComparisons, indexPlan.isReverse(), indexPlan.isStrictlySorted()))
                    .orElse(Ordering.emptyOrder());
        }

        @Nonnull
        @Override
        public Ordering visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan element) {
            // TODO This plan is sorted by anything it's flowing as its max cardinality is one.
            //      We cannot express that as of yet.
            return Ordering.emptyOrder();
        }

        @Nonnull
        @SuppressWarnings("java:S135")
        public Ordering visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
            final var childOrdering = orderingFromSingleChild(inJoinPlan);
            final var equalityBoundKeyMap = childOrdering.getEqualityBoundKeyMap();
            final var inSource = inJoinPlan.getInSource();
            final var inAlias = inJoinPlan.getInAlias();

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap =
                    HashMultimap.create(equalityBoundKeyMap);
            KeyExpression inKeyExpression = null;
            for (final var entry : equalityBoundKeyMap.entries()) {
                // TODO we only look for the first entry that matches. That is enough for the in-to-join case,
                //      however, it is possible that more than one different key expressions are equality-bound
                //      by this in. That would constitute to more than one concurrent order which we cannot
                //      express at the moment (we need the PartialOrder approach for that).
                final var comparison = entry.getValue();
                final var correlatedTo = comparison.getCorrelatedTo();
                if (correlatedTo.size() != 1) {
                    continue;
                }

                if (inAlias.equals(Iterables.getOnlyElement(correlatedTo))) {
                    inKeyExpression = entry.getKey();
                    resultEqualityBoundKeyMap.removeAll(inKeyExpression);
                    break;
                }
            }

            if (inKeyExpression == null || !inSource.isSorted()) {
                //
                // This can only really happen if the inSource is not sorted.
                // We can only propagate equality-bound information. Everything related to order and
                // distinctness is lost.
                //
                return new Ordering(resultEqualityBoundKeyMap, ImmutableList.of(), false);
            }

            //
            // Prepend the existing order with the key expression we just found.
            //
            final var resultOrderingKeyPartsBuilder = ImmutableList.<KeyPart>builder();
            resultOrderingKeyPartsBuilder.add(KeyPart.of(inKeyExpression, inSource.isReverse()));
            resultOrderingKeyPartsBuilder.addAll(childOrdering.getOrderingKeyParts());

            return new Ordering(resultEqualityBoundKeyMap,
                    resultOrderingKeyPartsBuilder.build(),
                    childOrdering.isDistinct());
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
            return deriveForUnionFromOrderings(
                    orderingsFromChildren(unionOnKeyExpressionPlan),
                    requestedOrderingFromComparisonKey(unionOnKeyExpressionPlan.getComparisonKeyExpression(), unionOnKeyExpressionPlan.isReverse()),
                    Ordering::intersectEqualityBoundKeys);
        }

        @Nonnull
        @Override
        public Ordering visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan element) {
            return orderingFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Ordering visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan element) {
            return orderingFromSingleChild(element);
        }

        @Nonnull
        @Override
        public Ordering visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
            final var childOrdering = orderingFromSingleChild(inUnionOnKeyExpressionPlan);
            final var equalityBoundKeyMap = childOrdering.getEqualityBoundKeyMap();
            final var comparisonKey = inUnionOnKeyExpressionPlan.getComparisonKeyExpression();

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap = HashMultimap.create(equalityBoundKeyMap);
            final var resultKeyPartBuilder = ImmutableList.<KeyPart>builder();
            final List<KeyExpression> normalizedComparisonKeys = comparisonKey.normalizeKeyForPositions();
            for (final var normalizedKeyExpression : normalizedComparisonKeys) {
                resultKeyPartBuilder.add(KeyPart.of(normalizedKeyExpression, inUnionOnKeyExpressionPlan.isReverse()));
            }

            final var sourceAliases =
                    inUnionOnKeyExpressionPlan.getInSources()
                            .stream()
                            .map(inSource -> CorrelationIdentifier.of(CORRELATION.identifier(inSource.getBindingName())))
                            .collect(ImmutableSet.toImmutableSet());

            for (final var entry : equalityBoundKeyMap.entries()) {
                final var correlatedTo = entry.getValue().getCorrelatedTo();

                if (correlatedTo.stream().anyMatch(sourceAliases::contains)) {
                    resultEqualityBoundKeyMap.removeAll(entry.getKey());
                }
            }

            return new Ordering(resultEqualityBoundKeyMap, resultKeyPartBuilder.build(), childOrdering.isDistinct());
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

            final var outerCardinalities = CardinalitiesProperty.evaluate(flatMapPlan.getOuterQuantifier());
            var maxCardinality = outerCardinalities.getMaxCardinality();
            if (!maxCardinality.isUnknown() && maxCardinality.getCardinality() == 1L) {
                return innerOrdering;
            }

            final var innerCardinalities = CardinalitiesProperty.evaluate(flatMapPlan.getInnerQuantifier());
            maxCardinality = innerCardinalities.getMaxCardinality();
            if (!maxCardinality.isUnknown() && maxCardinality.getCardinality() == 1L) {
                return outerOrdering;
            }
            
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitUnionOnValuePlan(@Nonnull final RecordQueryUnionOnValuePlan element) {
            // TODO strictly speaking not true but we don't have the vocabulary (yet) to express this ordering
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
            return deriveForUnionFromOrderings(orderingsFromChildren(unorderedUnionPlan),
                    RequestedOrdering.preserve(),
                    Ordering::intersectEqualityBoundKeys);
        }

        @Nonnull
        @Override
        public Ordering visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
            final var commonPrimaryKey = scanPlan.getCommonPrimaryKey();
            if (commonPrimaryKey == null) {
                return Ordering.emptyOrder();
            }
            return ValueIndexLikeMatchCandidate.computeOrderingFromKeyAndScanComparisons(commonPrimaryKey,
                    scanPlan.getComparisons(),
                    scanPlan.isReverse(),
                    false);
        }

        @Nonnull
        @Override
        public Ordering visitInUnionOnValuePlan(@Nonnull final RecordQueryInUnionOnValuePlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
            // TODO
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitSortPlan(@Nonnull final RecordQuerySortPlan element) {
            // TODO
            return Ordering.emptyOrder();
        }

        @Nonnull
        @Override
        public Ordering visitDefault(@Nonnull final RecordQueryPlan element) {
            return Ordering.emptyOrder();
        }

        @Nonnull
        private RequestedOrdering requestedOrderingFromComparisonKey(@Nonnull final KeyExpression comparisonKey, final boolean isReverse) {
            return new RequestedOrdering(
                    comparisonKey
                            .normalizeKeyForPositions()
                            .stream()
                            .map(normalizedKeyExpression -> KeyPart.of(normalizedKeyExpression, isReverse))
                            .collect(Collectors.toList()),
                    RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);
        }

        @Nonnull
        private Ordering orderingFromSingleChild(@Nonnull final RelationalExpression expression) {
            final var quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return evaluateForReference(Iterables.getOnlyElement(quantifiers).getRangesOver());
            }
            return Ordering.emptyOrder();
        }

        @Nonnull
        private List<Ordering> orderingsFromChildren(@Nonnull final RelationalExpression expression) {
            return expression.getQuantifiers()
                    .stream()
                    .map(quantifier -> {
                        if (quantifier instanceof Quantifier.Existential) {
                            return Ordering.emptyOrder();
                        }
                        return evaluateForReference(quantifier.getRangesOver());
                    })
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        private Ordering evaluateForReference(@Nonnull ExpressionRef<? extends RelationalExpression> reference) {
            final var memberOrderings =
                    reference.getPlannerAttributeForMembers(ORDERING).values();

            final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                    Ordering.combineEqualityBoundKeys(memberOrderings, Ordering::intersectEqualityBoundKeys);
            if (commonEqualityBoundKeysMapOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }
            final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

            final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(memberOrderings, RequestedOrdering.preserve());
            if (commonOrderingKeysOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }

            final var commonOrderingKeys =
                    commonOrderingKeysOptional.get()
                            .stream()
                            .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            final var allAreDistinct =
                    memberOrderings
                            .stream()
                            .allMatch(Ordering::isDistinct);

            return new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, allAreDistinct);
        }

        public static Ordering deriveForUnionFromOrderings(@Nonnull final List<Ordering> orderings,
                                                           @Nonnull final RequestedOrdering requestedOrdering,
                                                           @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparisons.Comparison>> combineFn) {
            final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                    Ordering.combineEqualityBoundKeys(orderings, combineFn);
            if (commonEqualityBoundKeysMapOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }
            final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

            final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderings, requestedOrdering);
            if (commonOrderingKeysOptional.isEmpty()) {
                return Ordering.emptyOrder();
            }

            final var commonOrderingKeys =
                    commonOrderingKeysOptional.get()
                            .stream()
                            .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            return new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, false);
        }

        public static Ordering evaluate(@Nonnull RecordQueryPlan recordQueryPlan) {
            return new OrderingVisitor().visit(recordQueryPlan);
        }
    }
}
