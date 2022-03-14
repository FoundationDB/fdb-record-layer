/*
 * OrderingProperty.java
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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.temp.ValueIndexLikeMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
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
 * A property that determines whether the expression may produce duplicate entries. If the given expression is a
 * {@link RelationalExpression}, this will return whether the expression might produce multiple instances
 * of the same record. If the given expression is a {@link KeyExpression}, then this will return whether the
 * expression might return multiple results for the same record.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S3776")
public class OrderingProperty implements PlannerProperty<Optional<Ordering>> {
    @Nonnull
    private final PlanContext context;

    private OrderingProperty(@Nonnull PlanContext context) {
        this.context = context;
    }

    @Nonnull
    @Override
    public Optional<Ordering> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Optional<Ordering>> childResults) {
        if (expression instanceof RecordQueryScanPlan) {
            final var plan = (RecordQueryScanPlan)expression;
            return Optional.ofNullable(context.getCommonPrimaryKey())
                    .map(commonPrimaryKey -> ValueIndexLikeMatchCandidate.computeOrderingFromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                            plan.getComparisons(),
                            plan.isReverse(),
                            false));
        } else if (expression instanceof PrimaryScanExpression) {
            final var plan = (PrimaryScanExpression)expression;
            return Optional.ofNullable(context.getCommonPrimaryKey())
                    .map(commonPrimaryKey -> ValueIndexLikeMatchCandidate.computeOrderingFromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                            plan.scanComparisons(),
                            plan.isReverse(),
                            false));
        } else if (expression instanceof RecordQueryIndexPlan || expression instanceof RecordQueryCoveringIndexPlan) {
            return fromIndexScanOrCoveringIndexScan(context, (RecordQueryPlan)expression);
        } else if (expression instanceof LogicalSortExpression) {
            final Optional<Ordering> childOrderingOptional = Iterables.getOnlyElement(childResults);

            final KeyExpression sortKey = ((LogicalSortExpression)expression).getSort();
            if (sortKey == null) {
                return orderingFromSingleChild(expression, childResults);
            }

            final List<KeyExpression> normalizedSortKeys = sortKey.normalizeKeyForPositions();
            final ImmutableList.Builder<KeyPart> keyPartBuilder = ImmutableList.builder();
            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundKeyMap =
                    childOrderingOptional.map(Ordering::getEqualityBoundKeyMap).orElse(ImmutableSetMultimap.of());
            for (final KeyExpression keyExpression : normalizedSortKeys) {
                if (!equalityBoundKeyMap.containsKey(keyExpression)) {
                    keyPartBuilder.add(KeyPart.of(keyExpression, ((LogicalSortExpression)expression).isReverse()));
                }
            }

            return Optional.of(new Ordering(equalityBoundKeyMap, keyPartBuilder.build(), childOrderingOptional.map(Ordering::isDistinct).orElse(false)));
        } else if (expression instanceof RecordQueryIntersectionPlan) {
            final RecordQueryIntersectionPlan intersectionPlan = (RecordQueryIntersectionPlan)expression;
            return deriveForIntersectionFromOrderings(
                    childResults,
                    requestedOrderingFromComparisonKey(intersectionPlan.getComparisonKey(), intersectionPlan.isReverse()),
                    Ordering::unionEqualityBoundKeys);
        } else if (expression instanceof RecordQueryUnionPlan) {
            final RecordQueryUnionPlan unionPlan = (RecordQueryUnionPlan)expression;
            return deriveForUnionFromOrderings(
                    childResults,
                    requestedOrderingFromComparisonKey(unionPlan.getComparisonKey(), unionPlan.isReverse()),
                    Ordering::intersectEqualityBoundKeys);
        } else if (expression instanceof RecordQueryUnorderedUnionPlan) {
            return deriveForUnionFromOrderings(childResults,
                    RequestedOrdering.preserve(),
                    Ordering::intersectEqualityBoundKeys);
        } else if (expression instanceof RecordQueryInUnionPlan) {
            return deriveForInUnionFromOrderings(childResults, (RecordQueryInUnionPlan)expression);
        } else if (expression instanceof RecordQueryInJoinPlan) {
            return deriveForInJoinFromOrderings(childResults, (RecordQueryInJoinPlan)expression);
        } else if (expression instanceof RecordQueryPredicatesFilterPlan) {
            return deriveForPredicatesFilterFromOrderings(childResults, (RecordQueryPredicatesFilterPlan)expression);
        } else {
            return orderingFromSingleChild(expression, childResults);
        }
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

    private Optional<Ordering> orderingFromSingleChild(@Nonnull final RelationalExpression expression,
                                                       @Nonnull final List<Optional<Ordering>> childResults) {
        final List<? extends Quantifier> quantifiers = expression.getQuantifiers();
        if (quantifiers.size() == 1) {
            return Iterables.getOnlyElement(childResults);
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<Ordering> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Optional<Ordering>> orderingOptionals) {
        final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                Ordering.combineEqualityBoundKeys(orderingOptionals, Ordering::intersectEqualityBoundKeys);
        if (commonEqualityBoundKeysMapOptional.isEmpty()) {
            return Optional.empty();
        }
        final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

        final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderingOptionals, RequestedOrdering.preserve());
        if (commonOrderingKeysOptional.isEmpty()) {
            return Optional.empty();
        }

        final var commonOrderingKeys =
                commonOrderingKeysOptional.get()
                        .stream()
                        .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                        .collect(ImmutableList.toImmutableList());

        final var allAreDistinct = orderingOptionals
                .stream()
                .allMatch(orderingOptional -> orderingOptional.map(Ordering::isDistinct).orElse(false));

        return Optional.of(new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, allAreDistinct));

    }

    @Nonnull
    private static Optional<Ordering> fromIndexScanOrCoveringIndexScan(@Nonnull final PlanContext context, @Nonnull final RecordQueryPlan plan) {
        final RecordQueryIndexPlan recordQueryIndexPlan;
        if (plan instanceof RecordQueryIndexPlan) {
            recordQueryIndexPlan = (RecordQueryIndexPlan)plan;
        } else if (plan instanceof RecordQueryCoveringIndexPlan) {
            final RecordQueryPlanWithIndex planWithIndex = ((RecordQueryCoveringIndexPlan)plan).getIndexPlan();
            if (planWithIndex instanceof RecordQueryIndexPlan) {
                recordQueryIndexPlan = (RecordQueryIndexPlan)planWithIndex;
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }

        final String indexName = recordQueryIndexPlan.getIndexName();
        final RecordMetaData metaData = context.getMetaData();
        final Index index = metaData.getIndex(indexName);
        final ScanComparisons scanComparisons = recordQueryIndexPlan.getComparisons();
        return recordQueryIndexPlan.getMatchCandidateOptional()
                .map(matchCandidate -> matchCandidate.computeOrderingFromScanComparisons(scanComparisons, plan.isReverse(), index.isUnique()));
    }

    public static Optional<Ordering> deriveForUnionFromOrderings(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                                 @Nonnull final RequestedOrdering requestedOrdering,
                                                                 @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparisons.Comparison>> combineFn) {
        final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                Ordering.combineEqualityBoundKeys(orderingOptionals, combineFn);
        if (commonEqualityBoundKeysMapOptional.isEmpty()) {
            return Optional.empty();
        }
        final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

        final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderingOptionals, requestedOrdering);
        if (commonOrderingKeysOptional.isEmpty()) {
            return Optional.empty();
        }

        final var commonOrderingKeys =
                commonOrderingKeysOptional.get()
                        .stream()
                        .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                        .collect(ImmutableList.toImmutableList());

        return Optional.of(
                new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, false));
    }

    public static Optional<Ordering> deriveForIntersectionFromOrderings(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                                        @Nonnull final RequestedOrdering requestedOrdering,
                                                                        @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparisons.Comparison>> combineFn) {
        final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysMapOptional =
                Ordering.combineEqualityBoundKeys(orderingOptionals, combineFn);
        if (commonEqualityBoundKeysMapOptional.isEmpty()) {
            return Optional.empty();
        }
        final var commonEqualityBoundKeysMap = commonEqualityBoundKeysMapOptional.get();

        final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderingOptionals, requestedOrdering);
        if (commonOrderingKeysOptional.isEmpty()) {
            return Optional.empty();
        }

        final var commonOrderingKeys =
                commonOrderingKeysOptional.get()
                        .stream()
                        .filter(keyPart -> !commonEqualityBoundKeysMap.containsKey(keyPart.getNormalizedKeyExpression()))
                        .collect(ImmutableList.toImmutableList());


        final boolean allAreDistinct = orderingOptionals.stream()
                .anyMatch(orderingOptional -> orderingOptional.map(Ordering::isDistinct).orElse(false));

        return Optional.of(
                new Ordering(commonEqualityBoundKeysMap, commonOrderingKeys, allAreDistinct));
    }

    public static Optional<Ordering> deriveForInUnionFromOrderings(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                                   @Nonnull final RecordQueryInUnionPlan inUnionPlan) {
        final Optional<Ordering> childOrderingOptional = Iterables.getOnlyElement(orderingOptionals);
        if (childOrderingOptional.isPresent()) {
            final var childOrdering = childOrderingOptional.get();
            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundKeyMap = childOrdering.getEqualityBoundKeyMap();
            final KeyExpression comparisonKey = inUnionPlan.getComparisonKey();

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap = HashMultimap.create(equalityBoundKeyMap);
            final ImmutableList.Builder<KeyPart> resultKeyPartBuilder = ImmutableList.builder();
            final List<KeyExpression> normalizedComparisonKeys = comparisonKey.normalizeKeyForPositions();
            for (final KeyExpression normalizedKeyExpression : normalizedComparisonKeys) {
                resultKeyPartBuilder.add(KeyPart.of(normalizedKeyExpression, inUnionPlan.isReverse()));
            }

            final var sourceAliases =
                    inUnionPlan.getInSources()
                            .stream()
                            .map(inSource -> CorrelationIdentifier.of(CORRELATION.identifier(inSource.getBindingName())))
                            .collect(ImmutableSet.toImmutableSet());

            for (final var entry : equalityBoundKeyMap.entries()) {
                final var correlatedTo = entry.getValue().getCorrelatedTo();

                if (correlatedTo.stream().anyMatch(sourceAliases::contains)) {
                    resultEqualityBoundKeyMap.removeAll(entry.getKey());
                }
            }
            
            return Optional.of(new Ordering(resultEqualityBoundKeyMap, resultKeyPartBuilder.build(), childOrdering.isDistinct()));
        } else {
            return Optional.empty();
        }
    }

    @SuppressWarnings("java:S135")
    public static Optional<Ordering> deriveForInJoinFromOrderings(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                                  @Nonnull final RecordQueryInJoinPlan inJoinPlan) {
        final Optional<Ordering> childOrderingOptional = Iterables.getOnlyElement(orderingOptionals);
        if (childOrderingOptional.isPresent()) {
            final var childOrdering = childOrderingOptional.get();
            final var equalityBoundKeyMap = childOrdering.getEqualityBoundKeyMap();
            final var inSource = inJoinPlan.getInSource();
            final CorrelationIdentifier inAlias = inJoinPlan.getInAlias();

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
                return Optional.of(new Ordering(resultEqualityBoundKeyMap, ImmutableList.of(), false));
            }

            //
            // Prepend the existing order with the key expression we just found.
            //
            final var resultOrderingKeyPartsBuilder = ImmutableList.<KeyPart>builder();
            resultOrderingKeyPartsBuilder.add(KeyPart.of(inKeyExpression, inSource.isReverse()));
            resultOrderingKeyPartsBuilder.addAll(childOrdering.getOrderingKeyParts());

            return Optional.of(new Ordering(resultEqualityBoundKeyMap,
                    resultOrderingKeyPartsBuilder.build(),
                    childOrdering.isDistinct()));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Ordering> deriveForPredicatesFilterFromOrderings(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                                            @Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        final Optional<Ordering> childOrderingOptional = Iterables.getOnlyElement(orderingOptionals);
        if (childOrderingOptional.isPresent()) {
            final Ordering childOrdering = childOrderingOptional.get();

            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundFileKeyExpressions =
                    predicatesFilterPlan.getPredicates()
                            .stream()
                            .flatMap(queryPredicate -> {
                                if (!(queryPredicate instanceof ValuePredicate)) {
                                    return Stream.empty();
                                }
                                final ValuePredicate valuePredicate = (ValuePredicate)queryPredicate;
                                if (!valuePredicate.getComparison().getType().isEquality()) {
                                    return Stream.empty();
                                }

                                if (!(valuePredicate.getValue() instanceof FieldValue)) {
                                    return Stream.of();
                                }
                                return Stream.of(Pair.of((FieldValue)valuePredicate.getValue(), valuePredicate.getComparison()));
                            })
                            .map(valueComparisonPair -> {

                                final FieldValue fieldValue = valueComparisonPair.getLeft();
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

            final ImmutableList<KeyPart> resultOrderingKeyParts =
                    childOrdering.getOrderingKeyParts()
                            .stream()
                            .filter(keyPart -> !equalityBoundFileKeyExpressions.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap =
                    HashMultimap.create(childOrdering.getEqualityBoundKeyMap());

            equalityBoundFileKeyExpressions.forEach(resultEqualityBoundKeyMap::put);

            return Optional.of(new Ordering(resultEqualityBoundKeyMap, resultOrderingKeyParts, childOrdering.isDistinct()));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<Ordering> evaluate(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull PlanContext context) {
        return ref.acceptPropertyVisitor(new OrderingProperty(context));
    }

    public static Optional<Ordering> evaluate(@Nonnull RelationalExpression expression, @Nonnull PlanContext context) {
        // Won't actually be null for relational planner expressions.
        return expression.acceptPropertyVisitor(new OrderingProperty(context));
    }
}
