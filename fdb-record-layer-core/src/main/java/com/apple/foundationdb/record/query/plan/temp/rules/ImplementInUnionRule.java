/*
 * ImplementInUnionRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.temp.rules.PushInterestingOrderingThroughInLikeSelectRule.findInnerQuantifier;

/**
 * A rule that implements a SELECT over a VALUES and a correlated subexpression as a {@link RecordQueryInUnionPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementInUnionRule extends PlannerRule<SelectExpression> {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public ImplementInUnionRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @SuppressWarnings({"unchecked", "java:S135"})
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var context = call.getContext();
        final var bindings = call.getBindings();

        final var requestedOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();

        final var commonPrimaryKey = context.getCommonPrimaryKey();
        if (commonPrimaryKey == null) {
            return;
        }

        final var selectExpression = bindings.get(root);
        if (!selectExpression.getPredicates().isEmpty()) {
            return;
        }

        final var explodeQuantifiers = bindings.get(explodeQuantifiersMatcher);
        if (explodeQuantifiers.isEmpty()) {
            return;
        }

        final var explodeAliases = Quantifiers.aliases(explodeQuantifiers);
        final var innerQuantifierOptional =
                findInnerQuantifier(selectExpression,
                        explodeQuantifiers,
                        explodeAliases);
        if (innerQuantifierOptional.isEmpty()) {
            return;
        }
        final var innerQuantifier = innerQuantifierOptional.get();

        final var resultValue = selectExpression.getResultValue();
        if (!(resultValue instanceof QuantifiedValue) ||
                !((QuantifiedValue)resultValue).getAlias().equals(innerQuantifier.getAlias())) {
            return;
        }

        final var explodeExpressions = bindings.getAll(explodeExpressionMatcher);
        final var quantifierToExplodeBiMap = computeQuantifierToExplodeMap(explodeQuantifiers, explodeExpressions.stream().collect(LinkedIdentitySet.toLinkedIdentitySet()));
        final var explodeToQuantifierBiMap = quantifierToExplodeBiMap.inverse();

        final var sourcesBuilder = ImmutableList.<InSource>builder();

        for (final var explodeExpression : explodeExpressions) {
            final var explodeQuantifier = Objects.requireNonNull(explodeToQuantifierBiMap.getUnwrapped(explodeExpression));
            final Value explodeCollectionValue = explodeExpression.getCollectionValue();

            //
            // Create the source for the in-union plan
            //
            final InSource inSource;
            if (explodeCollectionValue instanceof LiteralValue<?>) {
                final Object literalValue = ((LiteralValue<?>)explodeCollectionValue).getLiteralValue();
                if (literalValue instanceof List<?>) {
                    inSource = new InValuesSource(CORRELATION.bindingName(explodeQuantifier.getAlias().getId()), (List<Object>)literalValue);
                } else {
                    return;
                }
            } else if (explodeCollectionValue instanceof QuantifiedObjectValue) {
                inSource = new InParameterSource(CORRELATION.bindingName(explodeQuantifier.getAlias().getId()),
                        ((QuantifiedObjectValue)explodeCollectionValue).getAlias().getId());
            } else {
                return;
            }
            sourcesBuilder.add(inSource);
        }

        final var inSources = sourcesBuilder.build();

        final Map<Ordering, ImmutableList<RecordQueryPlan>> groupedByOrdering =
                innerQuantifier
                        .getRangesOver()
                        .getMembers()
                        .stream()
                        .flatMap(relationalExpression -> relationalExpression.narrowMaybe(RecordQueryPlan.class).stream())
                        .flatMap(plan -> {
                            final Optional<Ordering> orderingForLegOptional =
                                    OrderingProperty.evaluate(plan, context);

                            return orderingForLegOptional
                                    .stream()
                                    .map(ordering -> Pair.of(ordering, plan));
                        })
                        .collect(Collectors.groupingBy(Pair::getLeft,
                                Collectors.mapping(Pair::getRight,
                                        ImmutableList.toImmutableList())));

        final int attemptFailedInJoinAsUnionMaxSize = call.getContext().getPlannerConfiguration().getAttemptFailedInJoinAsUnionMaxSize();

        for (final Map.Entry<Ordering, ImmutableList<RecordQueryPlan>> providedOrderingEntry : groupedByOrdering.entrySet()) {
            for (final RequestedOrdering requestedOrdering : requestedOrderings) {
                final var providedOrdering = providedOrderingEntry.getKey();
                final var matchingKeyExpressionsBuilder = ImmutableSet.<KeyExpression>builder();
                for (Map.Entry<KeyExpression, Comparisons.Comparison> expressionComparisonEntry : providedOrdering.getEqualityBoundKeyMap().entries()) {
                    final Comparisons.Comparison comparison = expressionComparisonEntry.getValue();
                    if (comparison.getType() == Comparisons.Type.EQUALS && comparison instanceof Comparisons.ParameterComparison) {
                        final Comparisons.ParameterComparison parameterComparison = (Comparisons.ParameterComparison)comparison;
                        if (parameterComparison.isCorrelation() && explodeAliases.containsAll(parameterComparison.getCorrelatedTo())) {
                            matchingKeyExpressionsBuilder.add(expressionComparisonEntry.getKey());
                        }
                    }
                }

                // Compute a comparison key that satisfies the requested ordering
                final Optional<Ordering> combinedOrderingOptional =
                        orderingForInUnion(providedOrdering, requestedOrdering, matchingKeyExpressionsBuilder.build());
                if (combinedOrderingOptional.isEmpty()) {
                    continue;
                }

                final Ordering combinedOrdering = combinedOrderingOptional.get();
                final List<KeyPart> orderingKeyParts = combinedOrdering.getOrderingKeyParts();

                final List<KeyExpression> orderingKeys =
                        orderingKeyParts
                                .stream()
                                .map(KeyPart::getNormalizedKeyExpression)
                                .collect(ImmutableList.toImmutableList());

                //
                // At this point we know we can implement the distinct union over the partitions of compatibly ordered plans
                //
                final KeyExpression comparisonKey =
                        orderingKeys.size() == 1
                        ? Iterables.getOnlyElement(orderingKeys) : Key.Expressions.concat(orderingKeys);

                final GroupExpressionRef<RecordQueryPlan> newInnerPlanReference = GroupExpressionRef.from(providedOrderingEntry.getValue());
                final Quantifier.Physical newInnerQuantifier = Quantifier.physical(newInnerPlanReference);
                call.yield(call.ref(
                        new RecordQueryInUnionPlan(newInnerQuantifier,
                                inSources,
                                comparisonKey,
                                RecordQueryUnionPlanBase.isReversed(ImmutableList.of(newInnerQuantifier)),
                                attemptFailedInJoinAsUnionMaxSize)));
            }
        }
    }

    private static IdentityBiMap<Quantifier.ForEach, ExplodeExpression> computeQuantifierToExplodeMap(@Nonnull final Collection<? extends Quantifier.ForEach> quantifiers,
                                                                                                      @Nonnull final Set<ExplodeExpression> explodeExpressions) {
        final var resultMap =
                IdentityBiMap.<Quantifier.ForEach,  ExplodeExpression>create();

        for (final var quantifier : quantifiers) {
            final var rangesOver = quantifier.getRangesOver();
            for (final var explodeExpression : explodeExpressions) {
                if (rangesOver.getMembers().contains(explodeExpression)) {
                    resultMap.putUnwrapped(quantifier, explodeExpression);
                    break; // only ever one match possible
                }
            }
        }
        return resultMap;
    }

    private static Optional<Ordering> orderingForInUnion(@Nonnull Ordering providedOrdering,
                                                         @Nonnull RequestedOrdering requestedOrdering,
                                                         @Nonnull Set<KeyExpression> innerBoundExpressions) {
        final var providedKeyPartIterator = Iterators.peekingIterator(providedOrdering.getOrderingKeyParts().iterator());
        final ImmutableList.Builder<KeyPart> resultingOrderingKeyPartBuilder = ImmutableList.builder();

        for (final var requestedKeyPart : requestedOrdering.getOrderingKeyParts()) {
            KeyPart toBeAdded = null;
            if (providedKeyPartIterator.hasNext()) {
                final var providedKeyPart = providedKeyPartIterator.peek();

                if (requestedKeyPart.equals(providedKeyPart)) {
                    toBeAdded = providedKeyPart;
                    providedKeyPartIterator.next();
                }
            }

            if (toBeAdded == null) {
                final var requestedKeyExpression = requestedKeyPart.getNormalizedKeyExpression();
                if (innerBoundExpressions.contains(requestedKeyExpression)) {
                    toBeAdded = requestedKeyPart;
                }
            }

            if (toBeAdded != null) {
                resultingOrderingKeyPartBuilder.add(toBeAdded);
            } else {
                return Optional.empty();
            }
        }

        //
        // Skip all inner bound expressions that are still available. We could potentially add them here, however,
        // doing so will be adverse to any hopes of getting an in-join planned as the provided orderings for IN-JOIN
        // and IN-UNION should be compatible if possible when created in their respective Implement... rules.
        //

        //
        // For all provided parts that are left-overs.
        //
        while (providedKeyPartIterator.hasNext()) {
            final var providedKeyPart = providedKeyPartIterator.next();
            resultingOrderingKeyPartBuilder.add(providedKeyPart);
        }

        final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap = HashMultimap.create(providedOrdering.getEqualityBoundKeyMap());
        innerBoundExpressions.forEach(resultEqualityBoundKeyMap::removeAll);

        return Optional.of(new Ordering(resultEqualityBoundKeyMap, resultingOrderingKeyPartBuilder.build(), providedOrdering.isDistinct()));
    }
}
