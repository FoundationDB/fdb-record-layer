/*
 * ImplementInJoinRule.java
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
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.SortedInParameterSource;
import com.apple.foundationdb.record.query.plan.plans.SortedInValuesSource;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
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
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
@SuppressWarnings({"PMD.TooManyStaticImports", "java:S4738", "java:S3776"})
public class ImplementInJoinRule extends PlannerRule<SelectExpression> {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public ImplementInJoinRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var context = call.getContext();
        final var bindings = call.getBindings();

        final var requestedOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();
        
        final var selectExpression = bindings.get(root);
        if (!selectExpression.getPredicates().isEmpty()) {
            return;
        }

        final var explodeQuantifiers = bindings.get(explodeQuantifiersMatcher);
        if (explodeQuantifiers.isEmpty()) {
            return;
        }

        final var explodeAliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(explodeQuantifiers);
        final var explodeAliases = explodeAliasToQuantifierMap.keySet();
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
        final var quantifierToExplodeBiMap =
                computeQuantifierToExplodeMap(explodeQuantifiers, explodeExpressions.stream().collect(LinkedIdentitySet.toLinkedIdentitySet()));

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

        for (final Map.Entry<Ordering, ImmutableList<RecordQueryPlan>> providedOrderingEntry : groupedByOrdering.entrySet()) {
            final var providedOrdering = providedOrderingEntry.getKey();

            for (final RequestedOrdering requestedOrdering : requestedOrderings) {
                final ImmutableList<InSource> sources =
                        getInSourcesForRequestedOrdering(explodeAliasToQuantifierMap,
                                explodeAliases,
                                quantifierToExplodeBiMap,
                                providedOrdering,
                                requestedOrdering);
                if (sources.isEmpty()) {
                    continue;
                }
                final var reverseSources = Lists.reverse(sources);

                GroupExpressionRef<RecordQueryPlan> newInnerPlanReference = GroupExpressionRef.from(providedOrderingEntry.getValue());
                for (final InSource inSource : reverseSources) {
                    final var inJoinPlan = inSource.toInJoinPlan(Quantifier.physical(newInnerPlanReference));
                    newInnerPlanReference = GroupExpressionRef.of(inJoinPlan);
                }

                call.yield(newInnerPlanReference);
            }
        }
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private ImmutableList<InSource> getInSourcesForRequestedOrdering(@Nonnull final Map<CorrelationIdentifier, Quantifier> explodeAliasToQuantifierMap,
                                                                     @Nonnull final Set<CorrelationIdentifier> explodeAliases,
                                                                     @Nonnull final IdentityBiMap<Quantifier.ForEach, ExplodeExpression> quantifierToExplodeBiMap,
                                                                     @Nonnull final Ordering providedInnerOrdering,
                                                                     @Nonnull final RequestedOrdering requestedOrdering) {
        final var availableExplodeAliases = Sets.newLinkedHashSet(explodeAliases);

        final var requestedOrderingKeyParts = requestedOrdering.getOrderingKeyParts();
        final var sourcesBuilder = ImmutableList.<InSource>builder();
        final var resultOrderingKeyPartsBuilder = ImmutableList.<KeyPart>builder();
        final var innerOrderingKeyParts = providedInnerOrdering.getOrderingKeyParts();
        final var innerEqualityBoundKeyMap = providedInnerOrdering.getEqualityBoundKeyMap();
        final var resultOrderingEqualityBoundKeyMap  =
                HashMultimap.create(innerEqualityBoundKeyMap);

        for (var i = 0; i < requestedOrderingKeyParts.size() && !availableExplodeAliases.isEmpty(); i++) {
            final var requestedOrderingKeyPart = requestedOrderingKeyParts.get(i);
            final var comparisons = innerEqualityBoundKeyMap.get(requestedOrderingKeyPart.getNormalizedKeyExpression());
            if (comparisons.isEmpty()) {
                return ImmutableList.of();
            }

            final var comparisonsCorrelatedTo = comparisons.stream()
                    .flatMap(comparison -> comparison.getCorrelatedTo().stream())
                    .collect(ImmutableSet.toImmutableSet());

            if (comparisonsCorrelatedTo.size() > 1) {
                return ImmutableList.of();
            }

            if (Sets.intersection(comparisonsCorrelatedTo, explodeAliases).isEmpty()) {
                //
                // This case covers comparisons such as  a = <literal> or a = <correlation> where
                // <correlation> is anchored above and therefore this comparison causes an equality-bound
                // expression.
                //
                // Example: requested ordering: a, b, c
                //          INs: a explodes over (1, 2)
                //               c explodes over ('x', 'y')
                //          inner plan has equality-bound expressions over a, b, c
                //
                // b is not among the INs, but it is bound via equality somehow. That binding can be a constant or
                // another correlation that is anchored above the current SELECT.
                //
                continue;
            }

            final var explodeAlias = Iterables.getOnlyElement(comparisonsCorrelatedTo);

            //
            // The quantifier still has to be available for us to choose from.
            //
            if (!availableExplodeAliases.contains(explodeAlias)) {
                return ImmutableList.of();
            }

            //
            // We need to find the one quantifier over an explode expression that we can use to establish
            // the requested order.
            //
            final var explodeQuantifier =
                    Objects.requireNonNull(explodeAliasToQuantifierMap.get(explodeAlias));
            final var explodeExpression = Objects.requireNonNull(quantifierToExplodeBiMap.getUnwrapped(explodeQuantifier));

            //
            // At this point we have a bound key expression that matches the requested order at this position,
            // and we have our hands on a particular explode expression leading us directly do the in source.
            //

            final var explodeCollectionValue = explodeExpression.getCollectionValue();

            final InSource inSource;
            if (explodeCollectionValue instanceof LiteralValue<?>) {
                final Object literalValue = ((LiteralValue<?>)explodeCollectionValue).getLiteralValue();
                if (literalValue instanceof List<?>) {
                    inSource = new SortedInValuesSource(
                            CORRELATION.bindingName(explodeQuantifier.getAlias().getId()),
                            (List<Object>)literalValue,
                            requestedOrderingKeyPart.isReverse());
                } else {
                    return ImmutableList.of();
                }
            } else if (explodeCollectionValue instanceof QuantifiedColumnValue) {
                inSource = new SortedInParameterSource(CORRELATION.bindingName(explodeQuantifier.getAlias().getId()),
                        ((QuantifiedColumnValue)explodeCollectionValue).getAlias().getId(),
                        requestedOrderingKeyPart.isReverse());
            } else {
                return ImmutableList.of();
            }
            availableExplodeAliases.remove(explodeAlias);
            sourcesBuilder.add(inSource);

            resultOrderingEqualityBoundKeyMap.removeAll(requestedOrderingKeyPart.getNormalizedKeyExpression());
            resultOrderingKeyPartsBuilder.add(requestedOrderingKeyPart);
        }

        if (availableExplodeAliases.isEmpty()) {
            //
            // All available explode aliases have been depleted. Create an ordering and check against the requested
            // ordering.
            //
            resultOrderingKeyPartsBuilder.addAll(innerOrderingKeyParts);
            final var resultOrdering = new Ordering(resultOrderingEqualityBoundKeyMap, resultOrderingKeyPartsBuilder.build(), providedInnerOrdering.isDistinct());
            return Ordering.satisfiesRequestedOrdering(resultOrdering, requestedOrdering)
                   ? sourcesBuilder.build()
                   : ImmutableList.of();
        } else {
            //
            // We may still have some explodes available that we don't have a particular order requirement for.
            // Create unsorted sources for these 'left-overs'.
            //
            for (final var explodeAlias : availableExplodeAliases) {
                final var explodeQuantifier =
                        Objects.requireNonNull(explodeAliasToQuantifierMap.get(explodeAlias));
                final var explodeExpression = Objects.requireNonNull(quantifierToExplodeBiMap.getUnwrapped(explodeQuantifier));

                final var explodeCollectionValue = explodeExpression.getCollectionValue();

                final InSource inSource;
                if (explodeCollectionValue instanceof LiteralValue<?>) {
                    final Object literalValue = ((LiteralValue<?>)explodeCollectionValue).getLiteralValue();
                    if (literalValue instanceof List<?>) {
                        inSource = new InValuesSource(
                                CORRELATION.bindingName(explodeQuantifier.getAlias().getId()),
                                (List<Object>)literalValue);
                    } else {
                        return ImmutableList.of();
                    }
                } else if (explodeCollectionValue instanceof QuantifiedColumnValue) {
                    inSource = new InParameterSource(CORRELATION.bindingName(explodeQuantifier.getAlias().getId()),
                            ((QuantifiedColumnValue)explodeCollectionValue).getAlias().getId());
                } else {
                    return ImmutableList.of();
                }
                sourcesBuilder.add(inSource);
            }
        }

        //
        // We can finally build the sources and based on those a right-deep plan starting from the last
        // (most inner) source moving outward.
        //
        return sourcesBuilder.build();
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
}
