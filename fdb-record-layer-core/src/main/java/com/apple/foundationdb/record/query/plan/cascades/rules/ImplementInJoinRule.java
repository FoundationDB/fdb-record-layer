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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartitions;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.InComparandSource;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.SortedInComparandSource;
import com.apple.foundationdb.record.query.plan.plans.SortedInParameterSource;
import com.apple.foundationdb.record.query.plan.plans.SortedInValuesSource;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.rules.PushRequestedOrderingThroughInLikeSelectRule.findInnerQuantifier;

/**
 * A rule that implements a SELECT over a VALUES and a correlated subexpression as a {@link RecordQueryInUnionPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings({"PMD.TooManyStaticImports", "java:S4738", "java:S3776"})
public class ImplementInJoinRule extends ImplementationCascadesRule<SelectExpression> {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public ImplementInJoinRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @SuppressWarnings("java:S135")
    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var requestedOrderingsOptional = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING);
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
        if (!(resultValue instanceof QuantifiedObjectValue) ||
                !((QuantifiedObjectValue)resultValue).getAlias().equals(innerQuantifier.getAlias())) {
            return;
        }

        final var explodeExpressions = bindings.getAll(explodeExpressionMatcher);
        final var quantifierToExplodeBiMap =
                computeQuantifierToExplodeMap(explodeQuantifiers, explodeExpressions.stream().collect(LinkedIdentitySet.toLinkedIdentitySet()));
        
        final var innerReference = innerQuantifier.getRangesOver();
        final var planPartitions = PlanPartitions.rollUpTo(innerReference.toPlanPartitions(), OrderingProperty.ordering());

        for (final var planPartition  : planPartitions) {
            final var providedOrdering = planPartition.getPartitionPropertyValue(OrderingProperty.ordering());

            for (final RequestedOrdering requestedOrdering : requestedOrderings) {
                final var sourcesStream =
                        enumerateInSourcesForRequestedOrdering(explodeAliasToQuantifierMap, explodeAliases,
                                quantifierToExplodeBiMap, providedOrdering, requestedOrdering);
                sourcesStream.forEach(sources -> {
                    final var reverseSources = Lists.reverse(sources);

                    var newInnerPlanReference = call.memoizeMemberPlansBuilder(innerReference, planPartition.getPlans());
                    for (final InSource inSource : reverseSources) {
                        final var inJoinPlan = inSource.toInJoinPlan(Quantifier.physical(newInnerPlanReference.reference()));
                        newInnerPlanReference = call.memoizePlanBuilder(inJoinPlan);
                    }

                    call.yieldPlans(newInnerPlanReference.members());
                });
            }
        }
    }

    @Nonnull
    private Stream<List<InSource>> enumerateInSourcesForRequestedOrdering(@Nonnull final Map<CorrelationIdentifier, Quantifier> explodeAliasToQuantifierMap,
                                                                          @Nonnull final Set<CorrelationIdentifier> explodeAliases,
                                                                          @Nonnull final Map<Quantifier.ForEach, ExplodeExpression> quantifierToExplodeMap,
                                                                          @Nonnull final Ordering innerOrdering,
                                                                          @Nonnull final RequestedOrdering requestedOrdering) {
        Verify.verify(!explodeAliases.isEmpty());
        final var availableExplodeAliases = Sets.newLinkedHashSet(explodeAliases);
        final var requestedOrderingParts = requestedOrdering.getOrderingParts();
        final var innerBindingMap = innerOrdering.getBindingMap();
        final var resultOrderingBindingMap =
                HashMultimap.create(innerBindingMap);
        Stream<List<OrderingPartWithSource>> prefixStream = Stream.of(ImmutableList.of());

        for (var i = 0; i < requestedOrderingParts.size() && !availableExplodeAliases.isEmpty(); i++) {
            final var requestedOrderingPart = requestedOrderingParts.get(i);
            final var requestedOrderingValue = requestedOrderingPart.getValue();
            final var innerBindings = innerBindingMap.get(requestedOrderingValue);

            if (innerBindings.isEmpty() || Ordering.sortOrder(innerBindings).isDirectional()) {
                return Stream.of();
            }

            final var comparisonsCorrelatedTo =
                    innerBindings.stream()
                            .flatMap(binding -> binding.getComparison().getCorrelatedTo().stream())
                            .collect(ImmutableSet.toImmutableSet());

            if (comparisonsCorrelatedTo.size() > 1) {
                return Stream.of();
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
                return Stream.empty();
            }

            //
            // We need to find the one quantifier over an Explode expression that we can use to establish
            // the requested order.
            //
            final var explodeQuantifier =
                    Objects.requireNonNull(explodeAliasToQuantifierMap.get(explodeAlias));
            final var explodeExpression =
                    Objects.requireNonNull(quantifierToExplodeMap.get((Quantifier.ForEach)explodeQuantifier));

            //
            // At this point we have a bound key expression that matches the requested order at this position,
            // and we have our hands on a particular Explode expression leading us directly do the in source.
            //

            final var explodeValue = explodeExpression.getCollectionValue();
            if (!isSupportedExplodeValue(explodeValue)) {
                return Stream.empty();
            }

            final var requestedSortOrder =
                    requestedOrderingPart.getDirectionalSortOrderOrDefault(RequestedSortOrder.ANY);
            final List<ProvidedSortOrder> attemptedSortOrders =
                    requestedSortOrder == RequestedSortOrder.ANY
                    ? attemptedProvidedSortOrdersForAny(requestedOrdering.isExhaustive())
                    : ImmutableList.of(requestedSortOrder.toProvidedSortOrder());

            prefixStream = prefixStream.flatMap(prefix ->
                    attemptedSortOrders.stream()
                            .flatMap(attemptedSortOrder -> {
                                final InSource inSource =
                                        computeInSource(explodeValue, explodeQuantifier, attemptedSortOrder);

                                return inSource == null ? Stream.empty() :
                                       Stream.of(ImmutableList.<OrderingPartWithSource>builder().addAll(prefix)
                                               .add(new OrderingPartWithSource(new ProvidedOrderingPart(requestedOrderingPart.getValue(), attemptedSortOrder), inSource))
                                               .build());
                            }));
            availableExplodeAliases.remove(explodeAlias);
            resultOrderingBindingMap.removeAll(requestedOrderingValue);
        }

        if (availableExplodeAliases.isEmpty()) {
            prefixStream = prefixStream.flatMap(prefix -> {
                //
                // All available explode aliases have been depleted. Create an ordering and check against the requested
                // ordering.
                //
                final var outerOrderingValuesBuilder = ImmutableList.<Value>builder();
                final var outerOrderingBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
                for (final var orderingPartWithSource : prefix) {
                    final var outerProvidedOrderingPart = orderingPartWithSource.getProvidedOrderingPart();
                    final var outerOrderingValue = outerProvidedOrderingPart.getValue();
                    outerOrderingValuesBuilder.add(outerOrderingValue);
                    final var outerProvidedSortOrder = outerProvidedOrderingPart.getSortOrder();
                    Verify.verify(outerProvidedSortOrder.isDirectional());
                    outerOrderingBindingMapBuilder.put(outerOrderingValue,
                            Binding.sorted(outerProvidedSortOrder));
                }

                final var outerOrderingValues = outerOrderingValuesBuilder.build();
                final var outerOrdering = Ordering.ofOrderingSequence(outerOrderingBindingMapBuilder.build(),
                        outerOrderingValues, true);

                final var filteredInnerOrderingSet =
                        innerOrdering.getOrderingSet()
                                .filterElements(value -> innerOrdering.isSingularNonFixedValue(value) ||
                                        !outerOrderingValues.contains(value));
                final var filteredInnerOrdering = Ordering.ofOrderingSet(resultOrderingBindingMap,
                        filteredInnerOrderingSet, innerOrdering.isDistinct());
                final var concatenatedOrdering =
                        Ordering.concatOrderings(outerOrdering, filteredInnerOrdering);

                //
                // Note, that while we could potentially pull up the concatenated ordering along the result value of the
                // SELECT expression, the ordering would stay identical as we only pull up along a simple QOV over the
                // inner quantifier.
                //
                return concatenatedOrdering.satisfies(requestedOrdering)
                       ? Stream.of(prefix)
                       : Stream.empty();
            });
        } else {
            //
            // There are some explodes left, but we don't have any requested sort orders anymore.
            //
            @Nullable final var attemptedSortOrders =
                    requestedOrdering.isExhaustive()
                    ? attemptedProvidedSortOrdersForAny(requestedOrdering.isExhaustive())
                    : null;

            final Iterable<List<CorrelationIdentifier>> availableExplodeAliasesPermutations =
                    requestedOrdering.isExhaustive()
                    ? TopologicalSort.permutations(availableExplodeAliases)
                    : ImmutableList.of(ImmutableList.copyOf(availableExplodeAliases));

            prefixStream = prefixStream.flatMap(prefix ->
                    suffixForRemainingExplodes(explodeAliasToQuantifierMap, quantifierToExplodeMap, requestedOrdering,
                            availableExplodeAliasesPermutations, attemptedSortOrders)
                            .map(suffix -> ImmutableList.<OrderingPartWithSource>builder()
                                    .addAll(prefix)
                                    .addAll(suffix)
                                    .build()));
        }

        //
        // We can finally return the sources and based on those create a right-deep plan starting from the last
        // (most inner) source moving outward.
        //
        return prefixStream.map(orderingPartWithSources ->
                orderingPartWithSources.stream()
                        .map(OrderingPartWithSource::getInSource)
                        .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    private static Stream<List<OrderingPartWithSource>> suffixForRemainingExplodes(@Nonnull final Map<CorrelationIdentifier, Quantifier> explodeAliasToQuantifierMap,
                                                                                   @Nonnull final Map<Quantifier.ForEach, ExplodeExpression> quantifierToExplodeMap,
                                                                                   @Nonnull final RequestedOrdering requestedOrdering,
                                                                                   @Nonnull final Iterable<List<CorrelationIdentifier>> availableExplodeAliasesPermutations,
                                                                                   @Nullable final List<ProvidedSortOrder> attemptedSortOrders) {
        return Streams.stream(availableExplodeAliasesPermutations)
                .flatMap(availableExplodeAliasesPermutation -> {
                    final var suffix =
                            availableExplodeAliasesPermutation.stream()
                                    .map(explodeAlias -> {
                                        final var explodeQuantifier =
                                                Objects.requireNonNull(explodeAliasToQuantifierMap.get(explodeAlias));
                                        final var explodeExpression =
                                                Objects.requireNonNull(quantifierToExplodeMap.get((Quantifier.ForEach)explodeQuantifier));

                                        final var explodeValue = explodeExpression.getCollectionValue();

                                        final var orderingResultsBuilder = ImmutableList.<OrderingPartWithSource>builder();
                                        if (attemptedSortOrders == null) {
                                            Verify.verify(!requestedOrdering.isExhaustive());
                                            final InSource inSource =
                                                    computeInSource(explodeValue, explodeQuantifier, null);
                                            if (inSource != null) {
                                                orderingResultsBuilder.add(new OrderingPartWithSource(null, inSource));
                                            }
                                        } else {
                                            for (final var attemptedSortOrder : attemptedSortOrders) {
                                                final InSource inSource =
                                                        computeInSource(explodeValue, explodeQuantifier, attemptedSortOrder);

                                                if (inSource != null) {
                                                    orderingResultsBuilder.add(new OrderingPartWithSource(null, inSource));
                                                }
                                            }
                                        }
                                        return orderingResultsBuilder.build();
                                    })
                                    .collect(ImmutableList.toImmutableList());
                    return Streams.stream(CrossProduct.crossProduct(suffix));
                });
    }

    private static boolean isSupportedExplodeValue(@Nonnull final Value explodeValue) {
        return explodeValue instanceof LiteralValue<?> ||
                explodeValue instanceof QuantifiedObjectValue ||
                explodeValue instanceof ParameterObjectValue ||
                explodeValue.isConstant();
    }

    @Nonnull
    private static List<ProvidedSortOrder> attemptedProvidedSortOrdersForAny(final boolean isExhaustive) {
        if (isExhaustive) {
            // TODO consider creating the non-counterflow orders as well
            return ImmutableList.of(ProvidedSortOrder.ASCENDING, ProvidedSortOrder.DESCENDING);
        } else {
            return ImmutableList.of(ProvidedSortOrder.ASCENDING);
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static InSource computeInSource(@Nonnull final Value explodeValue,
                                            @Nonnull final Quantifier explodeQuantifier,
                                            @Nullable final ProvidedSortOrder attemptedSortOrder) {
        final String bindingName = CORRELATION.bindingName(explodeQuantifier.getAlias().getId());
        if (explodeValue instanceof LiteralValue<?>) {
            final Object literalValue = ((LiteralValue<?>)explodeValue).getLiteralValue();
            if (literalValue instanceof List<?>) {
                return attemptedSortOrder == null
                       ? new InValuesSource(bindingName, (List<Object>)literalValue)
                       : new SortedInValuesSource(bindingName, (List<Object>)literalValue,
                        attemptedSortOrder.isAnyDescending()); // TODO needs to distinguish between different descending orders
            } else {
                return null;
            }
        } else if (explodeValue instanceof QuantifiedObjectValue) {
            final var alias = ((QuantifiedObjectValue)explodeValue).getAlias().getId();
            return attemptedSortOrder == null
                   ? new InParameterSource(bindingName, alias)
                   : new SortedInParameterSource(bindingName, alias, attemptedSortOrder.isAnyDescending()); // TODO needs to distinguish between different descending orders
        } else if (explodeValue instanceof ParameterObjectValue) {
            final var parameterName = ((ParameterObjectValue)explodeValue).getParameterName();
            return attemptedSortOrder == null
                   ? new InParameterSource(bindingName, parameterName)
                   : new SortedInParameterSource(bindingName, parameterName, attemptedSortOrder.isAnyDescending()); // TODO needs to distinguish between different descending orders
        } else if (explodeValue.isConstant()) {
            return attemptedSortOrder == null
                   ? new InComparandSource(bindingName, new Comparisons.ValueComparison(Comparisons.Type.IN, explodeValue))
                   : new SortedInComparandSource(bindingName, new Comparisons.ValueComparison(Comparisons.Type.IN, explodeValue),
                    attemptedSortOrder.isAnyDescending()); // TODO needs to distinguish between different descending orders
        }
        return null;
    }

    @Nonnull
    private static Map<Quantifier.ForEach, ExplodeExpression> computeQuantifierToExplodeMap(@Nonnull final Collection<? extends Quantifier.ForEach> quantifiers,
                                                                                            @Nonnull final Set<ExplodeExpression> explodeExpressions) {
        final var resultMap =
                new LinkedIdentityMap<Quantifier.ForEach,  ExplodeExpression>();

        for (final var quantifier : quantifiers) {
            final var rangesOver = quantifier.getRangesOver();
            for (final var explodeExpression : explodeExpressions) {
                if (rangesOver.containsExactly(explodeExpression)) {
                    resultMap.put(quantifier, explodeExpression);
                    break; // only ever one match possible
                }
            }
        }
        return resultMap;
    }

    private static class OrderingPartWithSource {
        @Nullable
        private final ProvidedOrderingPart providedOrderingPart;
        @Nonnull
        private final InSource inSource;

        public OrderingPartWithSource(@Nullable final ProvidedOrderingPart providedOrderingPart,
                                      @Nonnull final InSource inSource) {
            this.providedOrderingPart = providedOrderingPart;
            this.inSource = inSource;
        }

        @Nonnull
        public ProvidedOrderingPart getProvidedOrderingPart() {
            return Objects.requireNonNull(providedOrderingPart);
        }

        @Nonnull
        public InSource getInSource() {
            return inSource;
        }
    }
}
