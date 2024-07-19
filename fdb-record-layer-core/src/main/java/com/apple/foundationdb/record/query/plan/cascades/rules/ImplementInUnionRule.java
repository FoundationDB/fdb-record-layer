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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.InComparandSource;
import com.apple.foundationdb.record.query.plan.plans.InParameterSource;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.InValuesSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementInUnionRule extends CascadesRule<SelectExpression> {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public ImplementInUnionRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @SuppressWarnings({"unchecked", "java:S135", "PMD.CompareObjectsWithEquals"})
    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
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
        if (!(resultValue instanceof QuantifiedObjectValue) ||
                !((QuantifiedObjectValue)resultValue).getAlias().equals(innerQuantifier.getAlias())) {
            return;
        }

        final var explodeExpressions = bindings.getAll(explodeExpressionMatcher);

        Verify.verify(explodeExpressions.size() == explodeQuantifiers.size());

        final var sourcesBuilder = ImmutableList.<InSource>builder();

        int i = 0;
        for (final var explodeQuantifier : explodeQuantifiers) {
            final var explodeExpression = explodeExpressions.get(i++);
            final Value explodeCollectionValue = explodeExpression.getCollectionValue();

            //
            // Create the source for the in-union plan
            //
            final InSource inSource;
            final String bindingName = CORRELATION.bindingName(explodeQuantifier.getAlias().getId());
            if (explodeCollectionValue instanceof LiteralValue<?>) {
                final Object literalValue = ((LiteralValue<?>)explodeCollectionValue).getLiteralValue();
                if (literalValue instanceof List<?>) {
                    inSource = new InValuesSource(bindingName, (List<Object>)literalValue);
                } else {
                    return;
                }
            } else if (explodeCollectionValue instanceof QuantifiedObjectValue) {
                inSource = new InParameterSource(bindingName,
                        ((QuantifiedObjectValue)explodeCollectionValue).getAlias().getId());
            } else if (explodeCollectionValue.isConstant()) {
                inSource = new InComparandSource(bindingName,
                        new Comparisons.ValueComparison(Comparisons.Type.IN, explodeCollectionValue));
            } else {
                return;
            }
            sourcesBuilder.add(inSource);
        }

        final var inSources = sourcesBuilder.build();
        
        final var innerReference = innerQuantifier.getRangesOver();
        final var planPartitions =
                PlanPartition.rollUpTo(
                        innerReference.getPlanPartitions(),
                        OrderingProperty.ORDERING);

        final int attemptFailedInJoinAsUnionMaxSize = call.getContext().getPlannerConfiguration().getAttemptFailedInJoinAsUnionMaxSize();

        for (final var planPartition : planPartitions) {
            final var providedOrdering = planPartition.getAttributeValue(OrderingProperty.ORDERING);

            for (final var requestedOrdering : requestedOrderings) {
                if (requestedOrdering.isPreserve()) {
                    continue;
                }
                
                final var valueRequestedSortOrderMap = requestedOrdering.getValueRequestedSortOrderMap();
                final var adjustedBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
                for (final var entry : providedOrdering.getBindingMap().asMap().entrySet()) {
                    final var value = entry.getKey();
                    adjustedBindingMapBuilder.putAll(value,
                            adjustBindings(entry.getValue(), explodeAliases, valueRequestedSortOrderMap.get(value)));
                }

                final var adjustedBindingsMap = adjustedBindingMapBuilder.build();

                final var inUnionOrdering =
                        Ordering.UNION.createOrdering(adjustedBindingsMap, providedOrdering.getOrderingSet(),
                                providedOrdering.isDistinct());

                for (final var satisfyingComparisonKeyValues : inUnionOrdering.enumerateSatisfyingComparisonKeyValues(requestedOrdering)) {
                    final var directionalOrderingParts =
                            inUnionOrdering.directionalOrderingParts(satisfyingComparisonKeyValues,
                                    valueRequestedSortOrderMap, OrderingPart.ProvidedSortOrder.FIXED);
                    final var comparisonDirectionOptional = Ordering.resolveComparisonDirectionMaybe(directionalOrderingParts);

                    if (comparisonDirectionOptional.isPresent()) {
                        //
                        // At this point we know we can implement the distinct union over the partitions of compatibly ordered plans
                        //
                        final Quantifier.Physical newInnerQuantifier = Quantifier.physical(call.memoizeMemberPlans(innerReference, planPartition.getPlans()));
                        call.yieldExpression(
                                RecordQueryInUnionPlan.from(newInnerQuantifier,
                                        inSources,
                                        ImmutableList.copyOf(satisfyingComparisonKeyValues),
                                        comparisonDirectionOptional.get(),
                                        attemptFailedInJoinAsUnionMaxSize,
                                        CORRELATION));
                    }
                }
            }
        }
    }

    /**
     * Method that adjusts bindings that refer to the explode-aliases (the in-sources).
     * @param bindings original bindings for a value
     * @param explodeAliases a set of explode-aliases
     * @param requestedSortOrder a requested sort order
     * @return an iterable of bindings, either the input or adjusted if there is a singular fixed binding referring
     *         to an explode-alias.
     */
    @Nonnull
    private static Iterable<Binding> adjustBindings(@Nonnull final Collection<Binding> bindings,
                                                    @Nonnull final Set<CorrelationIdentifier> explodeAliases,
                                                    @Nullable final RequestedSortOrder requestedSortOrder) {
        final var sortOrder = Ordering.sortOrder(bindings);

        if (sortOrder.isDirectional()) {
            return ImmutableList.of(Binding.sorted(sortOrder));
        }

        Debugger.sanityCheck(() -> Verify.verify(Ordering.areAllBindingsFixed(bindings)));
        if (Ordering.hasMultipleFixedBindings(bindings)) {
            return bindings;
        }

        final var binding = Ordering.fixedBinding(bindings);
        final var comparison = binding.getComparison();

        if (comparison.getType() != Comparisons.Type.EQUALS) {
            return bindings;
        }

        if (comparison instanceof Comparisons.ParameterComparison) {
            final var parameterComparison = (Comparisons.ParameterComparison)comparison;
            if (!parameterComparison.isCorrelation() ||
                    !explodeAliases.containsAll(parameterComparison.getCorrelatedTo())) {
                return bindings;
            }
        } else if (comparison instanceof Comparisons.ValueComparison) {
            final var valueComparison = (Comparisons.ValueComparison)comparison;
            if (!explodeAliases.containsAll(valueComparison.getCorrelatedTo())) {
                return bindings;
            }
        } else {
            return bindings;
        }

        //
        // This binding can be promoted into a directional binding as it is currently bound by the source of
        // the in-union.
        //

        //
        // If we cannot infer the requested order we will defer that decision by indicating to choose the sort
        // order when we enumerate the comparison keys.
        //
        if (requestedSortOrder == null || requestedSortOrder == RequestedSortOrder.ANY) {
            return ImmutableList.of(Binding.choose());
        }

        if (!requestedSortOrder.isDirectional()) {
            return bindings;
        }

        return ImmutableList.of(Binding.sorted(requestedSortOrder.toProvidedSortOrder()));
    }
}
