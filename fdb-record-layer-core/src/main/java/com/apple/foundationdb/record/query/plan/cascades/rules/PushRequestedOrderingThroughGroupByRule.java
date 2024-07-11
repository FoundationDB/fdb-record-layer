/*
 * PushRequestedOrderingThroughGroupByRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering.Distinctness;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.groupByExpression;

/**
 * Rule that passes sorting requirements from {@code GROUP BY} expression downstream. This limits the search space of available
 * access paths to the ones having a compatible sort order allowing physical grouping operator later on to generate the correct results.
 */
public class PushRequestedOrderingThroughGroupByRule extends CascadesRule<GroupByExpression> implements CascadesRule.PreOrderRule  {

    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<GroupByExpression> root =
            groupByExpression(innerQuantifierMatcher);

    public PushRequestedOrderingThroughGroupByRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var groupByExpression = bindings.get(root);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var requestedOrderings =
                call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING)
                        .orElse(ImmutableSet.of());

        final var refinedRequestedOrderings = collectCompatibleOrderings(groupByExpression, innerQuantifier, requestedOrderings);

        if (!refinedRequestedOrderings.isEmpty()) {
            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    refinedRequestedOrderings);
        }
    }

    @Nonnull
    private Set<RequestedOrdering> collectCompatibleOrderings(@Nonnull final GroupByExpression groupByExpression,
                                                              @Nonnull final Quantifier innerQuantifier,
                                                              @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final var correlatedTo = groupByExpression.getCorrelatedTo();
        final var resultValue = groupByExpression.getResultValue(); // full result value
        final var groupingValue = groupByExpression.getGroupingValue();
        final var currentGroupingValue = groupingValue == null ? null : groupingValue.rebase(AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current()));

        final var toBePushedRequestedOrderingsBuilder = ImmutableSet.<RequestedOrdering>builder();
        for (final var requestedOrdering : requestedOrderings) {
            if (requestedOrdering.isPreserve()) {
                if (groupingValue == null || groupingValue.isConstant()) {
                    toBePushedRequestedOrderingsBuilder.add(RequestedOrdering.preserve());
                } else {
                    final var orderingParts =
                            Values.primitiveAccessorsForType(currentGroupingValue.getResultType(), () -> currentGroupingValue, correlatedTo)
                                    .stream()
                                    .map(orderingValue -> new RequestedOrderingPart(orderingValue, RequestedSortOrder.ANY))
                                    .collect(ImmutableList.toImmutableList());

                    toBePushedRequestedOrderingsBuilder.add(new RequestedOrdering(orderingParts, Distinctness.PRESERVE_DISTINCTNESS));
                }
            } else {
                //
                // Synthesize the requested ordering from above and the grouping columns
                //

                //
                // Push the requested ordering through the result value. We need to do that in any case.
                //
                final var pushedRequestedOrdering =
                        requestedOrdering.pushDown(resultValue, innerQuantifier.getAlias(), AliasMap.emptyMap(), correlatedTo);

                if (groupingValue == null || groupingValue.isConstant()) {
                    //
                    // No grouping or constant grouping, there is only 0-1 record(s), and that can naturally be
                    // considered as ordered in any way needed.
                    //
                    toBePushedRequestedOrderingsBuilder.add(RequestedOrdering.preserve());
                } else {
                    //
                    // We have a requested ordering as well as a grouping. Today, we push down the grouping by key parts
                    // in the order they were written (or constructed). The exception is if a requested ordering is
                    // also considered (this case). So for instance:
                    //
                    //   requested ordering: a, b
                    // and
                    //   group by b, a, c, d
                    // results in
                    //   pushed requested ordering: a, b, c, d
                    //
                    // TODO we should push down all permutations
                    //
                    // Note in the remainder we use the terminology 'requested...' for the ordering imposed on this
                    // expression by a constraint push, and 'required...' for the ordering imposed by the grouping
                    // columns of this expression.
                    //
                    
                    // create a mutable set of required ordering values
                    final var requiredOrderingValues =
                            new LinkedHashSet<>(Values.primitiveAccessorsForType(currentGroupingValue.getResultType(),
                                    () -> currentGroupingValue, correlatedTo));

                    final var resultOrderingPartsBuilder = ImmutableList.<RequestedOrderingPart>builder();
                    boolean isPushedAndRequiredCompatible = true;
                    for (final var pushedRequestedOrderingPart : pushedRequestedOrdering.getOrderingParts()) {
                        if (requiredOrderingValues.remove(pushedRequestedOrderingPart.getValue())) {
                            resultOrderingPartsBuilder.add(pushedRequestedOrderingPart);
                        } else {
                            isPushedAndRequiredCompatible = false;
                            break;
                        }
                    }

                    if (isPushedAndRequiredCompatible) {
                        if (!pushedRequestedOrdering.isDistinct() || requiredOrderingValues.isEmpty()) {
                            // add the remaining key parts from the required set
                            requiredOrderingValues
                                    .stream()
                                    .map(value -> new RequestedOrderingPart(value, RequestedSortOrder.ASCENDING))
                                    .forEach(resultOrderingPartsBuilder::add);
                            toBePushedRequestedOrderingsBuilder.add(new RequestedOrdering(resultOrderingPartsBuilder.build(), pushedRequestedOrdering.getDistinctness()));
                        }
                    }
                }

            }
        }

        return toBePushedRequestedOrderingsBuilder.build();
    }
}
