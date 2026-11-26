/*
 * PushRequestedOrderingThroughWindowRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.windowExpression;

/**
 * Rule that passes the arrangement a {@link WindowExpression} needs downstream, the way
 * {@link PushRequestedOrderingThroughGroupByRule} does for a {@code GROUP BY}. A window function can only be computed
 * over an input that is clustered by the {@code PARTITION BY} columns and, within a partition, sorted the way the window
 * asks for, so pushing that requirement down narrows the access paths to the ones that can provide it.
 *
 * <p>An ordering requested from above is combined with that requirement rather than replacing it: its parts have to be
 * partitioning columns, in which case they lead and the remaining partitioning columns follow in any order, and the
 * window's own ordering parts come last because they only order rows within a partition. An ordering that asks for
 * anything else cannot be combined and is dropped.
 */
public class PushRequestedOrderingThroughWindowRule extends AbstractCascadesRule<WindowExpression>
        implements AbstractCascadesRule.PreOrderRule {

    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher =
            forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<WindowExpression> root = windowExpression(innerQuantifierMatcher);

    public PushRequestedOrderingThroughWindowRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var windowExpression = bindings.get(root);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var requestedOrderings = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING)
                        .orElse(ImmutableSet.of());

        if (requestedOrderings.isEmpty()) {
            return;
        }

        final var refinedRequestedOrderings =
                collectCompatibleOrderings(call.getEvaluationContext(), windowExpression, innerQuantifier,
                        requestedOrderings);

        if (!refinedRequestedOrderings.isEmpty()) {
            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    refinedRequestedOrderings);
        }
    }

    @Nonnull
    private Set<RequestedOrdering> collectCompatibleOrderings(@Nonnull final EvaluationContext evaluationContext,
                                                             @Nonnull final WindowExpression windowExpression,
                                                             @Nonnull final Quantifier innerQuantifier,
                                                             @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final var correlatedTo = windowExpression.getCorrelatedTo();
        final var toCurrentAliasMap = AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current());
        final var remappedPartitioningValues = windowExpression.getPartitioningValues().stream()
                .map(v -> v.rebase(toCurrentAliasMap)).collect(ImmutableList.toImmutableList());
        final var remappedWindowOrderingParts = windowExpression.getWindowOrderingParts().stream()
                .map(p -> p.rebase(toCurrentAliasMap)).collect(ImmutableList.toImmutableList());

        final var toBePushedRequestedOrderingsBuilder = ImmutableSet.<RequestedOrdering>builder();
        for (final var requestedOrdering : requestedOrderings) {
            if (requestedOrdering.isPreserve()) {
                // there are no requested orderings coming from the top. Refine the requested ordering based solely on the
                // ordering requirements of the window expression itself.
                toBePushedRequestedOrderingsBuilder.add(windowExpression.getRequestedOrdering());
                continue;
            }

            //
            // A window expression flows its input's rows, so an ordering requested from above travels through its
            // result value first.
            //
            final var pushedRequestedOrdering =
                    requestedOrdering.pushDown(windowExpression.getResultValue(), innerQuantifier.getAlias(),
                            evaluationContext, AliasMap.emptyMap(), correlatedTo);

            // The leading columns have to be the partitioning ones, or the two requirements cannot be satisfied at
            // once. Whichever of them the ordering from above names come first, in its order and with its sort orders;
            // the rest follow in any order.
            // this is currently limited since we're not pushing down all permutations, in the same way that grouped
            // aggregate planning is, resulting in planning limitation https://github.com/FoundationDB/fdb-record-layer/issues/4831,
            // fix of this issue should be leveraged to address this limitation.
            final var remainingPartitioningValues = new LinkedHashSet<>(remappedPartitioningValues);
            final var refinedOrderingParts = ImmutableList.<RequestedOrderingPart>builder();
            var isCompatible = true;
            for (final var pushedPart : pushedRequestedOrdering.getOrderingParts()) {
                if (remainingPartitioningValues.remove(pushedPart.getValue())) {
                    refinedOrderingParts.add(pushedPart);
                } else {
                    isCompatible = false;
                    break;
                }
            }
            if (!isCompatible) {
                continue;
            }
            if (pushedRequestedOrdering.isDistinct() && !remainingPartitioningValues.isEmpty()) {
                continue;
            }
            remainingPartitioningValues.forEach(v -> refinedOrderingParts.add(new RequestedOrderingPart(v, RequestedSortOrder.ANY)));
            remappedWindowOrderingParts.forEach(p -> refinedOrderingParts.add(p.toRequestedOrderingPart()));
            toBePushedRequestedOrderingsBuilder.add(
                    RequestedOrdering.ofPrimitiveParts(refinedOrderingParts.build(),
                            pushedRequestedOrdering.getDistinctness(), false));
        }

        return toBePushedRequestedOrderingsBuilder.build();
    }
}
