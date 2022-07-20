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

import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.groupByExpression;

/**
 * Rule that passes sorting requirements from {@code GROUP BY} expression downstream. This limits the search space of available
 * access paths to the ones having a compatible sort order allowing physical grouping operator later on to generate the correct results.
 */
public class PushRequestedOrderingThroughGroupByRule extends PlannerRule<GroupByExpression> implements PlannerRule.PreOrderRule  {

    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<GroupByExpression> root =
            groupByExpression(any(innerQuantifierMatcher));

    public PushRequestedOrderingThroughGroupByRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final PlannerRuleCall call) {
        final var bindings = call.getBindings();
        final var groupByExpression = bindings.get(root);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var requestedOrderings =
                call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING)
                        .orElse(ImmutableSet.of());

        final var refinedRequestedOrderings = collectCompatibleOrderings(groupByExpression, requestedOrderings);

        if (!refinedRequestedOrderings.isEmpty()) {
            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    refinedRequestedOrderings);
        }
    }

    @Nonnull
    private Set<RequestedOrdering> collectCompatibleOrderings(@Nonnull final GroupByExpression groupByExpression, @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final var groupByOrdering = groupByExpression.getOrderingRequirement();
        // case 1: if no ordering is required, simply specify the group by ordering as a requirement.
        if (requestedOrderings.isEmpty()) {
            return Set.of(groupByOrdering);
        }
        final var groupByOrderingAsSet = new LinkedHashSet<>(groupByOrdering.getOrderingKeyParts());
        ImmutableSet.Builder<RequestedOrdering> result = ImmutableSet.builder();
        for (final var requestedOrdering : requestedOrderings) {
            if (requestedOrdering.getDistinctness() == RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS) {
                // ignore preserve as it might cause e.g. an underlying FUSE operator to be substituted with an access path that is incompatibly-ordered.
                continue; // ignore
            }
            // todo this looks too strict, if requested ordering is more specific (e.g. (a,b,c)) than the requested group by ordering (e.g. (a,b))
            // it should still be accepted.
            if (!new LinkedHashSet<>(requestedOrdering.getOrderingKeyParts()).equals(groupByOrderingAsSet)) {
                // case 2: if an incompatible ordering is found, fail.
                return Set.of(); // fail
            } else {
                result.add(requestedOrdering);
            }
        }
        result.add(groupByOrdering);
        // case 3: return the group by ordering in addition to any other compatible ordering required from earlier steps.
        return result.build();
    }
}
