/*
 * PushRequestedOrderingThroughSortRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;

/**
 * A rule that pushes an ordering {@link RequestedOrderingConstraint} through an {@link UpdateExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushRequestedOrderingThroughUpdateRule extends CascadesRule<UpdateExpression> implements PreOrderRule {
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<UpdateExpression> root =
            RelationalExpressionMatchers.updateExpression(innerQuantifierMatcher);

    public PushRequestedOrderingThroughUpdateRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requestedOrderingsOptional.get();

        final var bindings = call.getBindings();

        final var updateExpression = bindings.get(root);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var pushedRequestedOrderingsBuilder = ImmutableSet.<RequestedOrdering>builder();

        for (final var requestedOrdering : requestedOrderings) {
            if (requestedOrdering.isPreserve()) {
                pushedRequestedOrderingsBuilder.add(requestedOrdering);
            } else {
                pushedRequestedOrderingsBuilder.add(
                        requestedOrdering.pushDown(UpdateExpression.makeComputationValue(innerQuantifier, updateExpression.getTargetType()),
                                innerQuantifier.getAlias(),
                                AliasMap.emptyMap(),
                                updateExpression.getCorrelatedTo()));
            }
        }

        call.pushConstraint(lowerRef,
                RequestedOrderingConstraint.REQUESTED_ORDERING,
                pushedRequestedOrderingsBuilder.build());
    }
}
