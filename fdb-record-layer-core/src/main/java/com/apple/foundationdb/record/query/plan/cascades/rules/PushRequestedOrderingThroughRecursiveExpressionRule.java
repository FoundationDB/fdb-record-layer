/*
 * PushRequestedOrderingThroughRecursiveRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveExpression;

/**
 * A rule that pushes an ordering {@link RequestedOrderingConstraint} through a {@link RecursiveExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public class PushRequestedOrderingThroughRecursiveExpressionRule extends CascadesRule<RecursiveExpression> implements PreOrderRule {
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<RecursiveExpression> root =
            recursiveExpression(all(innerQuantifierMatcher));

    public PushRequestedOrderingThroughRecursiveExpressionRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final Optional<Set<RequestedOrdering>> requestedOrderingOptional = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingOptional.isEmpty()) {
            return;
        }

        // TODO: This isn't right. I think we only can do this if the requested ordering is empty, since the output order will
        //  be the cursor's pre-order. We do need that case so that the two child quantifiers get plans from the data access rule.

        final PlannerBindings bindings = call.getBindings();
        final List<? extends Quantifier.ForEach> rangesOverQuantifiers = bindings.getAll(innerQuantifierMatcher);

        rangesOverQuantifiers
                .stream()
                .map(Quantifier.ForEach::getRangesOver)
                .forEach(lowerReference ->
                        call.pushConstraint(lowerReference,
                                RequestedOrderingConstraint.REQUESTED_ORDERING,
                                requestedOrderingOptional.get()));
    }
}
