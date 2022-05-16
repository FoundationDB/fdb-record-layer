/*
 * PushInterestingOrderingThroughUnionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.OrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
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
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalUnionExpression;

/**
 * A rule that pushes an ordering {@link OrderingConstraint} through a {@link LogicalUnionExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushInterestingOrderingThroughUnionRule extends PlannerRule<LogicalUnionExpression> implements PreOrderRule {
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<LogicalUnionExpression> root =
            logicalUnionExpression(all(innerQuantifierMatcher));

    public PushInterestingOrderingThroughUnionRule() {
        super(root, ImmutableSet.of(OrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final Optional<Set<RequestedOrdering>> requestedOrderingOptional = call.getPlannerConstraint(OrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingOptional.isEmpty()) {
            return;
        }

        final PlannerBindings bindings = call.getBindings();
        final List<? extends Quantifier.ForEach> rangesOverQuantifiers = bindings.getAll(innerQuantifierMatcher);

        rangesOverQuantifiers
                .stream()
                .map(Quantifier.ForEach::getRangesOver)
                .forEach(lowerReference ->
                        call.pushConstraint(lowerReference,
                                OrderingConstraint.REQUESTED_ORDERING,
                                requestedOrderingOptional.get()));
    }
}
