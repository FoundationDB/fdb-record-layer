/*
 * PushInterestingOrderingThroughDistinctRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalDistinctExpression;

/**
 * A rule that pushes an interesting {@link OrderingAttribute} through a {@link LogicalDistinctExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushInterestingOrderingThroughDistinctRule extends PlannerRule<LogicalDistinctExpression> implements PreOrderRule {
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<LogicalDistinctExpression> root =
            logicalDistinctExpression(exactly(innerQuantifierMatcher));

    public PushInterestingOrderingThroughDistinctRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final Optional<Set<RequestedOrdering>> requestedOrderingOptionals = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (requestedOrderingOptionals.isEmpty()) {
            return;
        }

        final PlannerBindings bindings = call.getBindings();
        final ExpressionRef<? extends RelationalExpression> lowerRef = bindings.get(lowerRefMatcher);

        call.pushRequirement(lowerRef,
                OrderingAttribute.ORDERING,
                requestedOrderingOptionals.get());
    }
}
