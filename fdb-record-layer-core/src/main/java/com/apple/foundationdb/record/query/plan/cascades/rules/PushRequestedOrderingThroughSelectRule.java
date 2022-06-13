/*
 * PushRequestedOrderingThroughSelectRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ReferencedFieldsConstraint;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that pushes a {@link ReferencedFieldsConstraint} through a {@link SelectExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushRequestedOrderingThroughSelectRule extends PlannerRule<SelectExpression> implements PreOrderRule {
    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(any(innerQuantifierMatcher));

    public PushRequestedOrderingThroughSelectRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var isInnerQuantifierOnlyForEach =
                selectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .allMatch(quantifier -> quantifier == innerQuantifier);

        final var requestedOrderings =
                call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING)
                        .orElse(ImmutableSet.of());

        if (isInnerQuantifierOnlyForEach) {
            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    requestedOrderings);
        } else {
            if (requestedOrderings.stream().anyMatch(RequestedOrdering::isPreserve)) {
                call.pushConstraint(lowerRef,
                        RequestedOrderingConstraint.REQUESTED_ORDERING,
                        ImmutableSet.of(RequestedOrdering.preserve()));
            }
        }
    }
}
