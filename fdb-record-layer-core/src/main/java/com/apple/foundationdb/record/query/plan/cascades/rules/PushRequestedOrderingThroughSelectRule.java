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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ReferencedFieldsConstraint;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
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
public class PushRequestedOrderingThroughSelectRule extends CascadesRule<SelectExpression> implements PreOrderRule {
    @Nonnull
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(any(innerQuantifierMatcher));

    public PushRequestedOrderingThroughSelectRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);
        final var lowerRef = bindings.get(lowerRefMatcher);

        final var requestedOrderings =
                call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING)
                        .orElse(ImmutableSet.of());

        final var resultValue = selectExpression.getResultValue();
        final var toBePushedRequestedOrderingsBuilder = ImmutableSet.<RequestedOrdering>builder();
        for (final var requestedOrdering : requestedOrderings) {
            if (requestedOrdering.isPreserve()) {
                toBePushedRequestedOrderingsBuilder.add(RequestedOrdering.preserve());
            } else {
                toBePushedRequestedOrderingsBuilder.add(
                        requestedOrdering.pushDown(resultValue,
                                innerQuantifier.getAlias(),
                                call.getEvaluationContext(),
                                AliasMap.emptyMap(),
                                selectExpression.getCorrelatedTo()));
            }
        }

        call.pushConstraint(lowerRef,
                RequestedOrderingConstraint.REQUESTED_ORDERING,
                toBePushedRequestedOrderingsBuilder.build());
    }
}
