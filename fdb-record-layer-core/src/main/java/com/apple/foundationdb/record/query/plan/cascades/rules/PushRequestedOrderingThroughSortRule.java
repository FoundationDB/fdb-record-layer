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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalSortExpression;

/**
 * A rule that pushes an ordering {@link RequestedOrderingConstraint} through a {@link LogicalSortExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushRequestedOrderingThroughSortRule extends CascadesRule<LogicalSortExpression> implements PreOrderRule {
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    private static final BindingMatcher<LogicalSortExpression> root =
            logicalSortExpression(exactly(innerQuantifierMatcher));

    public PushRequestedOrderingThroughSortRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final LogicalSortExpression logicalSortExpression = bindings.get(root);
        final Quantifier.ForEach innerQuantifier = bindings.get(innerQuantifierMatcher);
        final Reference lowerRef = bindings.get(lowerRefMatcher);

        final RequestedOrdering requestedOrdering = logicalSortExpression.getOrdering();
        if (requestedOrdering.isPreserve()) {
            // No translation needed.
            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    Set.of(requestedOrdering));
        } else {
            final AliasMap translationMap = AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current());

            final ImmutableList.Builder<RequestedOrderingPart> translatedBuilder = ImmutableList.builder();
            for (final var orderingPart : requestedOrdering.getOrderingParts()) {
                translatedBuilder.add(new RequestedOrderingPart(orderingPart.getValue().rebase(translationMap), orderingPart.getSortOrder()));
            }

            final var orderings =
                    Set.of(new RequestedOrdering(translatedBuilder.build(), requestedOrdering.getDistinctness()));

            call.pushConstraint(lowerRef,
                    RequestedOrderingConstraint.REQUESTED_ORDERING,
                    orderings);
        }
    }
}
