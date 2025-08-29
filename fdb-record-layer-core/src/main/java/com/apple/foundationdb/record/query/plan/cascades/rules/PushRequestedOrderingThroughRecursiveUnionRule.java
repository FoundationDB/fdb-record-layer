/*
 * PushRequestedOrderingThroughRecursiveUnionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveUnionExpression;

/**
 * A rule that pushes an ordering {@link RequestedOrderingConstraint} through a {@link RecursiveUnionExpression}. Currently,
 * it only allows pushing {@link RequestedOrdering#preserve()} type of ordering to both the {@code Initial} and {@code Recursive}
 * legs.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushRequestedOrderingThroughRecursiveUnionRule extends AbstractCascadesRule<RecursiveUnionExpression> implements PreOrderRule {

    @Nonnull
    private static final BindingMatcher<Reference> initialRefMatcher = anyRef();

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> initialQunMatcher = forEachQuantifierOverRef(initialRefMatcher);

    @Nonnull
    private static final BindingMatcher<Reference> recursiveRefMatcher = anyRef();

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> recursiveQunMatcher = forEachQuantifierOverRef(recursiveRefMatcher);

    @Nonnull
    private static final BindingMatcher<RecursiveUnionExpression> root = recursiveUnionExpression(initialQunMatcher, recursiveQunMatcher);

    public PushRequestedOrderingThroughRecursiveUnionRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var requestedOrderingsOptional = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        // push only preserve requested ordering (if any).
        final var requestedOrderings = requestedOrderingsOptional.get();
        final var preserveOrderingMaybe =
                requestedOrderings
                        .stream()
                        .filter(RequestedOrdering::isPreserve)
                        .findFirst();
        if (preserveOrderingMaybe.isEmpty()) {
            return;
        }

        final var toBePushedOrdering = ImmutableSet.of(preserveOrderingMaybe.get());
        final var bindings = call.getBindings();
        final var initialRef = bindings.get(initialRefMatcher);
        call.pushConstraint(initialRef, RequestedOrderingConstraint.REQUESTED_ORDERING, toBePushedOrdering);
        final var recursiveRef = bindings.get(recursiveRefMatcher);
        call.pushConstraint(recursiveRef, RequestedOrderingConstraint.REQUESTED_ORDERING, toBePushedOrdering);
    }
}
