/*
 * ImplementNestedLoopJoinRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.choose;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements a distinct union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementNestedLoopJoinRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> outerPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Quantifier> outerQuantifierMatcher = anyQuantifierOverRef(planPartitions(any(outerPlanPartitionsMatcher)));

    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Quantifier> innerQuantifierMatcher = anyQuantifierOverRef(planPartitions(any(innerPlanPartitionsMatcher)));

    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(choose(outerQuantifierMatcher, innerQuantifierMatcher));

    public ImplementNestedLoopJoinRule() {
        // TODO figure out which constraints this rule should be sensitive to
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var requiredOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requiredOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requiredOrderingsOptional.get();

        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);
        final var outerQuantifier = bindings.get(outerQuantifierMatcher);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);

        final var outerPartition = bindings.get(outerPlanPartitionsMatcher);
        final var innerPartition = bindings.get(innerPlanPartitionsMatcher);

        Debugger.withDebugger(debugger -> System.out.println("JOIN:" + debugger.nameForObject(outerQuantifier) + " ⨝ " + debugger.nameForObject(innerQuantifier)));

        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();

        final var outerDependencies = fullCorrelationOrder.get(outerQuantifier.getAlias());
        if (outerDependencies.contains(innerQuantifier.getAlias())) {
            // outer depends on inner, bail
            return;
        }

        //
        // Make sure that there is no other quantifier that is located "in the middle" between outer and inner,
        // as creating a join with that outer and inner combination would leave no place for the middle to be planned.
        //

        final var innerDependencies = fullCorrelationOrder.get(outerQuantifier.getAlias());

        for (final var quantifier : selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            if (fullCorrelationOrder.get(alias).contains(outerQuantifier.getAlias()) &&  // alias depends on outer
                    innerDependencies.contains(alias)) {                                 // inner depends on alias
                return;
            }
        }

    }
}
