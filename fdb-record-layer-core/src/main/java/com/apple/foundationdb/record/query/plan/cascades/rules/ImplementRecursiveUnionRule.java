/*
 * ImplementRecursiveUnionRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveUnionPlan;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.levelTraversalIsAllowed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveUnionExpression;

/**
 * A rule that implements a {@link RecursiveUnionExpression}. Currently, the implementation translates the recursive
 * union expression verbatim to a corresponding {@link RecordQueryRecursiveUnionPlan} that has the same topological structure,
 * i.e. an {@code Initial} union leg used to seed the recursion, and a {@code Recursive} leg used to compute all recursive
 * results repeatedly until reaching a fix-point.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementRecursiveUnionRule extends ImplementationCascadesRule<RecursiveUnionExpression> {

    @Nonnull
    private static final BindingMatcher<PlanPartition> initialPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> initialQunMatcher =
            forEachQuantifierOverRef(planPartitions(rollUpPartitions(any(initialPlanPartitionsMatcher))));

    @Nonnull
    private static final BindingMatcher<PlanPartition> recursivePlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> recursiveQunMatcher =
            forEachQuantifierOverRef(planPartitions(rollUpPartitions(any(recursivePlanPartitionsMatcher))));

    @Nonnull
    private static final BindingMatcher<RecursiveUnionExpression> root = recursiveUnionExpression(initialQunMatcher, recursiveQunMatcher)
            .where(levelTraversalIsAllowed());

    public ImplementRecursiveUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var recursiveUnionExpression = bindings.get(root);

        final var initialPlanPartitions = bindings.get(initialPlanPartitionsMatcher);
        final var initialQun = bindings.get(initialQunMatcher);
        final var initialPhysicalQun = Quantifier.physical(call.memoizeMemberPlansFromOther(initialQun.getRangesOver(), initialPlanPartitions.getPlans()));

        final var recursivePlanPartitions = bindings.get(recursivePlanPartitionsMatcher);
        final var recursiveQun = bindings.get(recursiveQunMatcher);
        final var recursivePhysicalQun = Quantifier.physical(call.memoizeMemberPlansFromOther(recursiveQun.getRangesOver(), recursivePlanPartitions.getPlans()));

        final var tempTableScanValueReference = recursiveUnionExpression.getTempTableScanAlias();
        final var tempTableInsertValueReference = recursiveUnionExpression.getTempTableInsertAlias();
        final var recursiveUnionPlan = new RecordQueryRecursiveUnionPlan(initialPhysicalQun, recursivePhysicalQun, tempTableScanValueReference, tempTableInsertValueReference);

        call.yieldPlan(recursiveUnionPlan);
    }
}
