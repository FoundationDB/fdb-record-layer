/*
 * ImplementRecursiveDfsUnionRule.java
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

import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursivePlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;

import javax.annotation.Nonnull;

import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.preOrderTraversalIsAllowed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableInsertPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableScanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.exploratoryMember;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveUnionExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tempTableInsertExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher.exactlyInAnyOrder;

@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementRecursiveDfsUnionRule extends ImplementationCascadesRule<RecursiveUnionExpression> {

    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> initialInnerPlanMatcher = anyPlan();

    @Nonnull
    private static final BindingMatcher<TempTableInsertPlan> initialPlanMatcher = tempTableInsertPlan(initialInnerPlanMatcher);

    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Collection<PlanPartition>> innerPlanPartitionsMatcher = rollUpPartitions(any(innerPlanPartitionMatcher));

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher = planPartitions(innerPlanPartitionsMatcher);

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQunMatcher = forEachQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> mapTempTableQunMatcher = forEachQuantifier(mapPlan(tempTableScanPlan()));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> tempTableQunMatcher = forEachQuantifier(tempTableScanPlan());

    @Nonnull
    private static final BindingMatcher<SelectExpression> recursiveInnerSelectMatcher = selectExpression(
            exactlyInAnyOrder(innerQunMatcher, tempTableQunMatcher.or(mapTempTableQunMatcher)));

    @Nonnull
    private static final BindingMatcher<TempTableInsertExpression> recursivePlanMatcher = tempTableInsertExpression(
            forEachQuantifierOverRef(exploratoryMember(recursiveInnerSelectMatcher)));

    @Nonnull
    private static final BindingMatcher<RecursiveUnionExpression> root = recursiveUnionExpression(forEachQuantifier(initialPlanMatcher),
            forEachQuantifier(recursivePlanMatcher)).where(preOrderTraversalIsAllowed());

    public ImplementRecursiveDfsUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var recursiveUnion = call.get(root);
        final var rootAlias = recursiveUnion.getInitialStateQuantifier().getAlias();
        final var recursiveAlias = recursiveUnion.getRecursiveStateQuantifier().getAlias();

        final var initialInnerPlan = call.get(initialInnerPlanMatcher);
        final var recursiveInnerSelect = call.get(recursiveInnerSelectMatcher);
        final var innerPlanPartition = call.get(innerPlanPartitionMatcher);
        final var innerRef = call.get(innerReferenceMatcher);
        final var innerQun = call.get(innerQunMatcher);
        final var priorValueCorrelation = (call.getBindings().containsKey(tempTableQunMatcher)
                                          ? call.get(tempTableQunMatcher)
                                          : call.get(mapTempTableQunMatcher)).getAlias();

        final var rootPlanRef = call.memoizePlan(initialInnerPlan);
        final var rootQun = Quantifier.physical(rootPlanRef, rootAlias);

        final var recursivePlanRef = ImplementSimpleSelectRule.implementSelectExpression(call, recursiveInnerSelect.getResultValue(),
                recursiveInnerSelect.getPredicates(), innerRef, innerQun, innerPlanPartition).reference();
        final var recursiveQun = Quantifier.physical(recursivePlanRef, recursiveAlias);

        call.yieldPlan(new RecordQueryRecursivePlan(rootQun, recursiveQun, priorValueCorrelation));
    }
}
