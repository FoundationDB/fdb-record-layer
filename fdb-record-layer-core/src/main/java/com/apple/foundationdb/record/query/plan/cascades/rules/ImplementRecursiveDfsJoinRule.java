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
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveDfsJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.hasNoPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.preOrderTraversalIsAllowed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableInsertPlanOverQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.exploratoryMember;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.finalMember;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveUnionExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tempTableInsertExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tempTableScanExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher.exactlyInAnyOrder;

@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementRecursiveDfsJoinRule extends ImplementationCascadesRule<RecursiveUnionExpression> {

    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> initialInnerPlanMatcher = anyPlan();

    @Nonnull
    private static final BindingMatcher<Reference> initialInnerPlanRefMatcher = finalMember(initialInnerPlanMatcher);

    @Nonnull
    private static final BindingMatcher<TempTableInsertPlan> initialPlanMatcher = tempTableInsertPlanOverQuantifier(physicalQuantifierOverRef(initialInnerPlanRefMatcher));

    @Nonnull
    private static final BindingMatcher<PlanPartition> recursiveInnerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Collection<PlanPartition>> recursiveInnerPlanPartitionsMatcher = rollUpPartitions(any(recursiveInnerPlanPartitionMatcher));

    @Nonnull
    private static final BindingMatcher<Reference> recursiveInnerReferenceMatcher = planPartitions(recursiveInnerPlanPartitionsMatcher);

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> recursiveInnerQunMatcher = forEachQuantifierOverRef(recursiveInnerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<SelectExpression> recursiveSelectFromTempTableScanMatcher = selectExpression(forEachQuantifier(tempTableScanExpression())).where(hasNoPredicates());

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> recursiveSelectFromTempTableScanQunMatcher = forEachQuantifier(recursiveSelectFromTempTableScanMatcher);

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> recursiveTempTableScanQunMatcher = forEachQuantifier(tempTableScanExpression());

    // Match temp table scan expressions with or without a select layer on top.
    // This accommodates a known limitation where select merge cannot merge correlated selects
    // with the recursive union's upper select. See: https://github.com/FoundationDB/fdb-record-layer/issues/3649
    // When resolved, this pattern can be simplified to match only temp table scan expressions.
    @Nonnull
    private static final BindingMatcher<SelectExpression> recursiveInnerSelectMatcher = selectExpression(
            exactlyInAnyOrder(recursiveInnerQunMatcher, recursiveTempTableScanQunMatcher.or(recursiveSelectFromTempTableScanQunMatcher)));

    @Nonnull
    private static final BindingMatcher<TempTableInsertExpression> recursiveSelectExpressionMatcher = tempTableInsertExpression(
            forEachQuantifierOverRef(exploratoryMember(recursiveInnerSelectMatcher)));

    @Nonnull
    private static final BindingMatcher<RecursiveUnionExpression> recursiveUnionExpressionMatcher = recursiveUnionExpression(forEachQuantifier(initialPlanMatcher),
            forEachQuantifier(recursiveSelectExpressionMatcher)).where(preOrderTraversalIsAllowed());

    public ImplementRecursiveDfsJoinRule() {
        super(recursiveUnionExpressionMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var recursiveUnionExpression = call.get(recursiveUnionExpressionMatcher);
        final var initialStateAlias = recursiveUnionExpression.getInitialStateQuantifier().getAlias();
        final var recursiveStateAlias = recursiveUnionExpression.getRecursiveStateQuantifier().getAlias();

        // In case there is a SELECT on top of the temp table scan expression, check whether the SELECT corresponds
        // to a simple SELECT * that can be ignored.
        // Note: this check will be removed once we have a fix for https://github.com/FoundationDB/fdb-record-layer/issues/3649
        // because the select-merge rewrite tool would've already merged this SELECT with the upper SELECT.
        if (call.getBindings().containsKey(recursiveSelectFromTempTableScanQunMatcher)) {
            final var selectFromTempTableScan = call.get(recursiveSelectFromTempTableScanMatcher);
            final var output = selectFromTempTableScan.getResultValue();
            var isSimpleSelect = QuantifiedObjectValue.isSimpleQuantifiedObjectValueOver(output, Iterables.getOnlyElement(selectFromTempTableScan.getQuantifiers()).getAlias())
                    || (output instanceof RecordConstructorValue && Values.collapseSimpleSelectMaybe((RecordConstructorValue)output).isPresent());
            if (!isSimpleSelect) {
                return;
            }
        }

        final var initialInnerPlan = call.get(initialInnerPlanMatcher);
        final var initialInnerPlanRef = call.get(initialInnerPlanRefMatcher);

        final var recursiveInnerSelect = call.get(recursiveInnerSelectMatcher);
        final var innerPlanPartition = call.get(recursiveInnerPlanPartitionMatcher);
        final var innerRef = call.get(recursiveInnerReferenceMatcher);
        final var innerQun = call.get(recursiveInnerQunMatcher);
        final var priorValueCorrelation = (call.getBindings().containsKey(recursiveTempTableScanQunMatcher)
                                          ? call.get(recursiveTempTableScanQunMatcher)
                                          : call.get(recursiveSelectFromTempTableScanQunMatcher)).getAlias();

        final var rootPlanRef = call.memoizeMemberPlansFromOther(initialInnerPlanRef, List.of(initialInnerPlan));
        final var rootQun = Quantifier.physical(rootPlanRef, initialStateAlias);

        final var recursivePlanRef = ImplementSimpleSelectRule.implementSelectExpression(call, recursiveInnerSelect.getResultValue(),
                recursiveInnerSelect.getPredicates(), innerRef, innerQun, innerPlanPartition).reference();
        final var recursiveQun = Quantifier.physical(recursivePlanRef, recursiveStateAlias);

        call.yieldPlan(new RecordQueryRecursiveDfsJoinPlan(rootQun, recursiveQun, priorValueCorrelation));
    }
}
