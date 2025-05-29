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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.canBeImplemented;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.SetMatcher.exactlyInAnyOrder;

/**
 * A rule that implements an existential nested loop join of its (already implemented) children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementNestedLoopJoinRule extends ImplementationCascadesRule<SelectExpression> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ImplementNestedLoopJoinRule.class);

    @Nonnull
    private static final BindingMatcher<PlanPartition> outerPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> outerReferenceMatcher =
            planPartitions(rollUpPartitions(all(outerPlanPartitionsMatcher)));
    @Nonnull
    private static final BindingMatcher<Quantifier> outerQuantifierMatcher = anyQuantifierOverRef(outerReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(rollUpPartitions(all(innerPlanPartitionsMatcher)));
    @Nonnull
    private static final BindingMatcher<Quantifier> innerQuantifierMatcher = anyQuantifierOverRef(innerReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(exactlyInAnyOrder(outerQuantifierMatcher, innerQuantifierMatcher)).where(canBeImplemented());

    public ImplementNestedLoopJoinRule() {
        // TODO figure out which constraints this rule should be sensitive to
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    @SuppressWarnings({"java:S135", "java:S2629", "checkstyle:VariableDeclarationUsageDistance", "PMD.GuardLogStatement"})
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var requestedOrderingsOptional = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requestedOrderingsOptional.get();
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("matched SelectExpression", "legs", selectExpression.getQuantifiers().size())));

        final var outerQuantifier = bindings.get(outerQuantifierMatcher);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);

        final var outerReference = bindings.get(outerReferenceMatcher);
        final var innerReference = bindings.get(innerReferenceMatcher);

        final var outerPartition = bindings.get(outerPlanPartitionsMatcher);
        final var innerPartition = bindings.get(innerPlanPartitionsMatcher);

        final var joinName = Debugger.mapDebugger(debugger -> debugger.nameForObject(call.getRoot()) + "[" + debugger.nameForObject(selectExpression) + "]: " + outerQuantifier.getAlias() + " ⨝ " + innerQuantifier.getAlias()).orElse("not in debug mode");
        Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("attempting join", "joinedTables", joinName, "requestedOrderings", requestedOrderings)));

        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();
        final var aliasToQuantifierMap = selectExpression.getAliasToQuantifierMap();

        final var outerAlias = outerQuantifier.getAlias();
        final var innerAlias = innerQuantifier.getAlias();

        final var outerDependencies = fullCorrelationOrder.get(outerAlias);
        if (outerDependencies.contains(innerAlias)) {
            // outer depends on inner, bail
            return;
        }

        //
        // Classify predicates according to their correlations. Some predicates are dependent on other aliases
        // within the current select expression, those are not eligible yet. Subtract those out to reach
        // the set of newly eligible predicates that can be applied as part of joining outer and inner.
        //
        final var outerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var outerInnerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedToInExpression =
                    Sets.intersection(predicate.getCorrelatedTo(), aliasToQuantifierMap.keySet());
            final var isEligible =
                    correlatedToInExpression.stream()
                            .allMatch(alias -> alias.equals(outerAlias) ||
                                               alias.equals(innerAlias));
            Verify.verify(isEligible);
            final var residualPredicate = predicate.toResidualPredicate();
            if (correlatedToInExpression.contains(innerAlias)) {
                outerInnerPredicatesBuilder.add(residualPredicate);
            } else {
                Verify.verify(correlatedToInExpression.contains(outerAlias) || correlatedToInExpression.isEmpty());
                outerPredicatesBuilder.add(residualPredicate);
            }
        }

        final List<QueryPredicate> outerPredicates = outerPredicatesBuilder.build();
        final List<QueryPredicate> outerInnerPredicates = outerInnerPredicatesBuilder.build();

        var outerRef = call.memoizeMemberPlansFromOther(outerReference, outerPartition.getPlans());

        if (outerQuantifier instanceof Quantifier.Existential) {
            outerRef = call.memoizePlan(
                    new RecordQueryFirstOrDefaultPlan(Quantifier.physicalBuilder().withAlias(outerAlias).build(outerRef),
                            new NullValue(outerQuantifier.getFlowedObjectType())));
        }  else if (outerQuantifier instanceof Quantifier.ForEach && ((Quantifier.ForEach)outerQuantifier).isNullOnEmpty()) {
            outerRef = call.memoizePlan(
                    new RecordQueryDefaultOnEmptyPlan(
                            Quantifier.physicalBuilder().withAlias(outerAlias).build(outerRef),
                            new NullValue(outerQuantifier.getFlowedObjectType())));
        }

        if (!outerPredicates.isEmpty()) {
            // create a new quantifier using a new alias
            final var newOuterLowerQuantifier = Quantifier.physicalBuilder().withAlias(outerAlias).build(outerRef);
            outerRef = call.memoizePlan(new RecordQueryPredicatesFilterPlan(newOuterLowerQuantifier, outerPredicates));
        }

        final var newOuterQuantifier =
                Quantifier.physicalBuilder().withAlias(outerAlias).build(outerRef);

        var innerRef =
                call.memoizeMemberPlansFromOther(innerReference, innerPartition.getPlans());

        if (innerQuantifier instanceof Quantifier.Existential) {
            innerRef = call.memoizePlan(new RecordQueryFirstOrDefaultPlan(Quantifier.physicalBuilder().withAlias(innerAlias).build(innerRef),
                    new NullValue(innerQuantifier.getFlowedObjectType())));
        }

        if (!outerInnerPredicates.isEmpty()) {
            final var newInnerLowerQuantifier = Quantifier.physicalBuilder().withAlias(innerAlias).build(innerRef);
            innerRef = call.memoizePlan(new RecordQueryPredicatesFilterPlan(newInnerLowerQuantifier, outerInnerPredicates));
        }

        final var newInnerQuantifier = Quantifier.physicalBuilder().withAlias(innerAlias).build(innerRef);

        call.yieldPlan(new RecordQueryFlatMapPlan(newOuterQuantifier, newInnerQuantifier,
                selectExpression.getResultValue(), innerQuantifier instanceof Quantifier.Existential));
    }
}
