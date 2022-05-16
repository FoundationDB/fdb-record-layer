/*
 * ImplementExistentialNestedLoopJoinRule.java
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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.choose;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements an existential nested loop join of its (already implemented) children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementExistentialNestedLoopJoinRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ImplementExistentialNestedLoopJoinRule.class);

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

    public ImplementExistentialNestedLoopJoinRule() {
        // TODO figure out which constraints this rule should be sensitive to
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    @SuppressWarnings({"java:S135", "java:S2629"})
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requestedOrderingsOptional.get();

        final var bindings = call.getBindings();

        final var selectExpression = bindings.get(root);
        final var outerQuantifier = bindings.get(outerQuantifierMatcher);
        final var innerQuantifier = bindings.get(innerQuantifierMatcher);

        final var outerPartition = bindings.get(outerPlanPartitionsMatcher);
        final var innerPartition = bindings.get(innerPlanPartitionsMatcher);

        final var joinName = Debugger.mapDebugger(debugger -> debugger.nameForObject(outerQuantifier) + " ⨝ " + debugger.nameForObject(innerQuantifier)).orElse("not in debug mode");

        Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("attempting join", "joinedTables", "JOIN:" + joinName)));

        final var fullCorrelationOrder =
                selectExpression.getCorrelationOrder().getTransitiveClosure();
        final var aliasToQuantifierMap = selectExpression.getAliasToQuantifierMap();

        if (!(outerQuantifier instanceof Quantifier.ForEach &&
              innerQuantifier instanceof Quantifier.Existential)) {
            return;
        }

        final var outerAlias = outerQuantifier.getAlias();
        final var innerAlias = innerQuantifier.getAlias();

        final var outerDependencies = fullCorrelationOrder.get(outerAlias);
        if (outerDependencies.contains(innerAlias)) {
            logger.warn(KeyValueLogMessage.of("for each depends on existential using unexpected correlation"));
            Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("for each depends on existential outside of exists()",
                    "joinedTables", "JOIN:" + joinName)));
            // outer depends on inner, bail
            return;
        }

        //
        // Make sure that there is no other quantifier that is located "in the middle" between outer and inner,
        // as creating a join with that outer and inner combination would leave no place for the middle to be planned.
        //

        final var innerDependencies = fullCorrelationOrder.get(outerAlias);

        for (final var quantifier : selectExpression.getQuantifiers()) {
            final var alias = quantifier.getAlias();
            if (fullCorrelationOrder.get(alias).contains(outerAlias) &&  // alias depends on outer
                    innerDependencies.contains(alias)) {                 // inner depends on alias
                return;
            }
        }

        //
        // Classify predicates according to their correlations. Some predicates are dependent on other aliases
        // within the current select expression, those are not eligible yet. Subtract those out to reach
        // the set of newly eligible predicates that can be applied as part of joining outer and inner.
        //
        final var outerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var outerInnerPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var otherPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var predicate : selectExpression.getPredicates()) {
            final var correlatedToInExpression =
                    Sets.intersection(predicate.getCorrelatedTo(), aliasToQuantifierMap.keySet());
            final var isEligible =
                    correlatedToInExpression.stream()
                            .allMatch(alias -> alias.equals(outerAlias) ||
                                               alias.equals(innerAlias));
            if (isEligible) {
                final var residualPredicate = predicate.toResidualPredicate();
                if (correlatedToInExpression.contains(innerAlias)) {
                    if (!(predicate instanceof ExistsPredicate)) {
                        logger.warn(KeyValueLogMessage.of("predicate depends on existential but it is not an exist()"));
                        Debugger.withDebugger(debugger -> logger.debug(KeyValueLogMessage.of("predicate depends on existential but it is not an exist()",
                                "joinedTables", "JOIN:" + joinName)));
                        return;
                    }
                    outerInnerPredicatesBuilder.add(residualPredicate);
                } else {
                    Verify.verify(correlatedToInExpression.contains(outerAlias) || correlatedToInExpression.isEmpty());
                    outerPredicatesBuilder.add(residualPredicate);
                }
            } else {
                otherPredicatesBuilder.add(predicate);
            }
        }

        var outerPredicates = outerPredicatesBuilder.build();
        var outerInnerPredicates = outerInnerPredicatesBuilder.build();
        final var otherPredicates = otherPredicatesBuilder.build();

        Verify.verify(!outerInnerPredicates.isEmpty());
        // there can be more than one exists(), but they are all the same
        final var existsPredicate = outerInnerPredicates.get(0);

        //
        // Find and apply all predicates that can be applied as part of this nested loop join (outer ⨝ inner).
        //
        // There are three cases to consider:
        // 1. predicates that only depend on the outer: They can be applied before the join on the outer side.
        // 2. predicates that depend on outer and/or inner: They can be applied on top of the inner prior to the NLJN
        // 3. EXISTS(outer) and EXISTS(inner) which cause their own plan operator to be injected.
        //
        // If we are joining only for each quantifiers:
        //
        //                           SELECT
        //                            |  \\\\
        //                            |
        //         +--------------- NLJN ---------------+
        //        /                                      \
        //     FILTER outerPredicates               FILTER WHERE NOT NULL
        //       |                                        |
        //     outer                                FIRST inner OR null
        //                                                |
        //                                              inner
        //

        var newOuterQuantifier =
                Quantifier.physicalBuilder().withAlias(outerAlias).build(GroupExpressionRef.from(outerPartition.getPlans()));
        if (!outerPredicates.isEmpty()) {
            newOuterQuantifier =
                    Quantifier.physical(GroupExpressionRef.of(new RecordQueryPredicatesFilterPlan(newOuterQuantifier, outerPredicates)));
        }

        var newInnerQuantifier =
                Quantifier.physicalBuilder().withAlias(innerAlias).build(GroupExpressionRef.from(innerPartition.getPlans()));
        Verify.verify(outerInnerPredicates.size() == 1);
        final QueryPredicate outerInnerPredicate = Iterables.getOnlyElement(outerInnerPredicates);
        Verify.verify(outerInnerPredicate instanceof ExistsPredicate);

        newInnerQuantifier =
                Quantifier.physical(GroupExpressionRef.of(new RecordQueryFirstOrDefaultPlan(newInnerQuantifier, new NullValue(innerQuantifier.getFlowedObjectType()))));

        outerInnerPredicates = ImmutableList.of(new ValuePredicate(QuantifiedObjectValue.of(newInnerQuantifier.getAlias(), newInnerQuantifier.getFlowedObjectType()),
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL)));

        newInnerQuantifier =
                Quantifier.physical(GroupExpressionRef.of(new RecordQueryPredicatesFilterPlan(newInnerQuantifier, outerInnerPredicates)));

        final var outerValue = QuantifiedObjectValue.of(newOuterQuantifier.getAlias(), outerQuantifier.getFlowedObjectType());
        final var innerValue = QuantifiedObjectValue.of(newInnerQuantifier.getAlias(), innerQuantifier.getFlowedObjectType());

        final var joinedResultValue = outerQuantifier.getFlowedObjectValue();

        final var joinedAlias = CorrelationIdentifier.uniqueID();
        final var joinedQuantifier =
                Quantifier.forEachBuilder()
                        .withAlias(joinedAlias)
                        .build(GroupExpressionRef.of(
                                new RecordQueryFlatMapPlan(newOuterQuantifier, newInnerQuantifier, joinedResultValue, true)));

        //
        // Translate all the references to outer to the joined alias
        //

        final var remainingQuantifiers =
                selectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier != innerQuantifier && quantifier != outerQuantifier)
                        .map(quantifier -> (Quantifier)quantifier)
                        .collect(ImmutableList.toImmutableList());

        final var translationMap = TranslationMap.rebaseWithAliasMap(AliasMap.of(outerAlias, joinedAlias));

        final var translatedQuantifiers = Quantifiers.translateCorrelations(remainingQuantifiers, translationMap);

        final var newQuantifiers =
                ImmutableList.<Quantifier>builder()
                        .addAll(translatedQuantifiers)
                        .add(joinedQuantifier)
                        .build();

        final var newPredicates =
                otherPredicates.stream()
                        .map(otherPredicate ->
                                otherPredicate.translateCorrelations(translationMap))
                        .collect(ImmutableList.toImmutableList());

        final var resultValue = selectExpression.getResultValue();
        final var newResultValue =
                resultValue.translateCorrelations(translationMap);

        final var of = GroupExpressionRef.of(new SelectExpression(newResultValue, newQuantifiers, newPredicates));
        of.show(false);
        call.yield(of);
    }
}
