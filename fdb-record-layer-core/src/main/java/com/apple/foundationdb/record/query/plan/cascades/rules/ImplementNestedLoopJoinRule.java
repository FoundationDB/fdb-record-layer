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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartitions;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Function;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitionsTo;
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
            planPartitions(rollUpPartitionsTo(all(outerPlanPartitionsMatcher), ImmutableSet.of(OrderingProperty.ordering())));
    @Nonnull
    private static final BindingMatcher<Quantifier> outerQuantifierMatcher = anyQuantifierOverRef(outerReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(rollUpPartitionsTo(all(innerPlanPartitionsMatcher), OrderingProperty.ordering()));
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
            if (predicate.isIndexOnly()) {
                return;
            }
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

        //
        // The ordering of the flat map is based on the outer, unless the outer has max cardinality one, in
        // which case it is based on the inner. Separate out the outer plan partitions to handle those two
        // cases
        //
        final NonnullPair<List<PlanPartition>, List<PlanPartition>> outerPlanPartitionsByCardinality =
                separateByMaxCardinalityOne(outerQuantifier, bindings.getAll(outerPlanPartitionsMatcher));
        final List<PlanPartition> outerMaxCardinalityOnePartitions = outerPlanPartitionsByCardinality.getLeft();
        final List<PlanPartition> outerMaxCardinalityNonOnePartitions = outerPlanPartitionsByCardinality.getRight();

        for (RequestedOrdering requestedOrdering : requestedOrderings) {
            // Case 1: Ordering is based on the inner
            for (PlanPartition outerPlanPartition : outerMaxCardinalityOnePartitions) {
                final Quantifier.Physical newOuterQuantifier = planPartitionToPhysical(call, outerQuantifier, outerReference, outerPredicates, outerPlanPartition);
                for (PlanPartition innerPlanPartition : rollUpIfSatisfyOrdering(requestedOrdering, innerQuantifier, bindings.getAll(innerPlanPartitionsMatcher), Ordering.empty(),
                        o -> pullUpOrderingFromSelectChild(o, selectExpression, innerAlias))) {
                    final Quantifier.Physical newInnerQuantifier = planPartitionToPhysical(call, innerQuantifier, innerReference, outerInnerPredicates, innerPlanPartition);
                    call.yieldPlan(new RecordQueryFlatMapPlan(newOuterQuantifier, newInnerQuantifier, selectExpression.getResultValue(), innerQuantifier instanceof Quantifier.Existential));
                }
            }

            // Case 2: Ordering is based on the outer
            final NonnullPair<List<PlanPartition>, Map<Ordering, PlanPartition>> satisfyingAndDistinctOuters = partitionOuterBySatisfyingAndDistinct(
                    requestedOrdering, outerMaxCardinalityNonOnePartitions,
                    o -> pullUpOrderingFromSelectChild(o, selectExpression, outerAlias));

            // Case 2a: Non-distinct ordering means that the ordering is based solely on the outer. Roll up the inners and return an outer which must satisfy the ordering
            for (PlanPartition outerPlanPartition : satisfyingAndDistinctOuters.getLeft()) {
                final Quantifier.Physical newOuterQuantifier = planPartitionToPhysical(call, outerQuantifier, outerReference, outerPredicates, outerPlanPartition);
                for (PlanPartition innerPlanPartition : PlanPartitions.rollUpTo(bindings.getAll(innerPlanPartitionsMatcher), ImmutableSet.of())) {
                    final Quantifier.Physical newInnerQuantifier = planPartitionToPhysical(call, innerQuantifier, innerReference, outerInnerPredicates, innerPlanPartition);
                    call.yieldPlan(new RecordQueryFlatMapPlan(newOuterQuantifier, newInnerQuantifier, selectExpression.getResultValue(), innerQuantifier instanceof Quantifier.Existential));
                }
            }

            // Case 2b: Distinct outer ordering means that the ordering is based on the outer and the inner together. Find the
            // inner values which produce a valid ordering
            for (Map.Entry<Ordering, PlanPartition> distinctOrderingAndOuter : satisfyingAndDistinctOuters.getRight().entrySet()) {
                final Ordering outerOrdering = distinctOrderingAndOuter.getKey();
                final PlanPartition outerPlanPartition = distinctOrderingAndOuter.getValue();
                final Quantifier.Physical newOuterQuantifier = planPartitionToPhysical(call, outerQuantifier, outerReference, outerPredicates, outerPlanPartition);
                for (PlanPartition innerPlanPartition : rollUpIfSatisfyOrdering(requestedOrdering, innerQuantifier, bindings.getAll(innerPlanPartitionsMatcher), outerOrdering,
                        o -> pullUpOrderingFromSelectChild(o, selectExpression, innerAlias))) {
                    final Quantifier.Physical newInnerQuantifier = planPartitionToPhysical(call, innerQuantifier, innerReference, outerInnerPredicates, innerPlanPartition);
                    call.yieldPlan(new RecordQueryFlatMapPlan(newOuterQuantifier, newInnerQuantifier, selectExpression.getResultValue(), innerQuantifier instanceof Quantifier.Existential));
                }
            }
        }
    }

    @Nonnull
    private Ordering pullUpOrderingFromSelectChild(@Nonnull Ordering ordering, @Nonnull SelectExpression selectExpression, @Nonnull CorrelationIdentifier childAlias) {
        return ordering.pullUp(selectExpression.getResultValue(), EvaluationContext.empty(), AliasMap.ofAliases(childAlias, Quantifier.current()), selectExpression.getResultValue().getCorrelatedTo());
    }

    @Nonnull
    private NonnullPair<List<PlanPartition>, List<PlanPartition>> separateByMaxCardinalityOne(@Nonnull Quantifier quantifier, @Nonnull List<PlanPartition> planPartitions) {
        if (quantifier instanceof Quantifier.Existential) {
            // Existential quantifiers always have an effective cardinality of exactly one. Group all the plans together in the max-cardinality-one bucket
            return NonnullPair.of(PlanPartitions.rollUpTo(planPartitions, ImmutableSet.of()), ImmutableList.of());
        }

        //
        // Separate out the plans with max cardinality one from the other plans
        //
        // In an ideal world, all plans in a reference should share the same cardinality. This is because the cardinality
        // is a property of the semantics of the expression they are implementing, and so it should be invariant to
        // the actual implementation. However, there are circumstances where we can prove this property for some plans,
        // but not for all of them. See: https://github.com/FoundationDB/fdb-record-layer/issues/3968
        //
        final ImmutableList.Builder<PlanPartition> maxCardinalityOnePartitions = ImmutableList.builderWithExpectedSize(planPartitions.size());
        final ImmutableList.Builder<PlanPartition> maxCardinalityNonOnePartitions = ImmutableList.builderWithExpectedSize(planPartitions.size());

        for (PlanPartition planPartition : planPartitions) {
            final PlanPartition maxCardinalityOnePartition = planPartition.filter(p -> {
                final CardinalitiesProperty.Cardinalities planCardinality = CardinalitiesProperty.cardinalities().evaluate(p);
                return !planCardinality.getMaxCardinality().isUnknown() && planCardinality.getMaxCardinality().getCardinality() == 1L;
            });
            if (maxCardinalityOnePartition.getPlans().isEmpty()) {
                // No max cardinality one plans. Put the whole partition into the non-one bucket
                maxCardinalityNonOnePartitions.add(planPartition);
            } else {
                // Some max cardinality one plans. Put the those plans into the max-one bucket, and then place the remaining plans
                // (if there are any) into the non-one bucket
                maxCardinalityOnePartitions.add(maxCardinalityOnePartition);
                final PlanPartition maxCardinalityNonOnePartition = planPartition.filter(p -> !maxCardinalityOnePartition.getPlans().contains(p));
                if (!maxCardinalityNonOnePartition.getPlans().isEmpty()) {
                    maxCardinalityNonOnePartitions.add(maxCardinalityNonOnePartition);
                }
            }
        }

        return NonnullPair.of(
                // The ones with max cardinality 1 we want to collapse into a single plan partition, as the order of the
                // final plan will depend only on the inner plan's order, so we don't need to keep the outer plans segmented
                PlanPartitions.rollUpTo(maxCardinalityOnePartitions.build(), ImmutableSet.of()),
                // The ordering of these other plans will be dictated primarily by the outer, so retain their original
                // partitioning (by ordering)
                maxCardinalityNonOnePartitions.build()
        );
    }

    @Nonnull
    private List<PlanPartition> rollUpIfSatisfyOrdering(@Nonnull final RequestedOrdering requestedOrdering, @Nonnull Quantifier quantifier, @Nonnull List<PlanPartition> planPartitions, @Nonnull Ordering prefix, @Nonnull Function<Ordering, Ordering> pullUpFn) {
        final ImmutableList.Builder<PlanPartition> satisfyingOrdering = ImmutableList.builderWithExpectedSize(planPartitions.size());
        for (PlanPartition planPartition : planPartitions) {
            final Ordering pulledUpOrdering = quantifier instanceof Quantifier.Existential ? Ordering.empty() : pullUpFn.apply(planPartition.getPartitionPropertyValue(OrderingProperty.ordering()));
            final Ordering ordering = prefix.isEmpty() ? pulledUpOrdering : Ordering.concatOrderings(prefix, pulledUpOrdering);
            if (ordering.satisfies(requestedOrdering)) {
                satisfyingOrdering.add(planPartition);
            }
        }
        if (requestedOrdering.isExhaustive()) {
            // We want all unique orderings. Return the entire thing
            return satisfyingOrdering.build();
        } else {
            // We don't care about the individual orderings as long as they satisfy things, so roll up the plans
            return PlanPartitions.rollUpTo(satisfyingOrdering.build(), ImmutableSet.of());
        }
    }

    @Nonnull
    private NonnullPair<List<PlanPartition>, Map<Ordering, PlanPartition>> partitionOuterBySatisfyingAndDistinct(@Nonnull final RequestedOrdering requestedOrdering, @Nonnull List<PlanPartition> planPartitions, @Nonnull Function<Ordering, Ordering> pullUpFn) {
        final ImmutableList.Builder<PlanPartition> satisfyingOrderings = ImmutableList.builderWithExpectedSize(planPartitions.size());
        final LinkedIdentityMap<Ordering, PlanPartition> distinctPartitionsByOrdering = new LinkedIdentityMap<>();

        for (PlanPartition planPartition : planPartitions) {
            final Ordering pulledUpOrdering = pullUpFn.apply(planPartition.getPartitionPropertyValue(OrderingProperty.ordering()));
            if (pulledUpOrdering.satisfies(requestedOrdering)) {
                satisfyingOrderings.add(planPartition);
            } else if (pulledUpOrdering.isDistinct()) {
                distinctPartitionsByOrdering.put(pulledUpOrdering, planPartition);
            }
        }

        final List<PlanPartition> finalSatisfying = requestedOrdering.isDistinct() ? satisfyingOrderings.build() : PlanPartitions.rollUpTo(satisfyingOrderings.build(), ImmutableSet.of());
        return NonnullPair.of(finalSatisfying, distinctPartitionsByOrdering);
    }

    @Nonnull
    private Quantifier.Physical planPartitionToPhysical(@Nonnull ImplementationCascadesRuleCall call, @Nonnull Quantifier quantifier, @Nonnull Reference reference, @Nonnull List<QueryPredicate> predicates, @Nonnull PlanPartition planPartition) {
        var ref = call.memoizeMemberPlansFromOther(reference, planPartition.getPlans());

        if (quantifier instanceof Quantifier.Existential) {
            ref = call.memoizePlan(
                    new RecordQueryFirstOrDefaultPlan(Quantifier.physicalBuilder().withAlias(quantifier.getAlias()).build(ref),
                            new NullValue(quantifier.getFlowedObjectType())));
        }  else if (quantifier instanceof Quantifier.ForEach && ((Quantifier.ForEach)quantifier).isNullOnEmpty()) {
            ref = call.memoizePlan(
                    new RecordQueryDefaultOnEmptyPlan(
                            Quantifier.physicalBuilder().withAlias(quantifier.getAlias()).build(ref),
                            new NullValue(quantifier.getFlowedObjectType())));
        }

        if (!predicates.isEmpty()) {
            final var newLowerQuantifier = Quantifier.physicalBuilder().withAlias(quantifier.getAlias()).build(ref);
            ref = call.memoizePlan(new RecordQueryPredicatesFilterPlan(newLowerQuantifier, predicates));
        }

        return Quantifier.physicalBuilder().withAlias(quantifier.getAlias()).build(ref);
    }
}
