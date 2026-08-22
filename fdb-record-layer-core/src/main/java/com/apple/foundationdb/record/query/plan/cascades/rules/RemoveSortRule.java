/*
 * RemoveSortRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitionsTo;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalSortExpression;

/**
 * Implementation rule for {@link LogicalSortExpression}. Rather than introducing a physical sort operator, this rule
 * <em>absorbs</em> the sort expression by inspecting the {@link Ordering} of the inner plan partition and yielding the
 * inner plans directly when that ordering already satisfies the requested one.
 *
 * <p>The rule covers three cases:
 * <ol>
 * <li><em>Preserve-order request.</em> If the requested ordering is {@link RequestedOrdering#isPreserve() preserve},
 * then any inner ordering is acceptable and the inner plans are yielded as-is.
 * <li><em>Distinct, fully-covered ordering.</em> If the records in the inner partition are distinct and every value
 * in the inner ordering is either bound by an equality predicate or appears in the requested ordering, then the
 * inner plans cannot tie on the requested keys. Each is therefore marked as
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan#strictlySorted strictly sorted} and yielded.
 * <li><em>Strict ordering via unique index.</em> Plans backed by a unique
 * {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate MatchCandidate} where the requested ordering
 * (plus equality-bound prefix) covers all key columns are similarly marked as strictly sorted. Every inner plan is
 * then yielded, while those that do not qualify remain unchanged.
 * </ol>
 *
 * <p>If the inner ordering does not satisfy the request, the rule does not fire and the planner will have to rely on a
 * different strategy (for example, picking a differently-ordered inner plan) to produce a candidate.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class RemoveSortRule extends AbstractCascadesRule<LogicalSortExpression> implements ImplementationCascadesRule<LogicalSortExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = PlanPartitionMatchers.anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(rollUpPartitionsTo(any(innerPlanPartitionMatcher), ImmutableSet.of(OrderingProperty.ordering(),
                    DistinctRecordsProperty.distinctRecords(),
                    PrimaryKeyProperty.primaryKey())));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(innerReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalSortExpression> root = logicalSortExpression(exactly(innerQuantifierMatcher));

    public RemoveSortRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final LogicalSortExpression sortExpression = call.get(root);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        final PlanPartition innerPlanPartition = call.get(innerPlanPartitionMatcher);

        final Set<RecordQueryPlan> resultPlans = satisfyingPlans(call, sortExpression.getOrdering(), innerPlanPartition);
        if (resultPlans.isEmpty()) {
            // The inner ordering does not satisfy the request, so the sort cannot be absorbed.
            return;
        }

        // If the ƒ quantifier below the sort expression has null-on-empty semantics, make sure to re-establish those
        // semantics here. We do so by injecting an ON EMPTY NULL node _above_ the yielded plans (rather than below
        // them, where the ƒ used to sit). That is correct because a sort passes a lone null row through unchanged, and
        // it never turns a non-empty input into an empty one.
        if (Quantifiers.isForEachWithNullOnEmpty(innerQuantifier)) {
            final Reference plansReference = call.memoizePlansBuilder(resultPlans).reference();
            call.yieldPlan(RecordQueryDefaultOnEmptyPlan.forNullOnEmpty(innerQuantifier, plansReference));
        } else {
            call.yieldPlans(resultPlans);
        }
    }

    /**
     * Returns the inner plans that satisfy the requested ordering, each marked as
     * {@link RecordQueryPlan#strictlySorted} where that can be established, or an empty set if the inner ordering does
     * not satisfy the request at all.
     */
    @Nonnull
    private static Set<RecordQueryPlan> satisfyingPlans(@Nonnull final ImplementationCascadesRuleCall call,
                                                        @Nonnull final RequestedOrdering requestedOrdering,
                                                        @Nonnull final PlanPartition innerPlanPartition) {
        if (requestedOrdering.isPreserve()) {
            return innerPlanPartition.getPlans();
        }

        final List<OrderingPart.RequestedOrderingPart> requestedOrderingParts = requestedOrdering.getOrderingParts();
        final Set<Value> sortValuesSet = requestedOrderingParts.stream().map(OrderingPart::getValue).collect(Collectors.toSet());

        final Ordering ordering = innerPlanPartition.getPartitionPropertyValue(OrderingProperty.ordering());
        final Set<Value> equalityBoundKeys = ordering.getEqualityBoundValues();
        int equalityBoundUnsorted = equalityBoundKeys.size();

        for (final OrderingPart.RequestedOrderingPart requestedPart : requestedOrderingParts) {
            if (equalityBoundKeys.contains(requestedPart.getValue())) {
                equalityBoundUnsorted --;
            }
        }

        final boolean isSatisfyingOrdering =
                ordering.satisfies(requestedOrdering.withDistinctness(RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS));

        if (!isSatisfyingOrdering) {
            return ImmutableSet.of();
        }

        final var resultExpressions = new LinkedIdentitySet<RecordQueryPlan>();

        final boolean isDistinct = innerPlanPartition.getPartitionPropertyValue(DistinctRecordsProperty.distinctRecords());
        if (isDistinct) {
            if (ordering.getOrderingSet()
                    .getSet()
                    .stream()
                    .allMatch(value -> sortValuesSet.contains(value) || equalityBoundKeys.contains(value))) {
                innerPlanPartition.getPlans()
                        .stream()
                        .map(plan -> plan.strictlySorted(call))
                        .forEach(resultExpressions::add);
            }
        }

        for (final var innerPlan : innerPlanPartition.getPlans()) {
            final boolean strictOrdered =
                    // Also a unique index if we have gone through declared fields.
                    strictlyOrderedIfUnique(innerPlan, requestedOrderingParts.size() + equalityBoundUnsorted);

            if (strictOrdered) {
                resultExpressions.add(innerPlan.strictlySorted(call));
            } else {
                resultExpressions.add(innerPlan);
            }
        }

        return resultExpressions;
    }

    private static boolean strictlyOrderedIfUnique(@Nonnull RecordQueryPlan orderedPlan, final int numKeys) {
        if (orderedPlan instanceof RecordQueryCoveringIndexPlan) {
            orderedPlan = ((RecordQueryCoveringIndexPlan)orderedPlan).getIndexPlan();
        }
        if (orderedPlan instanceof RecordQueryIndexPlan) {
            RecordQueryIndexPlan indexPlan = (RecordQueryIndexPlan)orderedPlan;
            final var matchCandidateOptional = indexPlan.getMatchCandidateMaybe();
            if (matchCandidateOptional.isPresent()) {
                final var matchCandidate = matchCandidateOptional.get();
                return matchCandidate.isUnique() && numKeys >= matchCandidate.getColumnSize();
            }
        }
        return false;
    }
}
