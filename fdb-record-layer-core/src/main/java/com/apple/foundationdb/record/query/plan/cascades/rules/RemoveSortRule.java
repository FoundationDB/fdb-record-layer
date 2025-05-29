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
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
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
 * A rule that implements a sort expression by removing this expression if appropriate.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class RemoveSortRule extends ImplementationCascadesRule<LogicalSortExpression> {
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
        final PlanPartition innerPlanPartition = call.get(innerPlanPartitionMatcher);

        final RequestedOrdering requestedOrdering = sortExpression.getOrdering();
        if (requestedOrdering.isPreserve()) {
            call.yieldPlans(innerPlanPartition.getPlans());
            return;
        }

        final List<OrderingPart.RequestedOrderingPart> requestedOrderingParts = requestedOrdering.getOrderingParts();
        final Set<Value> sortValuesSet = requestedOrderingParts.stream().map(OrderingPart::getValue).collect(Collectors.toSet());

        final Ordering ordering = innerPlanPartition.getPropertyValue(OrderingProperty.ordering());
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
            return;
        }

        final boolean isDistinct = innerPlanPartition.getPropertyValue(DistinctRecordsProperty.distinctRecords());
        if (isDistinct) {
            if (ordering.getOrderingSet()
                    .getSet()
                    .stream()
                    .allMatch(value -> sortValuesSet.contains(value) || equalityBoundKeys.contains(value))) {
                final var strictlySortedInnerPlans =
                        innerPlanPartition.getPlans()
                                .stream()
                                .map(plan -> plan.strictlySorted(call))
                                .collect(LinkedIdentitySet.toLinkedIdentitySet());
                call.yieldPlans(strictlySortedInnerPlans);
            }
        }

        final var resultExpressions = new LinkedIdentitySet<RecordQueryPlan>();

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

        call.yieldPlans(resultExpressions);
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
