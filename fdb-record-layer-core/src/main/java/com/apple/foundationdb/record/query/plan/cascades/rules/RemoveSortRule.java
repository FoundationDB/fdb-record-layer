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
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.rollUpTo;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalSortExpression;
import static com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty.DISTINCT_RECORDS;
import static com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty.ORDERING;
import static com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty.PRIMARY_KEY;

/**
 * A rule that implements a sort expression by removing this expression if appropriate.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class RemoveSortRule extends CascadesRule<LogicalSortExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = ReferenceMatchers.anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> innerReferenceMatcher =
            planPartitions(rollUpTo(any(innerPlanPartitionMatcher), ImmutableSet.of(ORDERING, DISTINCT_RECORDS, PRIMARY_KEY)));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(innerReferenceMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalSortExpression> root = logicalSortExpression(exactly(innerQuantifierMatcher));

    public RemoveSortRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var sortExpression = call.get(root);
        final var innerPlanPartition = call.get(innerPlanPartitionMatcher);

        final var sortValues = sortExpression.getSortValues();
        if (sortValues.isEmpty()) {
            call.yield(innerPlanPartition.getPlans());
            return;
        }

        final var sortValuesSet = ImmutableSet.copyOf(sortValues);

        final var ordering = innerPlanPartition.getAttributeValue(ORDERING);
        final Set<Value> equalityBoundKeys = ordering.getEqualityBoundValues();
        int equalityBoundUnsorted = equalityBoundKeys.size();

        for (final var sortValue : sortValues) {
            if (equalityBoundKeys.contains(sortValue)) {
                equalityBoundUnsorted --;
            }
        }

        final boolean isSatisfyingOrdering =
                ordering.satisfies(
                        RequestedOrdering.fromSortValues(sortValues, sortExpression.isReverse(), RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS));

        if (!isSatisfyingOrdering) {
            return;
        }

        final var isDistinct = innerPlanPartition.getAttributeValue(DISTINCT_RECORDS);

        if (isDistinct) {
            if (ordering.getOrderingSet()
                    .getSet()
                    .stream()
                    .allMatch(keyPart -> sortValuesSet.contains(keyPart.getValue()) || equalityBoundKeys.contains(keyPart.getValue()))) {
                final var strictlySortedInnerPlans =
                        innerPlanPartition.getPlans()
                                .stream()
                                .map(plan -> plan.strictlySorted(call))
                                .collect(LinkedIdentitySet.toLinkedIdentitySet());
                call.yield(strictlySortedInnerPlans);
            }
        }

        final var resultExpressions = new LinkedIdentitySet<RelationalExpression>();

        for (final var innerPlan : innerPlanPartition.getPlans()) {
            final boolean strictOrdered =
                    // Also a unique index if have gone through declared fields.
                    strictlyOrderedIfUnique(innerPlan, sortValues.size() + equalityBoundUnsorted);

            if (strictOrdered) {
                resultExpressions.add(innerPlan.strictlySorted(call));
            } else {
                resultExpressions.add(innerPlan);
            }
        }

        call.yield(resultExpressions);
    }

    private static boolean strictlyOrderedIfUnique(@Nonnull RecordQueryPlan orderedPlan, final int nkeys) {
        if (orderedPlan instanceof RecordQueryCoveringIndexPlan) {
            orderedPlan = ((RecordQueryCoveringIndexPlan)orderedPlan).getIndexPlan();
        }
        if (orderedPlan instanceof RecordQueryIndexPlan) {
            RecordQueryIndexPlan indexPlan = (RecordQueryIndexPlan)orderedPlan;
            final var matchCandidateOptional = indexPlan.getMatchCandidateMaybe();
            if (matchCandidateOptional.isPresent()) {
                final var matchCandidate = matchCandidateOptional.get();
                return matchCandidate.isUnique() && nkeys >= matchCandidate.getColumnSize();
            }
        }
        return false;
    }
}
