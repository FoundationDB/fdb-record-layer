/*
 * ImplementGroupByRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.groupByExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.streamableAggregateValue;

/**
 * Rule for implementing logical {@code GROUP BY} into a physical streaming aggregate operator {@link RecordQueryStreamingAggregationPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementStreamingAggregationRule extends CascadesRule<GroupByExpression> {
    @Nonnull
    private static final BindingMatcher<Reference> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<GroupByExpression> root =
            groupByExpression(recordConstructorValue(all(streamableAggregateValue())), any(innerQuantifierMatcher));

    public ImplementStreamingAggregationRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();

        final var groupByExpression = bindings.get(root);
        final var correlatedTo = groupByExpression.getCorrelatedTo();
        final var innerQuantifier = Iterables.getOnlyElement(groupByExpression.getQuantifiers());

        final var groupingValue = groupByExpression.getGroupingValue();

        final var currentGroupingValue = groupingValue == null ? null : groupingValue.rebase(AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current()));

        // TODO: isConstant is not implemented correctly.
        // for the following FV(col1, QOV( --> RCV(FV(col1(Literal(42))...) ) it is returning false while it should return true.
        final var requiredOrderingKeyValues =
                currentGroupingValue == null
                ? null
                : ImmutableSet.copyOf(
                        Values.simplify(Values.primitiveAccessorsForType(currentGroupingValue.getResultType(),
                                () -> currentGroupingValue),
                                DefaultValueSimplificationRuleSet.instance(),
                                AliasMap.emptyMap(), correlatedTo));

        final var innerReference = innerQuantifier.getRangesOver();
        final var planPartitions = PlanPartition.rollUpTo(innerReference.getPlanPartitions(), OrderingProperty.ORDERING);

        for (final var planPartition : planPartitions) {
            final var providedOrdering = planPartition.getAttributeValue(OrderingProperty.ORDERING);
            if (requiredOrderingKeyValues == null || providedOrdering.satisfiesGroupingValues(requiredOrderingKeyValues)) {
                call.yieldExpression(implementGroupBy(call, planPartition, groupByExpression));
            }
        }
    }

    @Nonnull
    private RecordQueryStreamingAggregationPlan implementGroupBy(@Nonnull final CascadesRuleCall call,
                                                                 @Nonnull final PlanPartition planPartition,
                                                                 @Nonnull final GroupByExpression groupByExpression) {
        final var innerQuantifier = Iterables.getOnlyElement(groupByExpression.getQuantifiers());
        final var newInnerPlanReference = call.memoizeMemberPlans(innerQuantifier.getRangesOver(), planPartition.getPlans());
        final var newPlanQuantifier = Quantifier.physical(newInnerPlanReference);
        final var aliasMap = AliasMap.ofAliases(innerQuantifier.getAlias(), newPlanQuantifier.getAlias());
        final var rebasedAggregatedValue = groupByExpression.getAggregateValue().rebase(aliasMap);
        final var rebasedGroupingValue = groupByExpression.getGroupingValue() == null ? null : groupByExpression.getGroupingValue().rebase(aliasMap);
        return RecordQueryStreamingAggregationPlan.of(newPlanQuantifier, rebasedGroupingValue,
                (AggregateValue)rebasedAggregatedValue,
                groupByExpression.getResultValueFunction());
    }
}
