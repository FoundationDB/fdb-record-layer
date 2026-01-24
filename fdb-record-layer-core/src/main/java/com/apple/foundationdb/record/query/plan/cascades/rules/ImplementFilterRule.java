/*
 * ImplementFilterRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyCompensatablePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalFilterExpression;

/**
 * A rule that implements a logical filter around a {@link RecordQueryPlan} as a {@link RecordQueryFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementFilterRule extends ImplementationCascadesRule<LogicalFilterExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(any(innerPlanPartitionMatcher));

    @Nonnull
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyCompensatablePredicate();

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> quantifierMatcher = forEachQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<LogicalFilterExpression> root =
            logicalFilterExpression(all(predicateMatcher), only(quantifierMatcher));

    public ImplementFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var innerPlanPartition = call.get(innerPlanPartitionMatcher);
        final var innerReference = call.get(innerReferenceMatcher);
        final var queryPredicates = bindings.getAll(predicateMatcher);
        final var innerQuantifier = bindings.get(quantifierMatcher);

        if (queryPredicates.stream().allMatch(QueryPredicate::isTautology)) {
            call.yieldPlans(innerPlanPartition.getPlans());
        } else {
            call.yieldPlan(new RecordQueryPredicatesFilterPlan(
                    Quantifier.physicalBuilder()
                            .morphFrom(innerQuantifier)
                            .build(call.memoizeMemberPlansFromOther(innerReference, innerPlanPartition.getPlans())),
                    queryPredicates.stream().map(QueryPredicate::toResidualPredicate).collect(ImmutableList.toImmutableList())));
        }
    }
}
