/*
 * ImplementUnorderedUnionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalUnionExpression;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnorderedUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementUnorderedUnionRule extends ImplementationCascadesRule<LogicalUnionExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> unionLegPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> unionLegReferenceMatcher =
            planPartitions(rollUpPartitions(any(unionLegPlanPartitionsMatcher)));

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> allForEachQuantifiersMatcher =
            all(forEachQuantifierOverRef(unionLegReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<LogicalUnionExpression> root =
            logicalUnionExpression(allForEachQuantifiersMatcher);

    public ImplementUnorderedUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var planPartitions = bindings.getAll(unionLegPlanPartitionsMatcher);
        final var allQuantifiers = bindings.get(allForEachQuantifiersMatcher);

        final ImmutableList<Quantifier.Physical> quantifiers =
                Streams.zip(planPartitions.stream(), allQuantifiers.stream(),
                                (planPartition, quantifier) -> call.memoizeMemberPlansFromOther(quantifier.getRangesOver(), planPartition.getPlans()))
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList());

        call.yieldPlan(RecordQueryUnorderedUnionPlan.fromQuantifiers(quantifiers));
    }
}
