/*
 * ImplementRecursiveUnionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.plans.RecursiveUnionQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.rollUp;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.recursiveUnionExpression;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementRecursiveUnionRule extends CascadesRule<RecursiveUnionExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> unionLegPlanPartitionsMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> unionLegReferenceMatcher =
            planPartitions(rollUp(any(unionLegPlanPartitionsMatcher)));

    @Nonnull
    private static final CollectionMatcher<Quantifier.ForEach> allForEachQuantifiersMatcher =
            all(forEachQuantifierOverRef(unionLegReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<RecursiveUnionExpression> root =
            recursiveUnionExpression(allForEachQuantifiersMatcher);

    public ImplementRecursiveUnionRule() {
        super(root);
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var recursiveUnionExpression = bindings.get(root);
        final var planPartitions = bindings.getAll(unionLegPlanPartitionsMatcher);
        final var allQuantifiers = bindings.get(allForEachQuantifiersMatcher);



        final ImmutableList<Quantifier.Physical> quantifiers =
                Streams.zip(planPartitions.stream(), allQuantifiers.stream(),
                                (planPartition, quantifier) -> call.memoizeMemberPlans(quantifier.getRangesOver(), planPartition.getPlans()))
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList());

        final var tempTableScanValueReference = recursiveUnionExpression.getTempTableScanValueReference();
        final var tempTableInsertValueReference = recursiveUnionExpression.getTempTableInsertValueReference();
        final var recursiveUnionPlan = new RecursiveUnionQueryPlan(quantifiers, tempTableScanValueReference, tempTableInsertValueReference);

        call.yieldExpression(recursiveUnionPlan);
    }
}
