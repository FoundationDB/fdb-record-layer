/*
 * ImplementUpdateRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.insertExpression;

/**
 * A rule that implements an {@link InsertExpression} delete expression by creating
 * a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementInsertRule extends CascadesRule<InsertExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> innerReferenceMatcher =
            planPartitions(any(innerPlanPartitionMatcher));

    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher =
            forEachQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<InsertExpression> root =
            insertExpression(innerQuantifierMatcher);

    public ImplementInsertRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var innerPlanPartition = call.get(innerPlanPartitionMatcher);
        final var innerQuantifier = call.get(innerQuantifierMatcher);
        final var insertExpression = call.get(root);

        final ExpressionRef<? extends RelationalExpression> plansReference =
                GroupExpressionRef.from(innerPlanPartition.getPlans());

        final var physicalQuantifier =
                Quantifier.physicalBuilder()
                        .morphFrom(innerQuantifier)
                        .build(plansReference);
        call.yield(GroupExpressionRef.of(insertExpression.toPlan(physicalQuantifier)));
    }
}