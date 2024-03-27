/*
 * ImplementUniqueRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.rollUp;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.where;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalUniqueExpression;

/**
 * This rule implements {@link LogicalUniqueExpression} by absorbing it if the inner reference is already distinct.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementUniqueRule extends CascadesRule<LogicalUniqueExpression> {

    @Nonnull
    private static final CollectionMatcher<PlanPartition> anyPlanPartitionMatcher = all(anyPlanPartition());

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher = planPartitions(
            where(planPartition -> planPartition.getAttributesMap().containsKey(DistinctRecordsProperty.DISTINCT_RECORDS)
                                   && planPartition.getAttributeValue(PrimaryKeyProperty.PRIMARY_KEY).isPresent(),
                    rollUp(anyPlanPartitionMatcher)));

    @Nonnull
    private static final BindingMatcher<LogicalUniqueExpression> root = logicalUniqueExpression(only(forEachQuantifierOverRef(innerReferenceMatcher)));

    public ImplementUniqueRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var innerPlanPartitions = call.get(anyPlanPartitionMatcher);
        innerPlanPartitions.forEach(partition -> call.yieldExpression(partition.getPlans()));
    }
}
