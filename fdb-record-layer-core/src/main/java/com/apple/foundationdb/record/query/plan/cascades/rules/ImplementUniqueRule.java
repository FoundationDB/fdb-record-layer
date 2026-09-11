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

import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
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
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.filterPlanPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.rollUpPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyForEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalUniqueExpression;

/**
 * This rule implements {@link LogicalUniqueExpression} by absorbing it if the inner reference is already distinct.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementUniqueRule extends AbstractCascadesRule<LogicalUniqueExpression> implements ImplementationCascadesRule<LogicalUniqueExpression> {

    @Nonnull
    private static final CollectionMatcher<PlanPartition> anyPlanPartitionMatcher = all(anyPlanPartition());

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher = planPartitions(
            filterPlanPartitions(planPartition -> planPartition.getPartitionPropertiesMap().containsKey(DistinctRecordsProperty.distinctRecords())
                                   && planPartition.getPartitionPropertyValue(PrimaryKeyProperty.primaryKey()).isPresent(),
                    rollUpPartitions(anyPlanPartitionMatcher)));

    // This rule establishes null-on-empty semantics if desired, so it can match _any_ for-each quantifier.
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = anyForEachQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<LogicalUniqueExpression> root = logicalUniqueExpression(only(innerQuantifierMatcher));

    public ImplementUniqueRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var innerQuantifier = call.get(innerQuantifierMatcher);
        final var innerReference = call.get(innerReferenceMatcher);
        final var innerPlanPartitions = call.get(anyPlanPartitionMatcher);
        for (final PlanPartition partition : innerPlanPartitions) {
            // Establish the null-on-empty semantics if necessary. Note that the ON EMPTY NULL, if it triggers, flows
            // a single record and will thus preserve the distinctness that this rule relies on.
            final var builder = Quantifiers.applyGlue(call, innerQuantifier,
                    call.memoizeMemberPlansBuilder(innerReference, partition.getPlans()));
            call.yieldPlans(builder.members());
        }
    }
}
