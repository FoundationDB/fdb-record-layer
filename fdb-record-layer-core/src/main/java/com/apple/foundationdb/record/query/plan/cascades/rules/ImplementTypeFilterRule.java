/*
 * ImplementTypeFilterRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.RecordTypesProperty.RecordTypesVisitor;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlanPartitionMatchers.filterPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalTypeFilterExpression;

/**
 * A rule that implements a logical type filter on an (already implemented) {@link RecordQueryPlan} as a
 * {@link RecordQueryTypeFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementTypeFilterRule extends CascadesRule<LogicalTypeFilterExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(filterPartition(planPartition -> planPartition.getPropertyValue(StoredRecordProperty.storedRecord()),
                    any(innerPlanPartitionMatcher)));

    @Nonnull
    private static final BindingMatcher<LogicalTypeFilterExpression> root =
            logicalTypeFilterExpression(exactly(forEachQuantifierOverRef(innerReferenceMatcher)));

    public ImplementTypeFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var logicalTypeFilterExpression = call.get(root);
        final var innerReference = call.get(innerReferenceMatcher);
        final var planPartition = call.get(innerPlanPartitionMatcher);
        final var noTypeFilterNeeded = new LinkedIdentitySet<RecordQueryPlan>();
        final var unsatisfiedMapBuilder = ImmutableMultimap.<Set<String>, RecordQueryPlan>builder();

        for (final var innerPlan : planPartition.getPlans()) {
            final var childRecordTypes = new RecordTypesVisitor(call.newAliasResolver()).visit(innerPlan);
            final var filterRecordTypes = Sets.newHashSet(logicalTypeFilterExpression.getRecordTypes());

            if (filterRecordTypes.containsAll(childRecordTypes)) {
                noTypeFilterNeeded.add(innerPlan);
            } else {
                unsatisfiedMapBuilder.put(Sets.intersection(filterRecordTypes, childRecordTypes), innerPlan);
            }
        }

        final var unsatisfiedMap = unsatisfiedMapBuilder.build();

        if (!noTypeFilterNeeded.isEmpty()) {
            call.yieldPlans(noTypeFilterNeeded);
        }

        for (Map.Entry<Set<String>, Collection<RecordQueryPlan>> unsatisfiedEntry : unsatisfiedMap.asMap().entrySet()) {
            call.yieldPlan(
                    new RecordQueryTypeFilterPlan(
                            Quantifier.physical(call.memoizeMemberPlans(innerReference, unsatisfiedEntry.getValue())),
                            unsatisfiedEntry.getKey(),
                            Type.Relation.scalarOf(logicalTypeFilterExpression.getResultType())));
        }
    }
}
