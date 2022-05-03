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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.RecordTypesProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;

/**
 * A rule that implements a logical type filter on an (already implemented) {@link RecordQueryPlan} as a
 * {@link RecordQueryTypeFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementTypeFilterRule extends PlannerRule<LogicalTypeFilterExpression> {
    @Nonnull
    private static final CollectionMatcher<RecordQueryPlan> innerPlansMatcher = some(anyPlan());
    @Nonnull
    private static final BindingMatcher<LogicalTypeFilterExpression> root =
            RelationalExpressionMatchers.logicalTypeFilterExpression(exactly(forEachQuantifierOverPlans(innerPlansMatcher)));

    public ImplementTypeFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var logicalTypeFilterExpression = call.get(root);
        final var innerPlans = call.get(innerPlansMatcher);
        final var noTypeFilterNeededBuilder = ImmutableList.<RecordQueryPlan>builder();
        final var unsatisfiedMapBuilder = ImmutableMultimap.<Set<String>, RecordQueryPlan>builder();

        for (final var innerPlan : innerPlans) {
            final var childRecordTypes = RecordTypesProperty.evaluate(call.getContext(), call.getAliasResolver(), innerPlan);
            final var filterRecordTypes = Sets.newHashSet(logicalTypeFilterExpression.getRecordTypes());

            if (filterRecordTypes.containsAll(childRecordTypes)) {
                noTypeFilterNeededBuilder.add(innerPlan);
            } else {
                unsatisfiedMapBuilder.put(Sets.intersection(filterRecordTypes, childRecordTypes), innerPlan);
            }
        }

        final var noTypeFilterNeeded = noTypeFilterNeededBuilder.build();
        final var unsatisfiedMap = unsatisfiedMapBuilder.build();

        if (!noTypeFilterNeeded.isEmpty()) {
            call.yield(GroupExpressionRef.from(noTypeFilterNeeded));
        }

        for (Map.Entry<Set<String>, Collection<RecordQueryPlan>> unsatisfiedEntry : unsatisfiedMap.asMap().entrySet()) {
            call.yield(call.ref(
                    new RecordQueryTypeFilterPlan(
                            Quantifier.physical(GroupExpressionRef.from(unsatisfiedEntry.getValue())),
                            unsatisfiedEntry.getKey(),
                            Type.Relation.scalarOf(logicalTypeFilterExpression.getResultType()))));
        }
    }
}
