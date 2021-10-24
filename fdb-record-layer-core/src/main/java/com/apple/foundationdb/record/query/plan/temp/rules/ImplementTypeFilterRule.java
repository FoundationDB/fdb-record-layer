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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.temp.properties.RecordTypesProperty;
import com.apple.foundationdb.record.query.predicates.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifierOverPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.anyPlan;

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
        final LogicalTypeFilterExpression typeFilter = call.get(root);
        final Collection<? extends RecordQueryPlan> innerPlans = call.get(innerPlansMatcher);
        final ImmutableList.Builder<RecordQueryPlan> noTypeFilterNeededBuilder = ImmutableList.builder();
        final ImmutableMultimap.Builder<Set<String>, RecordQueryPlan> unsatisfiedMapBuilder = ImmutableMultimap.builder();

        for (final RecordQueryPlan innerPlan : innerPlans) {
            final Set<String> childRecordTypes = RecordTypesProperty.evaluate(call.getContext(), call.getAliasResolver(), innerPlan);
            final Set<String> filterRecordTypes = Sets.newHashSet(typeFilter.getRecordTypes());

            if (filterRecordTypes.containsAll(childRecordTypes)) {
                noTypeFilterNeededBuilder.add(innerPlan);
            } else {
                unsatisfiedMapBuilder.put(Sets.intersection(filterRecordTypes, childRecordTypes), innerPlan);
            }
        }

        final ImmutableList<RecordQueryPlan> noTypeFilterNeeded = noTypeFilterNeededBuilder.build();
        final ImmutableMultimap<Set<String>, RecordQueryPlan> unsatisfiedMap = unsatisfiedMapBuilder.build();

        if (!noTypeFilterNeeded.isEmpty()) {
            call.yield(GroupExpressionRef.from(noTypeFilterNeeded));
        }

        for (Map.Entry<Set<String>, Collection<RecordQueryPlan>> unsatisfiedEntry : unsatisfiedMap.asMap().entrySet()) {
            call.yield(call.ref(
                    new RecordQueryTypeFilterPlan(
                            Quantifier.physical(GroupExpressionRef.from(unsatisfiedEntry.getValue())),
                            unsatisfiedEntry.getKey(),
                            (Type.Record)typeFilter.getResultType().getInnerType())));
        }
    }
}
