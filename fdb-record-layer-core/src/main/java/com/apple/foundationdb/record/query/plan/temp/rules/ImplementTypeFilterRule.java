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
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.RecordTypesProperty;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A rule that implements a logical type filter on an (already implemented) {@link RecordQueryPlan} as a
 * {@link RecordQueryTypeFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementTypeFilterRule extends PlannerRule<LogicalTypeFilterExpression> {
    private static ExpressionMatcher<RecordQueryPlan> innerMatcher = TypeMatcher.of(RecordQueryPlan.class,
            AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<Quantifier.ForEach> innerQuantifierMatcher = QuantifierMatcher.forEach(innerMatcher);
    private static ExpressionMatcher<LogicalTypeFilterExpression> root =
            TypeMatcher.of(LogicalTypeFilterExpression.class, innerQuantifierMatcher);

    public ImplementTypeFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalTypeFilterExpression typeFilter = call.get(root);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        final RecordQueryPlan inner = call.get(innerMatcher);

        final Set<String> childRecordTypes = RecordTypesProperty.evaluate(call.getContext(), call.getAliasResolver(), inner);
        final Set<String> filterRecordTypes = Sets.newHashSet(typeFilter.getRecordTypes());
        if (filterRecordTypes.containsAll(childRecordTypes)) {
            // type filter is completely redundant, so remove it entirely
            call.yield(call.ref(inner));
        } else {
            // otherwise, keep a filter on record types which the inner might produce and are included in the filter
            final Set<String> unsatisfiedTypeFilters = Sets.intersection(filterRecordTypes, childRecordTypes);
            call.yield(GroupExpressionRef.of(
                    new RecordQueryTypeFilterPlan(
                            Quantifier.physicalBuilder()
                                    .morphFrom(innerQuantifier)
                                    .build(GroupExpressionRef.of(inner)),
                            unsatisfiedTypeFilters)));
        }
    }
}
