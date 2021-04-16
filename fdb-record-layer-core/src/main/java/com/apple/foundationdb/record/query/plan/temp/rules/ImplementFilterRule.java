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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression.logicalFilterExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TMultiMatcher.all;

/**
 * A rule that implements a logical filter around a {@link RecordQueryPlan} as a {@link RecordQueryFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementFilterRule extends PlannerRule<LogicalFilterExpression> {
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier(innerPlanMatcher);
    private static final BindingMatcher<QueryPredicate> filterMatcher = anyPredicate();
    private static final BindingMatcher<LogicalFilterExpression> root =
            logicalFilterExpression(all(filterMatcher), exactly(innerQuantifierMatcher));

    public ImplementFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final RecordQueryPlan innerPlan = bindings.get(innerPlanMatcher);
        final Quantifier.ForEach innerQuantifier = bindings.get(innerQuantifierMatcher);
        final List<? extends QueryPredicate> queryPredicates = bindings.getAll(filterMatcher);

        if (queryPredicates.stream().allMatch(QueryPredicate::isTautology)) {
            call.yield(GroupExpressionRef.of(innerPlan));
        } else {
            call.yield(GroupExpressionRef.of(
                    new RecordQueryPredicatesFilterPlan(
                            Quantifier.physicalBuilder()
                                    .morphFrom(innerQuantifier)
                                    .build(call.ref(innerPlan)),
                            queryPredicates)));
        }
    }
}
