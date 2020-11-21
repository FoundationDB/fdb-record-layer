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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicateFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;

/**
 * A rule that implements a logical filter around a {@link RecordQueryPlan} as a {@link RecordQueryFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementFilterRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<RecordQueryPlan> innerPlanMatcher = TypeMatcher.of(RecordQueryPlan.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<Quantifier.ForEach> innerQuantifierMatcher = QuantifierMatcher.forEach(innerPlanMatcher);
    private static final ExpressionMatcher<QueryPredicate> filterMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<LogicalFilterExpression> root =
            TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
                    filterMatcher,
                    innerQuantifierMatcher);

    public ImplementFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final RecordQueryPlan innerPlan = call.get(innerPlanMatcher);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        final QueryPredicate filter = call.get(filterMatcher);

        call.yield(GroupExpressionRef.of(
                new RecordQueryPredicateFilterPlan(
                        Quantifier.physicalBuilder()
                                .morphFrom(innerQuantifier)
                                .build(call.ref(innerPlan)),
                        filter)));
    }
}
