/*
 * CombineFilterRule.java
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
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * A simple rule that combines two nested filter plans and combines them into a single filter plan with a conjunction
 * of the two filters.
 */
@API(API.Status.EXPERIMENTAL)
public class CombineFilterRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> firstMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> secondMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<ExpressionRef<RelationalPlannerExpression>> childMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class,
            firstMatcher,
            TypeMatcher.of(LogicalFilterExpression.class,
                    secondMatcher, childMatcher));

    public CombineFilterRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        ExpressionRef<QueryComponent> first = call.get(firstMatcher);
        ExpressionRef<QueryComponent> second = call.get(secondMatcher);
        ExpressionRef<RelationalPlannerExpression> child = call.get(childMatcher);

        ExpressionRef<QueryComponent> combined = call.ref(new AndComponent(ImmutableList.of(first, second)));
        call.yield(call.ref(new LogicalFilterExpression(combined, child)));
        return ChangesMade.MADE_CHANGES;
    }
}
