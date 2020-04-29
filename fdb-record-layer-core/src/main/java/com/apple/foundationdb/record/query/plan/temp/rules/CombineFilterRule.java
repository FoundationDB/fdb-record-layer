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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * A simple rule that combines two nested filter plans and combines them into a single filter plan with a conjunction
 * of the two filters.
 */
@API(API.Status.EXPERIMENTAL)
public class CombineFilterRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<QueryPredicate> firstMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<QueryPredicate> secondMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<ExpressionRef<RelationalExpression>> childMatcher = ReferenceMatcher.anyRef();

    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeWithPredicateMatcher.ofPredicate(
            LogicalFilterExpression.class,
            firstMatcher,
            TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class, secondMatcher, childMatcher));

    public CombineFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        LogicalFilterExpression filterExpression = call.get(root);
        QueryPredicate first = call.get(firstMatcher);
        QueryPredicate second = call.get(secondMatcher);
        ExpressionRef<RelationalExpression> child = call.get(childMatcher);

        QueryPredicate combined = new AndPredicate(ImmutableList.of(first, second));
        call.yield(call.ref(new LogicalFilterExpression(filterExpression.getBaseSource(), combined, child)));
    }
}
