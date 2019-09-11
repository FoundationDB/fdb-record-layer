/*
 * RemoveNestedContextRule.java
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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.NestedContextExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that finds a {@link RelationalPlannerExpression} inside of a {@link NestedContextExpression} and produces the
 * unnested equivalent planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class RemoveNestedContextRule extends PlannerRule<NestedContextExpression> {
    private static ExpressionMatcher<ExpressionRef<RelationalPlannerExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static ExpressionMatcher<NestedContextExpression> root = TypeMatcher.of(NestedContextExpression.class, innerMatcher);

    public RemoveNestedContextRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final NestedContextExpression context = call.get(root);
        final NestedContext nestedContext = context.getNestedContext();
        final ExpressionRef<RelationalPlannerExpression> unnestedInner = nestedContext.getUnnestedRelationalPlannerExpression(call.get(innerMatcher));
        if (unnestedInner == null) {
            return ChangesMade.NO_CHANGE;
        } else {
            call.yield(unnestedInner);
            return ChangesMade.MADE_CHANGES;
        }
    }
}
