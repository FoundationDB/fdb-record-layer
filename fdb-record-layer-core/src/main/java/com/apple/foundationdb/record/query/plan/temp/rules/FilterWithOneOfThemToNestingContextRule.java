/*
 * FilterWithOneOfThemToNestingContextRule.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.NestedContextExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that looks for a logical filter that contains a repeated nested predicate
 * ({@link com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent}) and transforms the inner expression
 * to a version with respect to the {@link NestedContext} defined by that nested field.
 */
public class FilterWithOneOfThemToNestingContextRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ExpressionRef<RelationalPlannerExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> nestedFilterMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<OneOfThemWithComponent> oneOfThemMatcher = TypeMatcher.of(OneOfThemWithComponent.class, nestedFilterMatcher);
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class,
            oneOfThemMatcher, innerMatcher);

    public FilterWithOneOfThemToNestingContextRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RelationalPlannerExpression> inner = call.get(innerMatcher);
        final OneOfThemWithComponent oneOfThem = call.get(oneOfThemMatcher);
        final ExpressionRef<QueryComponent> nestedFilter = call.get(nestedFilterMatcher);

        final NestedContext nestedContext = new NestedContext(
                Key.Expressions.field(oneOfThem.getFieldName(), KeyExpression.FanType.FanOut));
        final ExpressionRef<RelationalPlannerExpression> nestedInner = nestedContext.getNestedRelationalPlannerExpression(inner);

        if (nestedInner != null) {
            call.yield(call.ref(new NestedContextExpression(nestedContext,
                    new LogicalFilterExpression(nestedFilter, nestedInner))));
        }
    }
}
