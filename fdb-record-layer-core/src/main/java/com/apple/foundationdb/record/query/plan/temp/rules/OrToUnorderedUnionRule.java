/*
 * OrToUnorderedUnionRule.java
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
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AllChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Convert a filter on an {@linkplain OrComponent or} expression into a plan on the union. In particular, this will
 * produce a {@link LogicalUnorderedUnionExpression} with simple filter plans on each child.
 */
@API(API.Status.EXPERIMENTAL)
public class OrToUnorderedUnionRule extends PlannerRule<LogicalFilterExpression> {
    @Nonnull
    private static final ReferenceMatcher<QueryComponent> childMatcher = ReferenceMatcher.anyRef();
    @Nonnull
    private static final ExpressionMatcher<OrComponent> orMatcher = TypeMatcher.of(OrComponent.class, AllChildrenMatcher.allMatching(childMatcher));
    @Nonnull
    private static final ReferenceMatcher<RelationalPlannerExpression> innerMatcher = ReferenceMatcher.anyRef();
    @Nonnull
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class, orMatcher, innerMatcher);

    public OrToUnorderedUnionRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RelationalPlannerExpression> inner = call.get(innerMatcher);
        final List<ExpressionRef<QueryComponent>> children = call.getBindings().getAll(childMatcher);
        List<ExpressionRef<RelationalPlannerExpression>> relationalExpressionRefs = new ArrayList<>(children.size());
        for (ExpressionRef<QueryComponent> child : children) {
            relationalExpressionRefs.add(call.ref(new LogicalFilterExpression(child, inner)));
        }
        call.yield(SingleExpressionRef.of(new LogicalUnorderedUnionExpression(relationalExpressionRefs)));
        return ChangesMade.MADE_CHANGES;
    }
}
