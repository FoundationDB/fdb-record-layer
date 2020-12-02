/*
 * ImplementUnorderedUnionRule.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalIntersectionExpression} and create a
 * {@link RecordQueryUnorderedUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementIntersectionRule extends PlannerRule<LogicalIntersectionExpression> {
    @Nonnull
    private static final ExpressionMatcher<RecordQueryPlan> childMatcher = TypeMatcher.of(RecordQueryPlan.class, AnyChildrenMatcher.ANY);
    @Nonnull
    private static final ExpressionMatcher<LogicalIntersectionExpression> root =
            TypeMatcher.of(LogicalIntersectionExpression.class,
                    MultiChildrenMatcher.allMatching(QuantifierMatcher.forEach(childMatcher)));

    public ImplementIntersectionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final LogicalIntersectionExpression logicalIntersectionExpression = bindings.get(root);
        final List<RecordQueryPlan> planChildren = bindings.getAll(childMatcher);
        call.yield(call.ref(RecordQueryIntersectionPlan.from(planChildren, logicalIntersectionExpression.getComparisonKey())));
    }
}
