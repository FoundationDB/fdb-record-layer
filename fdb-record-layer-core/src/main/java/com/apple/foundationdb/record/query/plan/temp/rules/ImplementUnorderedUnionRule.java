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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AllChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will combine the
 * {@link RecordQueryPlan}s corresponding to its children.
 */
public class ImplementUnorderedUnionRule extends PlannerRule<LogicalUnorderedUnionExpression> {
    @Nonnull
    private static final ExpressionMatcher<ExpressionRef<RecordQueryPlan>> childMatcher = ReferenceMatcher.anyRef();
    @Nonnull
    private static final ExpressionMatcher<LogicalUnorderedUnionExpression> root = TypeMatcher.of(LogicalUnorderedUnionExpression.class,
            AllChildrenMatcher.allMatching(childMatcher));

    public ImplementUnorderedUnionRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final List<ExpressionRef<RecordQueryPlan>> planChildren = call.getBindings().getAll(childMatcher);
        call.yield(call.ref(RecordQueryUnorderedUnionPlan.fromRefs(planChildren)));
        return ChangesMade.MADE_CHANGES;
    }
}
