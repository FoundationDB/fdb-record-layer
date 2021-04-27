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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalUnionExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnorderedUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementUnorderedUnionRule extends PlannerRule<LogicalUnionExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> unionLegMatcher = RecordQueryPlanMatchers.anyPlan();
    @Nonnull
    private static final BindingMatcher<LogicalUnionExpression> root =
            logicalUnionExpression(all(forEachQuantifier(unionLegMatcher)));

    public ImplementUnorderedUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final List<? extends RecordQueryPlan> planChildren = call.getBindings().getAll(unionLegMatcher);
        call.yield(call.ref(RecordQueryUnorderedUnionPlan.from(planChildren)));
    }
}
