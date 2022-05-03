/*
 * ImplementIntersectionRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.logicalIntersectionExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;

/**
 * A rule that implements an intersection of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalIntersectionExpression} and create a
 * {@link RecordQueryIntersectionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementIntersectionRule extends PlannerRule<LogicalIntersectionExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> childMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<LogicalIntersectionExpression> root =
            logicalIntersectionExpression(all(forEachQuantifier(childMatcher)));

    public ImplementIntersectionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final LogicalIntersectionExpression logicalIntersectionExpression = bindings.get(root);
        final List<? extends RecordQueryPlan> planChildren = bindings.getAll(childMatcher);
        call.yield(call.ref(RecordQueryIntersectionPlan.from(planChildren, logicalIntersectionExpression.getComparisonKey())));
    }
}
