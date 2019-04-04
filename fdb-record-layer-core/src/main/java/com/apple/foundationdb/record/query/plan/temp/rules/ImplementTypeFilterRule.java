/*
 * ImplementTypeFilterRule.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that implements a logical type filter on an (already implemented) {@link RecordQueryPlan} as a
 * {@link RecordQueryTypeFilterPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementTypeFilterRule extends PlannerRule<LogicalTypeFilterExpression> {
    private static ExpressionMatcher<ExpressionRef<RecordQueryPlan>> childMatcher = ReferenceMatcher.anyRef();
    private static ExpressionMatcher<LogicalTypeFilterExpression> root = TypeMatcher.of(LogicalTypeFilterExpression.class, childMatcher);

    public ImplementTypeFilterRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalTypeFilterExpression typeFilter = call.get(root);
        final ExpressionRef<RecordQueryPlan> child = call.get(childMatcher);

        call.yield(SingleExpressionRef.of(new RecordQueryTypeFilterPlan(child, typeFilter.getRecordTypes())));
        return ChangesMade.MADE_CHANGES;
    }
}
