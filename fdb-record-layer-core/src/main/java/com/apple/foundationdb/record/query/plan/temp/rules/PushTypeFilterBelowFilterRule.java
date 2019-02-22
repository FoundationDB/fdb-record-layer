/*
 * PushTypeFilterBelowFilterRule.java
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
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * A rule that moves a {@link RecordQueryTypeFilterPlan} below a {@link RecordQueryFilterPlan}. While this doesn't make
 * a difference in terms of plan semantics it ensures that the generated plans have the same form as those produced by
 * the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}.
 */
@API(API.Status.EXPERIMENTAL)
public class PushTypeFilterBelowFilterRule extends PlannerRule<RecordQueryTypeFilterPlan> {
    private static final ExpressionMatcher<ExpressionRef<RecordQueryPlan>> innerMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> filterMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<RecordQueryFilterPlan> filterPlanMatcher = TypeMatcher.of(RecordQueryFilterPlan.class,
            innerMatcher, filterMatcher);
    private static final ExpressionMatcher<RecordQueryTypeFilterPlan> root = TypeMatcher.of(RecordQueryTypeFilterPlan.class, filterPlanMatcher);

    public PushTypeFilterBelowFilterRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RecordQueryPlan> inner = call.get(innerMatcher);
        final ExpressionRef<QueryComponent> filter = call.get(filterMatcher);
        final Collection<String> recordTypes = call.get(root).getRecordTypes();

        call.yield(SingleExpressionRef.of(new RecordQueryFilterPlan(
                SingleExpressionRef.of(new RecordQueryTypeFilterPlan(inner, recordTypes)), filter)));
        return ChangesMade.MADE_CHANGES;
    }
}
