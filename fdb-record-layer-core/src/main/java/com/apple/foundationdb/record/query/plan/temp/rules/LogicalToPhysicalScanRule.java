/*
 * LogicalToPhysicalIndexScanRule.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that converts a logical index scan expression to a {@link RecordQueryIndexPlan}. This rule simply converts
 * the logical index scan's to a
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons} to be used during query execution.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalToPhysicalScanRule extends PlannerRule<PrimaryScanExpression> {
    private static final ExpressionMatcher<PrimaryScanExpression> root = TypeMatcher.of(PrimaryScanExpression.class);

    public LogicalToPhysicalScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PrimaryScanExpression logical = call.get(root);
        call.yield(call.ref(new RecordQueryScanPlan(
                logical.getRecordTypes(),
                logical.scanComparisons(),
                logical.isReverse())));
    }
}
