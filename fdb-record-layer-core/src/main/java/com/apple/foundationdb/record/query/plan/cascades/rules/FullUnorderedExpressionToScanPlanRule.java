/*
 * FullUnorderedExpressionToScanPlanRule.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.fullUnorderedScanExpression;

/**
 * A rule for implementing a {@link FullUnorderedScanExpression} as a {@link RecordQueryScanPlan} of the full primary
 * key space. This rule is used to generate "fallback" plans in the case that the planner does not find anything better.
 */
public class FullUnorderedExpressionToScanPlanRule extends PlannerRule<FullUnorderedScanExpression> {
    private static final BindingMatcher<FullUnorderedScanExpression> root = fullUnorderedScanExpression();

    public FullUnorderedExpressionToScanPlanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var fullUnorderedScanExpression = call.get(root);

        call.yield(call.ref(new RecordQueryScanPlan(fullUnorderedScanExpression.getRecordTypes(),
                fullUnorderedScanExpression.getResultValue().getResultType().narrowMaybe(Type.Record.class).orElseThrow(() -> new RecordCoreException("type is of wrong implementor")),
                call.getContext().getCommonPrimaryKey(),
                ScanComparisons.EMPTY,
                false)));
    }
}
