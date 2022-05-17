/*
 * ImplementPhysicalScanRule.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.primaryScanExpression;

/**
 * A rule that converts a logical index scan expression to a {@link RecordQueryScanPlan}. This rule simply converts
 * the logical index scan's to a
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons} to be used during query execution.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementPhysicalScanRule extends PlannerRule<PrimaryScanExpression> {
    private static final BindingMatcher<PrimaryScanExpression> root = primaryScanExpression();

    public ImplementPhysicalScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PrimaryScanExpression logical = call.get(root);
        call.yield(call.ref(new RecordQueryScanPlan(
                logical.getRecordTypes(),
                logical.getResultValue().getResultType().narrowMaybe(Type.Record.class).orElseThrow(() -> new RecordCoreException("type is of wrong implementor")),
                call.getContext().getCommonPrimaryKey(),
                logical.scanComparisons(),
                logical.isReverse())));
    }
}
