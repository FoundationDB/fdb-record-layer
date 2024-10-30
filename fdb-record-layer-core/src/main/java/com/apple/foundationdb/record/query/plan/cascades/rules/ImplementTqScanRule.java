/*
 * ImplementTqScanRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TqScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.TqScanPlan;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tqScanExpression;

/**
 * A rule that implements a {@link TqScanExpression} producing a {@link TqScanPlan} operator.
 */
public class ImplementTqScanRule extends CascadesRule<TqScanExpression> {

    @Nonnull
    private static final BindingMatcher<TqScanExpression> root = tqScanExpression();

    public ImplementTqScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var tableQueueScanExpression = call.get(root);
        call.yieldExpression(new TqScanPlan(tableQueueScanExpression.getTableQueue(),
                tableQueueScanExpression.getResultValue().getResultType()));
    }
}
