/*
 * ImplementTempTableScanRule.java
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

import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.tempTableScanExpression;

/**
 * A rule that implements a {@link TempTableScanExpression} producing a {@link TempTableScanPlan} operator.
 */
public class ImplementTempTableScanRule extends AbstractCascadesRule<TempTableScanExpression> implements ImplementationCascadesRule<TempTableScanExpression> {

    @Nonnull
    private static final BindingMatcher<TempTableScanExpression> root = tempTableScanExpression();

    public ImplementTempTableScanRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final var tempTableScanExpression = call.get(root);
        call.yieldPlan(new TempTableScanPlan(tempTableScanExpression.getTempTableReferenceValue()));
    }
}
