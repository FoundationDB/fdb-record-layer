/*
 * CascadesPlannerTest.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.rules.FakePlanContext;
import com.apple.foundationdb.record.query.plan.temp.rules.FilterWithNestedToNestingContextRule;
import com.apple.foundationdb.record.query.plan.temp.rules.RemoveNestedContextRule;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Tests of the basic functionality of the {@link CascadesPlanner}.
 */
public class CascadesPlannerTest {
    @Test
    public void rulesFormingATwoCycle() {
        final PlannerRuleSet loopingRules = new PlannerRuleSet(ImmutableList.of(
                new FilterWithNestedToNestingContextRule(),
                new RemoveNestedContextRule()));
        final CascadesPlanner cascadesPlanner = new CascadesPlanner(null, new RecordStoreState(null, null), loopingRules);

        final PlannerExpression initialExpression = new LogicalFilterExpression(
                new NestedField("parent_field",
                        new FieldWithComparison("child_field", new Comparisons.NullComparison(Comparisons.Type.IS_NULL))),
                new FullUnorderedScanExpression());
        final GroupExpressionRef<PlannerExpression> transformedGroup = cascadesPlanner.planPartial(new FakePlanContext(), initialExpression);
        // Check that this test actually completes and doesn't get stuck in an infinite loop.
    }
}
