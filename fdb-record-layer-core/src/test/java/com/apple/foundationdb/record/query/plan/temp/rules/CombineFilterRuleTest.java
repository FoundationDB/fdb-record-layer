/*
 * CombineFilterRuleTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the rule for combining logical filter expressions does precisely what is expected of it.
 */
public class CombineFilterRuleTest {
    private static PlannerRule<LogicalFilterExpression> rule = new CombineFilterRule();
    private static PlanContext blankContext = new FakePlanContext();
    private static RecordQueryPlan[] basePlans = {
            new RecordQueryScanPlan(ScanComparisons.EMPTY, false),
            new RecordQueryIndexPlan("not_an_index", IndexScanComparisons.byValue(), false)
    };

    private static LogicalFilterExpression buildLogicalFilter(@Nonnull QueryComponent queryComponent,
                                                              @Nonnull RelationalExpression inner) {
        final var baseRef = GroupExpressionRef.of(inner);
        final Quantifier.ForEach innerQuantifier = Quantifier.forEach(baseRef);
        return new LogicalFilterExpression(
                queryComponent.expand(innerQuantifier, () -> Quantifier.forEach(baseRef)).getPredicates(),
                innerQuantifier);
    }

    @Test
    public void combineFilter() {
        for (RecordQueryPlan basePlan : basePlans) {
            QueryComponent filter1 = Query.field("testField").equalsValue(5);
            QueryComponent filter2 = Query.field("testField2").equalsValue(10);
            GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(
                    buildLogicalFilter(filter1, buildLogicalFilter(filter2, basePlan)));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root);
            assertTrue(execution.isRuleMatched());
            assertTrue(execution.getResult().containsInMemo(
                    buildLogicalFilter(Query.and(filter1, filter2), basePlan)));
        }
    }

    @Test
    public void doesNotCoalesce() {
        for (RecordQueryPlan basePlan : basePlans) {
            QueryComponent filter1 = Query.field("testField").equalsValue(5);
            GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(
                    buildLogicalFilter(filter1, buildLogicalFilter(filter1, basePlan)));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root);
            assertTrue(execution.isRuleMatched());
            // this rule should not try to coalesce the two filters
            assertTrue(root.containsInMemo(buildLogicalFilter(Query.and(filter1, filter1), basePlan)));
            assertFalse(root.containsInMemo(buildLogicalFilter(filter1, basePlan)));
        }
    }

    @Test
    public void doesNotMatchSingleFilter() {
        for (RecordQueryPlan basePlan : basePlans) {
            QueryComponent filter1 = Query.field("testField").equalsValue(5);
            GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(
                    buildLogicalFilter(filter1, basePlan));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root);
            assertFalse(execution.isRuleMatched());
        }
    }
}
