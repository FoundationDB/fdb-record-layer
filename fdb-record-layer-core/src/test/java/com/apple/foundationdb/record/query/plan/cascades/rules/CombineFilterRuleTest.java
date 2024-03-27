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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the rule for combining logical filter expressions does precisely what is expected of it.
 */
public class CombineFilterRuleTest {
    private static CascadesRule<LogicalFilterExpression> rule = new CombineFilterRule();
    private static PlanContext blankContext = new FakePlanContext();

    private static Type type = Type.Record.fromFields(false,
            ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("testField")),
                    Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("testField2"))));
    private static RecordQueryPlan[] basePlans = {
            new RecordQueryScanPlan(null, type, null, ScanComparisons.EMPTY, false, false, Optional.empty()),
            new RecordQueryIndexPlan("not_an_index",
                    null,
                    IndexScanComparisons.byValue(),
                    IndexFetchMethod.SCAN_AND_FETCH,
                    RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                    false,
                    false,
                    Optional.empty(),
                    type,
                    QueryPlanConstraint.tautology())
    };

    private static LogicalFilterExpression buildLogicalFilter(@Nonnull QueryComponent queryComponent,
                                                              @Nonnull RelationalExpression inner) {
        final var baseRef = Reference.of(inner);
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
            Reference root = Reference.of(
                    buildLogicalFilter(filter1, buildLogicalFilter(filter2, basePlan)));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root, EvaluationContext.empty());
            assertTrue(execution.isRuleMatched());
            assertTrue(execution.getResult().containsInMemo(
                    buildLogicalFilter(Query.and(filter1, filter2), basePlan)));
        }
    }

    @Test
    public void doesNotCoalesce() {
        for (RecordQueryPlan basePlan : basePlans) {
            QueryComponent filter1 = Query.field("testField").equalsValue(5);
            Reference root = Reference.of(
                    buildLogicalFilter(filter1, buildLogicalFilter(filter1, basePlan)));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root, EvaluationContext.empty());
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
            Reference root = Reference.of(
                    buildLogicalFilter(filter1, basePlan));
            TestRuleExecution execution = TestRuleExecution.applyRule(blankContext, rule, root, EvaluationContext.empty());
            assertFalse(execution.isRuleMatched());
        }
    }
}
