/*
 * PushFieldWithComparisonIntoExistingIndexScanRuleTest.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.RewriteRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the rule can push a filter on a simple equality down into a compatibly ordered index.
 */
public class PushFieldWithComparisonIntoExistingIndexScanRuleTest {
    private static PlannerRule<LogicalFilterExpression> rule = new PushFieldWithComparisonIntoExistingIndexScanRule();
    private static Index singleFieldIndex = new Index("singleField", field("aField"));
    private static Index concatIndex = new Index("concat", concat(field("aField"), field("anotherField")));
    private static PlanContext context = new FakePlanContext(ImmutableList.of(singleFieldIndex, concatIndex));

    @Test
    public void pushDownFilterToSingleFieldIndex() {
        RelationalPlannerExpression inner = new LogicalIndexScanExpression(singleFieldIndex.getName(), IndexScanType.BY_VALUE,
                new KeyExpressionComparisons(singleFieldIndex.getRootExpression()), false);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(new LogicalFilterExpression(
                Query.field("aField").equalsValue(5), inner));
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root).findFirst();
        assertTrue(possibleMatch.isPresent());
        rule.onMatch(possibleMatch.get());
        assertTrue(root.get() instanceof LogicalIndexScanExpression);
        LogicalIndexScanExpression result = (LogicalIndexScanExpression) root.get();
        assertEquals(singleFieldIndex.getName(), result.getIndexName());
        assertEquals(IndexScanType.BY_VALUE, result.getScanType());
        assertEquals(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), result.getComparisons().toScanComparisons());
        assertFalse(result.isReverse());
    }

    @Test
    public void pushDownFilterWithCompatibleIndex() {
        RelationalPlannerExpression inner = new LogicalIndexScanExpression(singleFieldIndex.getName(), IndexScanType.BY_VALUE,
                new KeyExpressionComparisons(singleFieldIndex.getRootExpression()), false);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(new LogicalFilterExpression(
                Query.field("aField").equalsValue(5), inner));

        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root).findFirst();
        assertTrue(possibleMatch.isPresent());
        rule.onMatch(possibleMatch.get());
        assertTrue(root.get() instanceof LogicalIndexScanExpression);
        LogicalIndexScanExpression result = (LogicalIndexScanExpression) root.get();
        assertEquals(singleFieldIndex.getName(), result.getIndexName());
        assertEquals(IndexScanType.BY_VALUE, result.getScanType());
        assertEquals(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), result.getComparisons().toScanComparisons());
        assertFalse(result.isReverse());
    }

    @Test
    public void doesNotPushDownWithIncompatibleIndex() {
        RelationalPlannerExpression inner = new LogicalIndexScanExpression(singleFieldIndex.getName(), IndexScanType.BY_VALUE,
                new KeyExpressionComparisons(singleFieldIndex.getRootExpression()), false);
        PlannerExpression original = new LogicalFilterExpression(Query.field("anotherField").equalsValue(5), inner);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(original);
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root).findFirst();
        assertTrue(possibleMatch.isPresent()); // the matcher should match, since the structure is right
        rule.onMatch(possibleMatch.get());
        assertEquals(original, root.get());
    }
}
