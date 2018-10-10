/*
 * FilterWithScanRuleTest.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.RewriteRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the rule can push a filter on a simple equality down into a compatibly ordered index.
 */
public class FilterWithScanRuleTest {
    private static PlannerRule<LogicalFilterExpression> rule = new FilterWithScanRule();
    private static Index singleFieldIndex = new Index("singleField", field("aField"));
    private static Index concatIndex = new Index("concat", concat(field("aField"), field("anotherField")));
    private static PlanContext context = new PlanContext(ImmutableList.of(singleFieldIndex, concatIndex));

    @Test
    public void pushDownFilterToSingleFieldIndex() {
        RecordQueryIndexPlan inner = new RecordQueryIndexPlan(singleFieldIndex.getName(), IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(new LogicalFilterExpression(
                Query.field("aField").equalsValue(5), inner));
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root);
        assertTrue(possibleMatch.isPresent());
        rule.onMatch(possibleMatch.get());
        assertEquals(new RecordQueryIndexPlan(singleFieldIndex.getName(), IndexScanType.BY_VALUE,
                ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), false),
                root.get());
    }

    @Test
    public void pushDownFilterWithCompatibleIndex() {
        RecordQueryIndexPlan inner = new RecordQueryIndexPlan(concatIndex.getName(), IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(new LogicalFilterExpression(
                Query.field("aField").equalsValue(5), inner));
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root);
        assertTrue(possibleMatch.isPresent());
        rule.onMatch(possibleMatch.get());
        assertEquals(new RecordQueryIndexPlan(concatIndex.getName(), IndexScanType.BY_VALUE,
                ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), false),
                root.get());

    }

    @Test
    public void doesNotPushDownWithIncompatibleIndex() {
        RecordQueryIndexPlan inner = new RecordQueryIndexPlan(concatIndex.getName(), IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false);
        PlannerExpression original = new LogicalFilterExpression(Query.field("anotherField").equalsValue(5), inner);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(original);
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root);
        assertTrue(possibleMatch.isPresent()); // the matcher should match, since the structure is right
        rule.onMatch(possibleMatch.get());
        assertEquals(original, root.get());
    }

    @Test
    public void doesNotPushDownFilterWithExistingComparison() {
        Comparisons.Comparison inequalityComparison = new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 8);
        RecordQueryIndexPlan inner = new RecordQueryIndexPlan(concatIndex.getName(), IndexScanType.BY_VALUE,
                ScanComparisons.from(inequalityComparison), false);
        PlannerExpression original = new LogicalFilterExpression(Query.field("aField").equalsValue(5), inner);
        SingleExpressionRef<PlannerExpression> root = SingleExpressionRef.of(original);
        Optional<RewriteRuleCall> possibleMatch = RewriteRuleCall.tryMatchRule(context, rule, root);
        assertTrue(possibleMatch.isPresent());
        rule.onMatch(possibleMatch.get());
        assertEquals(original, root.get());
    }
}
