/*
 * PushElementWithComparisonIntoExistingScanRuleTest.java
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
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.view.RecordTypeSource;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the rule can push a filter on a simple equality down into a compatibly ordered index.
 */
public class PushElementWithComparisonIntoExistingScanRuleTest {
    private static final Set<String> recordTypes = Collections.singleton("MyRecordType");
    private static Source baseSource = new RecordTypeSource(recordTypes);
    private static PlannerRule<LogicalFilterExpression> rule = new PushElementWithComparisonIntoExistingScanRule();
    private static Index singleFieldIndex = new Index("singleField", field("aField"));
    private static IndexEntrySource singleFieldIndexEntrySource = IndexEntrySource.fromIndexWithTypeStrings(recordTypes, singleFieldIndex);
    private static Index concatIndex = new Index("concat", concat(field("aField"), field("anotherField")));
    private static IndexEntrySource concatIndexEntrySource = IndexEntrySource.fromIndexWithTypeStrings(recordTypes, concatIndex);
    private static PlanContext context = new FakePlanContext(ImmutableList.of(singleFieldIndex, concatIndex));

    private static LogicalFilterExpression buildLogicalFilter(@Nonnull QueryComponent queryComponent,
                                                              @Nonnull RelationalExpression inner) {
        final Quantifier.ForEach innerQuantifier = Quantifier.forEach(GroupExpressionRef.of(inner));
        return new LogicalFilterExpression(
                baseSource,
                queryComponent.normalizeForPlanner(baseSource),
                innerQuantifier);
    }

    @Test
    public void pushDownFilterToSingleFieldIndex() {
        RelationalExpression inner = new IndexEntrySourceScanExpression(singleFieldIndexEntrySource, IndexScanType.BY_VALUE,
                singleFieldIndexEntrySource.getEmptyComparisons(), false);
        GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(buildLogicalFilter(
                Query.field("aField").equalsValue(5), inner));
        TestRuleExecution execution = TestRuleExecution.applyRule(context, rule, root);
        assertTrue(execution.isRuleMatched());
        IndexEntrySourceScanExpression result = execution.getResultMemberWithClass(IndexEntrySourceScanExpression.class);
        assertNotNull(result);
        assertEquals(singleFieldIndex.getName(), result.getIndexName());
        assertEquals(IndexScanType.BY_VALUE, result.getScanType());
        assertEquals(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), result.getComparisons().toScanComparisons());
        assertFalse(result.isReverse());
    }

    @Test
    public void pushDownFilterWithCompatibleIndex() {
        RelationalExpression inner = new IndexEntrySourceScanExpression(concatIndexEntrySource, IndexScanType.BY_VALUE,
                concatIndexEntrySource.getEmptyComparisons(), false);
        GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(buildLogicalFilter(
                Query.field("aField").equalsValue(5), inner));

        TestRuleExecution execution = TestRuleExecution.applyRule(context, rule, root);
        IndexEntrySourceScanExpression result = execution.getResultMemberWithClass(IndexEntrySourceScanExpression.class);
        assertNotNull(result);
        assertEquals(concatIndex.getName(), result.getIndexName());
        assertEquals(IndexScanType.BY_VALUE, result.getScanType());
        assertEquals(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5)), result.getComparisons().toScanComparisons());
        assertFalse(result.isReverse());
    }

    @Test
    public void doesNotPushDownWithIncompatibleIndex() {
        RelationalExpression inner = new IndexEntrySourceScanExpression(singleFieldIndexEntrySource, IndexScanType.BY_VALUE,
                singleFieldIndexEntrySource.getEmptyComparisons(), false);
        RelationalExpression original = buildLogicalFilter(Query.field("anotherField").equalsValue(5), inner);
        GroupExpressionRef<RelationalExpression> root = GroupExpressionRef.of(original);
        assertTrue(TestRuleExecution.applyRule(context, rule, root).isRuleMatched());
        assertEquals(original, root.get());
    }
}
