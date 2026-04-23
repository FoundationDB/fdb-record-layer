/*
 * ImplementStreamingAggregationRuleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

class ImplementStreamingAggregationRuleTest {
    @Nonnull
    private static final ImplementStreamingAggregationRule rule = new ImplementStreamingAggregationRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule, PlannerPhase.PLANNING);

    @Test
    void basicGroupByExpressionIsImplemented() {
        final var indexScanReference = Reference.plannedOf(indexScan());
        final var indexScanForEachQuantifier = Quantifier.forEach(indexScanReference);
        final var indexScanPhysicalQuantifier = Quantifier.physical(indexScanReference);
        final var groupingValue = RecordConstructorValue.ofUnnamed(ImmutableList.of());
        final var aggregateValue = RecordConstructorValue.ofUnnamed(List.of(new CountValue(indexScanForEachQuantifier.getFlowedObjectValue())));
        final var logicalGroupBy = new GroupByExpression(groupingValue, aggregateValue, GroupByExpression::nestedResults, indexScanForEachQuantifier);

        testHelper.assertYields(
                logicalGroupBy,
                expectedPhysicalPlan(indexScanPhysicalQuantifier, logicalGroupBy));
    }

    @Test
    void basicGroupByExpressionOverExpressionThatIsNotDistinctAcrossContinuationsIsNotImplemented() {
        final var unorderedDistinct = new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(Reference.plannedOf(indexScan())));
        final var unorderedDistinctQuantifier = Quantifier.forEach(Reference.plannedOf(unorderedDistinct));
        final var groupingValue = RecordConstructorValue.ofUnnamed(ImmutableList.of());
        final var aggregateValue = RecordConstructorValue.ofUnnamed(List.of(new CountValue(unorderedDistinctQuantifier.getFlowedObjectValue())));
        final var logicalGroupBy = new GroupByExpression(
                groupingValue,
                aggregateValue,
                GroupByExpression::nestedResults,
                unorderedDistinctQuantifier);

        testHelper.assertYieldsNothing(logicalGroupBy, false);
    }

    @Test
    void basicGroupByWithGroupingValueCompatibleWithOrderingIsImplemented() {
        // Wrap in a predicates filter with equality on the first column, which adds that column to the ordering
        final var filterInnerQun = Quantifier.physical(Reference.plannedOf(indexScan()));
        final var filterFieldValue = FieldValue.ofFieldName(filterInnerQun.getFlowedObjectValue(), "one");
        final var filterPlan = new RecordQueryPredicatesFilterPlan(
                filterInnerQun, List.of(new ValuePredicate(filterFieldValue, RuleTestHelper.EQUALS_42)));

        // Create group by expression with a grouping value with the first column
        final var filterPlanRef = Reference.plannedOf(filterPlan);
        final var filterPlanForEachQuantifier = Quantifier.forEach(filterPlanRef);
        final var groupingValue = RecordConstructorValue.ofUnnamed(
                ImmutableList.of(FieldValue.ofFieldName(filterPlanForEachQuantifier.getFlowedObjectValue(), "one")));
        final var aggregateValue = RecordConstructorValue.ofUnnamed(ImmutableList.of(new CountValue(filterPlanForEachQuantifier.getFlowedObjectValue())));
        final var logicalGroupBy = new GroupByExpression(groupingValue, aggregateValue, GroupByExpression::nestedResults, filterPlanForEachQuantifier);

        testHelper.assertYields(
                logicalGroupBy,
                expectedPhysicalPlan( Quantifier.physical(filterPlanRef), logicalGroupBy));
    }

    @Test
    void basicGroupByWithGroupingValueInCompatibleWithOrderingIsNotImplemented() {
        final var indexScanReference = Reference.plannedOf(indexScan());
        final var indexScanForEachQuantifier = Quantifier.forEach(indexScanReference);
        final var groupingValue = RecordConstructorValue.ofUnnamed(
                ImmutableList.of(FieldValue.ofFieldName(indexScanForEachQuantifier.getFlowedObjectValue(), "one")));
        final var aggregateValue = RecordConstructorValue.ofUnnamed(List.of(new CountValue(indexScanForEachQuantifier.getFlowedObjectValue())));
        final var logicalGroupBy = new GroupByExpression(groupingValue, aggregateValue, GroupByExpression::nestedResults, indexScanForEachQuantifier);

        testHelper.assertYieldsNothing(logicalGroupBy, true);
    }

    private static RecordQueryIndexPlan indexScan() {
        return new RecordQueryIndexPlan("IndexA", null, IndexScanComparisons.byValue(), IndexFetchMethod.SCAN_AND_FETCH, FetchIndexRecords.PRIMARY_KEY, false, false, Optional.empty(), RuleTestHelper.TYPE_S, QueryPlanConstraint.noConstraint());
    }

    private static RecordQueryStreamingAggregationPlan expectedPhysicalPlan(Quantifier.Physical innerPhysicalQuantifier, GroupByExpression logicalGroupByExpression) {
        final var aliasMap = AliasMap.ofAliases(logicalGroupByExpression.getInnerQuantifier().getAlias(), innerPhysicalQuantifier.getAlias());
        return RecordQueryStreamingAggregationPlan.of(
                innerPhysicalQuantifier,
                Objects.requireNonNull(logicalGroupByExpression.getGroupingValue()).rebase(aliasMap),
                (AggregateValue)logicalGroupByExpression.getAggregateValue().rebase(aliasMap),
                logicalGroupByExpression.getResultValueFunction());
    }

}
