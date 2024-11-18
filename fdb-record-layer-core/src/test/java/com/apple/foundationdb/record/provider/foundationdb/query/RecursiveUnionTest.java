/*
 * RecursiveUnionTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link com.apple.foundationdb.record.query.plan.plans.RecursiveUnionQueryPlan} planning and execution.
 */
public class RecursiveUnionTest extends TempTableTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase1() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 50L);
        assertEquals(ImmutableList.of(2L, 5L, 4L, 10L, 8L, 20L, 16L, 40L, 32L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase2() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 6L);
        assertEquals(ImmutableList.of(2L, 5L, 4L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase3() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 0L);
        assertEquals(ImmutableList.of(2L, 5L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase4() throws Exception {
        var result = multiplesOf(ImmutableList.of(), 1000L);
        assertEquals(ImmutableList.of(), result);
    }

    /**
     * Creates a recursive union plan that calculates multiple series recursively {@code F(X) = F(X-1) * 2} up until
     * a given limit.
     * @param initial The initial elements in the series, used to seed the recursion.
     * @param limit The (exclusive) limit of the series.
     * @return A multiple series starting with {@code initial} items up until the given {@code limit}. Note that the
     * initial items are still included in the final result even if they violate the limit.
     * @throws Exception If the execution of the recursive union plan fails.
     */
    @Nonnull
    private List<Long> multiplesOf(@Nonnull final List<Long> initial, long limit) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final var blaTempTable = TempTable.<QueryResult>newInstance();
            final var blaTempTableAlias = CorrelationIdentifier.of("Bla");
            initial.forEach(value -> blaTempTable.add(queryResult(value)));

            final var ttScanBla = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(blaTempTableAlias, blaTempTableAlias.getId(), getType())));
            var selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(getRecNoCol(ttScanBla), getStrValueCol(ttScanBla))).addQuantifier(ttScanBla)
                    .build().buildSelect();
            final var blaSelectQun = Quantifier.forEach(Reference.of(selectExpression));

            final var initTempTable = TempTable.<QueryResult>newInstance();
            final var initTempTableAlias = CorrelationIdentifier.of("Init");
            final var initialTempTableReferenceValue = ConstantObjectValue.of(initTempTableAlias, initTempTableAlias.getId(), new Type.Relation(getType()));

            final var initInsertQun = Quantifier.forEach(Reference.of(TempTableInsertExpression.ofConstant(blaSelectQun,
                    initTempTableAlias, initTempTableAlias.getId(), getType())));

            final var recuTempTable = TempTable.<QueryResult>newInstance();
            final var recuTempTableAlias = CorrelationIdentifier.of("Recu");
            final var recuTempTableReferenceValue = ConstantObjectValue.of(recuTempTableAlias, recuTempTableAlias.getId(), new Type.Relation(getType()));

            final var ttScanRecuQun = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(recuTempTableAlias, recuTempTableAlias.getId(), getType())));
            var recNoField2 = getRecNoCol(ttScanRecuQun);
            final var multByTwo = Column.of(Optional.of("rec_no"), (Value)new ArithmeticValue.MulFn().encapsulate(ImmutableList.of(recNoField2.getValue(), LiteralValue.ofScalar(2L))));
            selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(multByTwo, getStrValueCol(ttScanRecuQun)))
                    .addQuantifier(ttScanRecuQun)
                    .build().buildSelect();
            final var selectQun = Quantifier.forEach(Reference.of(selectExpression));
            recNoField2 = getRecNoCol(selectQun);
            final var lessThanForty = new ValuePredicate(recNoField2.getValue(), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, limit));
            selectExpression = GraphExpansion.builder()
                    .addPredicate(lessThanForty)
                    .addQuantifier(selectQun)
                    .build().buildSimpleSelectOverQuantifier(selectQun);
            final var recuSelectQun = Quantifier.forEach(Reference.of(selectExpression));

            final var recuInsertQun = Quantifier.forEach(Reference.of(TempTableInsertExpression.ofConstant(recuSelectQun,
                    initTempTableAlias, initTempTableAlias.getId(), getType())));

            final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, initialTempTableReferenceValue, recuTempTableReferenceValue);

            final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(recursiveUnionPlan))));
            final var cascadesPlanner = (CascadesPlanner)planner;
            final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();

            var evaluationContext = putTempTableInContext(blaTempTableAlias, blaTempTable, null);
            evaluationContext = putTempTableInContext(recuTempTableAlias, recuTempTable, evaluationContext);
            evaluationContext = putTempTableInContext(initTempTableAlias, initTempTable, evaluationContext);
            return extractResultsAsPairs(context, plan, evaluationContext).stream().map(Pair::getKey).collect(ImmutableList.toImmutableList());
        }
    }
}
