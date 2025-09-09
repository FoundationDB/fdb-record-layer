/*
 * TempTableTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import java.util.Optional;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.explodePlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableInsertPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableScanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for {@link TempTable} planning and execution.
 * Particularly, testing both {@code INSERT} into and {@code SCAN} from a {@link TempTable}.
 */
public class TempTableTest extends TempTableTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void scanTempTableWorksCorrectly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            // select id, value from <tempTable>.
            final var tempTable = tempTableInstance();
            final var tempTableId = CorrelationIdentifier.uniqueId();
            final var plan = createAndOptimizeTempTableScanPlan(tempTableId);
            addSampleDataToTempTable(tempTable);
            final var expectedResults = ImmutableList.of(Pair.of(42L, "fortySecondValue"),
                    Pair.of(45L, "fortyFifthValue"));
            assertEquals(expectedResults, collectResults(context, plan, tempTable, tempTableId));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void scanTempTableWithPredicateWorksCorrectly() throws Exception {
        // select id, value from <tempTable> where id < 44L.
        try (FDBRecordContext context = openContext()) {
            final var tempTable = tempTableInstance();
            addSampleDataToTempTable(tempTable);
            final var tempTableId = CorrelationIdentifier.uniqueId();
            final var tempTableScanQun = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(tempTableId, getTempTableType())));
            final var selectExpressionBuilder = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(getIdCol(tempTableScanQun), getValueCol(tempTableScanQun)))
                    .addPredicate(new ValuePredicate(getIdField(tempTableScanQun), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 44L)))
                    .addQuantifier(tempTableScanQun);
            final var logicalPlan = Reference.initialOf(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.initialOf(selectExpressionBuilder.build().buildSelect()))));
            final var cascadesPlanner = (CascadesPlanner)planner;
            final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
            assertMatchesExactly(plan, mapPlan(predicatesFilterPlan(tempTableScanPlan()).where(predicates(only(valuePredicate(fieldValueWithFieldNames("id"),
                    new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 44L)))))));
            assertEquals(ImmutableList.of(Pair.of(42L, "fortySecondValue")), collectResults(context, plan, tempTable, tempTableId));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void insertIntoTempTableWorksCorrectly() throws Exception {
        // insert into <tempTable> values ((1, 'first'), (2, 'second'))
        try (FDBRecordContext context = openContext()) {
            final var tempTable = tempTableInstance();
            final var tempTableId = CorrelationIdentifier.uniqueId();
            final var firstRecord = rcv(1L, "first");
            final var secondArray = rcv(2L, "second");
            final var explodeExpression = new ExplodeExpression(AbstractArrayConstructorValue.LightArrayConstructorValue.of(firstRecord, secondArray));
            var qun = Quantifier.forEach(Reference.initialOf(explodeExpression));

            qun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(qun,
                    tempTableId, getInnerType(qun), false)));
            final var insertPlan = Reference.initialOf(LogicalSortExpression.unsorted(qun));

            final var cascadesPlanner = (CascadesPlanner)planner;
            var plan = cascadesPlanner.planGraph(() -> insertPlan, Optional.empty(),
                    IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
            assertMatchesExactly(plan,  tempTableInsertPlan(explodePlan()));
            final var evaluationContext = putTempTableInContext(tempTableId, tempTable, null);
            fetchResultValues(context, plan, Function.identity(), evaluationContext, c -> { }, ExecuteProperties.SERIAL_EXECUTE);

            // select id, value from tq1 | tq1 is a temporary table.
            plan = createAndOptimizeTempTableScanPlan(tempTableId);
            assertEquals(ImmutableList.of(Pair.of(1L, "first"),
                    Pair.of(2L, "second")), collectResults(context, plan, tempTable, tempTableId));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void insertIntoTempTableWorksCorrectlyAcrossContinuations() {
        // insert into <tempTable> values ((1, 'first'), stop, resume, then insert (2, 'second'))
        byte[] continuation = null;
        RecordQueryPlan planToResume = null;
        final var tempTableId = CorrelationIdentifier.uniqueId();

        {
            final var tempTable = tempTableInstance();
            final var firstRecord = rcv(1L, "first");
            final var secondArray = rcv(2L, "second");
            final var explodeExpression = new ExplodeExpression(AbstractArrayConstructorValue.LightArrayConstructorValue.of(firstRecord, secondArray));
            var qun = Quantifier.forEach(Reference.initialOf(explodeExpression));

            qun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(qun, tempTableId, getInnerType(qun))));
            final var insertPlan = Reference.initialOf(LogicalSortExpression.unsorted(qun));

            final var cascadesPlanner = (CascadesPlanner)planner;
            planToResume = cascadesPlanner.planGraph(() -> insertPlan, Optional.empty(),
                    IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
            assertMatchesExactly(planToResume, tempTableInsertPlan(explodePlan()));
            final var evaluationContext = setUpPlanContext(planToResume, tempTableId, tempTable);
            try (RecordCursorIterator<QueryResult> cursor = planToResume.executePlan(recordStore, evaluationContext,
                    null, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                assertTrue(cursor.hasNext());
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(1L, "first"), asIdValue(message));
                continuation = cursor.getContinuation();
            }
            assertEquals(ImmutableList.of(Pair.of(1L, "first")), collectResults(tempTable));
        }

        {
            final var tempTable = tempTableInstance();
            final var evaluationContext = setUpPlanContext(planToResume, tempTableId, tempTable);
            try (RecordCursorIterator<QueryResult> cursor = planToResume.executePlan(recordStore, evaluationContext,
                    continuation, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                assertTrue(cursor.hasNext());
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(2L, "second"), asIdValue(message));
                assertFalse(cursor.hasNext());
                assertTrue(cursor.getNoNextReason().isSourceExhausted());
            }
            assertEquals(ImmutableList.of(Pair.of(1L, "first"), Pair.of(2L, "second")), collectResults(tempTable));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void scanTempTableWithPredicateWorksCorrectlyAcrossContinuations() {
        // select id, value from <tempTable> where id < 44L.
        byte[] continuation = null;
        RecordQueryPlan planToResume = null;
        final var tempTableId = CorrelationIdentifier.uniqueId();
        final var tempTable = tempTableInstance();
        try (FDBRecordContext context = openContext()) {
            tempTable.add(queryResult(1L, "one"));
            tempTable.add(queryResult(2L, "two"));
            tempTable.add(queryResult(3L, "three"));
            tempTable.add(queryResult(4L, "four"));
            planToResume = createAndOptimizeTempTableScanPlan(tempTableId);
            final var evaluationContext = setUpPlanContext(planToResume, tempTableId, tempTable);

            // Read the first two elements "one", "two".
            try (RecordCursorIterator<QueryResult> cursor = planToResume.executePlan(recordStore, evaluationContext,
                    null, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                assertTrue(cursor.hasNext());
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(1L, "one"), asIdValue(message));
                assertTrue(cursor.hasNext());
                message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(2L, "two"), asIdValue(message));
                continuation = cursor.getContinuation();
            }
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final var evaluationContext = setUpPlanContext(planToResume, tempTableId, tempTable);

            // Read the remaining elements "three", and "four".
            try (RecordCursorIterator<QueryResult> cursor = planToResume.executePlan(recordStore, evaluationContext,
                    continuation, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                assertTrue(cursor.hasNext());
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(3L, "three"), asIdValue(message));
                assertTrue(cursor.hasNext());
                message = Verify.verifyNotNull(cursor.next()).getMessage();
                assertEquals(Pair.of(4L, "four"), asIdValue(message));
                // reached the end of the cursor.
                assertFalse(cursor.hasNext());
            }
            context.commit();
        }
    }
}
