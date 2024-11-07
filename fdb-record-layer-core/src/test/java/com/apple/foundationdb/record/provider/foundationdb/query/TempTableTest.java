/*
 * TempTableTest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableValuedCorrelationScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.equalsObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.explodePlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.target;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tqInsertPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tqScanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link TempTable} planning and execution.
 * Particularly, testing both {@code INSERT} and {@code SELECT} capabilities from table queues.
 */
public class TempTableTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    void setupPlanner() {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void scanTableQueueWorksCorrectly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            // select rec_no, str_value_indexed from tq1 | tq1 is a TableQueue.
            final var tableQueue = TempTable.newInstance("tq1");
            final var plan = getTqSelectPlan(tableQueue, true);
            assertEquals(ImmutableSet.of(Pair.of(42L, "fortySecondValue"),
                    Pair.of(45L, "fortyFifthValue")), collectResults(context, plan, tableQueue));
        }
    }


    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void scanTableQueueWithPredicateWorksCorrectly() throws Exception {
        // select rec_no, str_value_indexed from tq1 where rec_no < 44L | tq1 is a TableQueue.
        try (FDBRecordContext context = openContext()) {
            final var type = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
            final var tableQueue = TempTable.newInstance("tq1");
            tableQueue.add(QueryResult.ofComputed(item(42L, "fortySecondValue")),
                    QueryResult.ofComputed(item(45L, "fortyFifthValue")));
            final var tableQueueScanQun = Quantifier.forEach(Reference.of(new TableValuedCorrelationScanExpression(type, tableQueue.getName())));
            final var recNoField = FieldValue.ofFieldName(tableQueueScanQun.getFlowedObjectValue(), "rec_no");
            final var recNoColumn = Column.of(Optional.of("rec_no"), FieldValue.ofFieldName(tableQueueScanQun.getFlowedObjectValue(), "rec_no"));
            final var strValueIndexedField = Column.of(Optional.of("str_value_indexed"), FieldValue.ofFieldName(tableQueueScanQun.getFlowedObjectValue(), "str_value_indexed"));
            final var selectExpressionBuilder = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(recNoColumn, strValueIndexedField))
                    .addPredicate(new ValuePredicate(recNoField, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 44L)))
                    .addQuantifier(tableQueueScanQun);
            final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(selectExpressionBuilder.build().buildSelect()))));
            final var cascadesPlanner = (CascadesPlanner)planner;
            final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
            assertMatchesExactly(plan, mapPlan(predicatesFilterPlan(tqScanPlan()).where(predicates(only(valuePredicate(fieldValueWithFieldNames("rec_no"), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 44L)))))));
            assertEquals(ImmutableSet.of(Pair.of(42L, "fortySecondValue")), collectResults(context, plan, tableQueue));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void insertTableQueueWorksCorrectly() throws Exception {
        // insert into tq1 values ((1, 'first', 10, 1), (2, 'second', 11, 2))
        try (FDBRecordContext context = openContext()) {
            final var tableQueue = TempTable.newInstance("tq1");
            final var firstRecord = RecordConstructorValue.ofUnnamed(
                    ImmutableList.of(LiteralValue.ofScalar(1L),
                            LiteralValue.ofScalar("first"),
                            LiteralValue.ofScalar(10),
                            LiteralValue.ofScalar(1),
                            LiteralValue.ofScalar(1),
                            emptyArray(Type.primitiveType(Type.TypeCode.INT))));
            final var secondArray = RecordConstructorValue.ofUnnamed(
                    ImmutableList.of(LiteralValue.ofScalar(2L),
                            LiteralValue.ofScalar("second"),
                            LiteralValue.ofScalar(11),
                            LiteralValue.ofScalar(2),
                            LiteralValue.ofScalar(2),
                            emptyArray(Type.primitiveType(Type.TypeCode.INT))));
            final var explodeExpression = new ExplodeExpression(AbstractArrayConstructorValue.LightArrayConstructorValue.of(firstRecord, secondArray));
            var qun = Quantifier.forEach(Reference.of(explodeExpression));

            qun = Quantifier.forEach(Reference.of(new TempTableInsertExpression(qun, "MySimpleRecord",
                    Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()), tableQueue.getName())));
            final var insertPlan = Reference.of(LogicalSortExpression.unsorted(qun));

            final var cascadesPlanner = (CascadesPlanner)planner;
            var plan = cascadesPlanner.planGraph(() -> insertPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
            assertMatchesExactly(plan,  tqInsertPlan(explodePlan()).where(target(equalsObject("MySimpleRecord"))));
            final var evaluationContext = EvaluationContext.empty().withBinding(Bindings.BindingKind.CORRELATION, tableQueue.getName(), tableQueue);
            fetchResultValues(context, plan, Function.identity(), evaluationContext, c -> { }, ExecuteProperties.SERIAL_EXECUTE);

            // select rec_no, str_value_indexed from tq1 | tq1 is a TableQueue.
            plan = getTqSelectPlan(tableQueue, false);
            assertEquals(ImmutableSet.of(Pair.of(1L, "first"),
                    Pair.of(2L, "second")), collectResults(context, plan, tableQueue));
        }
    }

    @Nonnull
    private Set<Pair<Long, String>> collectResults(@Nonnull FDBRecordContext context, @Nonnull RecordQueryPlan plan,
                                                   @Nonnull TempTable tempTable) throws Exception {
        ImmutableSet.Builder<Pair<Long, String>> resultBuilder = ImmutableSet.builder();
        final var evaluationContext = EvaluationContext.empty().withBinding(Bindings.BindingKind.CORRELATION, tempTable.getName(), tempTable);
        fetchResultValues(context, plan, record -> {
            final Descriptors.Descriptor recDescriptor = record.getDescriptorForType();
            Long recNo = (long) record.getField(recDescriptor.findFieldByName("rec_no"));
            String strValueIndexed = (String) record.getField(recDescriptor.findFieldByName("str_value_indexed"));
            resultBuilder.add(Pair.of(recNo, strValueIndexed));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().setDryRun(true).build());
        return resultBuilder.build();
    }

    @Nonnull
    private static Message item(long recNo, @Nonnull String strValueIndexed) {
        return TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strValueIndexed)
                .build();
    }

    @Nonnull
    private RecordQueryPlan getTqSelectPlan(@Nonnull TempTable tempTable, boolean addData) {
        final var type = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        if (addData) {
            tempTable.add(QueryResult.ofComputed(item(42L, "fortySecondValue")),
                    QueryResult.ofComputed(item(45L, "fortyFifthValue")));
        }
        final var tableQueueScanQun = Quantifier.forEach(Reference.of(new TableValuedCorrelationScanExpression(type, tempTable.getName())));
        final var recNoField = Column.of(Optional.of("rec_no"), FieldValue.ofFieldName(tableQueueScanQun.getFlowedObjectValue(), "rec_no"));
        final var strValueIndexedField = Column.of(Optional.of("str_value_indexed"), FieldValue.ofFieldName(tableQueueScanQun.getFlowedObjectValue(), "str_value_indexed"));
        final var selectExpressionBuilder = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(recNoField, strValueIndexedField))
                .addQuantifier(tableQueueScanQun);
        final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(selectExpressionBuilder.build().buildSelect()))));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
        assertMatchesExactly(plan, mapPlan(tqScanPlan()));
        return plan;
    }
}
