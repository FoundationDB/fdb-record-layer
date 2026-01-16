/*
 * FDBIncarnationQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IncarnationValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.executeCascades;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.getField;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.resultColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.sortExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of queries involving the {@code GET_VERSIONSTAMP_INCARNATION()} function.
 */
@Tag(Tags.RequiresFDB)
public class FDBIncarnationQueryTest extends FDBRecordStoreQueryTestBase {

    private void openStore(FDBRecordContext context) {
        openSimpleRecordStore(context, null);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void basicIncarnationQuery() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Set incarnation to a known value
            recordStore.updateIncarnation(current -> 42).join();

            // Save a simple record
            saveSimpleRecord(1L, 100);

            final RecordQueryPlan plan = getRecordQueryPlan();

            // Execute and verify
            try (RecordCursor<QueryResult> cursor = executeCascades(recordStore, plan)) {
                RecordCursorResult<QueryResult> result = cursor.getNext();
                assertThat(result.hasNext(), equalTo(true));

                QueryResult queryResult = Objects.requireNonNull(result.get());
                assertEquals(42, getIncarnation(queryResult));
                assertRecNoValue(queryResult, 1L);
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void incarnationQueryWithContinuations() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Set initial incarnation
            recordStore.updateIncarnation(current -> 100).join();

            // Save multiple records
            for (int i = 0; i < 10; i++) {
                saveSimpleRecord(i, i * 10);
            }

            // Plan a query that returns incarnation with each record
            final RecordQueryPlan plan = getRecordQueryPlan();

            // Build type repository for plan execution
            final EvaluationContext evalContext = getEvaluationContext(plan);

            // First batch: fetch 3 records with incarnation = 100
            RecordCursorContinuation continuation = getIncarnations(plan, evalContext, null, List.of(100, 100, 100));
            assertFalse(continuation.isEnd());

            // Change incarnation before second batch
            recordStore.updateIncarnation(current -> 200).join();

            continuation = getIncarnations(plan, evalContext, continuation, List.of(200, 200, 200));
            assertFalse(continuation.isEnd());

            continuation = getIncarnations(plan, evalContext, continuation, List.of(200, 200, 200));
            assertFalse(continuation.isEnd());

            recordStore.updateIncarnation(current -> 300).join();

            continuation = getIncarnations(plan, evalContext, continuation, List.of(300));
            assertTrue(continuation.isEnd());
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void incarnationQueryWithMultipleRows() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Set initial incarnation
            recordStore.updateIncarnation(current -> 50).join();

            // Save multiple records
            for (int i = 0; i < 5; i++) {
                saveSimpleRecord(i, i * 5);
            }

            // Plan a query that returns incarnation with each record
            final RecordQueryPlan plan = getRecordQueryPlan();

            List<Integer> incarnationsSeen = new ArrayList<>();

            // Execute query, changing incarnation between each row fetch
            try (RecordCursor<QueryResult> cursor = executeCascades(recordStore, plan)) {
                int expectedIncarnation = 50;
                RecordCursorResult<QueryResult> result = cursor.getNext();

                while (result.hasNext()) {
                    QueryResult queryResult = Objects.requireNonNull(result.get());
                    final Integer incarnation = getIncarnation(queryResult);
                    incarnationsSeen.add(incarnation);

                    // Verify current incarnation matches expected
                    assertEquals(expectedIncarnation, incarnation.intValue());

                    // Update incarnation for next row
                    expectedIncarnation += 10;
                    recordStore.updateIncarnation(current -> current + 10).join();

                    // Fetch next row
                    result = cursor.getNext();
                }
            }

            assertEquals(List.of(50, 60, 70, 80, 90), incarnationsSeen);
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void updateWithIncarnation() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Save records with different numValue2 values
            for (int i = 0; i < 3; i++) {
                saveSimpleRecord(i, i * 10);
            }

            // Create an update plan that sets numValue2 to the incarnation value
            // UPDATE MySimpleRecord SET num_value_2 = GET_VERSIONSTAMP_INCARNATION()
            RecordQueryPlan updatePlan = ((CascadesPlanner)planner).planGraph(() -> {
                final Type.Record recordType = Type.Record.fromDescriptor(MySimpleRecord.getDescriptor());
                Quantifier quantifier = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final GraphExpansion.Builder graphExpansionBuilder = GraphExpansion.builder();
                graphExpansionBuilder.addQuantifier(quantifier);
                // Remove any PseudoFields
                final Set<Type.Record.Field> properFields = Set.copyOf(recordType.getFields());
                final List<Column<? extends FieldValue>> resultColumns = quantifier.getFlowedColumns().stream()
                        .filter(column -> properFields.contains(column.getField()))
                        .collect(Collectors.toList());
                graphExpansionBuilder.addAllResultColumns(resultColumns);
                Quantifier.ForEach selectQuantifier = Quantifier.forEach(Reference.initialOf(
                        graphExpansionBuilder.build().buildSelect()));

                // Resolve the field path for num_value_2
                final FieldValue.FieldPath updatePath = FieldValue.resolveFieldPath(selectQuantifier.getFlowedObjectType(),
                        List.of(new FieldValue.Accessor("num_value_2", -1)));

                // Create the update value: incarnation value
                Quantifier.ForEach updateQun = Quantifier.forEach(Reference.initialOf(new UpdateExpression(selectQuantifier,
                        "MySimpleRecord",
                        recordType,
                        Map.of(updatePath, new IncarnationValue()))));

                return Reference.initialOf(LogicalSortExpression.unsorted(updateQun));
            }, Optional.empty(), IndexQueryabilityFilter.DEFAULT, EvaluationContext.empty()).getPlan();

            // Build type repository for plan execution
            final EvaluationContext evalContext = getEvaluationContext(updatePlan);

            for (final Integer incarnation : List.of(42, 100)) {
                recordStore.updateIncarnation(current -> incarnation).join();
                try (RecordCursor<QueryResult> cursor = updatePlan.executePlan(recordStore, evalContext,
                        null, ExecuteProperties.SERIAL_EXECUTE)) {
                    assertEquals(3, cursor.asList().join().size());
                }
                for (int i = 0; i < 3; i++) {
                    final MySimpleRecord rec = MySimpleRecord.newBuilder()
                            .mergeFrom(recordStore.loadRecord(Tuple.from(i)).getRecord())
                            .build();
                    assertEquals((int)incarnation, rec.getNumValue2());
                }
            }
        }
    }

    private void saveSimpleRecord(final long recNo, final int numValue2) {
        recordStore.saveRecord(MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .build());
    }

    @Nonnull
    private static EvaluationContext getEvaluationContext(final RecordQueryPlan updatePlan) {
        Set<Type> usedTypes = usedTypes().evaluate(updatePlan);
        TypeRepository typeRepository = TypeRepository.newBuilder()
                .addAllTypes(usedTypes)
                .build();
        return EvaluationContext.forTypeRepository(typeRepository);
    }

    @Nonnull
    private RecordQueryPlan getRecordQueryPlan() {
        // Plan a query approximating:
        //    SELECT GET_VERSIONSTAMP_INCARNATION() AS incarnation, MySimpleRecord.rec_no FROM MySimpleRecord
        RecordQueryPlan plan = ((CascadesPlanner)planner).planGraph(() -> {
            Quantifier quantifier = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

            final GraphExpansion.Builder graphExpansionBuilder = GraphExpansion.builder();
            graphExpansionBuilder.addQuantifier(quantifier);

            FieldValue recNoValue = FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "rec_no");
            IncarnationValue incarnationValue = new IncarnationValue();

            graphExpansionBuilder.addResultColumn(resultColumn(incarnationValue, "incarnation"));
            graphExpansionBuilder.addResultColumn(resultColumn(recNoValue, "rec_no"));

            Quantifier.ForEach select = Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect()));

            return Reference.initialOf(sortExpression(List.of(), false, select));
        }, Optional.empty(), IndexQueryabilityFilter.DEFAULT, EvaluationContext.empty()).getPlan();
        assertMatchesExactly(plan, mapPlan(coveringIndexPlan()));
        return plan;
    }

    private static int getIncarnation(final QueryResult queryResult) {
        Integer incarnation = getField(queryResult, Integer.class, "incarnation");
        assertNotNull(incarnation);
        return incarnation.intValue();
    }

    @Nonnull
    private RecordCursorContinuation getIncarnations(final RecordQueryPlan plan,
                                                     final EvaluationContext evalContext,
                                                     @Nullable final RecordCursorContinuation continuation,
                                                     List<Integer> expectedIncarnations) {
        try (RecordCursor<QueryResult> cursor = plan.executePlan(recordStore, evalContext,
                continuation == null ? null : continuation.toBytes(),
                ExecuteProperties.newBuilder().setReturnedRowLimit(3).build())) {
            final List<Integer> incarnationsSeen = new ArrayList<>();
            RecordCursorResult<QueryResult> result = cursor.getNext();
            while (result.hasNext()) {
                incarnationsSeen.add(getIncarnation(Objects.requireNonNull(result.get())));
                result = cursor.getNext();
            }
            assertEquals(expectedIncarnations, incarnationsSeen);
            return result.getContinuation();
        }
    }

    private static void assertRecNoValue(final QueryResult queryResult, long expected) {
        Long number = getField(queryResult, Long.class, "rec_no");
        assertNotNull(number);
        assertEquals(expected, number.longValue());
    }

}
