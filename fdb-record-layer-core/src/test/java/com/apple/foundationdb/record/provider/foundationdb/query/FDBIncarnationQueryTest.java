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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IncarnationValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;

import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.executeCascades;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.getField;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.resultColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.sortExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests of queries involving the {@code GET_VERSIONSTAMP_INCARNATION()} function.
 */
@Tag(Tags.RequiresFDB)
public class FDBIncarnationQueryTest extends FDBRecordStoreQueryTestBase {

    private void openStore(FDBRecordContext context) {
        openSimpleRecordStore(context, null);
    }

    // TODO Test with continuations
    // TODO test an update with setting a value to the incarnation

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void basicIncarnationQuery() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Set incarnation to a known value
            recordStore.updateIncarnation(current -> 42).join();

            // Save a simple record
            recordStore.saveRecord(MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .setNumValue2(100)
                    .build());

            // Plan a query approximating:
            //    SELECT GET_VERSIONSTAMP_INCARNATION() AS incarnation, MySimpleRecord.rec_no AS number FROM MySimpleRecord
            RecordQueryPlan plan = ((CascadesPlanner)planner).planGraph(() -> {
                var qun = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var graphExpansionBuilder = GraphExpansion.builder();
                graphExpansionBuilder.addQuantifier(qun);

                var recNoValue = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "rec_no");
                var incarnationValue = new IncarnationValue();

                graphExpansionBuilder.addResultColumn(resultColumn(incarnationValue, "incarnation"));
                graphExpansionBuilder.addResultColumn(resultColumn(recNoValue, "number"));

                var select = Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect()));

                return Reference.initialOf(sortExpression(java.util.List.of(), false, select));
            }, Optional.empty(), IndexQueryabilityFilter.DEFAULT, EvaluationContext.empty()).getPlan();

            assertMatchesExactly(plan, mapPlan(coveringIndexPlan()));

            // Execute and verify
            try (RecordCursor<QueryResult> cursor = executeCascades(recordStore, plan)) {
                RecordCursorResult<QueryResult> result = cursor.getNext();
                assertThat(result.hasNext(), equalTo(true));

                QueryResult queryResult = Objects.requireNonNull(result.get());
                Integer incarnation = getField(queryResult, Integer.class, "incarnation");
                assertNotNull(incarnation);
                assertEquals(42, incarnation.intValue());

                Long number = getField(queryResult, Long.class, "number");
                assertNotNull(number);
                assertEquals(1L, number.longValue());
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void incarnationQueryAfterUpdate() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            // Set initial incarnation
            recordStore.updateIncarnation(current -> 10).join();

            // Save a record
            recordStore.saveRecord(MySimpleRecord.newBuilder()
                    .setRecNo(2L)
                    .setNumValue2(200)
                    .build());

            // Update incarnation
            recordStore.updateIncarnation(current -> current + 5).join();

            // Plan a query to get the incarnation
            RecordQueryPlan plan = ((CascadesPlanner)planner).planGraph(() -> {
                var qun = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");

                final var graphExpansionBuilder = GraphExpansion.builder();
                graphExpansionBuilder.addQuantifier(qun);

                var incarnationValue = new IncarnationValue();
                graphExpansionBuilder.addResultColumn(resultColumn(incarnationValue, "incarnation"));

                var select = Quantifier.forEach(Reference.initialOf(graphExpansionBuilder.build().buildSelect()));

                return Reference.initialOf(sortExpression(java.util.List.of(), false, select));
            }, Optional.empty(), IndexQueryabilityFilter.DEFAULT, EvaluationContext.empty()).getPlan();

            // Execute and verify the incarnation value is 15 (10 + 5)
            try (RecordCursor<QueryResult> cursor = executeCascades(recordStore, plan)) {
                RecordCursorResult<QueryResult> result = cursor.getNext();
                assertThat(result.hasNext(), equalTo(true));

                QueryResult queryResult = Objects.requireNonNull(result.get());
                Integer incarnation = getField(queryResult, Integer.class, "incarnation");
                assertNotNull(incarnation);
                assertEquals(15, incarnation.intValue());
            }
        }
    }
}
