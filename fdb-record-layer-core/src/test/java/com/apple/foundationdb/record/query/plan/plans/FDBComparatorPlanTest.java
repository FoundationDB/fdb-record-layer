/*
 * FDBComparatorPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(Tags.RequiresFDB)
public class FDBComparatorPlanTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    void setup() throws Exception {
    }

    @DualPlannerTest
    void testOneInnerPlan() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query), primaryKey(), 0);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(67, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoSameInnerPlans() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(134, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansIndex0() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(2))
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(134, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansIndex1() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(2))
                .build();
        // Note the reference index is now 1
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 1);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(2)),
                context -> assertDiscardedAtMost(134, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansOneEndsSooner() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        // This plan has same records as the previous one, but ends sooner
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("rec_no").lessThan(5L)))
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(134, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansReferenceEndsSooner() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        // This plan has same records as the previous one, but end sooner
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("rec_no").lessThan(5L)))
                .build();

        // Note this time the reference plan is hte second one
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 1);

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(10, context));
        assertEquals(2, count);
    }

    @DualPlannerTest
    void testNoInnerPlansFail() throws Exception {
        complexQuerySetup(NO_HOOK);

        assertThrows(RecordCoreArgumentException.class, () -> RecordQueryComparatorPlan.from(Collections.emptyList(), primaryKey(), 0));
    }

    @DualPlannerTest
    void testTwoSameInnerPlansWithContinuation() throws Throwable {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        // This will select plan 1 for the first execution and later some illegal value. The idea is that after the
        // first iteration, the continuation should determine the selected plan and not the relative priorities
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(60, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(60, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 3, start with previous continuation, reach end
        continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(3)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(16, context));

        assertThat(result.hasNext(), is(false));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansWithContinuation() throws Throwable {
        // This test has the non-reference plan ends but the cursor continues until the end of the reference plan
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        // This plan has same records as the previous one, but end sooner
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("rec_no").lessThan(50L)))
                .build();

        // This will select plan 1 for the first execution and later some illegal value. The idea is that after the
        // first iteration, the continuation should determine the selected plan and not the relative priorities
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(60, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(60, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 3, start with previous continuation, reach end
        continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(3)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(16, context));

        assertThat(result.hasNext(), is(false));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
    }

    @DualPlannerTest
    void testTwoSameInnerPlansWithScannedRowLimit() throws Throwable {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        // This will select plan 1 for the first execution and later some illegal value. The idea is that after the
        // first iteration, the continuation should determine the selected plan and not the relative priorities
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                // Note that this execution uses setScannedRecordsLimit (as opposed to returned row limit)
                null, ExecuteProperties.newBuilder().setScannedRecordsLimit(200).build(),
                count -> assertThat(count, is(17)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(68, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation, reach end (before limit)
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setScannedRecordsLimit(200).build(),
                count -> assertThat(count, is(16)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(66, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansWithScannedRowLimit() throws Throwable {
        // This test has the non-reference plan ends but the cursor continues until the end of the reference plan
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        // This plan has same records as the previous one, but end sooner
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("rec_no").lessThan(50L)))
                .build();


        // This will select plan 1 for the first execution and later some illegal value. The idea is that after the
        // first iteration, the continuation should determine the selected plan and not the relative priorities
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                // Note that this execution uses setScannedRecordsLimit (as opposed to returned row limit)
                null, ExecuteProperties.newBuilder().setScannedRecordsLimit(200).build(),
                count -> assertThat(count, is(17)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(68, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation, reach end (before limit)
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setScannedRecordsLimit(200).build(),
                count -> assertThat(count, is(16)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(66, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
    }

    @DualPlannerTest
    void testIllegalReferencePlanIndex() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        assertThrows(RecordCoreArgumentException.class, () -> RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 3));

    }

    @DualPlannerTest
    void testRepeatedKeyFails() throws Exception {
        // Test when the comparison key is a repeated field
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        Descriptors.FieldDescriptor comparisonKey = recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getDescriptor().findFieldByName("repeater");
        KeyExpression keyExpression = Key.Expressions.fromDescriptor(comparisonKey);
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), keyExpression, 0);

        // For now, we can't compare keys that evaluate to multiple values and so the execution fails.
        assertThrows(RecordCoreException.class, () -> querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(134, context)));
    }

    @Nonnull
    private List<RecordQueryPlan> plan(final RecordQuery... queries) {
        return Arrays.stream(queries).map(planner::plan).collect(Collectors.toList());
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

}
