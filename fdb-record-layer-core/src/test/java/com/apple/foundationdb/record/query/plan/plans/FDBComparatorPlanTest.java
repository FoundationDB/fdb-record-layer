/*
 * SelectorPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.test.Tags;
import com.google.common.base.VerifyException;
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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query), primaryKey());

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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey());

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(134, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansFail() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey());

        assertThrows(RecordCoreException.class, () -> querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty, record -> { }, record -> { }));
    }

    @DualPlannerTest
    void testTwoDifferentInnerPlansOneEndsSoonerFail() throws Exception {
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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey());

        assertThrows(RecordCoreException.class, () -> querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty, record -> { }, record -> { }));
    }

    @DualPlannerTest
    void testNoInnerPlansFail() throws Exception {
        complexQuerySetup(NO_HOOK);

        assertThrows(VerifyException.class, () -> RecordQueryComparatorPlan.from(Collections.emptyList(), primaryKey()));
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey());

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
    void testTwoDifferentInnerPlansWithContinuationFail() throws Throwable {
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey());

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build(),
                count -> assertThat(count, is(5)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(20, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build(),
                count -> assertThat(count, is(5)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(20, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 3, start with previous continuation, fail since one plan ends sooner
        byte[] continuation2 = result.getContinuation().toBytes();
        assertThrows(RecordCoreException.class, () -> querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation2, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(), count -> { }, record -> { }, context -> { }));
    }

    @Nonnull
    private List<RecordQueryPlan> plan(final RecordQuery... queries) {
        return Arrays.stream(queries).map(planner::plan).collect(Collectors.toList());
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

}
