/*
 * FDBSelectorPlanTest.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePickerValue;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
public class FDBSelectorPlanTest extends FDBRecordStoreQueryTestBase {
    int mockSelectionCount;

    @BeforeEach
    void setup() throws Exception {
        mockSelectionCount = 0;
    }

    @DualPlannerTest
    void testOneInnerPlan() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        RecordQueryPlan planUnderTest = RecordQuerySelectorPlan.from(plan(query), Collections.singletonList(100));

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(67, context));
        assertEquals(33, count);
    }

    @DualPlannerTest
    void testTwoInnerPlan() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();

        // This will always select plan1 for execution
        RecordQueryPlan planUnderTest = RecordQuerySelectorPlan.from(plan(query1, query2), Arrays.asList(100, 0));

        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(67, context));
        assertEquals(33, count);
    }

    @Test
    void testNoInnerPlansFails() {
        assertThrows(RecordCoreArgumentException.class, () -> RecordQuerySelectorPlan.from(Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    void testMismatchPrioritiesNumber() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        assertThrows(RecordCoreArgumentException.class, () -> RecordQuerySelectorPlan.from(plan(query), Arrays.asList(10, 20, 70)));
    }

    @DualPlannerTest
    void testTwoInnerPlansWithContinuation() throws Throwable {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        // This will select plan 1 for the first execution and later some illegal value. The idea is that after the
        // first iteration, the continuation should determine the selected plan and not the relative priorities
        RecordQueryPlan planUnderTest = RecordQuerySelectorPlan.from(plan(query1, query2), mockSelector());

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(30, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 2, start with previous continuation
        byte[] continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(15)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(30, context));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED));

        // Iteration 3, start with previous continuation, reach end
        continuation = result.getContinuation().toBytes();
        result = querySimpleRecordStoreWithContinuation(NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build(),
                count -> assertThat(count, is(3)),
                record -> assertThat(record.getNumValue2(), is(1)),
                context -> assertDiscardedAtMost(8, context));

        assertThat(result.hasNext(), is(false));
        assertThat(result.getNoNextReason(), is(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
    }

    @DualPlannerTest
    void testPlanValues() throws Throwable {
        complexQuerySetup(NO_HOOK);

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();
        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        RecordQueryPlan plan = RecordQuerySelectorPlan.from(plan(query1, query2), List.of(50, 50));

        ValuePickerValue value = (ValuePickerValue)plan.getResultValue();
        List<Value> subValues = ImmutableList.copyOf(value.getChildren());
        assertThat(subValues.size(), is(2));
        assertThat(((QuantifiedObjectValue)subValues.get(0)).getAlias(), is(plan.getQuantifiers().get(0).getAlias()));
        assertThat(((QuantifiedObjectValue)subValues.get(1)).getAlias(), is(plan.getQuantifiers().get(1).getAlias()));
    }

    private PlanSelector mockSelector() {
        return new PlanSelector() {
            @Override
            public int selectPlan(final List<RecordQueryPlan> plans) {
                return (mockSelectionCount++ == 0) ? 1 : 1000;
            }

            @Override
            public int planHash(@Nonnull final PlanHashKind hashKind) {
                return 0;
            }
        };
    }

    @Nonnull
    private List<RecordQueryPlan> plan(RecordQuery... queries) {
        return Arrays.stream(queries).map(planner::plan).collect(Collectors.toList());
    }
}
