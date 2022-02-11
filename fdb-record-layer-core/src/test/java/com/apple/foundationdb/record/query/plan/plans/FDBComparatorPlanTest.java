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
public abstract class FDBComparatorPlanTest extends FDBRecordStoreQueryTestBase {

    private boolean abortOnComparisonFailure;

    @DualPlannerTest
    void testOneInnerPlan() throws Exception {
        complexQuerySetup(NO_HOOK);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(1))
                .build();

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query), primaryKey(), 0, abortOnComparisonFailure);

        assertSamePlans(planUnderTest, 1, 67, 33);
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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        assertSamePlans(planUnderTest, 1, 134, 33);
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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        assertDifferentPlans(planUnderTest, 1, 134, 33);
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 1, abortOnComparisonFailure);

        assertDifferentPlans(planUnderTest, 2, 134, 33);
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

        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        assertDifferentPlans(planUnderTest, 1, 134, 33);
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 1, abortOnComparisonFailure);

        assertDifferentPlans(planUnderTest, 1, 10, 2);
    }

    @DualPlannerTest
    void testNoInnerPlansFail() throws Exception {
        complexQuerySetup(NO_HOOK);

        assertThrows(RecordCoreArgumentException.class, () -> RecordQueryComparatorPlan.from(Collections.emptyList(), primaryKey(), 0, abortOnComparisonFailure));
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = assertSamePlansWithContinuation(planUnderTest, null, 15, 0, 15, 1, 60, false, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);

        // Iteration 2, start with previous continuation
        byte[] continuation = result.getContinuation().toBytes();
        result = assertSamePlansWithContinuation(planUnderTest, continuation, 15, 0, 15, 1, 60, false, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);

        // Iteration 3, start with previous continuation, reach end
        continuation = result.getContinuation().toBytes();
        assertSamePlansWithContinuation(planUnderTest, continuation, 15, 0, 3, 1, 16, false, RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        // Iteration 1, start with empty continuation.
        // The plans are the "same" here since we haven't reached the point at which the second scan ends (yet)
        RecordCursorResult<FDBQueriedRecord<Message>> result = assertSamePlansWithContinuation(planUnderTest, null, 15, 0, 15, 1, 60, false, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);

        // Iteration 2, start with previous continuation
        // For this iteration, the second plan ends sooner, so the comparison fails
        byte[] continuation = result.getContinuation().toBytes();
        result = assertDifferentPlansWithContinuation(planUnderTest, continuation, 15, 0, 15, 1, 60, false, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);

        if (result != null) {
            // Iteration 3, start with previous continuation, reach end
            // (in case we have a result from the first iteration)
            continuation = result.getContinuation().toBytes();
            assertDifferentPlansWithContinuation(planUnderTest, continuation, 15, 0, 3, 1, 16, false, RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
        }
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        // Iteration 1, start with empty continuation
        RecordCursorResult<FDBQueriedRecord<Message>> result = assertSamePlansWithContinuation(planUnderTest, null, 0, 200, 17, 1, 68, false, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);

        // Iteration 2, start with previous continuation, reach end (before limit)
        byte[] continuation = result.getContinuation().toBytes();
        assertSamePlansWithContinuation(planUnderTest, continuation, 0, 200,16, 1, 66, false, RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 0, abortOnComparisonFailure);

        // Iteration 1, start with empty continuation
        // The plans are the "same" here since we haven't reached the point at which the second scan ends (yet)
        RecordCursorResult<FDBQueriedRecord<Message>> result = assertSamePlansWithContinuation(planUnderTest, null, 0, 200, 17, 1, 68, false, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);

        // Iteration 2, start with previous continuation, reach end (before limit)
        byte[] continuation = result.getContinuation().toBytes();
        assertDifferentPlansWithContinuation(planUnderTest, continuation, 0, 200, 16, 1, 66, false, RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
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

        assertThrows(RecordCoreArgumentException.class, () -> RecordQueryComparatorPlan.from(plan(query1, query2), primaryKey(), 3, abortOnComparisonFailure));

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
        RecordQueryPlan planUnderTest = RecordQueryComparatorPlan.from(plan(query1, query2), keyExpression, 0, abortOnComparisonFailure);

        // Repeated keys will fail the comparison since they are not supported
        assertDifferentPlans(planUnderTest, 1, 134, 33);
    }

    public boolean isAbortOnComparisonFailure() {
        return abortOnComparisonFailure;
    }

    public void setAbortOnComparisonFailure(final boolean abortOnComparisonFailure) {
        this.abortOnComparisonFailure = abortOnComparisonFailure;
    }

    @Nonnull
    private List<RecordQueryPlan> plan(final RecordQuery... queries) {
        return Arrays.stream(queries).map(planner::plan).collect(Collectors.toList());
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

    /**
     * Assert that the comparator plan runs through with the comparison successfully, compare the results to the given.
     * parameters
     * @param planUnderTest the comparator plan
     * @param numValue2 the expected value of the numValue2 field in the read records
     * @param atMostDiscarded the expected maximum allowed number of discarded records
     * @param totalCount the total returned record count
     *
     * @throws Exception just in case
     */
    protected void assertSamePlans(final RecordQueryPlan planUnderTest, final int numValue2, final int atMostDiscarded, final int totalCount) throws Exception {
        int count = querySimpleRecordStore(NO_HOOK, planUnderTest, EvaluationContext::empty,
                record -> assertThat(record.getNumValue2(), is(numValue2)),
                context -> assertDiscardedAtMost(atMostDiscarded, context));
        assertEquals(totalCount, count);
    }

    /**
     * Assert that the comparator plan fails to compare the two plans. This is overridden by subclasses since the behavior
     * is different depending on the value of the abortOnComparisonFailure flag.
     * @param planUnderTest the comparator plan
     * @param numValue2 the expected value of the numValue2 field in the read records
     * @param atMostDiscarded the expected maximum allowed number of discarded records
     * @param totalCount the total returned record count
     *
     * @throws Exception just in case
     */
    protected abstract void assertDifferentPlans(final RecordQueryPlan planUnderTest, final int numValue2, final int atMostDiscarded, final int totalCount) throws Exception;

    /**
     * Assert that the comparator plan runs through with comparison successfully when given an (optional) continuation.
     * @param planUnderTest the comparator plan
     * @param continuation optional contionuation to pick up from
     * @param returnedRowLimit row limit to pass in to the query execution
     * @param numRecords the expected number of records that should be returned
     * @param numValue2 the expected value of the numValue2 field in the read records
     * @param atMostDiscarded the expected maximum allowed number of discarded records
     * @param hasNext the expected hasNext values from the result
     * @param noNextReason the optional expected noNextReason from the result
     * @return the query result
     * @throws Throwable just in case
     */
    protected RecordCursorResult<FDBQueriedRecord<Message>> assertSamePlansWithContinuation(
            final RecordQueryPlan planUnderTest, final byte[] continuation,
            final int returnedRowLimit, final int scannedRowLimit, final int numRecords, final int numValue2,
            final int atMostDiscarded, final boolean hasNext, final RecordCursor.NoNextReason noNextReason) throws Throwable {

        RecordCursorResult<FDBQueriedRecord<Message>> result = querySimpleRecordStoreWithContinuation(
                NO_HOOK, planUnderTest, EvaluationContext::empty,
                continuation, executePropertiesFor(returnedRowLimit, scannedRowLimit),
                count -> assertThat(count, is(numRecords)),
                record -> assertThat(record.getNumValue2(), is(numValue2)),
                context -> assertDiscardedAtMost(atMostDiscarded, context));

        assertThat(result.hasNext(), is(hasNext));
        assertThat(result.getNoNextReason(), is(noNextReason));
        return result;
    }

    /**
     * Assert that the comparator plan fails to compare the two plans. This is overridden by subclasses since the behavior
     * is different depending on the value of the abortOnComparisonFailure flag.
     * @param planUnderTest the comparator plan
     * @param continuation optional contionuation to pick up from
     * @param returnedRowLimit row limit to pass in to the query execution
     * @param numRecords the expected number of records that should be returned
     * @param numValue2 the expected value of the numValue2 field in the read records
     * @param atMostDiscarded the expected maximum allowed number of discarded records
     * @param hasNext the expected hasNext values from the result
     * @param noNextReason the optional expected noNextReason from the result
     * @return the query result
     * @throws Throwable just in case
     */
    protected abstract RecordCursorResult<FDBQueriedRecord<Message>> assertDifferentPlansWithContinuation(
            final RecordQueryPlan planUnderTest, final byte[] continuation,
            final int returnedRowLimit, final int scannedRowLimit, final int numRecords, final int numValue2,
            final int atMostDiscarded, final boolean hasNext, final RecordCursor.NoNextReason noNextReason) throws Throwable;

    protected ExecuteProperties executePropertiesFor(final int returnedRowLimit, int scannedRowLimit) {
        ExecuteProperties.Builder builder = ExecuteProperties.newBuilder();
        if (returnedRowLimit > 0) {
            builder.setReturnedRowLimit(returnedRowLimit);
        }
        if (scannedRowLimit > 0) {
            builder.setScannedRecordsLimit(scannedRowLimit);
        }
        return builder.build();
    }
}

