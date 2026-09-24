/*
 * RecordQueryMax1RecordPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreCardinalityViolationException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RecordQueryMax1RecordPlan}.
 */
@Tag(Tags.RequiresFDB)
class RecordQueryMax1RecordPlanTest extends FDBRecordStoreTestBase {
    @Nonnull
    private static final String NUM_VALUE_3_INDEX = "MySimpleRecord$num_value_3_indexed";
    @Nonnull
    private static final String STR_VALUE_INDEX = "MySimpleRecord$str_value_indexed";

    /**
     * Saves five records with distinct {@code num_value_3_indexed} values, three of which have an
     * {@code str_value_indexed} of {@code "even"}.
     */
    @BeforeEach
    void setUpStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValue3Indexed(i)
                        .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                        .build());
            }
            commit(context);
        }
    }

    @Nonnull
    private static RecordQueryPlan indexPlanEquals(@Nonnull final String indexName, @Nonnull final Object value) {
        return new RecordQueryIndexPlan(indexName,
                IndexScanComparisons.byValue(new ScanComparisons(
                        ImmutableList.of(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        ImmutableSet.of())),
                false);
    }

    @Nonnull
    private static RecordQueryMax1RecordPlan max1RecordPlan(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryMax1RecordPlan(Quantifier.physical(Reference.plannedOf(child)));
    }

    /**
     * Executes {@code plan} to completion and returns the record numbers of the records it returns.
     */
    @Nonnull
    private List<Long> execute(@Nonnull final RecordQueryPlan plan,
                               @Nullable final byte[] continuation,
                               @Nonnull final ExecuteProperties executeProperties) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor =
                         recordStore.executeQuery(plan, continuation, executeProperties)) {
                return context.asyncToSync(FDBStoreTimer.Waits.WAIT_EXECUTE_QUERY,
                        cursor.map(RecordQueryMax1RecordPlanTest::recNo).asList());
            }
        }
    }

    private static long recNo(@Nonnull final FDBQueriedRecord<Message> queriedRecord) {
        return TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(queriedRecord.getRecord()).getRecNo();
    }

    /**
     * Tests that the plan returns nothing if its inner returns no records.
     */
    @Test
    void testMax1Record1() throws Exception {
        final RecordQueryPlan plan = max1RecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 99));
        assertThat(execute(plan, null, ExecuteProperties.SERIAL_EXECUTE)).isEmpty();
    }

    /**
     * Tests that the plan returns the record of its inner if its inner returns exactly one record.
     */
    @Test
    void testMax1Record2() throws Exception {
        final RecordQueryPlan plan = max1RecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        assertThat(execute(plan, null, ExecuteProperties.SERIAL_EXECUTE)).containsExactly(2L);
    }

    /**
     * Tests that the plan raises a {@link RecordCoreCardinalityViolationException} carrying its plan hash if its inner
     * returns more than one record.
     */
    @Test
    void testMax1Record3() {
        final RecordQueryPlan plan = max1RecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even"));
        assertThatThrownBy(() -> execute(plan, null, ExecuteProperties.SERIAL_EXECUTE))
                .isInstanceOf(RecordCoreCardinalityViolationException.class)
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsKey(LogMessageKeys.PLAN_HASH.toString()));
    }

    /**
     * Tests that a returned-row limit of 1 does not hide the second record of the inner from the check.
     */
    @Test
    void testMax1Record4() {
        final ExecuteProperties limitOne = ExecuteProperties.newBuilder().setReturnedRowLimit(1).build();
        assertThatThrownBy(() -> execute(max1RecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, limitOne))
                .isInstanceOf(RecordCoreCardinalityViolationException.class);
    }

    /**
     * Tests that a skip applies to the records of the plan rather than to those of its inner.
     */
    @Test
    void testMax1Record5() throws Exception {
        final ExecuteProperties skipOne = ExecuteProperties.newBuilder().setSkip(1).build();
        assertThat(execute(max1RecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), null, skipOne)).isEmpty();
        // Skipping the first of several records of the inner must not hide the violation.
        assertThatThrownBy(() -> execute(max1RecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, skipOne))
                .isInstanceOf(RecordCoreCardinalityViolationException.class);
    }

    /**
     * Tests that the plan raises a {@link RecordCoreException} carrying the no-next reason if its inner stops early on
     * a scan limit before producing a second record.
     */
    @Test
    void testMax1Record6() {
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();
        assertThatThrownBy(() -> execute(max1RecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, scanLimitOne))
                .isExactlyInstanceOf(RecordCoreException.class)
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsEntry(LogMessageKeys.NO_NEXT_REASON.toString(),
                                RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
    }

    /**
     * Tests that resuming the plan from the continuation after its record returns nothing more.
     */
    @Test
    void testMax1Record7() throws Exception {
        final RecordQueryMax1RecordPlan plan = max1RecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        final byte[] continuation;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor =
                         recordStore.executeQuery(plan, null, ExecuteProperties.SERIAL_EXECUTE)) {
                final RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertThat(result.hasNext()).isTrue();
                assertThat(recNo(result.get())).isEqualTo(2L);
                continuation = result.getContinuation().toBytes();
            }
        }
        assertThat(continuation).isNotNull();
        assertThat(execute(plan, continuation, ExecuteProperties.SERIAL_EXECUTE)).isEmpty();
    }

    /**
     * Tests that the plan explains as a {@code MAX 1 RECORD} operator on top of its inner.
     */
    @Test
    void testMax1Record8() {
        assertThat(max1RecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)).toString()).endsWith("| MAX 1 RECORD");
    }
}
