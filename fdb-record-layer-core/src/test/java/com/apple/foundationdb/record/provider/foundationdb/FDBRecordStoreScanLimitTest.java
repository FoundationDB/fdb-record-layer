/*
 * FDBRecordStoreScanLimitTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanLimitReachedException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithNoChildren;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for scan limits in {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FDBRecordStoreScanLimitTest extends FDBRecordStoreTestBase {
    @BeforeAll
    public void init() {
        clearAndInitialize();
    }

    @BeforeAll
    @BeforeEach
    public void setupRecordStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i + 1000);
                recBuilder.setNumValue3Indexed(i % 3);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    private RecordQueryPlan indexPlanEquals(String indexName, Object value) {
        return new RecordQueryIndexPlan(indexName, IndexScanType.BY_VALUE,
                new ScanComparisons(Arrays.asList(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        Collections.emptyList()),
                false);
    }

    private KeyExpression primaryKey() {
        assertNotNull(recordStore);
        return recordStore
                .getRecordMetaData()
                .getRecordType("MySimpleRecord")
                .getPrimaryKey();
    }

    private Optional<Integer> getRecordScanned(FDBRecordContext context) {
        if (context.getTimer() == null) {
            return Optional.empty();
        }

        return Optional.of(context.getTimer().getCount(FDBStoreTimer.Counts.LOAD_KEY_VALUE));
    }

    private Optional<Integer> getRecordsScannedByPlan(RecordQueryPlan plan) throws Exception {
        return getRecordsScannedByPlan(plan, ExecuteProperties.SERIAL_EXECUTE);
    }

    private Optional<Integer> getRecordsScannedByPlan(RecordQueryPlan plan, ExecuteProperties executeProperties) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            if (context.getTimer() != null) {
                context.getTimer().reset();
            }

            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                while (cursor.hasNext()) {
                    cursor.next().getRecord();
                }
                Optional<Integer> scanned = getRecordScanned(context);
                if (context.getTimer() != null) {
                    context.getTimer().reset();
                }
                return scanned;
            }
        }
    }

    private void assertNumberOfRecordsScanned(int expected, RecordQueryPlan plan, ExecuteProperties executeProperties, String message) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            if (context.getTimer() != null) {
                context.getTimer().reset();
            }

            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                boolean caughtScanLimitReached = false;
                try {
                    while (cursor.hasNext()) {
                        cursor.next().getRecord();
                    }
                } catch (RecordCoreException ex) {
                    if (executeProperties.isFailOnScanLimitReached() && ex.getCause() instanceof ScanLimitReachedException) {
                        caughtScanLimitReached = true;
                    } else {
                        throw ex;
                    }
                }
                if (executeProperties.isFailOnScanLimitReached() && !caughtScanLimitReached) {
                    assertNotEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, cursor.getNoNextReason());
                }
                Optional<Integer> scanned = getRecordScanned(context);
                if (context.getTimer() != null) {
                    context.getTimer().reset();
                }
                int overrun = BaseCursorCountVisitor.getCount(cursor);
                scanned.ifPresent(value -> assertThat(message, value, lessThanOrEqualTo(expected + overrun)));
            }
        }
    }

    private int getMaximumToScan(RecordQueryPlan plan) throws Exception {
        if (plan instanceof RecordQueryPlanWithNoChildren) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.SERIAL_EXECUTE)) {
                    int maximumToScan = 0;
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> record = cursor.next();
                        maximumToScan += record.getStoredRecord().getKeyCount() + (record.getStoredRecord().isVersionedInline() ? 1 : 0);
                    }
                    return maximumToScan;
                }
            }
        }

        int maximumToScan = 0;
        for (RecordQueryPlan child : plan.getChildren()) {
            maximumToScan += getMaximumToScan(child);
        }
        return maximumToScan;
    }

    public Stream<Arguments> plansWithoutFail() {
        return plans(false);
    }

    public Stream<Arguments> plansWithFails() {
        return Stream.of(Boolean.FALSE, Boolean.TRUE).flatMap(this::plans);
    }

    private Stream<Arguments> plans(boolean fail) {
        RecordQueryPlan scanPlan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
        RecordQueryPlan indexPlan = new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed",
                IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false);
        QueryComponent filter = Query.field("str_value_indexed").equalsValue("odd");
        QueryComponent middleFilter = Query.and(
                Query.field("rec_no").greaterThan(24L),
                Query.field("rec_no").lessThan(60L));
        RecordQueryPlan firstChild = indexPlanEquals("MySimpleRecord$str_value_indexed", "even");
        RecordQueryPlan secondChild = indexPlanEquals("MySimpleRecord$num_value_3_indexed", 0);
        return Stream.of(
                Arguments.of("simple index scan", fail, indexPlan),
                Arguments.of("reverse index scan", fail, new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed",
                        IndexScanType.BY_VALUE, ScanComparisons.EMPTY, true)),
                Arguments.of("filter on scan plan", fail, new RecordQueryFilterPlan(scanPlan, filter)),
                Arguments.of("filter on index plan", fail, new RecordQueryFilterPlan(indexPlan, filter)),
                Arguments.of("type filter on scan plan", fail, new RecordQueryTypeFilterPlan(scanPlan, Collections.singletonList("MySimpleRecord"))),
                Arguments.of("type filter on index plan", fail, new RecordQueryTypeFilterPlan(indexPlan, Collections.singletonList("MySimpleRecord"))),
                Arguments.of("disjoint union", fail, new RecordQueryUnionPlan(
                        indexPlanEquals("MySimpleRecord$str_value_indexed", "odd"),
                        indexPlanEquals("MySimpleRecord$str_value_indexed", "even"),
                        primaryKey(), false, false)),
                Arguments.of("overlapping union", fail, new RecordQueryUnionPlan(firstChild, secondChild, primaryKey(), false, false)),
                Arguments.of("overlapping union (swapped args)", fail, new RecordQueryUnionPlan(secondChild, firstChild, primaryKey(), false, false)),
                Arguments.of("overlapping intersection", fail, new RecordQueryIntersectionPlan(firstChild, secondChild, primaryKey(), false)),
                Arguments.of("overlapping intersection", fail, new RecordQueryIntersectionPlan(secondChild, firstChild, primaryKey(), false)),
                Arguments.of("union with inner filter", fail, new RecordQueryUnionPlan(
                        new RecordQueryFilterPlan(firstChild, middleFilter), secondChild, primaryKey(), false, false)),
                Arguments.of("union with two inner filters", fail, new RecordQueryUnionPlan(
                        new RecordQueryFilterPlan(firstChild, middleFilter),
                        new RecordQueryFilterPlan(secondChild, Query.field("rec_no").lessThan(55L)),
                        primaryKey(), false, false)),
                Arguments.of("intersection with inner filter", fail, new RecordQueryIntersectionPlan(
                        new RecordQueryFilterPlan(firstChild, middleFilter), secondChild, primaryKey(), false)),
                Arguments.of("intersection with two inner filters", fail, new RecordQueryIntersectionPlan(
                        new RecordQueryFilterPlan(firstChild, middleFilter),
                        new RecordQueryFilterPlan(secondChild, Query.field("rec_no").lessThan(55L)),
                        primaryKey(), false)));
    }

    @ParameterizedTest(name = "testPlans() [{index}] {0} {1}")
    @MethodSource("plansWithFails")
    public void testPlans(String description, boolean fail, RecordQueryPlan plan) throws Exception {
        // include a scanLimit of 0, in which case all progress happens via the first "free" key-value scan.
        int maximumToScan = getMaximumToScan(plan);
        for (int limit = 0; limit <= maximumToScan * 2; limit = limit * 2 + 1) {
            assertNumberOfRecordsScanned(limit, plan,
                    ExecuteProperties.newBuilder().setFailOnScanLimitReached(fail).setScannedRecordsLimit(limit).build(),
                    "should be limited by record scan limit");
        }
        for (int limit = maximumToScan + 1; limit <= 100; limit++) {
            assertNumberOfRecordsScanned(maximumToScan, plan,
                    ExecuteProperties.newBuilder().setFailOnScanLimitReached(fail).setScannedRecordsLimit(limit).build(),
                    "should not be limited by record scan limit");
        }
    }

    @ParameterizedTest(name = "plansByContinuation() [{index}] {0}")
    @MethodSource("plansWithoutFail")
    public void plansByContinuation(String description, boolean fail, RecordQueryPlan plan) throws Exception {
        int maximumToScan = getMaximumToScan(plan);

        // include a scanLimit of 0, in which case all progress happens via the first "free" key-value scan.
        for (int scanLimit = 0; scanLimit <= maximumToScan * 2; scanLimit = 2 * scanLimit + 1) {
            final Function<FDBQueriedRecord<Message>, Long> getRecNo = r -> {
                TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
                record.mergeFrom(r.getRecord());
                return record.getRecNo();
            };

            final ExecuteProperties.Builder properties = ExecuteProperties.newBuilder().setScannedRecordsLimit(scanLimit);

            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                final List<Long> allAtOnce;
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                    allAtOnce = cursor.map(getRecNo).asList().get();
                }

                final List<Long> byContinuation = new ArrayList<>();
                byte[] continuation = null;
                do {
                    try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, properties.build())) {
                        if (context.getTimer() != null) {
                            context.getTimer().reset();
                        }
                        while (cursor.hasNext()) {
                            byContinuation.add(getRecNo.apply(cursor.next()));
                        }
                        continuation = cursor.getContinuation();
                        int overrun = BaseCursorCountVisitor.getCount(cursor);
                        Optional<Integer> recordScanned = getRecordScanned(context);
                        if (recordScanned.isPresent()) {
                            assertThat(recordScanned.get(), lessThanOrEqualTo(Math.min(scanLimit + overrun, maximumToScan)));
                        }
                    }
                } while (continuation != null);
                assertEquals(allAtOnce, byContinuation);
            }
        }
    }

    @Test
    public void testSplitContinuation() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.deleteAllRecords(); // Undo setupRecordStore().
            commit(context);
        }

        final String bigValue = Strings.repeat("X", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String smallValue = Strings.repeat("Y", 5);

        final List<FDBStoredRecord<Message>> createdRecords = new ArrayList<>();
        createdRecords.add(saveAndSplitSimpleRecord(1L, smallValue, 1));
        createdRecords.add(saveAndSplitSimpleRecord(2L, smallValue, 2));
        createdRecords.add(saveAndSplitSimpleRecord(3L, bigValue, 3));
        createdRecords.add(saveAndSplitSimpleRecord(4L, smallValue, 4));
        createdRecords.add(saveAndSplitSimpleRecord(5L, bigValue, 5));
        createdRecords.add(saveAndSplitSimpleRecord(6L, bigValue, 6));
        createdRecords.add(saveAndSplitSimpleRecord(7L, smallValue, 7));
        createdRecords.add(saveAndSplitSimpleRecord(8L, smallValue, 8));
        createdRecords.add(saveAndSplitSimpleRecord(9L, smallValue, 9));

        // Scan one record at a time using continuations
        final List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            Supplier<ScanProperties> props = () -> new ScanProperties(ExecuteProperties.newBuilder()
                    .setScannedRecordsLimit(0)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null, props.get());
            while (messageCursor.hasNext()) {
                scannedRecords.add(messageCursor.next());
                messageCursor = recordStore.scanRecords(messageCursor.getContinuation(), props.get());
            }
            commit(context);
        }
        assertEquals(createdRecords, scannedRecords);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 5, 10, 20}) // for this test, the scan limit must divide 100
    public void testExecuteStateReset(int scanLimit) throws Exception {
        final RecordQueryPlan plan = new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed",
                IndexScanType.BY_VALUE, ScanComparisons.EMPTY, false);
        ExecuteProperties properties = ExecuteProperties.newBuilder().setScannedRecordsLimit(scanLimit).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            byte[] continuation = null;
            do {
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, properties)) {
                    int retrieved = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        retrieved++;
                    }
                    continuation = cursor.getContinuation();
                    if (continuation != null) {
                        // if this is our last call, we might retrieve 0 results
                        assertEquals(scanLimit, retrieved);
                    }
                    properties = properties.resetState();
                }
            } while (continuation != null);
        }
    }

    private static class BaseCursorCountVisitor implements RecordCursorVisitor {
        private int keyValueCursorCount = 0;

        @Override
        public boolean visitEnter(RecordCursor<?> cursor) {
            if (cursor instanceof BaseCursor) {
                keyValueCursorCount++;
            }
            return true;
        }

        @Override
        public boolean visitLeave(RecordCursor<?> cursor) {
            return true;
        }

        public static int getCount(RecordCursor<?> cursor) {
            BaseCursorCountVisitor visitor = new BaseCursorCountVisitor();
            cursor.accept(visitor);
            return visitor.keyValueCursorCount;
        }
    }

}

