/*
 * FDBRecordStoreScanLimitTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.limits;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanLimitReachedException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.logging.TestLogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.record.provider.foundationdb.cursors.ProbableIntersectionCursor;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithNoChildren;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for scan limits in {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FDBRecordStoreScanLimitTest extends FDBRecordStoreLimitTestBase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreScanLimitTest.class);

    @BeforeAll
    public void init() {
        clearAndInitialize();
    }

    @BeforeEach
    private void setupRecordStore() throws Exception {
        setupSimpleRecordStore();
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

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                         recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
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

    private void assertNumberOfRecordsScanned(int expected, Function<byte[], RecordCursor<FDBQueriedRecord<Message>>> cursorFunction, boolean failOnLimitReached, String message) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            if (context.getTimer() != null) {
                context.getTimer().reset();
            }

            try (RecordCursor<FDBQueriedRecord<Message>> cursor = cursorFunction.apply(null)) {
                boolean caughtScanLimitReached = false;
                RecordCursorResult<FDBQueriedRecord<Message>> result = null;
                try {
                    do {
                        result = cursor.getNext();
                    } while (result.hasNext());
                } catch (RecordCoreException ex) {
                    if (failOnLimitReached && ex.getCause() instanceof ScanLimitReachedException) {
                        caughtScanLimitReached = true;
                    } else {
                        throw ex;
                    }
                }
                if (failOnLimitReached && !caughtScanLimitReached) {
                    assertNotEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, result.getNoNextReason());
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

    private void assertNumberOfRecordsScanned(int expected, RecordQueryPlan plan, ExecuteProperties executeProperties, String message) throws Exception {
        assertNumberOfRecordsScanned(expected, continuation -> recordStore.executeQuery(plan, null, executeProperties), executeProperties.isFailOnScanLimitReached(), message);
    }

    private int getMaximumToScan(QueryPlan<?> plan) throws Exception {
        if (plan instanceof RecordQueryPlanWithNoChildren) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                RecordQueryPlanWithNoChildren planWithNoChildren = (RecordQueryPlanWithNoChildren) plan;
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                             recordStore.executeQuery(planWithNoChildren, null, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
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
        for (QueryPlan<?> child : plan.getQueryPlanChildren()) {
            maximumToScan += getMaximumToScan(child);
        }
        return maximumToScan;
    }

    public Stream<Arguments> plansWithoutFail() {
        return plans(false);
    }

    public Stream<Arguments> plansWithFails() throws Exception {
        return Stream.of(Boolean.FALSE, Boolean.TRUE).flatMap(fail -> Stream.concat(plans(fail), unorderedPlans(fail)));
    }

    @ParameterizedTest(name = "testPlans() [{index}] {0} {1}")
    @MethodSource("plansWithFails")
    public void testPlans(String description, boolean fail, RecordQueryPlan plan) throws Exception {
        // include a scanLimit of 0, in which case all progress happens via the first "free" key-value scan.
        LOGGER.info(KeyValueLogMessage.of("running plan to check scan limit failures",
                        LogMessageKeys.DESCRIPTION, description,
                        LogMessageKeys.PLAN, plan,
                        TestLogMessageKeys.FAIL, fail));
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
                        RecordCursorResult<FDBQueriedRecord<Message>> result;
                        while ((result = cursor.getNext()).hasNext()) {
                            byContinuation.add(getRecNo.apply(result.get()));
                        }
                        continuation = result.getContinuation().toBytes();
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

    @ParameterizedTest(name = "unorderedIntersectionWithScanLimit [fail = {0}]")
    @BooleanSource
    public void unorderedIntersectionWithScanLimit(boolean fail) throws Exception {
        // TODO: When there is an UnorderedIntersectionPlan (or whatever) add that to the unordered plans stream
        RecordQueryPlanner planner = new RecordQueryPlanner(simpleMetaData(NO_HOOK), new RecordStoreState(null, null));
        RecordQueryPlan leftPlan = planner.plan(RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").startsWith("ev"))
                .build()
        );
        RecordQueryPlan rightPlan = planner.plan(RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").lessThanOrEquals(1))
                .build()
        );
        int maximumToScan;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            maximumToScan = recordStore.executeQuery(leftPlan).getCount().get() + recordStore.executeQuery(rightPlan).getCount().get();
        }
        for (int limit = 0; limit < 3 * maximumToScan; limit = 2 * limit + 1) {
            final int finalLimit = limit;
            Function<byte[], RecordCursor<FDBQueriedRecord<Message>>> cursorFunction = (continuation) -> {
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                        .setScannedRecordsLimit(finalLimit)
                        .setFailOnScanLimitReached(fail)
                        .build();
                return ProbableIntersectionCursor.create(
                        record -> record.getPrimaryKey().getItems(),
                        Arrays.asList(
                                leftContinuation -> leftPlan.execute(recordStore, EvaluationContext.EMPTY, leftContinuation, executeProperties),
                                rightContinuation -> rightPlan.execute(recordStore, EvaluationContext.EMPTY, rightContinuation, executeProperties)
                        ),
                        continuation,
                        recordStore.getTimer());
            };
            assertNumberOfRecordsScanned(limit, cursorFunction, fail, "should" + (limit >= maximumToScan ? "not " : "") + " be limited by record scan limit");
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
            RecordCursorIterator<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null, props.get()).asIterator();
            while (messageCursor.hasNext()) {
                scannedRecords.add(messageCursor.next());
                messageCursor = recordStore.scanRecords(messageCursor.getContinuation(), props.get()).asIterator();
            }
            commit(context);
        }
        assertEquals(createdRecords, scannedRecords);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 5, 10, 20}) // for this test, the scan limit must divide 100
    public void testExecuteStateReset(int scanLimit) throws Exception {
        final IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        final RecordQueryPlan plan = new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed",
                fullValueScan, false);
        ExecuteProperties properties = ExecuteProperties.newBuilder().setScannedRecordsLimit(scanLimit).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            byte[] continuation = null;
            do {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, properties).asIterator()) {
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

    @ParameterizedTest
    @BooleanSource
    public void testWithVersionsAndTimeLimit(boolean splitLongRecords) {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.setSplitLongRecords(splitLongRecords);
            metaDataBuilder.setStoreRecordVersions(true);
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            for (int i = 0; i < 100; i++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValue2(i % 5)
                        .setNumValue3Indexed(i % 3)
                        .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                        .build());
            }
            commit(context);
        }
        for (int i = 0; i < 100; i++) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, hook);
                assertScansUntilTimeLimit(recordStore, false);
                assertScansUntilTimeLimit(recordStore, true);
            }
        }
    }

    private static void assertScansUntilTimeLimit(@Nonnull FDBRecordStore recordStore,
                                                  boolean reverse) {
        try (RecordCursor<FDBStoredRecord<Message>> cursor = veryShortTimeLimitedScan(recordStore, reverse)) {
            RecordCursorResult<FDBStoredRecord<Message>> result;
            Tuple lastPrimaryKey = null;
            do {
                result = cursor.getNext();
                if (result.hasNext()) {
                    FDBStoredRecord<Message> storedRecord = result.get();
                    assertNotNull(storedRecord);
                    assertTrue(storedRecord.hasVersion());
                    assertNotNull(storedRecord.getVersion());
                    Tuple primaryKey = storedRecord.getPrimaryKey();
                    if (lastPrimaryKey != null) {
                        if (reverse) {
                            assertThat(primaryKey, lessThan(lastPrimaryKey));
                        } else {
                            assertThat(primaryKey, greaterThan(lastPrimaryKey));
                        }
                    }
                    lastPrimaryKey = primaryKey;
                }
            } while (result.hasNext());

            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, result.getNoNextReason());
        }
    }

    private static RecordCursor<FDBStoredRecord<Message>> veryShortTimeLimitedScan(@Nonnull FDBRecordStore recordStore, boolean reverse) {
        final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setTimeLimit(1L)
                .build();
        return recordStore.scanRecords(TupleRange.ALL, null, new ScanProperties(executeProperties, reverse));
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

