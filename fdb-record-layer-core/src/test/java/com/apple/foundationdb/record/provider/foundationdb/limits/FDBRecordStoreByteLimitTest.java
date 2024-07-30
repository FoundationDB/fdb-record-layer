/*
 * FDBRecordStoreByteLimitTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanLimitReachedException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.provider.common.text.PrefixTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the byte scan limit on query execution.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class FDBRecordStoreByteLimitTest extends FDBRecordStoreLimitTestBase {
    private static final StoreTimerByteCounter byteCounter = new StoreTimerByteCounter();

    private List<Long> getScannedByteCountsByRecord(@Nonnull FDBRecordContext context, @Nonnull RecordQueryPlan plan) {
        final List<Long> byteCountsByRecord = new ArrayList<>();
        byte[] continuation = null;
        context.getTimer().reset();
        do {
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, ExecuteProperties.SERIAL_EXECUTE)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                if (result.hasNext()) {
                    byteCountsByRecord.add(byteCounter.getBytesScanned(context));
                    context.getTimer().reset();
                }
                continuation = result.getContinuation().toBytes();
            }
        } while (continuation != null);

        return byteCountsByRecord;
    }

    static Stream<Arguments> plans() {
        return plans(false);
    }

    @ParameterizedTest(name = "testPlans() [{index}] {0} {1}")
    @MethodSource("plans")
    void testPlansWithPreciseExecution(String description, boolean notUsed, RecordQueryPlan plan) {
        final List<Long> byteCountsByRecord;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStoreWithSingletonPipeline(context);
            byteCountsByRecord = getScannedByteCountsByRecord(context, plan);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStoreWithSingletonPipeline(context);
            assertPlanLimitsWithCorrectExecution(byteCountsByRecord, context, plan);
        }
    }

    /**
     * Make detailed assertions about the number of bytes scanned when the byte scan limit is very close to the number
     * of records scanned between records.
     *
     * This helper method attempts to verify that the number of bytes being scanned during query execution is not too
     * much higher than the limit specified by {@link ExecuteProperties.Builder#setScannedBytesLimit(long)}. To do this,
     * it's given a list of the number of bytes scanned between individual records produced by the given plan. Then,
     * it sets the byte limit to that number of bytes, possibly increased or decreased by 1.
     *
     * Suppose that A, B, and C are successive records produced by the plan's cursor. Consider the execution of that
     * cursor in the absence of any limits. Let the number of bytes scanned between A and B be {@code m} and the number
     * of bytes scanned between B and C be {@code n}. This test relies on the following claims holding:
     * a) {@code m} and {@code n} are constant with respect to the asynchronous execution order of the underlying cursors.
     * b) If the byte scan limit is less than or equal to {@code m}, then at most {@code 2 * m} bytes are scanned between
     *   A and B, even if we resume the cursor by continuation. The same holds for {@code n}, B and C.
     * c) If the byte scan limit is {@code m + 1}, then at least {@code m} and at most {@code m + n} bytes are scanned.
     *
     * Note that claim (b) requires the factor of two because resuming certain cursors (such as a
     * {@link com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor}) from a continuation requires
     * rescanning records that have already been deserialized.
     *
     * These claims should hold for most query plans. Notably, even claim (a) does not hold for the
     * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan}.
     * @param byteCountsByRecord a list of the number of bytes scanned between successive records without any limits
     * @param context an open record store context
     * @param plan a query plan to execute
     */
    private void assertPlanLimitsWithCorrectExecution(@Nonnull List<Long> byteCountsByRecord,
                                                      @Nonnull FDBRecordContext context,
                                                      @Nonnull RecordQueryPlan plan) {
        byte[] continuation = null;
        int i = 0;
        context.getTimer().reset();
        while (i < byteCountsByRecord.size()) {
            final int currentIndex = i;
            // If the limit is slightly too low to scan the next record, we should scan it anyway and then stop.
            final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setScannedBytesLimit(byteCountsByRecord.get(currentIndex) - 1)
                    .build();

            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                final long bytesScanned = byteCounter.getBytesScanned(context);
                if (currentIndex == byteCountsByRecord.size() - 1) { // Final record
                    // If this is a final record, then there must be enough room to scan everything until the end,
                    // because of how the byteCountsByRecord are counted.
                    assertTrue(result.hasNext());
                }
                // If the execution order is a little bit different this time (especially for union/intersection
                // cursors) then we might not have a result when we expect to.
                if (result.hasNext()) {
                    i++;
                    context.getTimer().reset();
                } else {
                    // Check that we stopped because of the byte scan limit.
                    assertEquals(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED, result.getNoNextReason());
                }
                // Assertion of claim (b)
                assertThat(bytesScanned, lessThanOrEqualTo(2 * byteCountsByRecord.get(currentIndex)));
                continuation = result.getContinuation().toBytes();
            }
        }

        continuation = null;
        i = 0;
        while (i < byteCountsByRecord.size()) {
            final int currentIndex = i;
            // If the limit is exactly right we should scan the record and then stop.
            final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setScannedBytesLimit(byteCountsByRecord.get(currentIndex))
                    .build();
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                final long bytesScanned = byteCounter.getBytesScanned(context);
                // If the execution order is a little bit different this time (especially for union/intersection
                // cursors) then we might not have a result when we expect to.
                if (result.hasNext()) {
                    i++;
                    context.getTimer().reset();
                } else if (currentIndex < byteCountsByRecord.size() - 1) { // not the final record yet
                    // Check that we stopped because of the byte scan limit.
                    assertEquals(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED, result.getNoNextReason());
                }
                // Assertion of claim (b)
                assertThat(bytesScanned, lessThanOrEqualTo(2 * byteCountsByRecord.get(currentIndex)));
                continuation = result.getContinuation().toBytes();
            }
        }

        continuation = null;
        i = 0;
        while (i < byteCountsByRecord.size()) {
            // If the limit is slightly too high we should scan the record, proceed to the next one (if it exists), and then stop.
            final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setScannedBytesLimit(byteCountsByRecord.get(i) + 1)
                    .build();
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertTrue(result.hasNext());
                continuation = result.getContinuation().toBytes();
                result = cursor.getNext();

                long bytesScanned = byteCounter.getBytesScanned(context);
                // Assertion of claim (c)
                assertThat(bytesScanned, greaterThanOrEqualTo(byteCountsByRecord.get(i)));
                if (i < byteCountsByRecord.size() - 1) {
                    // Assertion of claim (c)
                    assertThat(bytesScanned, lessThanOrEqualTo(byteCountsByRecord.get(i) + byteCountsByRecord.get(i + 1)));
                }

                i++;
                if (result.hasNext()) {
                    result = cursor.getNext();
                    assertFalse(result.hasNext());
                }
                context.getTimer().reset();
            }
        }
    }

    @ParameterizedTest(name = "plansByContinuation() [{index}] {0}")
    @MethodSource("plans")
    void testPlansReturnSameRecordsRegardlessOfLimit(String description, boolean notUsed, RecordQueryPlan plan) throws Exception {
        final Function<FDBQueriedRecord<Message>, Long> getRecNo = r -> {
            TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
            record.mergeFrom(r.getRecord());
            return record.getRecNo();
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final List<Long> allAtOnce;
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                allAtOnce = cursor.map(getRecNo).asList().get();
            }

            for (long byteLimit = 0; byteLimit < 1000; byteLimit += 100) {
                final ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setScannedBytesLimit(byteLimit).build();
                final List<Long> byContinuation = new ArrayList<>(allAtOnce.size());
                byte[] continuation = null;
                do {
                    try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
                        RecordCursorResult<Long> result;
                        do {
                            result = cursor.onNext().get().map(getRecNo);
                            if (result.hasNext()) {
                                byContinuation.add(result.get());
                            }
                        } while (result.hasNext());
                        continuation = result.getContinuation().toBytes();
                    }
                } while (continuation != null);
                assertEquals(allAtOnce, byContinuation);
            }
        }
    }

    private void deleteSimpleRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.deleteAllRecords(); // Undo setupRecordStore().
            commit(context);
        }
    }

    @Test
    void testSplitContinuation() throws Exception {
        deleteSimpleRecords();

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
                    .setScannedBytesLimit(0)
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

    @BooleanSource
    @ParameterizedTest
    void testHitExhaustedDuringSplit(boolean reverse) throws Exception {
        deleteSimpleRecords();

        // Insert a single large record
        final FDBStoredRecord<Message> createdRecord = saveAndSplitSimpleRecord(1L,
                Strings.repeat("Z", SplitHelper.SPLIT_RECORD_SIZE + 10), 1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Create a limit that will be hit while reading the first record
            ScanProperties properties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setScannedBytesLimit(1)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build(), reverse);
            final RecordCursor<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null, properties);

            RecordCursorResult<FDBStoredRecord<Message>> result = messageCursor.getNext();
            assertTrue(result.hasNext());
            assertEquals(createdRecord, result.get());

            // Limit hit and also exhausted. Either one is a valid response, but the data should be internally
            // consistent (i.e., it should not return a non-end continuation if the source is exhausted)
            result = messageCursor.getNext();
            assertFalse(result.hasNext());
            if (result.getNoNextReason().isSourceExhausted()) {
                assertTrue(result.getContinuation().isEnd(), "second result should be at the end");
                assertNull(result.getContinuation().toBytes());
            } else {
                assertEquals(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED, result.getNoNextReason());
                assertFalse(result.getContinuation().isEnd());
                assertNotNull(result.getContinuation().toBytes());
            }
        }
    }

    private static final String SIMPLE_DOC = "SimpleDocument";
    private static final String COMPLEX_DOC = "ComplexDocument";
    private static final Index SIMPLE_TEXT_PREFIX = new Index("Simple$text_prefix", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "1"));
    private static final TransformedRecordSerializer<Message> COMPRESSING_SERIALIZER = TransformedRecordSerializer.newDefaultBuilder()
            .setCompressWhenSerializing(true)
            .build();

    private void openTextRecordStore(FDBRecordContext context) {
        openTextRecordStore(context, NO_HOOK);
    }

    private void openTextRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(COMPRESSING_SERIALIZER)
                .setPipelineSizer(pipelineOperation -> 1)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Test
    void simpleFullTextContainsQuery() {
        // Load a big (ish) data set
        final int recordCount = 100;
        final int batchSize = 10;

        for (int i = 0; i < recordCount; i += batchSize) {
            try (FDBRecordContext context = openContext()) {
                openTextRecordStore(context);
                for (int j = 0; j < batchSize; j++) {
                    TestRecordsTextProto.SimpleDocument document = TestRecordsTextProto.SimpleDocument.newBuilder()
                            .setDocId(i + j)
                            .setText((i + j) % 2 == 0 ? "some" : "text")
                            .build();
                    recordStore.saveRecord(document);
                }
                commit(context);
            }
        }

        setupPlanner(null);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(SIMPLE_DOC)
                .setFilter(Query.field("text").text().containsAll("some text"))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        final List<Long> byteCountsByRecord;
        try (FDBRecordContext context = openContext()) {
            openTextRecordStore(context);
            byteCountsByRecord = getScannedByteCountsByRecord(context, plan);
        }

        try (FDBRecordContext context = openContext()) {
            openTextRecordStore(context);
            assertPlanLimitsWithCorrectExecution(byteCountsByRecord, context, plan);
        }
    }

    static Stream<Arguments> complexTextQueries() {
        return Stream.of(
                Arguments.of(RecordQuery.newBuilder()
                        .setRecordType(SIMPLE_DOC)
                        .setFilter(Query.or(
                                Query.field("text").text().containsPrefix("ang"),
                                Query.field("text").text().containsPrefix("un")))
                        .setRemoveDuplicates(true)
                        .build(), 2),
                Arguments.of(RecordQuery.newBuilder()
                        .setRecordType(SIMPLE_DOC)
                        .setFilter(Query.or(
                                Query.field("text").text().containsPrefix("ang"),
                                Query.field("text").text().containsPrefix("un"),
                                Query.field("text").text().containsPrefix("tw"),
                                Query.field("text").text().containsPrefix("thie")))
                        .setRemoveDuplicates(true)
                        .build(), 4));
    }

    /**
     * Queries with an OR of {@link com.apple.foundationdb.record.query.expressions.Text#containsPrefix(String)}
     * predicates get planned as {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan}s,
     * which have unusual semantics where results are returned in an undefined order as soon as any child has one.
     * Therefore, the assertions made in {@link #assertPlanLimitsWithCorrectExecution(List, FDBRecordContext, RecordQueryPlan)}
     * are far too strong for plans like this. Instead, we make very weak assertions that the byte scan limit does
     * <em>something</em>.
     */
    @ParameterizedTest
    @MethodSource("complexTextQueries")
    void queryWithWideOrOfFullTextPrefixPredicates(@Nonnull RecordQuery query, int numPredicates) throws Exception {
        deleteSimpleRecords();
        final List<String> textSamples = ImmutableList.of(
                TextSamples.ANGSTROM,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.FRENCH,
                TextSamples.KOREAN
        );

        RecordMetaDataHook indexHook = metaDataBuilder ->
                    metaDataBuilder.addIndex(metaDataBuilder.getRecordType(SIMPLE_DOC), SIMPLE_TEXT_PREFIX);
        try (FDBRecordContext context = openContext()) {
            openTextRecordStore(context, indexHook);
            for (int i = 0; i < textSamples.size(); i++) {
                recordStore.saveRecord(TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setDocId(i)
                        .setGroup(i % 2)
                        .setText(textSamples.get(i))
                        .build());
            }
            commit(context);
        }

        setupPlanner(null);
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, descendant(unorderedUnion(Collections.nCopies(numPredicates, any(RecordQueryPlan.class)))));

        long totalBytes;
        Set<Long> noLimitRecordIds = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openTextRecordStore(context, indexHook);
            context.getTimer().reset();
            RecordCursor<FDBQueriedRecord<Message>> cursor =
                    recordStore.executeQuery(query, null, ExecuteProperties.SERIAL_EXECUTE);
            RecordCursorResult<FDBQueriedRecord<Message>> result;
            do {
                result = cursor.onNext().get();
                if (result.hasNext()) {
                    TestRecordsTextProto.SimpleDocument.Builder record = TestRecordsTextProto.SimpleDocument.newBuilder();
                    record.mergeFrom(result.get().getRecord());
                    noLimitRecordIds.add(record.getDocId());
                }
            } while (result.hasNext());
            totalBytes = byteCounter.getBytesScanned(context);
        }

        Set<Long> limitRecordIds = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openTextRecordStore(context);


            ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                    .setScannedBytesLimit(0);
            byte[] continuation = null;
            do {
                context.getTimer().reset();
                RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(query, continuation, executeProperties.build());
                RecordCursorResult<FDBQueriedRecord<Message>> result;
                do {
                    result = cursor.onNext().get();
                    if (result.hasNext()) {
                        TestRecordsTextProto.SimpleDocument.Builder record = TestRecordsTextProto.SimpleDocument.newBuilder();
                        record.mergeFrom(result.get().getRecord());
                        limitRecordIds.add(record.getDocId());
                    }
                } while (result.hasNext());
                assertThat(byteCounter.getBytesScanned(context), lessThan(totalBytes));
                continuation = result.getContinuation().toBytes();
                if (continuation != null) {
                    assertEquals(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED, result.getNoNextReason());
                }
            } while (continuation != null);
            assertEquals(noLimitRecordIds, limitRecordIds);
        }
    }

    @ParameterizedTest(name = "testWithFailOnByteScanLimitReached [{index}] {0} {1}")
    @MethodSource("plans")
    void testWithFailOnByteScanLimitReached(String description, boolean notUsed, @Nonnull RecordQueryPlan plan) throws Exception {
        for (long byteLimit = 0; byteLimit < 1000; byteLimit += 100) {
            try (FDBRecordContext context = openContext()) {
                ExecuteProperties properties = ExecuteProperties.newBuilder()
                        .setScannedBytesLimit(byteLimit)
                        .setFailOnScanLimitReached(true)
                        .build();
                openSimpleRecordStore(context);
                RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, properties);
                assertThrowsWithWrapper(ScanLimitReachedException.class, () -> cursor.asList().join());
            }
        }
    }

    private <T extends Throwable> void assertThrowsWithWrapper(@Nonnull Class<T> expectedType, @Nonnull Executable executable) {
        Throwable actualException = assertThrows(Throwable.class, executable);
        while (actualException instanceof CompletionException) {
            actualException = actualException.getCause();
        }
        assertThat(actualException, instanceOf(expectedType));
    }
}
