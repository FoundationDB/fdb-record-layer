/*
 * FDBRecordStoreSparseJoinPerformanceTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestSparseJoinPerfProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Performance tests of a "sparse join", that is, a nested loop join where many entries returned by the
 * first half of the join require looking up another record in a second index which may or may not have
 * any entries.
 *
 * <p>
 * To run, do the following:
 * </p>
 *
 * <ol>
 *     <li>Run the {@link #repopulateForTest()} test to insert records.</li>
 *     <li>The other tests can then be run to investigate certain scenarios, querying from that dataset.</li>
 * </ol>
 *
 * <p>
 * Certain testing parameters, like the number of {@code OuterRecord}s, can be adjusted, though the dataset
 * will need to be repopulated in order to pick up those changes.
 * </p>
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Performance)
public class FDBRecordStoreSparseJoinPerformanceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreSparseJoinPerformanceTest.class);

    private static final Object[] PATH = { "record-test", "performance", "join"};

    private static final String OUTER_RECORD = TestSparseJoinPerfProto.OuterRecord.getDescriptor().getName();
    private static final String INNER_RECORD = TestSparseJoinPerfProto.InnerRecord.getDescriptor().getName();

    private static final String TEXT_INDEX_NAME = "textIndex";
    private static final Object TEXT_INDEX_SUBSPACE_KEY = 1L;
    private static final String GROUP_OUTER_VALUE_INDEX = "groupOuterValueIndex";
    private static final Object GROUP_OUTER_VALUE_SUBSPACE_KEY = 2L;

    private static final String TEXT_FIELD = "text";
    private static final String GROUP_FIELD = "group";
    private static final String VAL_FIELD = "val";
    private static final String OUTER_ID_FIELD = "outer_id";
    private static final String REC_NO_FIELD = "rec_no";

    private static final String TEXT_PARAM = "text_param";
    private static final String GROUP_PARAM = "group_param";
    private static final String OUTER_PARAM = "outer_param";

    private static final PipelineOperation JOIN = new PipelineOperation("NESTED_LOOP_JOIN");

    private static final List<String> TEXTS = List.of("a", "b", "c", "d", "e", "f", "g");
    private static final int OUTER_RECORD_COUNT = 100_000;
    private static final int GROUP_COUNT = 50;

    private FDBDatabase database;

    private static RecordMetaData createMetaData() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestSparseJoinPerfProto.getDescriptor());

        Index textIndex = new Index(TEXT_INDEX_NAME, Key.Expressions.field(TEXT_FIELD));
        textIndex.setSubspaceKey(TEXT_INDEX_SUBSPACE_KEY);
        builder.addIndex(OUTER_RECORD, textIndex);

        Index groupOuterValIndex = new Index(GROUP_OUTER_VALUE_INDEX, Key.Expressions.concatenateFields(GROUP_FIELD, OUTER_ID_FIELD, VAL_FIELD));
        groupOuterValIndex.setSubspaceKey(GROUP_OUTER_VALUE_SUBSPACE_KEY);
        builder.addIndex(INNER_RECORD, groupOuterValIndex);

        return builder.build();
    }

    private static RecordQuery outerQuery() {
        // Roughly:
        //   SELECT rec_no FROM OuterRecord WHERE text = $text_param
        return RecordQuery.newBuilder()
                .setRecordType(OUTER_RECORD)
                .setFilter(Query.field(TEXT_FIELD).equalsParameter(TEXT_PARAM))
                .setRequiredResults(List.of(Key.Expressions.field(REC_NO_FIELD)))
                .build();
    }

    private static RecordQuery innerQuery() {
        // Roughly:
        //   SELECT val FROM InnerRecord WHERE group = $group_param AND outer_id = $outer_param
        return RecordQuery.newBuilder()
                .setRecordType(INNER_RECORD)
                .setFilter(Query.and(Query.field(GROUP_FIELD).equalsParameter(GROUP_PARAM), Query.field(OUTER_ID_FIELD).equalsParameter(OUTER_PARAM)))
                .setRequiredResults(List.of(Key.Expressions.field(VAL_FIELD)))
                .build();
    }

    private RecordCursor<Integer> executeJoin(FDBRecordStore store, int group, String text, @Nullable byte[] continuation, ExecuteProperties executeProperties) {
        // Combine the outer and inner queries to produce roughly this join:
        //   SELECT InnerRecord.val FROM OuterRecord JOIN InnerRecord
        //   WHERE OuterRecord.text = $text AND InnerRecord.group = $group AND InnerRecord.outer_id = OuterRecord.rec_no

        final RecordQuery outer = outerQuery();
        final RecordQueryPlan outerPlan = store.planQuery(outer);
        assertThat(outerPlan, coveringIndexScan(indexScan(allOf(indexName(TEXT_INDEX_NAME), bounds(hasTupleString("[EQUALS $" + TEXT_PARAM + "]"))))));
        final RecordQuery inner = innerQuery();
        final RecordQueryPlan innerPlan = store.planQuery(inner);
        assertThat(innerPlan, coveringIndexScan(indexScan(allOf(indexName(GROUP_OUTER_VALUE_INDEX), bounds(hasTupleString("[EQUALS $" + GROUP_PARAM + ", EQUALS $" + OUTER_PARAM + "]"))))));

        final EvaluationContext evalContext = EvaluationContext.newBuilder()
                .setBinding(GROUP_PARAM, group)
                .setBinding(TEXT_PARAM, text)
                .build(TypeRepository.EMPTY_SCHEMA);

        Descriptors.FieldDescriptor recNoDescriptor = TestSparseJoinPerfProto.OuterRecord.getDescriptor().findFieldByName(REC_NO_FIELD);
        Descriptors.FieldDescriptor valDescriptor = TestSparseJoinPerfProto.InnerRecord.getDescriptor().findFieldByName(VAL_FIELD);
        return RecordCursor.flatMapPipelined(
                outerContinuation -> store.executeQuery(outerPlan, outerContinuation, evalContext, executeProperties.clearSkipAndLimit()),
                (outerRecord, innerContinuation) -> {
                    Message outerMessage = outerRecord.getMessage();
                    final EvaluationContext innerEvalContext = evalContext.withBinding(OUTER_PARAM, outerMessage.getField(recNoDescriptor));
                    return store.executeQuery(innerPlan, innerContinuation, innerEvalContext, executeProperties.clearSkipAndLimit())
                            .map(innerRecord -> (Integer) innerRecord.getMessage().getField(valDescriptor));
                },
                outerRecord -> outerRecord.getQueriedRecord().getPrimaryKey().pack(),
                continuation,
                store.getPipelineSize(JOIN)
        ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    private List<Integer> executeMultiTransactionJoin(FDBRecordContextConfig contextConfig, int group, String text) {
        long startNanos = System.nanoTime();
        List<Integer> values = new ArrayList<>();
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;

        do {
            final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(100)
                    .setScannedBytesLimit(50_000)
                    .setScannedRecordsLimit(1000)
                    .setTimeLimit(3_000L)
                    .build();
            try (FDBRecordContext context = database.openContext(contextConfig)) {
                FDBRecordStore store = openStore(context);
                try (RecordCursor<Integer> cursor = executeJoin(store, group, text, continuation.toBytes(), executeProperties)) {
                    RecordCursorResult<Integer> result;
                    while ((result = cursor.getNext()).hasNext()) {
                        values.add(result.get());
                    }
                    continuation = result.getContinuation();
                }
            }

        } while (!continuation.isEnd());
        long endNanos = System.nanoTime();

        if (LOGGER.isInfoEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("multi-transaction join executed",
                    "group", group,
                    "text", text,
                    "result_count", values.size(),
                    "execute_micros", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
            FDBStoreTimer timer = contextConfig.getTimer();
            if (timer != null) {
                msg.addKeysAndValues(timer.getKeysAndValues());
            }
            LOGGER.info(msg.toString());
        }

        return values;
    }

    private static void addStats(KeyValueLogMessage message, List<Long> data, String prefix) {
        data.sort(Comparator.naturalOrder());
        message.addKeyAndValue(prefix + "_min", data.get(0));
        message.addKeyAndValue(prefix + "_max", data.get(data.size() - 1));
        message.addKeyAndValue(prefix + "_median", data.get(data.size() / 2));
        message.addKeyAndValue(prefix + "_avg", data.stream().mapToLong(Long::longValue).sum() / data.size());
    }

    private void profileQuery(int group, String text) {
        // Run some initial tests without profiling to warm up the JVM
        for (int i = 0; i < 50; i++) {
            executeMultiTransactionJoin(FDBRecordContextConfig.newBuilder().setTimer(new FDBStoreTimer()).build(), group, text);
        }

        List<Long> durations = new ArrayList<>();
        int resultCount = 0;
        long readsCount = 0;
        for (int i = 0; i < 50; i++) {
            long startNanos = System.nanoTime();
            final FDBStoreTimer timer = new FDBStoreTimer();
            resultCount = executeMultiTransactionJoin(FDBRecordContextConfig.newBuilder().setTimer(timer).build(), group, text).size();
            readsCount = timer.getCount(FDBStoreTimer.Counts.READS);
            long endNanos = System.nanoTime();
            durations.add(TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
        }

        if (LOGGER.isInfoEnabled()) {
            KeyValueLogMessage logMessage = KeyValueLogMessage.build("join query profiled")
                    .addKeyAndValue("group", group)
                    .addKeyAndValue("text", text)
                    .addKeyAndValue("result_count", resultCount)
                    .addKeyAndValue("reads_count", readsCount);

            addStats(logMessage, durations, "latency_micros");

            LOGGER.info(logMessage.toString());
        }
    }

    private FDBRecordStore openStore(FDBRecordContext context) {
        return FDBRecordStore.newBuilder()
                .setContext(context)
                .setMetaDataProvider(createMetaData())
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH))
                .createOrOpen();
    }

    private void populate(Random r) {
        long startNanos = System.nanoTime();
        Map<Integer, Integer> countsByGroup = new TreeMap<>();

        FDBRecordContext context = database.openContext();
        try {
            FDBRecordStore store = openStore(context);

            for (int i = 0; i < OUTER_RECORD_COUNT; i++) {
                TestSparseJoinPerfProto.OuterRecord outerRecord = TestSparseJoinPerfProto.OuterRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setText(TEXTS.get(i % TEXTS.size()))
                        .build();
                store.saveRecord(outerRecord);

                // Create an inner record pointing to OuterRecord, choosing a group with a normal distribution
                // centered around the middle group so that there are 3 z-values around the center
                int group = (int)(r.nextGaussian() * (GROUP_COUNT / 6) + (GROUP_COUNT / 2));
                group = Math.min(group, GROUP_COUNT - 1);
                group = Math.max(group, 0);
                TestSparseJoinPerfProto.InnerRecord innerRecord = TestSparseJoinPerfProto.InnerRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setGroup(group)
                        .setOuterId(outerRecord.getRecNo())
                        .setVal(i)
                        .build();
                store.saveRecord(innerRecord);
                countsByGroup.compute(group, (ignore, current) -> current == null ? 1 : current + 1);

                if (context.ensureActive().getApproximateSize().join() > 100_000) {
                    context.commit();
                    context.close();

                    context = database.openContext();
                    store = openStore(context);
                }
            }

            context.commit();

        } finally {
            context.close();

            long endNanos = System.nanoTime();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.of("data populated for sparse join test",
                        "outer_record_count", OUTER_RECORD_COUNT,
                        "inner_records_by_group", countsByGroup,
                        "populate_time_micros", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos)));
            }
        }
    }

    private void repopulate() {
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path = TestKeySpace.getKeyspacePath(PATH);
            path.deleteAllData(context);
            context.commit();
        }

        Random r = new Random();
        populate(r);
    }

    @BeforeEach
    void setUp() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    @Test
    void repopulateForTest() {
        repopulate();
    }

    @Test
    void emptyJoin() {
        try (FDBRecordContext context = database.openContext()) {
            FDBRecordStore store = openStore(context);
            RecordCursor<Integer> results = executeJoin(store, 42, "foo", null, ExecuteProperties.SERIAL_EXECUTE);
            assertEquals(0, results.getCount().join());
            context.commit();
        }
    }

    @Test
    void middleGroupJoin() {
        profileQuery(GROUP_COUNT / 2, "d");
    }

    @Test
    void zeroGroupJoin() {
        profileQuery(0, "d");
    }
}
