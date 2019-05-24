/*
 * OnlineIndexerTest.java
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.logging.TestLogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link OnlineIndexer}.
 */
@Tag(Tags.RequiresFDB)
public class OnlineIndexerTest extends FDBTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerTest.class);

    private RecordMetaData metaData;
    private RecordQueryPlanner planner;
    private FDBRecordStore recordStore;
    private FDBDatabase fdb;
    private Subspace subspace;

    private static long oldMaxDelayMillis;
    private static long oldInitialDelayMillis;
    private static int oldMaxAttempts;

    @BeforeAll
    public static void setUpForClass() {
        oldInitialDelayMillis = FDBDatabaseFactory.instance().getInitialDelayMillis();
        FDBDatabaseFactory.instance().setInitialDelayMillis(2L);
        oldMaxDelayMillis = FDBDatabaseFactory.instance().getMaxDelayMillis();
        FDBDatabaseFactory.instance().setMaxDelayMillis(4L);
        oldMaxAttempts = FDBDatabaseFactory.instance().getMaxAttempts();
        FDBDatabaseFactory.instance().setMaxAttempts(100);
    }

    @AfterAll
    public static void tearDownForClass() {
        FDBDatabaseFactory.instance().setMaxDelayMillis(oldMaxDelayMillis);
        FDBDatabaseFactory.instance().setInitialDelayMillis(oldInitialDelayMillis);
        FDBDatabaseFactory.instance().setMaxAttempts(oldMaxAttempts);
    }

    @BeforeEach
    public void setUp() {
        if (fdb == null) {
            fdb = FDBDatabaseFactory.instance().getDatabase();
            fdb.setAsyncToSyncTimeout(5, TimeUnit.MINUTES);
        }
        if (subspace == null) {
            subspace = DirectoryLayer.getDefault().createOrOpen(fdb.database(), Arrays.asList("record-test", "unit", "oib")).join();
        }
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
    }

    private void clearIndexData(@Nonnull Index index) {
        fdb.database().run(tr -> {
            tr.clear(Range.startsWith(recordStore.indexSubspace(index).pack()));
            tr.clear(recordStore.indexSecondarySubspace(index).range());
            tr.clear(recordStore.indexRangeSubspace(index).range());
            return null;
        });
    }

    private void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor, @Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(descriptor);
        hook.apply(metaDataBuilder);
        metaData = metaDataBuilder.getRecordMetaData();
    }

    private void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor) {
        openMetaData(descriptor, (metaDataBuilder) -> {
        });
    }

    private void openSimpleMetaData() {
        openMetaData(TestRecords1Proto.getDescriptor());
    }

    private void openSimpleMetaData(RecordMetaDataHook hook) {
        openMetaData(TestRecords1Proto.getDescriptor(), hook);
    }

    private FDBRecordContext openContext(boolean checked) {
        FDBRecordContext context = fdb.openContext();
        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData).setContext(context).setSubspace(subspace);
        if (checked) {
            recordStore = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
        } else {
            recordStore = builder.uncheckedOpen();
        }
        metaData = recordStore.getRecordMetaData();
        planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState(), recordStore.getTimer());
        return context;
    }

    private FDBRecordContext openContext() {
        return openContext(true);
    }

    private <T> void executeQuery(@Nonnull RecordQuery query, @Nonnull String planString, @Nonnull List<T> expected, @Nonnull Function<FDBQueriedRecord<Message>,T> projection) {
        RecordQueryPlan plan = planner.plan(query);
        assertEquals(planString, plan.toString());
        List<T> retrieved = recordStore.executeQuery(plan).map(projection).asList().join();
        assertEquals(expected, retrieved);
    }

    private void executeQuery(@Nonnull RecordQuery query, @Nonnull String planString, @Nonnull List<Message> expected) {
        executeQuery(query, planString, expected, FDBQueriedRecord::getRecord);
    }

    private <K,V extends Message> Map<K,List<Message>> group(@Nonnull List<V> values, @Nonnull Function<V,K> keyFunction) {
        Map<K,List<Message>> map = new HashMap<>();
        for (V value : values) {
            K key = keyFunction.apply(value);
            if (map.containsKey(key)) {
                map.get(key).add(value);
            } else {
                List<Message> toAdd = new ArrayList<>();
                toAdd.add(value);
                map.put(key, toAdd);
            }
        }
        return map;
    }

    @Nonnull
    private List<TestRecords1Proto.MySimpleRecord> updated(@Nonnull List<TestRecords1Proto.MySimpleRecord> origRecords, @Nonnull List<TestRecords1Proto.MySimpleRecord> addedRecords) {
        Map<Long,TestRecords1Proto.MySimpleRecord> lastRecordWithKey = new HashMap<>();
        for (TestRecords1Proto.MySimpleRecord record : origRecords) {
            if (record.hasRecNo()) {
                lastRecordWithKey.put(record.getRecNo(), record);
            } else {
                lastRecordWithKey.put(null, record);
            }
        }
        for (TestRecords1Proto.MySimpleRecord record : addedRecords) {
            if (record.hasRecNo()) {
                lastRecordWithKey.put(record.getRecNo(), record);
            } else {
                lastRecordWithKey.put(null, record);
            }
        }
        List<TestRecords1Proto.MySimpleRecord> updatedRecords = new ArrayList<>(lastRecordWithKey.size());
        for (TestRecords1Proto.MySimpleRecord record : lastRecordWithKey.values()) {
            updatedRecords.add(record);
        }
        updatedRecords.sort(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo));
        return updatedRecords;
    }

    private void singleRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                               int agents, boolean overlap, boolean splitLongRecords,
                               @Nonnull Index index, @Nonnull Runnable beforeBuild, @Nonnull Runnable afterBuild, @Nonnull Runnable afterReadable) {
        LOGGER.info(KeyValueLogMessage.of("beginning rebuild test",
                        TestLogMessageKeys.RECORDS, records.size(),
                        LogMessageKeys.RECORDS_WHILE_BUILDING, recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size(),
                        TestLogMessageKeys.AGENTS, agents,
                        TestLogMessageKeys.OVERLAP, overlap,
                        TestLogMessageKeys.SPLIT_LONG_RECORDS, splitLongRecords,
                        TestLogMessageKeys.INDEX, index)
        );

        final RecordMetaDataHook onlySplitHook = metaDataBuilder -> {
            if (splitLongRecords) {
                metaDataBuilder.setSplitLongRecords(true);
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            }
        };
        final RecordMetaDataHook hook = metaDataBuilder -> {
            onlySplitHook.apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };

        LOGGER.info(KeyValueLogMessage.of("inserting elements prior to test",
                        TestLogMessageKeys.RECORDS, records.size()));

        openSimpleMetaData(onlySplitHook);
        try (FDBRecordContext context = openContext()) {
            for (TestRecords1Proto.MySimpleRecord record : records) {
                // Check presence first to avoid overwriting version information of previously added records.
                Tuple primaryKey = Tuple.from(record.getRecNo());
                if (recordStore.loadRecord(primaryKey) == null) {
                    recordStore.saveRecord(record);
                }
            }
            context.commit();
        }

        LOGGER.info(KeyValueLogMessage.of("running before build for test"));
        beforeBuild.run();

        LOGGER.info(KeyValueLogMessage.of("adding index and marking write-only", TestLogMessageKeys.INDEX, index));
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        LOGGER.info(KeyValueLogMessage.of("creating online index builder",
                        TestLogMessageKeys.INDEX, index,
                        TestLogMessageKeys.RECORD_TYPES, metaData.recordTypesForIndex(index),
                        LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()),
                        LogMessageKeys.LIMIT, 20,
                        TestLogMessageKeys.RECORDS_PER_SECOND, OnlineIndexer.DEFAULT_RECORDS_PER_SECOND * 100));
        final OnlineIndexer.Builder builder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(20).setMaxRetries(Integer.MAX_VALUE).setRecordsPerSecond(OnlineIndexer.DEFAULT_RECORDS_PER_SECOND * 100);
        if (ThreadLocalRandom.current().nextBoolean()) {
            // randomly enable the progress logging to ensure that it doesn't throw exceptions,
            // or otherwise disrupt the build.
            LOGGER.info("Setting progress log interval");
            builder.setProgressLogIntervalMillis(0);
        }

        try (OnlineIndexer indexBuilder = builder.build()) {
            CompletableFuture<Void> buildFuture;
            LOGGER.info(KeyValueLogMessage.of("building index",
                            TestLogMessageKeys.INDEX, index,
                            TestLogMessageKeys.INDEX, agents,
                            LogMessageKeys.RECORDS_WHILE_BUILDING, recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size(),
                            TestLogMessageKeys.OVERLAP, overlap));
            if (agents == 1) {
                buildFuture = indexBuilder.buildIndexAsync(false);
            } else {
                if (overlap) {
                    CompletableFuture<?>[] futures = new CompletableFuture<?>[agents];
                    for (int i = 0; i < agents; i++) {
                        futures[i] = indexBuilder.buildIndexAsync(false);
                    }
                    buildFuture = CompletableFuture.allOf(futures);
                } else {
                    buildFuture = indexBuilder.buildEndpoints().thenCompose(tupleRange -> {
                        if (tupleRange != null) {
                            long start = tupleRange.getLow().getLong(0);
                            long end = tupleRange.getHigh().getLong(0);

                            CompletableFuture<?>[] futures = new CompletableFuture<?>[agents];
                            for (int i = 0; i < agents; i++) {
                                long itrStart = start + (end - start) / agents * i;
                                long itrEnd = (i == agents - 1) ? end : start + (end - start) / agents * (i + 1);
                                LOGGER.info(KeyValueLogMessage.of("building range",
                                                TestLogMessageKeys.INDEX, index,
                                                TestLogMessageKeys.AGENT, i,
                                                TestLogMessageKeys.BEGIN, itrStart,
                                                TestLogMessageKeys.END, itrEnd));
                                futures[i] = indexBuilder.buildRange(
                                        Key.Evaluated.scalar(itrStart),
                                        Key.Evaluated.scalar(itrEnd));
                            }
                            return CompletableFuture.allOf(futures);
                        } else {
                            return AsyncUtil.DONE;
                        }
                    });
                }
            }

            if (recordsWhileBuilding != null && recordsWhileBuilding.size() > 0) {
                int i = 0;
                while (i < recordsWhileBuilding.size()) {
                    List<TestRecords1Proto.MySimpleRecord> thisBatch = recordsWhileBuilding.subList(i, Math.min(i + 30, recordsWhileBuilding.size()));
                    fdb.run(context -> {
                        FDBRecordStore store = recordStore.asBuilder().setContext(context).build();
                        thisBatch.forEach(store::saveRecord);
                        return null;
                    });
                    i += 30;
                }
            }

            buildFuture.join();

            // if a record is added to a range that has already been built, it will not be counted, otherwise,
            // it will.
            long additionalScans = 0;
            if (recordsWhileBuilding != null && recordsWhileBuilding.size() > 0) {
                additionalScans += (long)recordsWhileBuilding.size();
            }
            assertThat(indexBuilder.getTotalRecordsScanned(),
                    allOf(
                            greaterThanOrEqualTo((long)records.size()),
                            lessThanOrEqualTo((long)records.size() + additionalScans)
                    ));
        }
        LOGGER.info(KeyValueLogMessage.of("building index - completed", TestLogMessageKeys.INDEX, index));

        LOGGER.info(KeyValueLogMessage.of("running post build checks", TestLogMessageKeys.INDEX, index));
        afterBuild.run();

        LOGGER.info(KeyValueLogMessage.of("verifying range set emptiness", TestLogMessageKeys.INDEX, index));
        try (FDBRecordContext context = openContext()) {
            RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(metaData.getIndex(index.getName())));
            System.out.println("Range set for " + records.size() + " records:\n" + rangeSet.rep(context.ensureActive()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(context.ensureActive()).asList().join());
            context.commit();
        }

        LOGGER.info(KeyValueLogMessage.of("marking index readable", TestLogMessageKeys.INDEX, index));
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.markIndexReadable(index).join());
            context.commit();
        }
        afterReadable.run();

        LOGGER.info(KeyValueLogMessage.of("ending rebuild test",
                        TestLogMessageKeys.RECORDS, records.size(),
                        LogMessageKeys.RECORDS_WHILE_BUILDING, recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size(),
                        TestLogMessageKeys.AGENTS, agents,
                        TestLogMessageKeys.OVERLAP, overlap,
                        TestLogMessageKeys.SPLIT_LONG_RECORDS, splitLongRecords,
                        TestLogMessageKeys.INDEX, index)
        );
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                              int agents, boolean overlap, boolean splitLongRecords) {
        Index index = new Index("newIndex", field("num_value_2"));
        Function<FDBQueriedRecord<Message>,Integer> projection = rec -> {
            TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
            if (simple.hasNumValue2()) {
                return simple.getNumValue2();
            } else {
                return Integer.MIN_VALUE;
            }
        };

        List<RecordQuery> queries = records.stream()
                .map(record -> {
                    Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                    return RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(value2 != null ?
                                    Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                    Query.field("num_value_2").isNull())
                            .build();
                })
                .collect(Collectors.toList());

        Function<TestRecords1Proto.MySimpleRecord,Integer> indexValue = msg -> msg.hasNumValue2() ? msg.getNumValue2() : null;
        Map<Integer, List<Message>> valueMap = group(records, indexValue);

        Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < queries.size(); i++) {
                    Integer value2 = (records.get(i).hasNumValue2()) ? records.get(i).getNumValue2() : null;
                    String planString = "Scan(<,>) | [MySimpleRecord] | " + ((value2 == null) ?  "num_value_2 IS_NULL" : "num_value_2 EQUALS " + value2);
                    executeQuery(queries.get(i), planString, valueMap.get(value2));
                }
                context.commit();
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        List<RecordQuery> updatedQueries;
        Map<Integer, List<Message>> updatedValueMap;
        if (recordsWhileBuilding == null || recordsWhileBuilding.size() == 0) {
            updatedRecords = records;
            updatedQueries = queries;
            updatedValueMap = valueMap;
        } else {
            updatedRecords = updated(records, recordsWhileBuilding);
            updatedQueries = updatedRecords.stream()
                .map(record -> {
                    Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                    return RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(value2 != null ?
                                    Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                    Query.field("num_value_2").isNull())
                            .build();
                })
                .collect(Collectors.toList());
            updatedValueMap = group(updatedRecords, indexValue);
        }

        Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    // The build job shouldn't affect the reads.
                    for (int i = 0; i < updatedQueries.size(); i++) {
                        Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                        String planString = "Scan(<,>) | [MySimpleRecord] | " + ((value2 == null) ? "num_value_2 IS_NULL" : "num_value_2 EQUALS " + value2);
                        OnlineIndexerTest.this.executeQuery(updatedQueries.get(i), planString, updatedValueMap.get(value2));
                    }
                }
            }
        };

        Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < updatedQueries.size(); i++) {
                    Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                    executeQuery(updatedQueries.get(i), "Index(newIndex [[" + value2 + "],[" + value2 + "]])", updatedValueMap.get(value2));
                }
                RecordQuery sortQuery = RecordQuery.newBuilder()
                        .setRecordType("MySimpleRecord")
                        .setSort(field("num_value_2"))
                        .build();
                executeQuery(sortQuery, "Index(newIndex <,>)", updatedRecords.stream().map(msg -> (msg.hasNumValue2()) ? msg.getNumValue2() : Integer.MIN_VALUE).sorted().collect(Collectors.toList()), projection);
                context.commit();
            }
        };

        singleRebuild(records, recordsWhileBuilding, agents, overlap, splitLongRecords, index, beforeBuild, afterBuild, afterReadable);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                              int agents, boolean overlap) {
        valueRebuild(records, recordsWhileBuilding, agents, overlap, false);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        valueRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, boolean splitLongRecords) {
        valueRebuild(records, null, 1, false, splitLongRecords);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        valueRebuild(records, null);
    }

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                             int agents, boolean overlap) {
        final Index index = new Index("newRankIndex", field("num_value_2").ungrouped(), IndexTypes.RANK);
        final IndexRecordFunction<Long> recordFunction = (IndexRecordFunction<Long>)Query.rank("num_value_2").getFunction();

        final Runnable beforeBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    for (TestRecords1Proto.MySimpleRecord record : records) {
                        try {
                            recordStore.evaluateRecordFunction(recordFunction, OnlineIndexerTest.this.createStoredMessage(record)).join();
                            fail("Somehow evaluated rank");
                        } catch (RecordCoreException e) {
                            assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getMessage());
                        }
                    }

                    // Either we get an exception, or the record store is empty.
                    try {
                        RecordQuery query = RecordQuery.newBuilder()
                                .setRecordType("MySimpleRecord")
                                .setFilter(Query.rank("num_value_2").equalsValue(0L))
                                .build();
                        RecordQueryPlan plan = planner.plan(query);
                        assertEquals("Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 0", plan.toString());
                        Optional<?> first = recordStore.executeQuery(plan).first().join();
                        assertTrue(!first.isPresent(), "non-empty range with rank rebuild");
                    } catch (CompletionException e) {
                        assertNotNull(e.getCause());
                        assertThat(e.getCause(), instanceOf(RecordCoreException.class));
                        assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getCause().getMessage());
                    }
                }
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        if (recordsWhileBuilding == null || recordsWhileBuilding.size() == 0) {
            updatedRecords = records;
        } else {
            updatedRecords = updated(records, recordsWhileBuilding);
        }

        final Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
                        try {
                            recordStore.evaluateRecordFunction(recordFunction, OnlineIndexerTest.this.createStoredMessage(record)).join();
                            fail("Somehow evaluated rank");
                        } catch (RecordCoreException e) {
                            assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getMessage());
                        }
                    }

                    // Either we get an exception, or the record store is empty.
                    try {
                        RecordQuery query = RecordQuery.newBuilder()
                                .setRecordType("MySimpleRecord")
                                .setFilter(Query.rank("num_value_2").equalsValue(0L))
                                .build();
                        RecordQueryPlan plan = planner.plan(query);
                        assertEquals("Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 0", plan.toString());
                        Optional<?> first = recordStore.executeQuery(plan).first().join();
                        assertTrue(!first.isPresent(), "non-empty range with rank rebuild");
                    } catch (CompletionException e) {
                        assertNotNull(e.getCause());
                        assertThat(e.getCause(), instanceOf(RecordCoreException.class));
                        assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getCause().getMessage());
                    }
                }
            }
        };

        TreeSet<Integer> values = new TreeSet<>();
        boolean hasNull = false;
        for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
            if (!record.hasNumValue2()) {
                hasNull = true;
            } else {
                values.add(record.getNumValue2());
            }
        }

        long curr = 0;
        Map<Integer,Long> ranks = new HashMap<>();
        if (hasNull) {
            curr += 1;
        }
        for (Integer value : values) {
            ranks.put(value, curr);
            curr += 1;
        }

        final Runnable afterReadable = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
                        Long rank = recordStore.evaluateRecordFunction(recordFunction, OnlineIndexerTest.this.createStoredMessage(record)).join();
                        if (!record.hasNumValue2()) {
                            assertEquals(0L, rank.longValue());
                        } else {
                            assertEquals(ranks.get(record.getNumValue2()), rank);
                        }

                        RecordQuery query = RecordQuery.newBuilder()
                                .setRecordType("MySimpleRecord")
                                .setFilter(Query.rank("num_value_2").equalsValue(rank))
                                .build();
                        RecordQueryPlan plan = planner.plan(query);
                        assertEquals("Index(newRankIndex [[" + rank + "],[" + rank + "]] BY_RANK)", plan.toString());
                        Optional<TestRecords1Proto.MySimpleRecord> retrieved = recordStore.executeQuery(plan).map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                                .filter(rec -> rec.getRecNo() == record.getRecNo()).first().join();
                        assertTrue(retrieved.isPresent(), "empty range after rank index build");
                        assertEquals(record, retrieved.get());
                    }
                }
            }
        };

        singleRebuild(records, recordsWhileBuilding, agents, overlap, false, index, beforeBuild, afterBuild, afterReadable);
    }

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        rankRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        rankRebuild(records, null);
    }

    private FDBStoredRecord<Message> createStoredMessage(@Nonnull TestRecords1Proto.MySimpleRecord record) {
        return FDBStoredRecord.newBuilder()
                .setPrimaryKey(Tuple.from(record.getRecNo()))
                .setRecordType(recordStore.getRecordMetaData().getRecordType("MySimpleRecord"))
                .setRecord(record)
                .build();
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                            int agents, boolean overlap) {
        Index index = new Index("newSumIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());

        Runnable beforeBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    metaData.getIndex(index.getName());
                } catch (MetaDataException e) {
                    assertEquals("Index newSumIndex not defined", e.getMessage());
                }
            }
        };

        Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                Index indexToUse = metaData.getIndex(index.getName());
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"), aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT);
                } catch (RecordCoreException e) {
                    assertEquals("Aggregate function newSumIndex.sum(Field { 'num_value_2' None} group 1) requires appropriate index", e.getMessage());
                }
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        if (recordsWhileBuilding == null || recordsWhileBuilding.size() == 0) {
            updatedRecords = records;
        } else {
            updatedRecords = updated(records, recordsWhileBuilding);
        }

        Runnable afterReadable = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    long sum = recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"), aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0);
                    long expected = updatedRecords.stream().mapToInt(msg -> msg.hasNumValue2() ? msg.getNumValue2() : 0).sum();
                    assertEquals(expected, sum);
                }
            }
        };

        singleRebuild(records, recordsWhileBuilding, agents, overlap, false, index, beforeBuild, afterBuild, afterReadable);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        sumRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        sumRebuild(records, null);
    }

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                              int agents, boolean overlap) {
        final Index index = new Index("newVersionIndex", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        final Function<FDBQueriedRecord<Message>,Tuple> projection = rec -> {
            TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
            Integer numValue2 = (simple.hasNumValue2()) ? simple.getNumValue2() : null;
            FDBRecordVersion version = rec.hasVersion() ? rec.getVersion() : null;
            if (version != null) {
                assertTrue(version.isComplete());
            }
            return Tuple.from(numValue2, (version == null) ? null : version.toVersionstamp());
        };

        List<RecordQuery> queries = records.stream()
                .map(record -> {
                    Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                    return RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(value2 != null ?
                                    Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                    Query.field("num_value_2").isNull())
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                })
                .collect(Collectors.toList());

        Function<TestRecords1Proto.MySimpleRecord,Integer> indexValue = msg -> msg.hasNumValue2() ? msg.getNumValue2() : null;
        Map<Integer, List<Message>> valueMap = group(records, indexValue);
        Map<Long, FDBRecordVersion> versionMap = new HashMap<>(records.size() + (recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size()));
        AtomicReference<FDBRecordVersion> greatestVersion = new AtomicReference<>(null);

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < queries.size(); i++) {
                    Integer value2 = (records.get(i).hasNumValue2()) ? records.get(i).getNumValue2() : null;
                    try {
                        executeQuery(queries.get(i), "Index(newVersionIndex [[" + value2 + "],[" + value2 + "])", valueMap.get(value2));
                        fail("somehow executed query with new index before build");
                    } catch (RecordCoreException e) {
                        assertEquals("Cannot sort without appropriate index: Version", e.getMessage());
                    }
                }

                // Load all the version information for the records that were initially there.
                for (TestRecords1Proto.MySimpleRecord simple : records) {
                    recordStore.loadRecordVersion(Tuple.from(simple.getRecNo())).ifPresent(version -> {
                        versionMap.put(simple.getRecNo(), version);
                        if (greatestVersion.get() == null || version.compareTo(greatestVersion.get()) > 0) {
                            greatestVersion.set(version);
                        }
                    });
                }

                context.commit();
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        List<RecordQuery> updatedQueries;
        Map<Integer, List<Message>> updatedValueMap;
        if (recordsWhileBuilding == null || recordsWhileBuilding.size() == 0) {
            updatedRecords = records;
            updatedQueries = queries;
            updatedValueMap = valueMap;
        } else {
            updatedRecords = updated(records, recordsWhileBuilding);
            updatedQueries = updatedRecords.stream()
                    .map(record -> {
                        Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                        return RecordQuery.newBuilder()
                                .setRecordType("MySimpleRecord")
                                .setFilter(value2 != null ?
                                        Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                        Query.field("num_value_2").isNull())
                                .setSort(VersionKeyExpression.VERSION)
                                .build();
                    })
                    .collect(Collectors.toList());
            updatedValueMap = group(updatedRecords, indexValue);
        }

        Map<Long, FDBRecordVersion> updatedVersionMap = new HashMap<>(versionMap.size());
        Set<Long> newRecordKeys = (recordsWhileBuilding == null) ? Collections.emptySet() : recordsWhileBuilding.stream().map(TestRecords1Proto.MySimpleRecord::getRecNo).collect(Collectors.toSet());

        Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = OnlineIndexerTest.this.openContext()) {
                    // The build job shouldn't affect the reads.
                    for (int i = 0; i < updatedQueries.size(); i++) {
                        Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                        try {
                            OnlineIndexerTest.this.executeQuery(updatedQueries.get(i), "Index(newVersionIndex [[" + value2 + "],[" + value2 + "])", updatedValueMap.get(value2));
                            fail("somehow executed query with new index before readable");
                        } catch (RecordCoreException e) {
                            assertEquals("Cannot sort without appropriate index: Version", e.getMessage());
                        }
                    }

                    // Load all the version information for records that are there now and that values are sane.
                    for (TestRecords1Proto.MySimpleRecord simple : updatedRecords) {
                        recordStore.loadRecordVersion(Tuple.from(simple.getRecNo())).ifPresent(version -> {
                            assertTrue(version.isComplete());
                            if (newRecordKeys.contains(simple.getRecNo())) {
                                assertThat(version, greaterThan(greatestVersion.get()));
                                if (versionMap.containsKey(simple.getRecNo())) {
                                    assertThat(version, greaterThan(versionMap.get(simple.getRecNo())));
                                }
                            } else {
                                if (versionMap.containsKey(simple.getRecNo())) {
                                    assertEquals(versionMap.get(simple.getRecNo()), version);
                                }
                            }
                            updatedVersionMap.put(simple.getRecNo(), version);
                        });
                    }
                }
            }
        };

        Runnable afterReadable = () -> {
            Descriptors.FieldDescriptor recNoFieldDescriptor = TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("rec_no");
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < updatedQueries.size(); i++) {
                    Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                    List<Tuple> sortedValues = updatedValueMap.get(value2).stream()
                            .map(msg -> {
                                FDBRecordVersion version = updatedVersionMap.get(((Number)msg.getField(recNoFieldDescriptor)).longValue());
                                return Tuple.from(value2, version == null ? null : version.toVersionstamp());
                            })
                            .sorted()
                            .collect(Collectors.toList());
                    executeQuery(updatedQueries.get(i), "Index(newVersionIndex [[" + value2 + "],[" + value2 + "]])", sortedValues, projection);
                }
                context.commit();
            }
        };

        singleRebuild(records, recordsWhileBuilding, agents, overlap, false, index, beforeBuild, afterBuild, afterReadable);
    }

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        versionRebuild(records, null);
    }

    @Test
    public void emptyRange() {
        valueRebuild(Collections.emptyList());
    }

    @Test
    public void singleElement() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        valueRebuild(Collections.singletonList(record));
    }

    @Test
    public void tenElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 457).setNumValue2(Math.abs(val * 2)).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    public void tenAdjacentElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(val).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    public void fiftyElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-25, 25).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 37).setNumValue2(Math.abs(val) % 5).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElements() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallel() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlap() {
        Random r = new Random(0xf005ba11);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, null, 5, true);
    }

    @Test
    public void tenSplitElements() {
        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 431).setNumValue2(Math.abs(val) % 5).setStrValueIndexed(bigOlString).build()
        ).collect(Collectors.toList());
        valueRebuild(records, true);
    }

    @Test
    public void fiftySplitElements() {
        // Surely this can all fit in memory, no problem, right?
        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-25, 25).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 39).setNumValue2(Math.abs(val) % 5).setStrValueIndexed(bigOlString).build()
        ).collect(Collectors.toList());
        valueRebuild(records, true);
    }

    @Test
    public void withNullKey1() {
        List<TestRecords1Proto.MySimpleRecord> records = Arrays.asList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(17).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(76).setRecNo(123).build()
        );
        valueRebuild(records);
    }

    @Test
    public void withNullKey2() {
        List<TestRecords1Proto.MySimpleRecord> records = Collections.singletonList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(17).build()
        );
        valueRebuild(records);
    }

    @Test
    public void withNullValue() {
        List<TestRecords1Proto.MySimpleRecord> records = Arrays.asList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1776).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1828).setNumValue2(100).build()
        );
        valueRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void somePreloaded() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(75).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newIndex", field("num_value_2"));
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newIndex").join();
            context.commit();
        }
        valueRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuilding() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingParallel() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuilding() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingParallel() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    public void emptyRangeRank() {
        rankRebuild(Collections.emptyList());
    }

    @Test
    public void singleElementRank() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        rankRebuild(Collections.singletonList(record));
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsRank() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelRank() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlapRank() {
        Random r = new Random(0xf005ba11);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, null, 5, true);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingRank() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingParallelRank() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void somePreloadedRank() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newRankIndex", field("num_value_2").ungrouped(), IndexTypes.RANK);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newRankIndex").join();
            context.commit();
        }
        rankRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingRank() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingParallelRank() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    public void emptyRangeSum() {
        sumRebuild(Collections.emptyList());
    }

    @Test
    public void singleElementSum() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        sumRebuild(Collections.singletonList(record));
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlapSum() {
        Random r = new Random(0xf005ba11);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, 5, true);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingSum() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingParallelSum() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void somePreloadedSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newSumIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            recordStore.markIndexWriteOnly("newSumIndex").join();
            context.commit();
        }
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newSumIndex").join();
            context.commit();
        }
        sumRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingSum() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingParallelSum() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlapVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, null, 5, true);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingParallelVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingParallelVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void someWithoutVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 1, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void someWithoutVersionParallel() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingWithoutVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingWithoutVersionParallel() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void sequentialWithoutVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 1, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void sequentialWhileBuildingWithoutVersion() {
        Random r = new Random(0x8badf00d);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }

    @Test
    public void uniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        // Case 1: Entirely in build.
        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();

            // Case 2: While in write-only mode.
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteAllRecords();
                recordStore.markIndexWriteOnly(index).join();
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                records.forEach(recordStore::saveRecord);
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 3: Some in write-only mode.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 4: Some in write-only mode with an initial range build that shouldn't affect anything.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(0L), Key.Evaluated.scalar(5L)).join();
            try (FDBRecordContext context = openContext()) {
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(5L), Key.Evaluated.scalar(10L)).join();
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 5: Should be caught by write-only writes after build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
            fail("Did not catch uniqueness violation when done after build by write-only writes");
        } catch (RecordIndexUniquenessViolation e) {
            // passed.
        }

        // Case 6: Should be caught by write-only writes after partial build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(0L), Key.Evaluated.scalar(5L)).join();
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 7: The second of these two transactions should fail on not_committed, and then
        // there should be a uniqueness violation.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            try (FDBRecordContext context = openContext()) {
                context.ensureActive().getReadVersion().join();
                try (FDBRecordContext context2 = fdb.openContext()) {
                    context2.ensureActive().getReadVersion().join();
                    FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context2).build();

                    indexBuilder.buildUnbuiltRange(recordStore, null, Key.Evaluated.scalar(5L)).join();
                    recordStore2.saveRecord(records.get(8));

                    context.commit();
                    context2.commitAsync().handle((ignore, e) -> {
                        assertNotNull(e);
                        RuntimeException runE = FDBExceptions.wrapException(e);
                        assertThat(runE, instanceOf(RecordCoreRetriableTransactionException.class));
                        assertNotNull(runE.getCause());
                        assertThat(runE.getCause(), instanceOf(FDBException.class));
                        FDBException fdbE = (FDBException)runE.getCause();
                        assertEquals(1020, fdbE.getCode()); // not_committed
                        return null;
                    }).join();
                }
            }
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }
    }

    @Test
    public void resolveUniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < 5; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = 5; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }

            recordStore.markIndexReadable(index).join();
            context.commit();
        }
    }

    @Test
    public void buildEndpointIdempotency() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit the change.
            }
            return ret;
        };

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            final RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(index));

            // Build the endpoints
            TupleRange range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(10L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(0L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(9L).pack(), null).join());
            List<Range> middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(0L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(9L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Make sure running this again doesn't change anything.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(10L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(0L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(9L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(0L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(9L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Remove the first and last records.
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteRecord(Tuple.from(0L));
                recordStore.deleteRecord(Tuple.from(9L));
                context.commit();
            }
            assertEquals(Tuple.from(0L), getAggregate.get());

            // Rerun endpoints with new data.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(1L), range.getLow());
            assertEquals(Tuple.from(8L), range.getHigh());
            assertEquals(Tuple.from(9L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Run it again to show that nothing has happened.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(1L), range.getLow());
            assertEquals(Tuple.from(8L), range.getHigh());
            assertEquals(Tuple.from(9L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Add back the previous first and last records.
            try (FDBRecordContext context = openContext()) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(0L).setNumValue2(1).build());
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(9L).setNumValue2(10).build());
                context.commit();
            }
            assertEquals(Tuple.from(20L), getAggregate.get());

            // Rerun endpoints with new data.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(20L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Run it again to show that nothing has happened.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(20L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Straight up build the whole index.
            indexBuilder.buildIndex(false);
            assertEquals(Tuple.from(55L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database()).join());
        }
    }

    @Test
    public void buildRangeTransactional() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit changes
            }
            return ret;
        };

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            try (FDBRecordContext context = openContext()) {
                recordStore.markIndexWriteOnly(index).join();
                context.commit();
            }

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, null, null).join();
                indexBuilder.buildRange(recordStore, null, null).join();
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());

            clearIndexData(index);

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, null, Key.Evaluated.scalar(130L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(8515L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), Key.Evaluated.scalar(130L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(8515L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), Key.Evaluated.scalar(150L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(11325L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), null).join();
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());

            clearIndexData(index);

            try (FDBRecordContext context = openContext()) {
                for (long l = 0L; l < 200L; l += 10) {
                    indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(l), Key.Evaluated.scalar(l + 5L)).join();
                }
                context.commit();
            }
            assertEquals(Tuple.from(9800L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                for (long l = 0L; l < 200L; l += 10) {
                    indexBuilder.buildRange(recordStore, null, null).join();
                }
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());
        }

    }

    @Test
    public void buildRangeWithNull() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit changes
            }
            return ret;
        };

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildRange(null, null).join();
            assertEquals(Tuple.from(20100L), getAggregate.get());
        }
    }

    @SuppressWarnings("try")
    @Test
    public void readableAtEnd() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 50).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndex();
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
        }
    }

    @Test
    public void run() {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(100).setMaxRetries(3).setRecordsPerSecond(10000)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(2)
                .build()) {

            AtomicInteger attempts = new AtomicInteger();

            // Non-FDB error
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new IllegalStateException("illegal state");
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(IllegalStateException.class));
                assertEquals("illegal state", e.getMessage());
                assertNull(e.getCause());
                assertEquals(1, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Retriable error that is not in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Retriable", new FDBException("commit_unknown_result", 1021));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreRetriableTransactionException.class));
                assertEquals("Retriable", e.getMessage());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("commit_unknown_result", e.getCause().getMessage());
                assertEquals(1021, ((FDBException)e.getCause()).getCode());
                assertEquals(2, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Non-retriable error that is in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreException.class));
                assertEquals("Non-retriable", e.getMessage());
                assertNotNull(e.getCause());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("transaction_too_large", e.getCause().getMessage());
                assertEquals(2101, ((FDBException)e.getCause()).getCode());
                assertEquals(4, attempts.get()); // lessenWorkCodes is maxRetries
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Retriable error that is in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Retriable and lessener", new FDBException("not_committed", 1020));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreRetriableTransactionException.class));
                assertEquals("Retriable and lessener", e.getMessage());
                assertNotNull(e.getCause());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("not_committed", e.getCause().getMessage());
                assertEquals(1020, ((FDBException)e.getCause()).getCode());
                assertEquals(8, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();
        }
    }

    private <R> CompletableFuture<R> runAndHandleLessenWorkCodes(OnlineIndexer indexBuilder, @Nonnull Function<FDBRecordStore, CompletableFuture<R>> function) {
        return indexBuilder.runAsync(function, Pair::of, indexBuilder::decreaseLimit, null);
    }

    @Test
    public void lessenLimits() {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(100).setMaxRetries(30).setRecordsPerSecond(10000)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(3)
                .build()) {

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger limit = new AtomicInteger(100);

            // Non-retriable error that is in lessen work codes.
            attempts.set(0);
            indexBuilder.buildAsync((store, recordsScanned) -> {
                assertEquals(attempts.getAndIncrement(), indexBuilder.getLimit(),
                        limit.getAndUpdate(x -> Math.max(x, (3 * x) / 4)));
                throw new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
            }, true, null).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreException.class));
                assertEquals("Non-retriable", e.getMessage());
                return null;
            }).join();
            assertEquals(31, attempts.get());
        }
    }

    @Test
    void notReincreaseLimit() {
        // Non-retriable error that is in lessen work codes.
        Supplier<RuntimeException> createException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));

        Queue<Pair<Integer, Supplier<RuntimeException>>> queue = new LinkedList<>();
        // failures until it hits 42
        for (int i = 100; i > 42; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // a whole bunch of successes
        for (int i = 0; i < 100; i++) {
            queue.add(Pair.of(42, null));
        }
        reincreaseLimit(queue, index ->
                OnlineIndexer.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                        .setLimit(100).setMaxRetries(queue.size() + 3).setRecordsPerSecond(10000)
                        .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                        .setMaxAttempts(3)
                        .build());
    }

    @Test
    public void reincreaseLimit() {
        // Non-retriable error that is in lessen work codes.
        Supplier<RuntimeException> createException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));

        Queue<Pair<Integer, Supplier<RuntimeException>>> queue = new LinkedList<>();
        // failures until it hits 1
        for (int i = 100; i > 1; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // queue size = 13
        // success for a while
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(1, null));
        }
        // queue size = 23
        // now starts re-increasing
        queue.add(Pair.of(2, null));
        queue.add(Pair.of(3, null));
        queue.add(Pair.of(4, null));
        for (int i = 5; i < 100; i = (i * 4) / 3) {
            queue.add(Pair.of(i, null));
        }
        // queue size = 38
        // does not pass original max
        queue.add(Pair.of(100, null));
        queue.add(Pair.of(100, null));
        queue.add(Pair.of(100, null));
        for (int i = 100; i > 42; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // queue size = 44
        // success for a while
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(42, null));
        }
        // queue size = 54
        // fail once
        queue.add(Pair.of(56, createException));
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(42, null));
        }
        // queue size = 65
        queue.add(Pair.of(56, createException));
        queue.add(Pair.of(42, null));

        reincreaseLimit(queue, index ->
                OnlineIndexer.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                        .setLimit(100).setMaxRetries(queue.size() + 3).setRecordsPerSecond(10000)
                        .setIncreaseLimitAfter(10)
                        .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                        .setMaxAttempts(3)
                        .setProgressLogIntervalMillis(0)
                        .build());
    }

    private void reincreaseLimit(Queue<Pair<Integer, Supplier<RuntimeException>>> queue,
                                 final Function<Index, OnlineIndexer> buildOnlineIndexer) {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = buildOnlineIndexer.apply(index)) {

            AtomicInteger attempts = new AtomicInteger();
            attempts.set(0);
            AsyncUtil.whileTrue(() ->
                    indexBuilder.buildAsync((store, recordsScanned) -> {
                        Pair<Integer, Supplier<RuntimeException>> behavior = queue.poll();
                        if (behavior == null) {
                            return AsyncUtil.READY_FALSE;
                        } else {
                            int currentAttempt = attempts.getAndIncrement();
                            assertEquals(behavior.getLeft().intValue(), indexBuilder.getLimit(),
                                    "Attempt " + currentAttempt);
                            if (behavior.getRight() != null) {
                                throw behavior.getRight().get();
                            }
                            return AsyncUtil.READY_TRUE;
                        }
                    }, true, null)).join();
            assertNull(queue.poll());
        }
    }

    @Test
    public void recordsScanned() {
        Supplier<RuntimeException> nonRetriableException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
        Supplier<RuntimeException> retriableException =
                () -> new RecordCoreRetriableTransactionException("Retriable", new FDBException("not_committed", 1020));
        Queue<Pair<Long, Supplier<RuntimeException>>> queue = new LinkedList<>();

        queue.add(Pair.of(0L, retriableException));
        queue.add(Pair.of(0L, nonRetriableException));
        queue.add(Pair.of(0L, null));
        queue.add(Pair.of(1L, null));
        queue.add(Pair.of(2L, null));
        queue.add(Pair.of(3L, null));
        queue.add(Pair.of(4L, retriableException));
        queue.add(Pair.of(4L, retriableException));
        queue.add(Pair.of(4L, nonRetriableException));
        queue.add(Pair.of(4L, nonRetriableException));
        queue.add(Pair.of(4L, null));
        Index index = runAsyncSetup();


        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(100).setMaxRetries(queue.size() + 3).setRecordsPerSecond(10000)
                .setIncreaseLimitAfter(10)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(3)
                .setProgressLogIntervalMillis(30) // log some of the time, to make sure that doesn't impact things
                .build()) {

            AtomicInteger attempts = new AtomicInteger();
            attempts.set(0);
            AsyncUtil.whileTrue(() -> indexBuilder.buildAsync(
                    (store, recordsScanned) -> {
                        Pair<Long, Supplier<RuntimeException>> behavior = queue.poll();
                        if (behavior == null) {
                            return AsyncUtil.READY_FALSE;
                        } else {
                            int currentAttempt = attempts.getAndIncrement();
                            assertEquals(1, recordsScanned.incrementAndGet());
                            assertEquals(behavior.getLeft().longValue(), indexBuilder.getTotalRecordsScanned(),
                                    "Attempt " + currentAttempt);
                            if (behavior.getRight() != null) {
                                throw behavior.getRight().get();
                            }
                            return AsyncUtil.READY_TRUE;
                        }
                    },
                    true,
                    Arrays.asList(LogMessageKeys.CALLING_METHOD, "OnlineIndexerTest.recordsScanned"))
            ).join();
            assertNull(queue.poll());
            assertEquals(5L, indexBuilder.getTotalRecordsScanned());
        }
    }

    private Index runAsyncSetup() {
        Index index = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));

        try (FDBRecordContext context = openContext()) {
            // OnlineIndexer.runAsync checks that the index is not readable
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }
        return index;
    }

    @Test
    public void illegalConstructorParams() {
        Index newIndex = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", newIndex));
        Index indexPrime = metaData.getIndex("newIndex");
        Collection<RecordType> recordTypes = metaData.recordTypesForIndex(indexPrime);
        // Absent index
        try {
            Index absentIndex = new Index("absent", field("num_value_2"));
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(absentIndex).setSubspace(subspace).build();
            fail("Did not catch absent index.");
        } catch (MetaDataException e) {
            assertEquals("Index absent not contained within specified metadata", e.getMessage());
        }
        // Limit
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setLimit(-1).build();
            fail("Did not catch negative limit.");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value -1 given for record limit", e.getMessage());
        }
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setLimit(0).build();
            fail("Did not catch zero limit.");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value 0 given for record limit", e.getMessage());
        }
        // Retries
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setMaxRetries(-1).build();
            fail("Did not catch negative retries.");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value -1 given for maximum retries", e.getMessage());
        }
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setMaxRetries(0).build();
            fail("Did not catch zero retries.");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value 0 given for maximum retries", e.getMessage());
        }
        // Records per second
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setRecordsPerSecond(-1).build();
            fail("Did not catch negative RPS");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value -1 given for records per second value", e.getMessage());
        }
        try {
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setRecordsPerSecond(0).build();

            fail("Did not catch zero RPS");
        } catch (RecordCoreException e) {
            assertEquals("Non-positive value 0 given for records per second value", e.getMessage());
        }
    }

    @Test
    public void closeWhileBuilding() throws Exception {
        final Index index = new Index("newIndex", field("num_value_2"));
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 1000; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final CompletableFuture<Void> future;
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(1).setMaxRetries(Integer.MAX_VALUE).setRecordsPerSecond(Integer.MAX_VALUE)
                .setTimer(timer)
                .build()) {
            future = indexBuilder.buildIndexAsync();
            // Let the builder get some work done.
            int pass = 0;
            while (!future.isDone() && timer.getCount(FDBStoreTimer.Events.COMMIT) < 10 && pass++ < 100) {
                Thread.sleep(100);
            }
            assertThat("Should have done several transactions in a few seconds", pass, lessThan(100));
        }
        int count1 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        assertThrows(FDBDatabaseRunner.RunnerClosed.class, () -> fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, future));
        Thread.sleep(50);
        int count2 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        // Might close just after committing but before recording.
        assertThat("At most one more commits should have occurred", count2, isOneOf(count1, count1 + 1));
        Thread.sleep(50);
        int count3 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        assertThat("No more commits should have occurred", count3, is(count2));
    }

    @Test
    public void markReadable() {
        Index index = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));

        try (FDBRecordContext context = openContext()) {
            // OnlineIndexer.runAsync checks that the index is not readable
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            // No need to build range because there is no record.
            indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.buildEndpoints());

            // Do mark the the index as readable.
            assertTrue(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadableIfBuilt()));

            // When the index is readable:
            assertFalse(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadable())); // The status is not modified by markReadable.
            assertTrue(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadableIfBuilt()));
        }
    }
}
