/*
 * OnlineIndexerBuildIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.logging.TestLogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * A base class for testing building indexes with {@link OnlineIndexer#buildIndex()} (or similar APIs).
 */
abstract class OnlineIndexerBuildIndexTest extends OnlineIndexerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerBuildIndexTest.class);

    private boolean safeBuild;

    OnlineIndexerBuildIndexTest(boolean safeBuild) {
        this.safeBuild = safeBuild;
    }

    void singleRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
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
        final FDBStoreTimer timer = new FDBStoreTimer();

        final FDBRecordStoreTestBase.RecordMetaDataHook onlySplitHook = metaDataBuilder -> {
            if (splitLongRecords) {
                metaDataBuilder.setSplitLongRecords(true);
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            }
        };
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
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


        openSimpleMetaData(hook);

        LOGGER.info(KeyValueLogMessage.of("adding index", TestLogMessageKeys.INDEX, index));
        openSimpleMetaData(hook);

        final boolean isAlwaysReadable;
        try (FDBRecordContext context = openContext()) {
            // If it is a safe build, it should work without setting the index state to write-only, which will be taken
            // care of by OnlineIndexer.
            if (!safeBuild) {
                LOGGER.info(KeyValueLogMessage.of("marking write-only", TestLogMessageKeys.INDEX, index));
                recordStore.clearAndMarkIndexWriteOnly(index).join();
            }
            isAlwaysReadable = safeBuild && recordStore.isIndexReadable(index);
            context.commit();
        }

        LOGGER.info(KeyValueLogMessage.of("creating online index builder",
                TestLogMessageKeys.INDEX, index,
                TestLogMessageKeys.RECORD_TYPES, metaData.recordTypesForIndex(index),
                LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()),
                LogMessageKeys.LIMIT, 20,
                TestLogMessageKeys.RECORDS_PER_SECOND, OnlineIndexer.DEFAULT_RECORDS_PER_SECOND * 100));
        final OnlineIndexer.Builder builder = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setMetaData(metaData)
                .setIndex(index)
                .setSubspace(subspace)
                .setConfigLoader(old -> {
                    OnlineIndexer.Config.Builder conf = OnlineIndexer.Config.newBuilder()
                            .setMaxLimit(20)
                            .setMaxRetries(Integer.MAX_VALUE)
                            .setRecordsPerSecond(OnlineIndexer.DEFAULT_RECORDS_PER_SECOND * 100);
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        // randomly enable the progress logging to ensure that it doesn't throw exceptions,
                        // or otherwise disrupt the build.
                        LOGGER.info("Setting progress log interval");
                        conf.setProgressLogIntervalMillis(0);
                    }
                    return conf.build();
                }).setTimer(timer);
        if (ThreadLocalRandom.current().nextBoolean()) {
            LOGGER.info("Setting priority to DEFAULT");
            builder.setPriority(FDBTransactionPriority.DEFAULT);
        }
        if (fdb.isTrackLastSeenVersion()) {
            LOGGER.info("Setting weak read semantics");
            builder.setWeakReadSemantics(new FDBDatabase.WeakReadSemantics(0L, Long.MAX_VALUE, true));
        }
        if (!safeBuild) {
            builder.setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                    .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                    .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR));
            builder.setUseSynchronizedSession(false);
        }

        try (OnlineIndexer indexBuilder = builder.build()) {
            CompletableFuture<Void> buildFuture;
            LOGGER.info(KeyValueLogMessage.of("building index",
                    TestLogMessageKeys.INDEX, index,
                    TestLogMessageKeys.AGENT, agents,
                    LogMessageKeys.RECORDS_WHILE_BUILDING, recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size(),
                    TestLogMessageKeys.OVERLAP, overlap));
            if (agents == 1) {
                buildFuture = indexBuilder.buildIndexAsync(false);
            } else {
                if (overlap) {
                    CompletableFuture<?>[] futures = new CompletableFuture<?>[agents];
                    for (int i = 0; i < agents; i++) {
                        final int agent = i;
                        futures[i] = safeBuild ?
                                     indexBuilder.buildIndexAsync(false)
                                               .exceptionally(exception -> {
                                                   // (agents - 1) of the agents should stop with SynchronizedSessionLockedException
                                                   // because the other one is already working on building the index.
                                                   if (exception.getCause() instanceof SynchronizedSessionLockedException) {
                                                       LOGGER.info(KeyValueLogMessage.of("Detected another worker processing this index",
                                                               TestLogMessageKeys.INDEX, index,
                                                               TestLogMessageKeys.AGENT, agent), exception);
                                                       return null;
                                                   } else {
                                                       throw new CompletionException(exception);
                                                   }
                                               }) :
                                     indexBuilder.buildIndexAsync(false);
                    }
                    buildFuture = CompletableFuture.allOf(futures);
                } else {
                    // Safe builds do not support building ranges yet.
                    assumeFalse(safeBuild);
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
            if (safeBuild) {
                buildFuture = MoreAsyncUtil.composeWhenComplete(
                        buildFuture,
                        (result, ex) -> indexBuilder.checkAnyOngoingOnlineIndexBuildsAsync().thenAccept(Assertions::assertFalse),
                        fdb::mapAsyncToSyncException);
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

            try (FDBRecordContext context = openContext()) {
                IndexBuildState indexBuildState = context.asyncToSync(FDBStoreTimer.Waits.WAIT_GET_INDEX_BUILD_STATE,
                        IndexBuildState.loadIndexBuildStateAsync(recordStore, index));
                IndexState indexState = indexBuildState.getIndexState();
                if (isAlwaysReadable) {
                    assertEquals(IndexState.READABLE, indexState);
                } else {
                    assertEquals(IndexState.WRITE_ONLY, indexState);
                    assertEquals(indexBuilder.getTotalRecordsScanned(), indexBuildState.getRecordsScanned());
                    // Count index is not defined so we cannot determine the records in total from it.
                    assertNull(indexBuildState.getRecordsInTotal());
                }
            }

            assertThat(indexBuilder.getTotalRecordsScanned(),
                    allOf(
                            greaterThanOrEqualTo((long)records.size()),
                            lessThanOrEqualTo((long)records.size() + additionalScans)
                    ));
        }
        KeyValueLogMessage msg = KeyValueLogMessage.build("building index - completed", TestLogMessageKeys.INDEX, index);
        msg.addKeysAndValues(timer.getKeysAndValues());
        LOGGER.info(msg.toString());

        LOGGER.info(KeyValueLogMessage.of("running post build checks", TestLogMessageKeys.INDEX, index));
        // Do not check afterBuild if it is a safe build and the index was readable before build, because the tests may
        // expect that it does not use the index in queries since the index is not readable yet, but the fact is that it
        // uses the index in quereis since the index is readable.
        if (!isAlwaysReadable) {
            afterBuild.run();
        }

        LOGGER.info(KeyValueLogMessage.of("verifying range set emptiness", TestLogMessageKeys.INDEX, index));
        try (FDBRecordContext context = openContext()) {
            RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(metaData.getIndex(index.getName())));
            System.out.println("Range set for " + records.size() + " records:\n" + rangeSet.rep(context.ensureActive()).join());
            if (!isAlwaysReadable) {
                assertEquals(Collections.emptyList(), rangeSet.missingRanges(context.ensureActive()).asList().join());
            }
            context.commit();
        }

        LOGGER.info(KeyValueLogMessage.of("marking index readable", TestLogMessageKeys.INDEX, index));
        try (FDBRecordContext context = openContext()) {
            boolean updated = recordStore.markIndexReadable(index).join();
            if (isAlwaysReadable) {
                assertFalse(updated);
            } else {
                assertTrue(updated);
            }
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

    <T> void executeQuery(@Nonnull RecordQuery query, @Nonnull String planString, @Nonnull List<T> expected, @Nonnull Function<FDBQueriedRecord<Message>, T> projection) {
        RecordQueryPlan plan = planner.plan(query);
        assertEquals(planString, plan.toString());
        List<T> retrieved = recordStore.executeQuery(plan).map(projection).asList().join();
        assertEquals(expected, retrieved);
    }

    void executeQuery(@Nonnull RecordQuery query, @Nonnull String planString, @Nonnull List<Message> expected) {
        executeQuery(query, planString, expected, FDBQueriedRecord::getRecord);
    }

    <K,V extends Message> Map<K,List<Message>> group(@Nonnull List<V> values, @Nonnull Function<V, K> keyFunction) {
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
    List<TestRecords1Proto.MySimpleRecord> updated(@Nonnull List<TestRecords1Proto.MySimpleRecord> origRecords, @Nonnull List<TestRecords1Proto.MySimpleRecord> addedRecords) {
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

    FDBStoredRecord<Message> createStoredMessage(@Nonnull TestRecords1Proto.MySimpleRecord record) {
        return FDBStoredRecord.newBuilder()
                .setPrimaryKey(Tuple.from(record.getRecNo()))
                .setRecordType(recordStore.getRecordMetaData().getRecordType("MySimpleRecord"))
                .setRecord(record)
                .build();
    }
}
