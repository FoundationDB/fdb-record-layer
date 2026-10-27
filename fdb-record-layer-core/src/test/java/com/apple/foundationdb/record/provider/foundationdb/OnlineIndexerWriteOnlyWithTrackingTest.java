/*
 * OnlineIndexerWriteOnlyWithTrackingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingTrackTable;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building a non-idempotent index in the {@link com.apple.foundationdb.record.IndexState#WRITE_ONLY_WITH_TRACKING} state.
 */
class OnlineIndexerWriteOnlyWithTrackingTest extends OnlineIndexerTest {

    // A SUM index is not idempotent: indexing (or un-indexing) a record twice would corrupt the sum
    private final Index sumIndex = new Index("simple$sum_tracking", field("num_value_2").ungrouped(), IndexTypes.SUM);
    private final Index countIndex = new Index("simple$count_tracking", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
    private final Index valueIndex = new Index("simple$value_tracking", field("num_value_2"), IndexTypes.VALUE);
    // A rank index that counts duplicates is not idempotent
    private final Index rankIndex = new Index("simple$rank_tracking", field("num_value_2").ungrouped(), IndexTypes.RANK,
            Map.of(IndexOptions.RANK_COUNT_DUPLICATES, "true"));

    private static final String UNIQUE_SOURCE_INDEX = "MySimpleRecord$num_value_unique";

    private final Map<Integer, Integer> expectedNumValue2 = new TreeMap<>();

    // ---- helpers ----

    private void populateEvenRecords(final int numRecords) {
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                save(2 * i, 2 * i * 19);
            }
            context.commit();
        }
    }

    private void save(final int recNo, final int numValue2) {
        saveSimple(recordStore, recNo, numValue2);
        expectedNumValue2.put(recNo, numValue2);
    }

    private static void saveSimple(@Nonnull final FDBRecordStore store, final int recNo, final int numValue2) {
        saveSimple(store, recNo, numValue2, recNo * 1139);
    }

    private static void saveSimple(@Nonnull final FDBRecordStore store, final int recNo, final int numValue2, final int numValueUnique) {
        store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .setNumValueUnique(numValueUnique)
                .build());
    }

    private static void saveOther(@Nonnull final FDBRecordStore store, final int recNo) {
        store.saveRecord(TestRecords1Proto.MyOtherRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(recNo * 7)
                .build());
    }

    private void userWrites() {
        // Each operation is a separate transaction, applied to records in the indexed area (< 20), in the not yet
        // indexed area (> 20), and to records that were inserted by user io.
        inTransaction(() -> {
            save(1, 19);    // insert, indexed area
            save(41, 777);  // insert, not indexed area
        });
        inTransaction(() -> save(4, 111));    // update, indexed area
        inTransaction(() -> save(30, 222));   // update, not indexed area
        inTransaction(() -> save(41, 333));   // update, inserted record
        inTransaction(() -> delete(6));       // delete, indexed area
        inTransaction(() -> delete(32));      // delete, not indexed area
        inTransaction(() -> delete(1));       // delete, inserted record
    }

    private void delete(final int recNo) {
        recordStore.deleteRecord(Tuple.from(recNo));
        expectedNumValue2.remove(recNo);
    }

    private void inTransaction(@Nonnull final Runnable runnable) {
        try (FDBRecordContext context = openContext()) {
            runnable.run();
            context.commit();
        }
    }

    /** Run {@code action} in its own, retried, transaction (usable from any thread). */
    private void io(@Nonnull final Consumer<FDBRecordStore> action) {
        fdb.run(context -> {
            action.accept(createStoreBuilder().setContext(context).createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE));
            return null;
        });
    }

    private OnlineIndexer.Builder trackingIndexerBuilder(@Nonnull final Index index, final boolean mutual) {
        final OnlineIndexer.IndexingPolicy.Builder policy = OnlineIndexer.IndexingPolicy.newBuilder()
                .setUseWriteOnlyWithTracking(List.of(index));
        if (mutual) {
            policy.setMutualIndexing(true);
        }
        return newIndexerBuilder(List.of(index))
                .setLimit(10)
                .setIndexingPolicy(policy.build());
    }

    private void assertSum(@Nonnull final Index index) {
        final long expectedSum = expectedNumValue2.values().stream().mapToLong(Integer::longValue).sum();
        assertEquals(expectedSum, aggregate(FunctionNames.SUM, index));
    }

    private long aggregate(@Nonnull final String function, @Nonnull final Index index) {
        final IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(function, index.getRootExpression(), index.getName());
        try (FDBRecordContext context = openContext()) {
            final long value = recordStore.evaluateAggregateFunction(List.of("MySimpleRecord"), aggregateFunction,
                    TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0);
            context.commit();
            return value;
        }
    }

    @Nonnull
    private List<TestRecords1Proto.MySimpleRecord> simpleRecords() {
        try (FDBRecordContext context = openContext()) {
            final List<TestRecords1Proto.MySimpleRecord> records = recordStore.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .filter(rec -> rec.getRecordType().getName().equals("MySimpleRecord"))
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .asList().join();
            context.commit();
            return records;
        }
    }

    /** Compare the built indexes with the actual records, regardless of how the test got to its final state. */
    private void assertIndexesMatchRecords(@Nonnull final List<Index> indexes) {
        final List<TestRecords1Proto.MySimpleRecord> records = simpleRecords();
        for (Index index : indexes) {
            if (index.getName().equals(sumIndex.getName())) {
                assertEquals(records.stream().mapToLong(TestRecords1Proto.MySimpleRecord::getNumValue2).sum(),
                        aggregate(FunctionNames.SUM, index), index.getName());
            } else if (index.getName().equals(rankIndex.getName())) {
                assertRanksMatchRecords(index, records);
            } else if (index.getName().equals(countIndex.getName())) {
                assertEquals(records.size(), aggregate(FunctionNames.COUNT, index), index.getName());
            } else {
                try (FDBRecordContext context = openContext()) {
                    assertEquals(records.size(), recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                            .getCount().join(), index.getName());
                    context.commit();
                }
            }
        }
    }

    /** The rank of each record is the number of records with a smaller value. */
    private void assertRanksMatchRecords(@Nonnull final Index index, @Nonnull final List<TestRecords1Proto.MySimpleRecord> records) {
        final IndexRecordFunction<Long> rank = new IndexRecordFunction<>(FunctionNames.RANK, (GroupingKeyExpression)index.getRootExpression(), index.getName());
        try (FDBRecordContext context = openContext()) {
            for (TestRecords1Proto.MySimpleRecord rec : records) {
                final long expectedRank = records.stream().filter(other -> other.getNumValue2() < rec.getNumValue2()).count();
                final FDBStoredRecord<Message> stored = recordStore.loadRecord(Tuple.from(rec.getRecNo()));
                assertEquals(expectedRank, recordStore.evaluateRecordFunction(rank, stored).join(), "rank of " + rec.getRecNo());
            }
            context.commit();
        }
    }

    private boolean isTrackTableEmpty(@Nonnull final Index index) {
        try (FDBRecordContext context = openContext()) {
            final Range range = IndexingSubspaces.indexTrackTableSubspace(recordStore, index).range();
            final boolean empty = context.ensureActive().getRange(range, 1).asList().join().isEmpty();
            context.commit();
            return empty;
        }
    }

    /**
     * Build with the given builder in a background thread, pausing after the first pass. While paused, run
     * {@code whilePaused}.
     */
    private void buildPausingAfterFirstPass(@Nonnull final OnlineIndexer.Builder builder, @Nonnull final Runnable whilePaused) throws InterruptedException {
        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicBoolean passed = new AtomicBoolean(false);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();
        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = builder
                    .setConfigLoader(old -> pauseAfterOnePass(old, passed, startBuildingSemaphore, pauseSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            } catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        indexerThread.start();
        startBuildingSemaphore.acquire();
        startBuildingSemaphore.release();
        try {
            whilePaused.run();
        } finally {
            pauseSemaphore.release();
            indexerThread.join();
        }
        assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());
    }

    // ---- basic flows ----

    @Test
    void testUserWritesWhileIndexerIsPaused() throws Exception {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        buildPausingAfterFirstPass(trackingIndexerBuilder(sumIndex, false), () -> {
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.getIndexState(sumIndex).isWriteOnlyWithTracking());
                context.commit();
            }
            userWrites();
            assertFalse(isTrackTableEmpty(sumIndex), "inserted records should be tracked");
        });

        assertReadable(sumIndex);
        assertSum(sumIndex);
        assertTrue(isTrackTableEmpty(sumIndex), "the track table should be cleared when the index is readable");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUserWritesBeforeIndexerStarts(final boolean mutual) {
        // The indexer continues a build that user io has already been working on. Mutual indexing must behave the same.
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());

        userWrites();
        assertFalse(isTrackTableEmpty(sumIndex));

        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, mutual).build()) {
            indexer.buildIndex(true);
        }
        assertReadable(sumIndex);
        assertSum(sumIndex);
        assertTrue(isTrackTableEmpty(sumIndex));
    }

    @Test
    void testConcurrentInsertsDoNotConflictWithIndexer() throws Exception {
        // The indexer scans with snapshot resolution: inserts into (or after) the scanned range do not abort it
        populateEvenRecords(100);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final Map<Integer, Integer> inserted = new ConcurrentHashMap<>();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final Thread writerThread = new Thread(() -> {
            try {
                for (int recNo = 1; recNo < 240; recNo += 2) {
                    final int insertRecNo = recNo;
                    // Retry: the writer may conflict with the indexer's store state changes. The indexer is under test, not the writer.
                    io(store -> saveSimple(store, insertRecNo, insertRecNo * 19));
                    inserted.put(recNo, recNo * 19);
                    Thread.sleep(10);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            }
        });
        final FDBStoreTimer timer = new FDBStoreTimer();
        writerThread.start();
        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false)
                .setTimer(timer)
                .setLimit(5)
                .build()) {
            indexer.buildIndex(true);
        }
        writerThread.join();
        assertNull(writerFailure.get(), () -> "the writer failed: " + writerFailure.get());

        assertEquals(0, timer.getCount(FDBStoreTimer.Events.COMMIT_FAILURE), "the indexer should not have conflicted");
        expectedNumValue2.putAll(inserted);
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    @Test
    void testRestartClearsTrackTable() {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());
        userWrites();
        assertFalse(isTrackTableEmpty(sumIndex));

        // Restarting the build clears the index, its range set and the track table: records are indexed from scratch
        inTransaction(() -> recordStore.clearAndMarkIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());
        assertTrue(isTrackTableEmpty(sumIndex));

        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).build()) {
            indexer.buildIndex(true);
        }
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    @Test
    void testIdempotentIndexFallsBackToWriteOnly() throws Exception {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(valueIndex)));
        disableAll(List.of(valueIndex));

        final AtomicBoolean checked = new AtomicBoolean(false);
        buildPausingAfterFirstPass(trackingIndexerBuilder(valueIndex, false), () -> {
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.getIndexState(valueIndex).isWriteOnlyNoQueue());
                checked.set(true);
                context.commit();
            }
        });
        assertTrue(checked.get());
        assertReadable(valueIndex);
    }

    @Test
    void testOtherTypesAreNotTracked() {
        // A sub-index of a shared store: only inserts of the indexed type are tracked
        populateEvenRecords(10);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());

        inTransaction(() -> {
            for (int recNo = 1; recNo < 20; recNo += 2) {
                saveOther(recordStore, recNo);
            }
        });
        assertTrue(isTrackTableEmpty(sumIndex), "records of other types should not be tracked");
        inTransaction(() -> save(21, 21 * 19));
        assertFalse(isTrackTableEmpty(sumIndex));
    }

    private boolean isTracked(@Nonnull final Index index, final int recNo) {
        try (FDBRecordContext context = openContext()) {
            final boolean tracked = IndexingTrackTable.of(recordStore, index).isTrackedSnapshotAsync(Tuple.from(recNo)).join();
            context.commit();
            return tracked;
        }
    }

    /** The value of an ungrouped SUM index, which is not readable yet - read as stored. */
    private long rawSum(@Nonnull final Index index) {
        try (FDBRecordContext context = openContext()) {
            final byte[] value = context.ensureActive().get(recordStore.indexSubspace(index).getKey()).join();
            context.commit();
            return value == null ? 0 : ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
    }

    @Test
    void testUpdateOfRecordThatIsNotIndexedYetIndexesAndTracksIt() {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());
        assertEquals(0, rawSum(sumIndex));

        // not indexed yet: the update indexes the new value (only), and tracks the record
        inTransaction(() -> save(4, 111));
        assertTrue(isTracked(sumIndex, 4));
        assertEquals(111, rawSum(sumIndex));
        // tracked: the update replaces the value in the index
        inTransaction(() -> save(4, 222));
        assertEquals(222, rawSum(sumIndex));
        // a delete of a record that is not indexed yet does not index nor track it
        inTransaction(() -> delete(6));
        assertFalse(isTracked(sumIndex, 6));
        assertEquals(222, rawSum(sumIndex));
        // a delete of a tracked record removes it from the index
        inTransaction(() -> delete(4));
        assertEquals(0, rawSum(sumIndex));

        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).build()) {
            indexer.buildIndex(true);
        }
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    @Test
    void testRecordThatIsUpdatedAgainAndAgainDoesNotStarveTheIndexer() {
        // A record is updated in the middle of every attempt of the indexer. The first attempt conflicts: it has read the
        // record and indexes it. After that, the record is tracked, and the indexer skips it without any conflict.
        // (Only when building by records: the records that are loaded through index entries are always read with a conflict.)
        populateEvenRecords(60);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final AtomicInteger indexUpdates = new AtomicInteger();
        final AtomicInteger updates = new AtomicInteger();
        final AtomicBoolean inIo = new AtomicBoolean(false);
        final AtomicReference<Throwable> ioFailure = new AtomicReference<>();
        // the first update of record 0 in each attempt (limit is 5), the record 4 is processed later in the same attempt
        setIndexMaintenanceFilter((index, rec) -> {
            if (index.getName().equals(sumIndex.getName()) && !inIo.get() && indexUpdates.incrementAndGet() % 5 == 1 && updates.get() < 10) {
                inIo.set(true);
                final int value = 1000 + updates.incrementAndGet();
                final Thread ioThread = new Thread(() -> {
                    try {
                        io(store -> saveSimple(store, 4, value));
                        expectedNumValue2.put(4, value);
                    } catch (Throwable t) {
                        ioFailure.set(t);
                    }
                });
                ioThread.start();
                try {
                    ioThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inIo.set(false);
                }
            }
            return IndexMaintenanceFilter.IndexValues.ALL;
        });

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).setTimer(timer).setLimit(5).build()) {
            indexer.buildIndex(true);
        }
        assertNull(ioFailure.get(), () -> "the concurrent io failed: " + ioFailure.get());
        setIndexMaintenanceFilter(null);

        assertTrue(updates.get() >= 3, "the record should have been updated in several attempts: " + updates.get());
        assertEquals(1, timer.getCount(FDBStoreTimer.Events.COMMIT_FAILURE), "only the first attempt should conflict");
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"WRITE_ONLY", "WRITE_ONLY_WITH_QUEUE", "WRITE_ONLY_WITH_TRACKING"})
    void testClearAndMarkIndexWithAWriteOnlyState(final IndexState writeOnlyState) {
        populateEvenRecords(10);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnly(sumIndex, IndexState.WRITE_ONLY_WITH_TRACKING).join());
        inTransaction(() -> save(1, 19));
        assertFalse(isTrackTableEmpty(sumIndex));

        inTransaction(() -> recordStore.clearAndMarkIndexWriteOnly(sumIndex, writeOnlyState).join());
        inTransaction(() -> assertEquals(writeOnlyState, recordStore.getIndexState(sumIndex)));
        assertTrue(isTrackTableEmpty(sumIndex), "the index data should have been cleared");
        assertEquals(0, rawSum(sumIndex));
    }

    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"WRITE_ONLY", "WRITE_ONLY_WITH_QUEUE", "WRITE_ONLY_WITH_TRACKING"})
    void testMarkIndexWithAWriteOnlyState(final IndexState writeOnlyState) {
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> assertTrue(recordStore.markIndexWriteOnly(sumIndex, writeOnlyState).join()));
        inTransaction(() -> assertEquals(writeOnlyState, recordStore.getIndexState(sumIndex)));
        // by name, and no change
        inTransaction(() -> assertFalse(recordStore.markIndexWriteOnly(sumIndex.getName(), writeOnlyState).join()));
    }

    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"READABLE", "READABLE_UNIQUE_PENDING", "DISABLED"})
    void testMarkAndClearAndMarkIndexRequireAWriteOnlyState(final IndexState state) {
        populateEvenRecords(10);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> {
            assertThrows(RecordCoreArgumentException.class, () -> recordStore.markIndexWriteOnly(sumIndex, state));
            assertThrows(RecordCoreArgumentException.class, () -> recordStore.markIndexWriteOnly(sumIndex.getName(), state));
            assertThrows(RecordCoreArgumentException.class, () -> recordStore.clearAndMarkIndexWriteOnly(sumIndex, state));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 100})
    void testTrackTablePrefetchFollowsThePipelineSizer(final int pipelineSize) {
        populateEvenRecords(60);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final Set<PipelineOperation> requestedOperations = ConcurrentHashMap.newKeySet();
        final FDBRecordStore.Builder storeBuilder = createStoreBuilder().setPipelineSizer(operation -> {
            requestedOperations.add(operation);
            if (operation == PipelineOperation.TRACK_TABLE_READ) {
                return pipelineSize;
            }
            return FDBRecordStore.DEFAULT_PIPELINE_SIZER.getPipelineSize(operation);
        });
        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).setRecordStoreBuilder(storeBuilder).build()) {
            indexer.buildIndex(true);
        }
        assertTrue(requestedOperations.contains(PipelineOperation.TRACK_TABLE_READ), "the pipeline sizer should size the track table reads");
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    // ---- races between the indexer's read and its commit ----

    private enum Race {
        INSERT_IN_BATCH(false, store -> saveSimple(store, 7, 7 * 19)),
        INSERT_AFTER_BATCH(false, store -> saveSimple(store, 101, 101 * 19)),
        INSERT_OTHER_TYPE_IN_BATCH(false, store -> saveOther(store, 9)),
        UPDATE_IN_BATCH(true, store -> saveSimple(store, 8, 1234)),
        DELETE_IN_BATCH(true, store -> store.deleteRecord(Tuple.from(8))),
        UPDATE_AFTER_BATCH(false, store -> saveSimple(store, 100, 4321)),
        DELETE_AFTER_BATCH(false, store -> store.deleteRecord(Tuple.from(102)));

        private final boolean conflictsWithIndexer;
        private final Consumer<FDBRecordStore> action;

        Race(final boolean conflictsWithIndexer, final Consumer<FDBRecordStore> action) {
            this.conflictsWithIndexer = conflictsWithIndexer;
            this.action = action;
        }
    }

    /**
     * Build the targets, committing {@code race} in the middle of the indexer's first transaction - after it has
     * started scanning and before it commits. The first batch covers records 0-8, with 10 as its continuation.
     * @return the number of indexer commits that failed
     */
    private int runRace(@Nonnull final List<Index> targets, @Nonnull final List<Index> trackingTargets, @Nonnull final Race race) {
        return runRace(targets, trackingTargets, race.action, null);
    }

    private int runRace(@Nonnull final List<Index> targets, @Nonnull final List<Index> trackingTargets,
                        @Nonnull final Consumer<FDBRecordStore> raceAction, @Nullable final String sourceIndex) {
        populateEvenRecords(100);
        openSimpleMetaData(allIndexesHook(targets));
        disableAll(targets);

        // The index maintenance filter is called while the indexer updates the index, i.e. mid-transaction
        final AtomicBoolean armed = new AtomicBoolean(true);
        final AtomicReference<Throwable> ioFailure = new AtomicReference<>();
        setIndexMaintenanceFilter((index, rec) -> {
            if (index.getName().equals(sumIndex.getName()) && armed.compareAndSet(true, false)) {
                final Thread ioThread = new Thread(() -> {
                    try {
                        io(raceAction);
                    } catch (Throwable t) {
                        ioFailure.set(t);
                    }
                });
                ioThread.start();
                try {
                    ioThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return IndexMaintenanceFilter.IndexValues.ALL;
        });

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexer indexer = newIndexerBuilder(targets, timer)
                .setLimit(5)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().setUseWriteOnlyWithTracking(trackingTargets).setSourceIndex(sourceIndex).build())
                .build()) {
            indexer.buildIndex(true);
        }
        assertFalse(armed.get(), "the race was never triggered");
        assertNull(ioFailure.get(), () -> "the concurrent io failed: " + ioFailure.get());

        setIndexMaintenanceFilter(null);
        assertReadable(targets);
        assertIndexesMatchRecords(targets);
        return timer.getCount(FDBStoreTimer.Events.COMMIT_FAILURE);
    }

    @ParameterizedTest
    @EnumSource(Race.class)
    void testRaceBetweenIndexerReadAndCommit(final Race race) {
        final int failures = runRace(List.of(sumIndex), List.of(sumIndex), race);
        if (race.conflictsWithIndexer) {
            // An update or delete of a record the indexer processed: it must fail and retry, or the index would be wrong
            assertTrue(failures > 0, "the indexer should have conflicted with " + race);
        } else {
            assertEquals(0, failures, "the indexer should not have conflicted with " + race);
        }
    }

    @Test
    void testSourceKeyMovesIntoRangeBeingCommitted() {
        // Building by source index: while the indexer builds the first batch (the source keys of records 0-8), user io moves
        // the source key of record 100 into that batch. It is not indexed yet, so user io leaves it to the indexer, which
        // neither sees it (it was elsewhere when the indexer read) nor conflicts (no range conflict) - but the range is
        // then marked as built.
        runRace(List.of(sumIndex), List.of(sumIndex),
                store -> saveSimple(store, 100, 1000, 3 * 1139 + 5), UNIQUE_SOURCE_INDEX);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStaleIoTransactionCommitsAfterIndexer(final boolean delete) {
        // A user transaction treats record 4 as not indexed yet (it is neither tracked nor in a built range), and only commits
        // after the indexer has committed the batch that indexed record 4. Without the indexer's write conflict on the record's
        // entry in the track table, that user transaction would commit, and the record would be in the index twice (update),
        // or stay in the index (delete).
        populateEvenRecords(100);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final AtomicInteger indexUpdates = new AtomicInteger(0);
        final AtomicBoolean inUserTransaction = new AtomicBoolean(false);
        final AtomicBoolean firstAttempt = new AtomicBoolean(true);
        final CountDownLatch ioHasRead = new CountDownLatch(1);
        final CountDownLatch releaseIo = new CountDownLatch(1);
        final AtomicReference<Throwable> ioFailure = new AtomicReference<>();
        final Thread ioThread = new Thread(() -> {
            try {
                io(store -> {
                    inUserTransaction.set(true);
                    if (delete) {
                        store.deleteRecord(Tuple.from(4));
                    } else {
                        saveSimple(store, 4, 9999);
                    }
                    inUserTransaction.set(false);
                    if (firstAttempt.compareAndSet(true, false)) {
                        ioHasRead.countDown();
                        try {
                            releaseIo.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            } catch (Throwable t) {
                ioFailure.set(t);
            }
        });
        // The first indexer transaction updates the index for 5 records (limit); the 6th update is in the second transaction
        setIndexMaintenanceFilter((index, rec) -> {
            // Only the indexer's updates are counted - not those of the user transaction (an update indexes the record)
            if (index.getName().equals(sumIndex.getName()) && !inUserTransaction.get()) {
                try {
                    final int count = indexUpdates.incrementAndGet();
                    if (count == 1) {
                        ioThread.start();
                        ioHasRead.await();
                    } else if (count == 6) {
                        releaseIo.countDown();
                        ioThread.join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return IndexMaintenanceFilter.IndexValues.ALL;
        });
        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).setLimit(5).build()) {
            indexer.buildIndex(true);
        }
        assertNull(ioFailure.get(), () -> "the concurrent io failed: " + ioFailure.get());
        assertFalse(firstAttempt.get());
        assertTrue(indexUpdates.get() >= 6);

        setIndexMaintenanceFilter(null);
        assertReadable(sumIndex);
        assertIndexesMatchRecords(List.of(sumIndex));
    }

    @Test
    void testReaderOfIndexedRecordDoesNotConflictWithIndexer() {
        // A user transaction reads record 4 and writes something unrelated to the index. The indexer indexes record 4, and
        // commits, before the user transaction does. The user transaction has no business with the index, it must not fail.
        populateEvenRecords(100);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final AtomicInteger indexUpdates = new AtomicInteger();
        final AtomicInteger userAttempts = new AtomicInteger();
        final AtomicBoolean firstAttempt = new AtomicBoolean(true);
        final CountDownLatch userHasRead = new CountDownLatch(1);
        final CountDownLatch releaseUser = new CountDownLatch(1);
        final AtomicReference<Throwable> userFailure = new AtomicReference<>();
        final Thread userThread = new Thread(() -> {
            try {
                io(store -> {
                    userAttempts.incrementAndGet();
                    assertNotNull(store.loadRecord(Tuple.from(4)));
                    saveOther(store, 1001);
                    if (firstAttempt.compareAndSet(true, false)) {
                        userHasRead.countDown();
                        try {
                            releaseUser.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            } catch (Throwable t) {
                userFailure.set(t);
            }
        });
        // The first indexer transaction updates the index for 5 records (limit); the 6th update is in the second transaction
        setIndexMaintenanceFilter((index, rec) -> {
            if (index.getName().equals(sumIndex.getName())) {
                try {
                    final int count = indexUpdates.incrementAndGet();
                    if (count == 1) {
                        userThread.start();
                        userHasRead.await();
                    } else if (count == 6) {
                        releaseUser.countDown();
                        userThread.join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return IndexMaintenanceFilter.IndexValues.ALL;
        });
        try (OnlineIndexer indexer = trackingIndexerBuilder(sumIndex, false).setLimit(5).build()) {
            indexer.buildIndex(true);
        }
        setIndexMaintenanceFilter(null);
        assertNull(userFailure.get(), () -> "the user transaction failed: " + userFailure.get());
        assertTrue(indexUpdates.get() >= 6);
        assertEquals(1, userAttempts.get(), "the user transaction should not have conflicted with the indexer");

        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    @Test
    void testSnapshotResolutionRequiresAllTargets() {
        // One non-idempotent target that is not write-only-with-tracking: the scan remains serializable, an insert in the range conflicts
        assertTrue(runRace(List.of(sumIndex, countIndex), List.of(sumIndex), Race.INSERT_IN_BATCH) > 0);
    }

    @Test
    void testSnapshotResolutionWithMixedTargets() {
        // an idempotent target (that falls back to plain write-only) does not prevent snapshot resolution
        assertEquals(0, runRace(List.of(sumIndex, valueIndex), List.of(sumIndex, valueIndex), Race.INSERT_IN_BATCH));
    }

    @Test
    void testSnapshotResolutionMultiTarget() {
        assertEquals(0, runRace(List.of(sumIndex, countIndex), List.of(sumIndex, countIndex), Race.INSERT_IN_BATCH));
    }

    @ParameterizedTest
    @CsvSource({"sum,value", "sum,count", "count,sum"})
    void testMultiTargetWithMixedWriteOnlyStatesCanContinue(final String trackingTarget, final String otherTarget) {
        // The targets of a build end up in different write-only states: the idempotent one falls back to plain write-only,
        // or only some of the targets requested tracking. The write-only variants are the same state for continuing a build.
        final Map<String, Index> indexes = Map.of("sum", sumIndex, "count", countIndex, "value", valueIndex);
        populateEvenRecords(60);
        final List<Index> targets = List.of(indexes.get(trackingTarget), indexes.get(otherTarget));
        openSimpleMetaData(allIndexesHook(targets));
        disableAll(targets);
        // the first target requests idem; the second doesn't, or it is idempotent and falls back
        final OnlineIndexer.IndexingPolicy policy = OnlineIndexer.IndexingPolicy.newBuilder()
                .setUseWriteOnlyWithTracking(otherTarget.equals("value") ? targets : List.of(targets.get(0))).build();

        final AtomicBoolean interrupted = new AtomicBoolean(false);
        try (OnlineIndexer indexer = newIndexerBuilder(targets).setLimit(5).setIndexingPolicy(policy)
                .setConfigLoader(old -> {
                    if (interrupted.get()) {
                        throw new IllegalStateException("interrupted indexer");
                    }
                    interrupted.set(true);
                    return old;
                }).build()) {
            assertThrows(Exception.class, () -> indexer.buildIndex(true));
        }
        inTransaction(() -> {
            assertTrue(recordStore.getIndexState(targets.get(0)).isWriteOnlyWithTracking());
            assertFalse(recordStore.getIndexState(targets.get(1)).isWriteOnlyWithTracking());
        });

        try (OnlineIndexer indexer = newIndexerBuilder(targets).setLimit(5).setIndexingPolicy(policy).build()) {
            indexer.buildIndex(true);
        }
        assertReadable(targets);
        assertIndexesMatchRecords(targets);
    }

    @Test
    void testOnlyIdempotentTargetsAreNotBuiltWithIdem() throws Exception {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(valueIndex)));
        disableAll(List.of(valueIndex));
        buildPausingAfterFirstPass(trackingIndexerBuilder(valueIndex, false), () ->
                inTransaction(() -> assertTrue(recordStore.getIndexState(valueIndex).isWriteOnlyNoQueue())));
        assertReadable(valueIndex);
    }

    @Test
    void testFallsBackToWriteOnlyBelowFormatVersion() throws Exception {
        formatVersion = FormatVersionTestUtils.previous(FormatVersion.WRITE_ONLY_WITH_TRACKING);
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        buildPausingAfterFirstPass(trackingIndexerBuilder(sumIndex, false), () -> {
            inTransaction(() -> {
                assertFalse(recordStore.getIndexState(sumIndex).isWriteOnlyWithTracking(), "below the required format version the index must not be write-only-with-tracking");
                assertTrue(recordStore.getIndexState(sumIndex).isWriteOnlyNoQueue(), "the index should fall back to plain write-only");
            });
            userWrites();
            assertTrue(isTrackTableEmpty(sumIndex), "nothing is tracked in plain write-only");
        });
        assertTrue(formatVersion.compareTo(FormatVersion.WRITE_ONLY_WITH_TRACKING) < 0);
        assertReadable(sumIndex);
        assertSum(sumIndex);
    }

    // ---- sequences in a single transaction ----

    private void sequences(final int base) {
        // insert, update, delete
        inTransaction(() -> {
            save(base + 1, 100);
            save(base + 1, 200);
            delete(base + 1);
        });
        // insert, update
        inTransaction(() -> {
            save(base + 3, 300);
            save(base + 3, 400);
        });
        // delete, re-insert
        inTransaction(() -> {
            delete(base + 4);
            save(base + 4, 500);
        });
        // update, delete, re-insert
        inTransaction(() -> {
            save(base + 6, 600);
            delete(base + 6);
            save(base + 6, 700);
        });
        // type change: simple -> other (permanent), and simple -> other -> simple
        inTransaction(() -> {
            saveOther(recordStore, base + 10);
            expectedNumValue2.remove(base + 10);
        });
        inTransaction(() -> saveOther(recordStore, base + 8));
        inTransaction(() -> save(base + 8, 800));
        // other -> simple
        inTransaction(() -> saveOther(recordStore, base + 12));
        inTransaction(() -> save(base + 12, 900));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSequencesInSingleTransaction(final boolean afterFirstPass) throws Exception {
        populateEvenRecords(100);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex, countIndex)));
        disableAll(List.of(sumIndex, countIndex));
        final List<Index> targets = List.of(sumIndex, countIndex);
        final OnlineIndexer.Builder builder = newIndexerBuilder(targets)
                .setLimit(5)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().setUseWriteOnlyWithTracking(targets).build());
        final Runnable allSequences = () -> {
            sequences(2);     // within the first pass
            sequences(150);   // not indexed yet
        };
        if (afterFirstPass) {
            buildPausingAfterFirstPass(builder, allSequences);
        } else {
            inTransaction(() -> targets.forEach(index -> recordStore.markIndexWriteOnly(index, IndexState.WRITE_ONLY_WITH_TRACKING).join()));
            allSequences.run();
            try (OnlineIndexer indexer = builder.build()) {
                indexer.buildIndex(true);
            }
        }
        assertReadable(targets);
        assertIndexesMatchRecords(targets);
    }

    // ---- interleaving fuzzer ----

    private enum FuzzMode { BY_RECORDS, MUTUAL, BY_INDEX, RANK_BY_RECORDS, RANK_BY_INDEX, REVERSE_BY_RECORDS, REVERSE_BY_INDEX, WITH_IDEMPOTENT_TARGET }

    static Stream<Arguments> fuzzParams() {
        final Stream.Builder<Arguments> builder = java.util.stream.Stream.builder();
        for (FuzzMode mode : FuzzMode.values()) {
            // A few seeds keep the test fast. Set the FUZZ_SEEDS environment variable to run more of them, when hunting for bugs.
            for (long seed = 1; seed <= Long.parseLong(System.getenv().getOrDefault("FUZZ_SEEDS", "5")); seed++) {
                builder.add(Arguments.of(mode, seed));
            }
        }
        return builder.build();
    }

    /** The source index key of a record: its own (base) one, or one next to the base key of record {@code slot}. Unique per record. */
    private static int fuzzUniqueValue(final int recNo, final int slot) {
        return slot < 0 ? recNo * 1139 : slot * 1139 + recNo + 1;
    }

    /**
     * One random user transaction, over a random record: insert, update, delete, re-insert, type change or source key move.
     * @param progressHint roughly the number of the record that the indexer is working on. Source keys are moved to
     * around this position half of the time, into the batch that the indexer is committing.
     */
    private void randomIo(@Nonnull final Random random, final int progressHint, final boolean reverse) {
        final int recNo = random.nextInt(130);
        final int value = random.nextInt(40); // few values: many duplicates
        // The records are numbered by 2, up to 118: the record number that the indexer is working on
        final int position = reverse ? 118 - 2 * progressHint : 2 * progressHint;
        final int slot = random.nextBoolean() ? -1 : Math.max(0, position + random.nextInt(13) - 6);
        final int operation = random.nextInt(5);
        final boolean insertOther = random.nextInt(4) == 0;
        io(store -> {
            final boolean exists = store.loadRecord(Tuple.from(recNo)) != null;
            if (!exists) {
                if (insertOther) {
                    saveOther(store, recNo);
                } else {
                    saveSimple(store, recNo, value, fuzzUniqueValue(recNo, slot));
                }
                return;
            }
            switch (operation) {
                case 0:
                case 4:
                    saveSimple(store, recNo, value, fuzzUniqueValue(recNo, slot));
                    break;
                case 1:
                    store.deleteRecord(Tuple.from(recNo));
                    break;
                case 2:
                    store.deleteRecord(Tuple.from(recNo));
                    saveSimple(store, recNo, value, fuzzUniqueValue(recNo, slot));
                    break;
                default:
                    saveOther(store, recNo);
                    break;
            }
        });
    }

    /**
     * A user transaction is committed in the middle of every third index update of the indexer: that is, inside the
     * indexer's transactions, after they have read their records and before they commit.
     */
    @ParameterizedTest
    @MethodSource("fuzzParams")
    void testRandomIoInTheMiddleOfIndexerTransactions(final FuzzMode mode, final long seed) {
        populateEvenRecords(60);
        final boolean byIndex = mode == FuzzMode.BY_INDEX || mode == FuzzMode.RANK_BY_INDEX || mode == FuzzMode.REVERSE_BY_INDEX;
        final boolean rank = mode == FuzzMode.RANK_BY_RECORDS || mode == FuzzMode.RANK_BY_INDEX;
        final boolean reverse = mode == FuzzMode.REVERSE_BY_RECORDS || mode == FuzzMode.REVERSE_BY_INDEX;
        final List<Index> targets = rank ? List.of(rankIndex) :
                                    byIndex ? List.of(sumIndex) :
                                    mode == FuzzMode.WITH_IDEMPOTENT_TARGET ? List.of(sumIndex, valueIndex) :
                                    List.of(sumIndex, countIndex);
        openSimpleMetaData(allIndexesHook(targets));
        disableAll(targets);

        final Random random = new Random(seed);
        final AtomicInteger indexUpdates = new AtomicInteger();
        final AtomicInteger ioTransactions = new AtomicInteger();
        final AtomicBoolean inIo = new AtomicBoolean(false);
        final AtomicReference<Throwable> ioFailure = new AtomicReference<>();
        setIndexMaintenanceFilter((index, rec) -> {
            final int updatesSoFar = indexUpdates.incrementAndGet();
            if (index.getName().equals(targets.get(0).getName()) && !inIo.get() && updatesSoFar % 3 == 0) {
                inIo.set(true);
                final Thread ioThread = new Thread(() -> {
                    try {
                        randomIo(random, updatesSoFar, reverse);
                        ioTransactions.incrementAndGet();
                    } catch (Throwable t) {
                        ioFailure.set(t);
                    }
                });
                ioThread.start();
                try {
                    ioThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inIo.set(false);
                }
            }
            return IndexMaintenanceFilter.IndexValues.ALL;
        });

        final OnlineIndexer.IndexingPolicy.Builder policy = OnlineIndexer.IndexingPolicy.newBuilder().setUseWriteOnlyWithTracking(targets);
        if (reverse) {
            policy.setReverseScanOrder(true);
        }
        if (byIndex) {
            policy.setSourceIndex(UNIQUE_SOURCE_INDEX);
        } else if (mode == FuzzMode.MUTUAL) {
            policy.setMutualIndexing(true);
        }
        try (OnlineIndexer indexer = newIndexerBuilder(targets).setLimit(5).setIndexingPolicy(policy.build()).build()) {
            indexer.buildIndex(true);
        }
        assertNull(ioFailure.get(), () -> "the concurrent io failed: " + ioFailure.get());
        assertTrue(ioTransactions.get() > 5, "the user transactions should have run in the middle of the build");

        setIndexMaintenanceFilter(null);
        assertReadable(targets);
        assertIndexesMatchRecords(targets);
    }

    // ---- randomized concurrent workload ----

    private static int uniqueValue(final int recNo, final boolean mirrored) {
        // base and mirrored values never collide (mirrored are 7 modulo 1139) - the mirrored moves the record across the source index
        return mirrored ? (400 - recNo) * 1139 + 7 : recNo * 1139;
    }

    private void randomWorkload(final int threadId, final long seed, final Set<Integer> live) {
        final Random random = new Random(seed * 31 + threadId);
        final Set<Integer> others = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            final int recNo = (random.nextInt(60) * 4) + (threadId * 2) + (random.nextBoolean() ? 0 : 1); // thread-owned
            final int value = random.nextInt(10_000);
            final boolean mirrored = random.nextBoolean();
            if (others.contains(recNo)) {
                // other -> simple
                io(store -> saveSimple(store, recNo, value, uniqueValue(recNo, mirrored)));
                others.remove(recNo);
                live.add(recNo);
            } else if (live.contains(recNo)) {
                switch (random.nextInt(4)) {
                    case 0:
                        io(store -> saveSimple(store, recNo, value, uniqueValue(recNo, mirrored)));
                        break;
                    case 1:
                        io(store -> store.deleteRecord(Tuple.from(recNo)));
                        live.remove(recNo);
                        break;
                    case 2:
                        io(store -> {
                            store.deleteRecord(Tuple.from(recNo));
                            saveSimple(store, recNo, value, uniqueValue(recNo, mirrored));
                        });
                        break;
                    default:
                        io(store -> saveOther(store, recNo));
                        live.remove(recNo);
                        others.add(recNo);
                        break;
                }
            } else {
                io(store -> saveSimple(store, recNo, value, uniqueValue(recNo, mirrored)));
                live.add(recNo);
            }
            try {
                Thread.sleep(random.nextInt(15));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"1,false", "2,false", "3,false", "4,true", "5,true", "6,true"})
    void testRandomConcurrentWorkload(final long seed, final boolean byIndex) throws Exception {
        // Initial records are numbered by 2: thread 0 owns recNo%4 == 0 or 1, thread 1 owns 2 or 3
        populateEvenRecords(120);
        final List<Index> targets = byIndex ? List.of(sumIndex) : List.of(sumIndex, countIndex);
        openSimpleMetaData(allIndexesHook(targets));
        disableAll(targets);

        final OnlineIndexer.IndexingPolicy.Builder policy = OnlineIndexer.IndexingPolicy.newBuilder().setUseWriteOnlyWithTracking(targets);
        if (byIndex) {
            policy.setSourceIndex(UNIQUE_SOURCE_INDEX);
        }
        final List<Thread> writers = new ArrayList<>();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        for (int threadId = 0; threadId < 2; threadId++) {
            final int id = threadId;
            final Set<Integer> live = new HashSet<>();
            for (int recNo = 0; recNo < 240; recNo += 2) {
                if (recNo % 4 == id * 2) {
                    live.add(recNo);
                }
            }
            final Thread writer = new Thread(() -> {
                try {
                    randomWorkload(id, seed, live);
                } catch (Throwable t) {
                    writerFailure.set(t);
                }
            });
            writers.add(writer);
            writer.start();
        }
        try (OnlineIndexer indexer = newIndexerBuilder(targets).setLimit(5).setIndexingPolicy(policy.build()).build()) {
            indexer.buildIndex(true);
        }
        for (Thread writer : writers) {
            writer.join();
        }
        assertNull(writerFailure.get(), () -> "a writer failed: " + writerFailure.get());

        assertReadable(targets);
        assertIndexesMatchRecords(targets);
    }
}
