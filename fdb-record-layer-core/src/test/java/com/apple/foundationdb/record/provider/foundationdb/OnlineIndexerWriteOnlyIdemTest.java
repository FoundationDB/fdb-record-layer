/*
 * OnlineIndexerWriteOnlyIdemTest.java
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
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building a non-idempotent index in the {@link com.apple.foundationdb.record.IndexState#WRITE_ONLY_IDEM} state.
 */
class OnlineIndexerWriteOnlyIdemTest extends OnlineIndexerTest {

    // A SUM index is not idempotent: indexing (or un-indexing) a record twice would corrupt the sum
    private final Index sumIndex = new Index("simple$sum_idem", field("num_value_2").ungrouped(), IndexTypes.SUM);
    private final Index valueIndex = new Index("simple$value_idem", field("num_value_2"), IndexTypes.VALUE);

    private final Map<Integer, Integer> expectedNumValue2 = new TreeMap<>();

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
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .setNumValueUnique(recNo * 1139)
                .build());
        expectedNumValue2.put(recNo, numValue2);
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

    private OnlineIndexer.Builder idemIndexerBuilder(@Nonnull final Index index, final boolean mutual) {
        final OnlineIndexer.IndexingPolicy.Builder policy = OnlineIndexer.IndexingPolicy.newBuilder()
                .setUseWriteOnlyIdem(List.of(index));
        if (mutual) {
            policy.setMutualIndexing(true);
        }
        return newIndexerBuilder(List.of(index))
                .setLimit(10)
                .setIndexingPolicy(policy.build());
    }

    private void assertSum(@Nonnull final Index index) {
        final long expectedSum = expectedNumValue2.values().stream().mapToLong(Integer::longValue).sum();
        final IndexAggregateFunction sumFunction =
                new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        try (FDBRecordContext context = openContext()) {
            assertEquals(expectedSum, recordStore.evaluateAggregateFunction(List.of("MySimpleRecord"), sumFunction,
                    TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0));
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

    @Test
    void testUserWritesWhileIndexerIsPaused() throws Exception {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));

        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicBoolean passed = new AtomicBoolean(false);
        final AtomicReference<Throwable> buildFailure = new AtomicReference<>();
        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = idemIndexerBuilder(sumIndex, false)
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

        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.getIndexState(sumIndex).isWriteOnlyIdem());
            context.commit();
        }
        userWrites();
        assertFalse(isTrackTableEmpty(sumIndex), "inserted records should be tracked");

        pauseSemaphore.release();
        indexerThread.join();
        assertNull(buildFailure.get(), () -> "the build failed: " + buildFailure.get());

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
        inTransaction(() -> recordStore.markIndexWriteOnlyIdem(sumIndex).join());

        userWrites();
        assertFalse(isTrackTableEmpty(sumIndex));

        try (OnlineIndexer indexer = idemIndexerBuilder(sumIndex, mutual).build()) {
            indexer.buildIndex(true);
        }
        assertReadable(sumIndex);
        assertSum(sumIndex);
        assertTrue(isTrackTableEmpty(sumIndex));
    }

    @Test
    void testRestartClearsTrackTable() {
        populateEvenRecords(21);
        openSimpleMetaData(allIndexesHook(List.of(sumIndex)));
        disableAll(List.of(sumIndex));
        inTransaction(() -> recordStore.markIndexWriteOnlyIdem(sumIndex).join());
        userWrites();
        assertFalse(isTrackTableEmpty(sumIndex));

        // Restarting the build clears the index, its range set and the track table: records are indexed from scratch
        inTransaction(() -> recordStore.clearAndMarkIndexWriteOnlyIdem(sumIndex).join());
        assertTrue(isTrackTableEmpty(sumIndex));

        try (OnlineIndexer indexer = idemIndexerBuilder(sumIndex, false).build()) {
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
        final Semaphore pauseSemaphore = new Semaphore(1);
        final Semaphore startBuildingSemaphore = new Semaphore(1);
        pauseSemaphore.acquire();
        startBuildingSemaphore.acquire();
        final AtomicBoolean passed = new AtomicBoolean(false);
        final Thread indexerThread = new Thread(() -> {
            try (OnlineIndexer indexer = idemIndexerBuilder(valueIndex, false)
                    .setConfigLoader(old -> pauseAfterOnePass(old, passed, startBuildingSemaphore, pauseSemaphore))
                    .build()) {
                indexer.buildIndex(true);
            }
        });
        indexerThread.start();
        startBuildingSemaphore.acquire();
        startBuildingSemaphore.release();
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.getIndexState(valueIndex).isWriteOnlyNoQueue());
            checked.set(true);
            context.commit();
        }
        pauseSemaphore.release();
        indexerThread.join();
        assertTrue(checked.get());
        assertReadable(valueIndex);
    }
}
