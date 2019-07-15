/*
 * FDBDatabaseTest.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBDatabase}.
 */
@Tag(Tags.RequiresFDB)
public class FDBDatabaseTest extends FDBTestBase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseTest.class);

    @Test
    public void cachedVersionMaintenanceOnReadsTest() throws Exception {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setTrackLastSeenVersion(true);
        FDBDatabase database = factory.getDatabase();
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        // For the purpose of this test, commits will not change the cached read version
        database.setTrackLastSeenVersionOnCommit(false);
        assertFalse(database.isTrackLastSeenVersionOnCommit());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // First time, does a GRV from FDB
        long readVersion1 = getReadVersion(database, 0L, 2000L);

        // Store a record (advances future GRV, but not cached version)
        testStoreAndRetrieveSimpleRecord(database, metaData);

        // We're fine with any version obtained up to 2s ago, so will get readVersion
        assertEquals(readVersion1, getReadVersion(database, 0L, 2000L));

        Thread.sleep(10L);

        // Set a short staleness bound, this will cause a GRV call to FDB and will update the cached version
        long readVersion2 = getReadVersion(database, 0L, 11L);
        assertTrue(readVersion1 < readVersion2);

        // Store another record
        testStoreAndRetrieveSimpleRecord(database, metaData);

        assertEquals(readVersion2, getReadVersion(database, 0L, 2000L));
        assertEquals(readVersion2, getReadVersion(database, readVersion2, 2000L));

        // Now we want at least readVersion2 + 1, so this will cause a GRV
        long readVersion3 = getReadVersion(database, readVersion2 + 1, 2000L);

        assertTrue(readVersion2 < readVersion3);

        // Store another record
        testStoreAndRetrieveSimpleRecord(database, metaData);

        // Don't use a stored version
        assertTrue(readVersion3 < getReadVersion(database, null, null));
    }

    @Test
    public void cachedVersionMaintenanceOnCommitTest() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setTrackLastSeenVersion(true);
        FDBDatabase database = factory.getDatabase();
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // First time, does a GRV from FDB
        long readVersion1 = getReadVersion(database, 0L, 2000L);

        // Store a record (advances future GRV, but not cached version)
        testStoreAndRetrieveSimpleRecord(database, metaData);

        // We're fine with any version obtained up to 5s ago, but storing the record updated the cached version so we'll get a newer one
        long readVersion2 = getReadVersion(database, 0L, 5000L);
        assertTrue(readVersion1 < readVersion2);
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "cachedReadVersionWithRetryLoops [async = {0}]")
    public void cachedReadVersionWithRetryLoops(TestHelpers.BooleanEnum asyncEnum) throws InterruptedException, ExecutionException {
        final boolean async = asyncEnum.toBoolean();
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setTrackLastSeenVersion(true);
        FDBDatabase database = factory.getDatabase();
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        long readVersion1 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        long readVersion2 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        assertEquals(readVersion1, readVersion2);

        testStoreAndRetrieveSimpleRecord(database, metaData);

        long readVersion3 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        assertThat(readVersion3, greaterThan(readVersion2));

        // Force a commit that doesn't cache the read version
        database.database().run(tr -> {
            tr.addWriteConflictRange(new byte[0], new byte[]{(byte)0xff});
            return null;
        });
        long outOfBandReadVersion = database.database().runAsync(Transaction::getReadVersion).get();

        long readVersion4 = getReadVersionInRetryLoop(database, 0L, 5000L, async);
        assertEquals(readVersion3, readVersion4);
        assertThat(outOfBandReadVersion, greaterThan(readVersion4));

        // Sleep to make sure the value falls out of the cache.
        Thread.sleep(10);

        long readVersion5 = getReadVersionInRetryLoop(database, 0L, 5L, async);
        assertThat(readVersion5, greaterThanOrEqualTo(outOfBandReadVersion));
    }

    @Test
    public void testBlockingInAsyncException() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.IGNORE_COMPLETE_EXCEPTION_BLOCKING);

        // Make sure that we aren't holding on to previously created databases
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        assertEquals(BlockingInAsyncDetection.IGNORE_COMPLETE_EXCEPTION_BLOCKING, database.getBlockingInAsyncDetection());
        assertThrows(BlockingInAsyncException.class, () -> callAsyncBlocking(database));
    }

    @Test
    public void testBlockingInAsyncWarning() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.IGNORE_COMPLETE_WARN_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_IN_ASYNC_CONTEXT_MESSAGE,
                () -> {
                    callAsyncBlocking(database, true);
                    return null;
                });
    }

    @Test
    public void testCompletedBlockingInAsyncWarning() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_IN_ASYNC_CONTEXT_MESSAGE,
                () -> database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK,
                        CompletableFuture.supplyAsync(() ->
                                database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK, CompletableFuture.completedFuture(10L)))));
    }

    @Test
    public void testBlockingCreatingAsyncDetection() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_RETURNING_ASYNC_MESSAGE,
                () -> returnAnAsync(database, MoreAsyncUtil.delayedFuture(200L, TimeUnit.MILLISECONDS)));
    }

    @Test
    public void testCompletedBlockingCreatingAsyncDetection() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        TestHelpers.assertDidNotLog(FDBDatabase.class, FDBDatabase.BLOCKING_RETURNING_ASYNC_MESSAGE,
                () -> returnAnAsync(database, CompletableFuture.completedFuture(10L)));
    }

    @ParameterizedTest(name = "testJoinNowOnCompletedFuture (behavior = {0})")
    @EnumSource(BlockingInAsyncDetection.class)
    public void testJoinNowOnCompletedFuture(BlockingInAsyncDetection behavior) {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(behavior);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        TestHelpers.assertDidNotLog(FDBDatabase.class, FDBDatabase.BLOCKING_FOR_FUTURE_MESSAGE, () -> {
            long val = database.joinNow(CompletableFuture.completedFuture(1066L));
            assertEquals(1066L, val);
            return null;
        });
    }

    @ParameterizedTest(name = "testJoinNowOnNonCompletedFuture (behavior = {0})")
    @EnumSource(BlockingInAsyncDetection.class)
    public void testJoinNowOnNonCompletedFuture(BlockingInAsyncDetection behavior) {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setBlockingInAsyncDetection(behavior);
        factory.clear();

        FDBDatabase database = factory.getDatabase();
        if (behavior.throwExceptionOnBlocking()) {
            assertThrows(BlockingInAsyncException.class, () -> database.joinNow(new CompletableFuture<>()));
        } else {
            FDBDatabase database2 = factory.getDatabase();
            TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_FOR_FUTURE_MESSAGE, () -> {
                long val = database2.joinNow(MoreAsyncUtil.delayedFuture(100, TimeUnit.MILLISECONDS)
                        .thenApply(vignore -> 1066L));
                assertEquals(1066L, val);
                return null;
            });
        }
    }

    @Test
    public void testGetReadVersionLatencyInjection() throws Exception {
        testLatencyInjection(FDBLatencySource.GET_READ_VERSION, 300L, context -> {
            context.getDatabase().getReadVersion(context).join();
        });
    }

    @Test
    public void testCommitLatencyInjection() throws Exception {
        testLatencyInjection(FDBLatencySource.COMMIT_ASYNC, 300L, context -> {
            final Transaction tr = context.ensureActive();
            tr.clear(new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef });
            context.commit();
        });
    }

    public void testLatencyInjection(FDBLatencySource latencySource, long expectedLatency, Consumer<FDBRecordContext> thingToDo) throws Exception {
        final FDBDatabaseFactory factory = FDBDatabaseFactory.instance();

        // Databases only pick up the latency injector upon creation, so clear out any cached database
        factory.clear();
        factory.setLatencyInjector(
                requestedLatency -> requestedLatency == latencySource ? expectedLatency : 0L);

        FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            long grvStart = System.currentTimeMillis();
            thingToDo.accept(context);
            assertTrue(System.currentTimeMillis() - grvStart >= expectedLatency, "latency not injected");
        } finally {
            factory.clearLatencyInjector();
            factory.clear();
        }
    }

    @Test
    public void testPostCommitHooks() throws Exception {
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final AtomicInteger counter = new AtomicInteger(0);

        try (FDBRecordContext context = database.openContext()) {
            FDBRecordContext.PostCommit incrementPostCommit = context.getOrCreatePostCommit("foo",
                    name -> () -> CompletableFuture.runAsync(counter::incrementAndGet));

            // Cannot add a commit by the same name
            assertThrows(RecordCoreArgumentException.class, () -> {
                context.addPostCommit("foo", incrementPostCommit);
            });

            // We can fetch the post-commit by name
            assertTrue(context.getPostCommit("foo") == incrementPostCommit, "Failed to fetch post-commit");
            assertNull(context.getPostCommit("bar"));

            context.addPostCommit(incrementPostCommit);
            context.commit();
        }
        assertEquals(2, counter.get());
    }

    private CompletableFuture<Long> returnAnAsync(FDBDatabase database, CompletableFuture<?> toComplete) {
        database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK, toComplete);
        return CompletableFuture.completedFuture(10L);
    }

    private void callAsyncBlocking(FDBDatabase database) {
        callAsyncBlocking(database, false);
    }

    private void callAsyncBlocking(FDBDatabase database, boolean shouldTimeOut) {
        final CompletableFuture<Long> incomplete = new CompletableFuture<>();
        final Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> existingTimeouts = database.getAsyncToSyncTimeout();

        try {
            database.setAsyncToSyncTimeout(200L, TimeUnit.MILLISECONDS);
            database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK, CompletableFuture.supplyAsync(
                    () -> database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK, incomplete)));
            incomplete.complete(10L);
        } catch (RecordCoreException e) {
            if (e.getCause() instanceof TimeoutException) {
                if (!shouldTimeOut) {
                    throw e;
                }
            } else {
                throw e;
            }
        } finally {
            database.setAsyncToSyncTimeout(existingTimeouts);
        }
    }

    private long getReadVersionInRetryLoop(FDBDatabase database, Long minVersion, Long stalenessBoundMillis, boolean async) throws InterruptedException, ExecutionException {
        FDBDatabase.WeakReadSemantics weakReadSemantics = minVersion == null ? null : new FDBDatabase.WeakReadSemantics(minVersion, stalenessBoundMillis, false);
        if (async) {
            return database.runAsync(null, null, weakReadSemantics, database::getReadVersion).get();
        } else {
            return database.run(null, null, weakReadSemantics, context -> database.getReadVersion(context).join());
        }
    }

    private long getReadVersion(FDBDatabase database, Long minVersion, Long stalenessBoundMillis) {
        FDBDatabase.WeakReadSemantics weakReadSemantics = minVersion == null ? null : new FDBDatabase.WeakReadSemantics(minVersion, stalenessBoundMillis, false);
        try (FDBRecordContext context = database.openContext(Collections.emptyMap(), null, weakReadSemantics)) {
            return database.getReadVersion(context).join();
        }
    }

    public static void testStoreAndRetrieveSimpleRecord(FDBDatabase database, RecordMetaData metaData) {
        TestRecords1Proto.MySimpleRecord simpleRecord = storeSimpleRecord(database, metaData, 1066L);
        TestRecords1Proto.MySimpleRecord retrieved = retrieveSimpleRecord(database, metaData, 1066L);
        assertNotNull(retrieved);
        assertEquals(simpleRecord, retrieved);
    }

    private static TestRecords1Proto.MySimpleRecord storeSimpleRecord(FDBDatabase database, RecordMetaData metaData, long recordNumber) {
        TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recordNumber)
                .setNumValue2(42)
                .setNumValue3Indexed(100)
                .setNumValueUnique(1)
                .addRepeater(4)
                .addRepeater(5)
                .build();

        database.run(context -> {
            FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                    .setKeySpacePath(TestKeySpace.getKeyspacePath(FDBRecordStoreTestBase.PATH_OBJECTS))
                    .build();
            store.deleteAllRecords();
            store.saveRecord(simpleRecord);
            return null;
        });
        return simpleRecord;
    }

    private static TestRecords1Proto.MySimpleRecord retrieveSimpleRecord(FDBDatabase database, RecordMetaData metaData, long recordNumber) {
        // Tests to make sure the database operations are run and committed.
        TestRecords1Proto.MySimpleRecord retrieved = database.run(context -> {
            FDBRecordStore store = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
                    .setKeySpacePath(TestKeySpace.getKeyspacePath(FDBRecordStoreTestBase.PATH_OBJECTS))
                    .build();
            TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            FDBStoredRecord<Message> rec = store.loadRecord(Tuple.from(recordNumber));
            return builder.mergeFrom(rec.getRecord()).build();
        });
        return retrieved;
    }

    @Test
    public void performNoOp() {
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        FDBStoreTimer timer = new FDBStoreTimer();
        database.performNoOp(timer);
        assertEquals(1, timer.getCount(FDBStoreTimer.Events.PERFORM_NO_OP));
        assertThat(timer.getCount(FDBStoreTimer.Waits.WAIT_PERFORM_NO_OP), lessThanOrEqualTo(1));

        if (LOGGER.isInfoEnabled()) {
            KeyValueLogMessage logMessage = KeyValueLogMessage.build("performed no-op");
            logMessage.addKeysAndValues(timer.getKeysAndValues());
            LOGGER.info(logMessage.toString());
        }
    }

    @Test
    public void performNoOpAgainstFakeCluster() throws IOException {
        final String clusterFile = FDBTestBase.createFakeClusterFile("perform_no_op_");
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(clusterFile);

        // Should not be able to get a real read version from the fake cluster
        assertThrows(TimeoutException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                database.getReadVersion(context).get(100L, TimeUnit.MILLISECONDS);
            }
        });

        // Should still be able to perform a no-op
        FDBStoreTimer timer = new FDBStoreTimer();
        database.performNoOp(timer);
        assertEquals(1, timer.getCount(FDBStoreTimer.Events.PERFORM_NO_OP));
        assertThat(timer.getCount(FDBStoreTimer.Waits.WAIT_PERFORM_NO_OP), lessThanOrEqualTo(1));

        if (LOGGER.isInfoEnabled()) {
            KeyValueLogMessage logMessage = KeyValueLogMessage.build("performed no-op");
            logMessage.addKeysAndValues(timer.getKeysAndValues());
            LOGGER.info(logMessage.toString());
        }
    }
}
