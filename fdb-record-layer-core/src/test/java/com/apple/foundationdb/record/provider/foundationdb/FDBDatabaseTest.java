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

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.FakeClusterFileUtil;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
@Execution(ExecutionMode.CONCURRENT)
class FDBDatabaseTest {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseTest.class);
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    @Test
    void cachedVersionMaintenanceOnReadsTest() throws Exception {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setTrackLastSeenVersion(true);

        FDBDatabase database = dbExtension.getDatabase();

        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        // For the purpose of this test, commits will not change the cached read version
        database.setTrackLastSeenVersionOnCommit(false);
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertFalse(database.isTrackLastSeenVersionOnCommit());
        assertTrue(database.isTrackLastSeenVersion());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // First time, does a GRV from FDB
        long readVersion1 = getReadVersion(database, 0L, 2_000L);

        // Store a record (advances future GRV, but not cached version)
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(0L, 2_000L, false);
        try (FDBRecordContext context = database.openContext(FDBRecordContextConfig.newBuilder().setWeakReadSemantics(weakReadSemantics).setMdcContext(MDC.getCopyOfContextMap()).build())) {
            assertEquals(readVersion1, context.getReadVersion());
            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData)
                    .create();
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .build());
            context.commit();
        }

        // We're fine with any version obtained up to 2s ago, so will get the original readVersion
        assertEquals(readVersion1, getReadVersion(database, 0L, 2000L));

        Thread.sleep(10L);

        // Set a short staleness bound, this will cause a GRV call to FDB and will update the cached version
        long readVersion2 = getReadVersion(database, 0L, 11L);
        assertThat(readVersion1, lessThan(readVersion2));

        // Store another record
        testStoreAndRetrieveSimpleRecord(database, metaData, path);

        assertEquals(readVersion2, getReadVersion(database, 0L, 2000L));
        assertEquals(readVersion2, getReadVersion(database, readVersion2, 2000L));

        // Now we want at least readVersion2 + 1, so this will cause a GRV
        long readVersion3 = getReadVersion(database, readVersion2 + 1, 2000L);

        assertTrue(readVersion2 < readVersion3);

        // Store another record
        testStoreAndRetrieveSimpleRecord(database, metaData, path);

        // Don't use a stored version
        assertTrue(readVersion3 < getReadVersion(database, null, null));
    }

    @Test
    void cachedVersionMaintenanceOnCommitTest() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setTrackLastSeenVersion(true);
        FDBDatabase database = dbExtension.getDatabase();
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        // First time, does a GRV from FDB
        long readVersion1 = getReadVersion(database, 0L, 2000L);

        // Store a record (advances future GRV, but not cached version)
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        testStoreAndRetrieveSimpleRecord(database, metaData, path);

        // We're fine with any version obtained up to 5s ago, but storing the record updated the cached version so we'll get a newer one
        long readVersion2 = getReadVersion(database, 0L, 5000L);
        assertThat(readVersion1, lessThan(readVersion2));
    }

    @ParameterizedTest(name = "cachedReadVersionWithRetryLoops [async = {0}]")
    @BooleanSource
    void cachedReadVersionWithRetryLoops(boolean async) throws InterruptedException, ExecutionException {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setTrackLastSeenVersion(true);
        FDBDatabase database = dbExtension.getDatabase();
        assertTrue(database.isTrackLastSeenVersionOnRead());
        assertTrue(database.isTrackLastSeenVersionOnCommit());

        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());

        long readVersion1 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        long readVersion2 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        assertEquals(readVersion1, readVersion2);

        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        testStoreAndRetrieveSimpleRecord(database, metaData, path);

        long readVersion3 = getReadVersionInRetryLoop(database, 0L, 500L, async);
        assertThat(readVersion3, greaterThan(readVersion2));

        // Force a commit that doesn't cache the read version
        Subspace pathSubspace = database.run(path::toSubspace);
        database.database().run(tr -> {
            Range r = pathSubspace.range();
            tr.addWriteConflictRange(r.begin, r.end);
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

    @ParameterizedTest(name = "testJoinNowOnCompletedFuture (behavior = {0})")
    @EnumSource(BlockingInAsyncDetection.class)
    void testJoinNowOnCompletedFuture(BlockingInAsyncDetection behavior) {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(behavior);
        factory.clear();

        FDBDatabase database = dbExtension.getDatabase();
        TestHelpers.assertDidNotLog(FDBDatabase.class, FDBDatabase.BLOCKING_FOR_FUTURE_MESSAGE, () -> {
            long val = database.joinNow(CompletableFuture.completedFuture(1066L));
            assertEquals(1066L, val);
            return null;
        });
    }

    @ParameterizedTest(name = "testJoinNowOnNonCompletedFuture (behavior = {0})")
    @EnumSource(BlockingInAsyncDetection.class)
    void testJoinNowOnNonCompletedFuture(BlockingInAsyncDetection behavior) {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(behavior);
        factory.clear();

        FDBDatabase database = dbExtension.getDatabase();
        if (behavior.throwExceptionOnBlocking()) {
            assertThrows(BlockingInAsyncException.class, () -> database.joinNow(new CompletableFuture<>()));
        } else {
            FDBDatabase database2 = factory.getDatabase(database.getClusterFile());
            TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_FOR_FUTURE_MESSAGE, () -> {
                long val = database2.joinNow(MoreAsyncUtil.delayedFuture(100, TimeUnit.MILLISECONDS, database2.getScheduledExecutor())
                        .thenApply(vignore -> 1066L));
                assertEquals(1066L, val);
                return null;
            });
        }
    }

    @Test
    void loggableTimeoutException() {
        CompletableFuture<Void> delayed = new CompletableFuture<>();
        FDBDatabase database = dbExtension.getDatabase();
        FDBStoreTimer timer = new FDBStoreTimer();
        database.setAsyncToSyncTimeout(1, TimeUnit.MILLISECONDS);
        assertThrows(Exception.class, () -> database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT, delayed));
        KeyValueLogMessage message = KeyValueLogMessage.build("Testing logging of timeout events.");
        message.addKeysAndValues(timer.getKeysAndValues());
        assertTrue(message.getKeyValueMap().containsKey("wait_commit_timeout_micros"));
        assertTrue(Long.parseLong(message.getKeyValueMap().get("wait_commit_timeout_micros")) > 0L);
        assertTrue(message.getKeyValueMap().containsKey("wait_commit_timeout_count"));
        assertEquals(Integer.parseInt(message.getKeyValueMap().get("wait_commit_timeout_count")), 1);
    }

    @Test
    void testGetReadVersionLatencyInjection() {
        testLatencyInjection(FDBLatencySource.GET_READ_VERSION, 300L, FDBRecordContext::getReadVersion);
    }

    @Test
    void testCommitLatencyInjection() {
        testLatencyInjection(FDBLatencySource.COMMIT_ASYNC, 300L, context -> {
            final Transaction tr = context.ensureActive();
            tr.clear(new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef });
            context.commit();
        });
    }

    private void testLatencyInjection(FDBLatencySource latencySource, long expectedLatency, Consumer<FDBRecordContext> thingToDo) {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();

        // Databases only pick up the latency injector upon creation, so clear out any cached database
        factory.clear();
        factory.setLatencyInjector(
                requestedLatency -> requestedLatency == latencySource ? expectedLatency : 0L);

        FDBDatabase database = dbExtension.getDatabase();
        try {
            try (FDBRecordContext context = database.openContext()) {
                long grvStart = System.currentTimeMillis();
                thingToDo.accept(context);
                assertThat("latency not injected without timer", System.currentTimeMillis() - grvStart, greaterThanOrEqualTo(expectedLatency));
            }
            FDBStoreTimer timer = new FDBStoreTimer();
            try (FDBRecordContext context = database.openContext(null, timer)) {
                long grvStart = System.currentTimeMillis();
                thingToDo.accept(context);
                assertThat("latency not injected with timer set", System.currentTimeMillis() - grvStart, greaterThanOrEqualTo(expectedLatency));
                assertEquals(1, timer.getCount(latencySource.getTimerEvent()));
            }
        } finally {
            factory.clearLatencyInjector();
            factory.clear();
        }
    }

    @Test
    void testPostCommitHooks() {
        final FDBDatabase database = dbExtension.getDatabase();
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

    private long getReadVersionInRetryLoop(FDBDatabase database, Long minVersion, Long stalenessBoundMillis, boolean async) throws InterruptedException, ExecutionException {
        FDBDatabase.WeakReadSemantics weakReadSemantics = minVersion == null ? null : new FDBDatabase.WeakReadSemantics(minVersion, stalenessBoundMillis, false);
        if (async) {
            return database.runAsync(null, null, weakReadSemantics, FDBRecordContext::getReadVersionAsync).get();
        } else {
            return database.run(null, null, weakReadSemantics, FDBRecordContext::getReadVersion);
        }
    }

    private long getReadVersion(FDBDatabase database, Long minVersion, Long stalenessBoundMillis) {
        FDBDatabase.WeakReadSemantics weakReadSemantics = minVersion == null ? null : new FDBDatabase.WeakReadSemantics(minVersion, stalenessBoundMillis, false);
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setWeakReadSemantics(weakReadSemantics)
                .setMdcContext(MDC.getCopyOfContextMap())
                .build();
        try (FDBRecordContext context = database.openContext(config)) {
            return context.getReadVersion();
        }
    }

    static void testStoreAndRetrieveSimpleRecord(FDBDatabase database, RecordMetaData metaData, KeySpacePath path) {
        TestRecords1Proto.MySimpleRecord simpleRecord = storeSimpleRecord(database, metaData, path, 1066L);
        TestRecords1Proto.MySimpleRecord retrieved = retrieveSimpleRecord(database, metaData, path, 1066L);
        assertNotNull(retrieved);
        assertEquals(simpleRecord, retrieved);
    }

    private static TestRecords1Proto.MySimpleRecord storeSimpleRecord(FDBDatabase database, RecordMetaData metaData, KeySpacePath path, long recordNumber) {
        TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recordNumber)
                .setNumValue2(42)
                .setNumValue3Indexed(100)
                .setNumValueUnique(1)
                .addRepeater(4)
                .addRepeater(5)
                .build();

        database.run(null, MDC.getCopyOfContextMap(), context -> {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setMetaDataProvider(metaData)
                    .setContext(context)
                    .setKeySpacePath(path)
                    .createOrOpen();
            store.deleteAllRecords();
            store.saveRecord(simpleRecord);
            return null;
        });
        return simpleRecord;
    }

    private static TestRecords1Proto.MySimpleRecord retrieveSimpleRecord(FDBDatabase database, RecordMetaData metaData, KeySpacePath path, long recordNumber) {
        // Tests to make sure the database operations are run and committed.
        return database.run(null, MDC.getCopyOfContextMap(), context -> {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setMetaDataProvider(metaData)
                    .setContext(context)
                    .setKeySpacePath(path)
                    .build();
            TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            FDBStoredRecord<Message> rec = store.loadRecord(Tuple.from(recordNumber));
            return builder.mergeFrom(rec.getRecord()).build();
        });
    }

    @Test
    void performNoOp() {
        final FDBDatabase database = dbExtension.getDatabase();
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
    void performNoOpAgainstFakeCluster() throws IOException {
        final String clusterFile = FakeClusterFileUtil.createFakeClusterFile("perform_no_op_");
        final FDBDatabase database = dbExtension.getDatabaseFactory().getDatabase(clusterFile);

        // Should not be able to get a real read version from the fake cluster
        assertThrows(TimeoutException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                context.getReadVersionAsync().get(100L, TimeUnit.MILLISECONDS);
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

    @Test
    void testAssertionsOnKeySize() {
        testSizeAssertion(context ->
                        context.ensureActive().set(Tuple.from(1, new byte[InstrumentedTransaction.MAX_KEY_LENGTH]).pack(), Tuple.from(1).pack()),
                FDBExceptions.FDBStoreKeySizeException.class);
    }

    @Test
    void testAssertionsOnValueSize() {
        testSizeAssertion(context ->
                        context.ensureActive().set(Tuple.from(1).pack(), Tuple.from(2, new byte[InstrumentedTransaction.MAX_VALUE_LENGTH]).pack()),
                FDBExceptions.FDBStoreValueSizeException.class);
    }

    private void testSizeAssertion(Consumer<FDBRecordContext> consumer, Class<? extends Exception> exception) {
        FDBDatabase database = dbExtension.getDatabase();

        // By default key size validation happens in the FDB driver at commit time
        try (FDBRecordContext context = database.openContext()) {
            consumer.accept(context);
            assertThrows(exception, () -> context.commit());
        }

        // enabling assertions causes checks to happen in record layer code
        try (FDBRecordContext context = database.openContext(
                FDBRecordContextConfig.newBuilder().setEnableAssertions(true).build())) {
            assertThrows(exception, () -> consumer.accept(context));
        }
    }

    @Test
    void apiVersionIsSet() {
        final FDBDatabase database = dbExtension.getDatabase();
        assertEquals(dbExtension.getAPIVersion(), database.getAPIVersion());
        assertEquals(FDB.instance().getAPIVersion(), database.getAPIVersion().getVersionNumber());
        try (FDBRecordContext context = database.openContext()) {
            assertEquals(database.getAPIVersion(), context.getAPIVersion());
            assertTrue(context.isAPIVersionAtLeast(context.getAPIVersion()));
            assertTrue(context.isAPIVersionAtLeast(APIVersion.API_VERSION_6_3));
        }
    }

    @Test
    void cannotChangeAPIVersionAfterInit() {
        final FDBDatabase database = dbExtension.getDatabase();
        final APIVersion initApiVersion = database.getAPIVersion();

        for (APIVersion newApiVersion : APIVersion.values()) {
            Executable setVersion = () -> database.getFactory().setAPIVersion(newApiVersion);
            if (newApiVersion == initApiVersion) {
                assertDoesNotThrow(setVersion,
                        "should be able to set the API version to the same value it was already set to");
            } else {
                assertThrows(RecordCoreException.class, setVersion, "should not be able to change the API version after it has been set");
            }
            assertEquals(initApiVersion, database.getFactory().getAPIVersion());
            assertEquals(initApiVersion, database.getAPIVersion());
        }
    }

    @Test
    void canAccessMultipleClusters() {
        FDBTestEnvironment.assumeClusterCount(2);
        final FDBDatabase database0 = dbExtension.getDatabase(0);
        final FDBDatabase database1 = dbExtension.getDatabase(1);
        final byte[] key = Tuple.from(UUID.randomUUID()).pack();
        final byte[] value0 = Tuple.from("cluster0").pack();
        final byte[] value1 = Tuple.from("cluster1").pack();
        try (FDBRecordContext context0 = database0.openContext()) {
            context0.ensureActive().set(key, value0);
            context0.commit();
        }
        try (FDBRecordContext context1 = database1.openContext()) {
            assertNull(context1.ensureActive().get(key).join());
            context1.ensureActive().set(key, value1);
            context1.commit();
        }
        try (FDBRecordContext context0 = database0.openContext()) {
            assertArrayEquals(value0, context0.ensureActive().get(key).join());
            context0.commit();
        }
        try (FDBRecordContext context1 = database1.openContext()) {
            assertArrayEquals(value1, context1.ensureActive().get(key).join());
            context1.commit();
        }
    }
}
