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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
class FDBDatabaseTest extends FDBTestBase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseTest.class);

    @Test
    void cachedVersionMaintenanceOnReadsTest() throws Exception {
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
    void cachedVersionMaintenanceOnCommitTest() {
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

    @ParameterizedTest(name = "cachedReadVersionWithRetryLoops [async = {0}]")
    @BooleanSource
    void cachedReadVersionWithRetryLoops(boolean async) throws InterruptedException, ExecutionException {
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

    @ParameterizedTest(name = "testJoinNowOnCompletedFuture (behavior = {0})")
    @EnumSource(BlockingInAsyncDetection.class)
    void testJoinNowOnCompletedFuture(BlockingInAsyncDetection behavior) {
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
    void testJoinNowOnNonCompletedFuture(BlockingInAsyncDetection behavior) {
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
    void loggableTimeoutException() {
        CompletableFuture<Void> delayed = new CompletableFuture<Void>();
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        FDBDatabase database = factory.getDatabase();
        FDBStoreTimer timer = new FDBStoreTimer();
        database.setAsyncToSyncTimeout(1, TimeUnit.MILLISECONDS);
        try {
            database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT, delayed);
        } catch (Exception ex) {
            KeyValueLogMessage message = KeyValueLogMessage.build("Testing logging of timeout events.");
            message.addKeysAndValues(timer.getKeysAndValues());
            assertTrue(message.getKeyValueMap().containsKey("wait_commit_timeout_micros"));
            assertTrue(Long.parseLong(message.getKeyValueMap().get("wait_commit_timeout_micros")) > 0L);
            assertTrue(message.getKeyValueMap().containsKey("wait_commit_timeout_count"));
            assertEquals(Integer.parseInt(message.getKeyValueMap().get("wait_commit_timeout_count")), 1);
        } finally {
            factory.clear();
            timer.reset();
        }
    }

    @Test
    void testGetReadVersionLatencyInjection() throws Exception {
        testLatencyInjection(FDBLatencySource.GET_READ_VERSION, 300L, FDBRecordContext::getReadVersion);
    }

    @Test
    void testCommitLatencyInjection() throws Exception {
        testLatencyInjection(FDBLatencySource.COMMIT_ASYNC, 300L, context -> {
            final Transaction tr = context.ensureActive();
            tr.clear(new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef });
            context.commit();
        });
    }

    private void testLatencyInjection(FDBLatencySource latencySource, long expectedLatency, Consumer<FDBRecordContext> thingToDo) throws Exception {
        final FDBDatabaseFactory factory = FDBDatabaseFactory.instance();

        // Databases only pick up the latency injector upon creation, so clear out any cached database
        factory.clear();
        factory.setLatencyInjector(
                requestedLatency -> requestedLatency == latencySource ? expectedLatency : 0L);

        FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
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
    void testPostCommitHooks() throws Exception {
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
        try (FDBRecordContext context = database.openContext(Collections.emptyMap(), null, weakReadSemantics)) {
            return context.getReadVersion();
        }
    }

    static void testStoreAndRetrieveSimpleRecord(FDBDatabase database, RecordMetaData metaData) {
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
    void performNoOp() {
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
    void performNoOpAgainstFakeCluster() throws IOException {
        final String clusterFile = FDBTestBase.createFakeClusterFile("perform_no_op_");
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(clusterFile);

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
        FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();

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
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        assertEquals(FDBTestBase.getAPIVersion(), database.getAPIVersion());
        assertEquals(FDB.instance().getAPIVersion(), database.getAPIVersion().getVersionNumber());
        try (FDBRecordContext context = database.openContext()) {
            assertEquals(database.getAPIVersion(), context.getAPIVersion());
            assertTrue(context.isAPIVersionAtLeast(context.getAPIVersion()));
            assertTrue(context.isAPIVersionAtLeast(APIVersion.API_VERSION_6_3));
        }
    }

    @Test
    void cannotChangeAPIVersionAfterInit() {
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
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
}
