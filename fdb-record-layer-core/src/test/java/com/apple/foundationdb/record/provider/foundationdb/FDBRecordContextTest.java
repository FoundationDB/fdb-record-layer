/*
 * FDBRecordContextTest.java
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.TaskNotifyingExecutor;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.FakeClusterFileUtil;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests of the {@link FDBRecordContext} class.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordContextTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    // A list of transaction IDs where the left item is the original ID and the right item is the expected
    // sanitized ID. It is a list of pairs rather than a map to support null as the expected value.
    @Nonnull
    private static final List<Pair<String, String>> trIds;
    @Nonnull
    private static final FDBDatabase.WeakReadSemantics UNLIMITED_STALE_READ = new FDBDatabase.WeakReadSemantics(0L, Long.MAX_VALUE, true);
    protected FDBDatabase fdb;

    static {
        final ImmutableList.Builder<Pair<String, String>> listBuilder = ImmutableList.builder();
        listBuilder.add(Pair.of("my_fake_tr_id", "my_fake_tr_id"));

        // There is a maximum length allowed on transaction IDs. Ensure that if the user-specified ID is too
        // long that that doesn't propagate its way down to FDB.
        final String longString = Strings.repeat("id_", 2 * FDBRecordContext.MAX_TR_ID_SIZE);
        listBuilder.add(Pair.of(longString, longString.substring(0, FDBRecordContext.MAX_TR_ID_SIZE - 3) + "..."));

        // Try a string with non-ASCII characters, though that isn't necessarily advised.
        final String nonAsciiString = "универсальный уникальный идентификатор";
        assertThat(Utf8.encodedLength(nonAsciiString), lessThanOrEqualTo(FDBRecordContext.MAX_TR_ID_SIZE));
        listBuilder.add(Pair.of(nonAsciiString, nonAsciiString));

        // A long string like this is not truncatable as it contains non-ASCII characters, so it should instead
        // be set to null by the sanitizer.
        final String longNonAsciiString = nonAsciiString + " " + nonAsciiString;
        assertThat(longNonAsciiString.length(), lessThanOrEqualTo(FDBRecordContext.MAX_TR_ID_SIZE));
        assertThat(Utf8.encodedLength(longNonAsciiString), greaterThan(FDBRecordContext.MAX_TR_ID_SIZE));
        listBuilder.add(Pair.of(longNonAsciiString, null));
        trIds = listBuilder.build();
    }

    @BeforeEach
    public void getFDB() {
        fdb = dbExtension.getDatabase();
    }

    /**
     * Validate that if the {@link FDBRecordContext#getReadVersion()} is called twice that the
     * same value is returned each time and that the second time does not actually call the
     * FDB method.
     */
    @Test
    public void getReadVersionTwice() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            long readVersion1 = context.getReadVersion();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
            long grvNanos = timer.getTimeNanos(FDBStoreTimer.Events.GET_READ_VERSION);
            long readVersion2 = context.getReadVersion();
            assertEquals(readVersion1, readVersion2);
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
            assertEquals(grvNanos, timer.getTimeNanos(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @Test
    public void manyParallelReadVersions() throws InterruptedException, ExecutionException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            List<CompletableFuture<Long>> futures = Stream.generate(context::getReadVersionAsync)
                    .limit(10)
                    .collect(Collectors.toList());
            List<Long> readVersions = AsyncUtil.getAll(futures).get();
            long firstReadVersion = readVersions.get(0);
            for (long readVersion : readVersions) {
                assertEquals(firstReadVersion, readVersion);
            }
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @Test
    public void getReadVersionTimingWithInjectedLatency() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        FDBDatabaseFactory factory = fdb.getFactory();
        Function<FDBLatencySource, Long> oldLatencyInjector = factory.getLatencyInjector();
        factory.setLatencyInjector(latencySource -> {
            if (latencySource.equals(FDBLatencySource.GET_READ_VERSION)) {
                return 50L;
            } else {
                return 10L;
            }
        });
        factory.clear();
        fdb = factory.getDatabase(fdb.getClusterFile());
        try {
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                context.getReadVersion();
            }
            assertEquals(1, timer.getCount(FDBStoreTimer.Waits.WAIT_GET_READ_VERSION));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.INJECTED_GET_READ_VERSION_LATENCY));

            long waitNanos = timer.getTimeNanos(FDBStoreTimer.Waits.WAIT_GET_READ_VERSION);
            long grvNanos = timer.getTimeNanos(FDBStoreTimer.Events.GET_READ_VERSION);
            long injectedNanos = timer.getTimeNanos(FDBStoreTimer.Events.INJECTED_GET_READ_VERSION_LATENCY);

            // Validate that the grvNanos includes the injected nanos. It is very likely that GRV is higher than
            // the injected nanos, but theoretically could be different
            assertThat(injectedNanos, lessThanOrEqualTo(grvNanos));
            assertThat(waitNanos, greaterThan(0L));

        } finally {
            factory.setLatencyInjector(oldLatencyInjector);
            factory.clear();
        }
    }

    @ParameterizedTest(name = "closeWithOutstandingGetReadVersion [inject latency = {0}]")
    @BooleanSource
    public void closeWithOutstandingGetReadVersion(boolean injectLatency) throws InterruptedException, ExecutionException {
        FDBDatabaseFactory factory = fdb.getFactory();
        Function<FDBLatencySource, Long> oldLatencyInjector = factory.getLatencyInjector();

        FDBRecordContext context = null;
        try {
            if (injectLatency) {
                factory.setLatencyInjector(latencySource -> 5L);
            } else {
                factory.setLatencyInjector(latencySource -> 0L);
            }
            factory.clear();
            fdb = factory.getDatabase(fdb.getClusterFile());
            context = fdb.openContext();
            CompletableFuture<Long> readVersionFuture = context.getReadVersionAsync();
            context.close();
            context = null;
            readVersionFuture.handle((val, err) -> {
                // If the future happens to complete before the transaction is closed (possible, but unlikely,
                // then this might not throw an error.
                if (err != null) {
                    Throwable currentErr = err;
                    while (currentErr != null && (currentErr instanceof ExecutionException || currentErr instanceof CompletionException)) {
                        currentErr = currentErr.getCause();
                    }

                    if (currentErr instanceof FDBException) {
                        // Error from the FDB native code if an operation is cancelled
                        // This is the probable error if latency is not injected
                        FDBException fdbException = (FDBException)currentErr;
                        assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbException.getCode());
                    } else if (currentErr instanceof IllegalStateException) {
                        // This is the error the FDB Java bindings throw if one closes a transaction and then tries to use it.
                        // This error can happen if the exact order of events is (1) injected latency completes then
                        // (2) ensureActive is called and completes then (3) the context is closed then (4) "getReadVersion" is called.
                        assertThat(currentErr.getMessage(), containsString("Cannot access closed object"));
                    } else if (currentErr instanceof RecordCoreStorageException) {
                        // Generated by FDB if one attempts to do something on an object that has been closed
                        // This is the probable error if latency is injected
                        assertThat(currentErr.getMessage(), containsString("Transaction is no longer active."));
                    } else {
                        fail("Unexpected exception encountered", err);
                    }
                }
                return null;
            }).get();
        } finally {
            if (context != null) {
                context.close();
            }
            factory.setLatencyInjector(oldLatencyInjector);
            factory.clear();
        }
    }

    @Test
    public void concurrentGetAndSet() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 10; i++) {
            final FDBStoreTimer timer = new FDBStoreTimer();
            try (FDBRecordContext context = fdb.openContext(null, timer)) {
                context.ensureActive().setReadVersion(1459L); // to increase the probability of getReadVersion completing first
                CompletableFuture<Long> readVersionFuture = context.getReadVersionAsync();
                // Setting a read version with an outstanding read version request may or not succeed
                // depending on whether the future is completed by the time the set request is called.
                try {
                    long readVersionSet = context.setReadVersion(1066);
                    assertEquals(readVersionSet, (long)readVersionFuture.get());
                } catch (RecordCoreException e) {
                    assertEquals("Cannot set read version as read version request is outstanding", e.getMessage());
                }
                assertEquals(1459L, (long)readVersionFuture.get());
                assertEquals(1459L, context.getReadVersion());
            }
        }
    }

    @Test
    public void setReadVersionTwice() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            assertEquals(1066L, context.setReadVersion(1066L));
            assertEquals(1066L, context.setReadVersion(1459L));
            assertEquals(1066L, context.getReadVersion());
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
            context.commit();
        }
    }

    @Test
    public void setReadVersionAfterGet() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            long readVersion = context.getReadVersion();
            assertEquals(readVersion, context.setReadVersion(1066L));
            assertEquals(readVersion, context.getReadVersion());
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @Test
    public void setReadVersionOutOfBandThenGet() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            context.ensureActive().setReadVersion(1066L);
            assertEquals(1066L, context.getReadVersion());
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @Test
    public void setReadVersionOutOfBandThenSet() {
        try (FDBRecordContext context = fdb.openContext()) {
            context.ensureActive().setReadVersion(1066L);
            assertEquals(1459L, context.setReadVersion(1459L));
            assertEquals(1459L, context.getReadVersion());
            FDBExceptions.FDBStoreException err = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(err.getCause());
            assertThat(err.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)err.getCause();
            assertEquals(FDBError.READ_VERSION_ALREADY_SET.code(), fdbE.getCode());
        }
    }

    @Test
    public void getReadVersionWithWeakReadSemantics() {
        fdb.setTrackLastSeenVersion(true);
        long firstReadVersion;
        try (FDBRecordContext context = fdb.openContext()) {
            firstReadVersion = context.getReadVersion();
        }
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer, UNLIMITED_STALE_READ)) {
            assertEquals(firstReadVersion, context.getReadVersion());
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @Test
    public void getReadVersionAtBatch() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer, null, FDBTransactionPriority.BATCH)) {
            context.getReadVersion();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.BATCH_GET_READ_VERSION));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.GET_READ_VERSION));
        }
    }

    @ParameterizedTest(name = "setTrIdThroughMdc [traced = {0}]")
    @BooleanSource
    public void setTrIdThroughMdc(boolean traced) {
        final FDBDatabaseFactory factory = fdb.getFactory();
        final Supplier<Boolean> oldTrIsTracedSupplier = factory.getTransactionIsTracedSupplier();
        factory.clear(); // clear out caches from cluster files to databases
        try {
            factory.setTransactionIsTracedSupplier(() -> traced);
            fdb = factory.getDatabase(fdb.getClusterFile());
            for (Pair<String, String> idPair : trIds) {
                final String trId = idPair.getLeft();
                final String expectedId = idPair.getRight();
                Map<String, String> fakeMdc = Collections.singletonMap("uuid", trId);
                try (FDBRecordContext context = fdb.openContext(fakeMdc, null)) {
                    assertThat(context.getMdcContext(), hasEntry("uuid", trId));
                    assertEquals(traced && expectedId != null, context.isLogged());
                    context.ensureActive().getReadVersion().join();
                    assertEquals(expectedId, context.getTransactionId());
                    context.commit();
                } catch (RuntimeException e) {
                    fail("unable to set id to " + trId, e);
                }
            }
        } finally {
            fdb.close();
            factory.setTransactionIsTracedSupplier(oldTrIsTracedSupplier);
            factory.clear();
        }
    }

    @ParameterizedTest(name = "setTrIdThroughMethod [logged = {0}]")
    @BooleanSource
    public void setTrIdThroughMethod(boolean logged) {
        for (Pair<String, String> idPair : trIds) {
            final String trId = idPair.getLeft();
            final String expectedId = idPair.getRight();
            try (FDBRecordContext context = fdb.openContext(null, null, null, FDBTransactionPriority.DEFAULT, trId)) {
                if (expectedId != null && logged) {
                    context.logTransaction();
                }
                context.ensureActive().getReadVersion().join();
                assertEquals(expectedId, context.getTransactionId());
                assertEquals((logged || FDBDatabaseExtension.TRACE) && expectedId != null, context.isLogged());
                context.commit();
            } catch (RuntimeException e) {
                fail("unable to set id to " + trId, e);
            }
        }
    }

    @ParameterizedTest(name = "setTrIdThroughConfig [logged = {0}]")
    @BooleanSource
    public void setTrIdThroughConfig(boolean logged) {
        for (Pair<String, String> idPair : trIds) {
            final String trId = idPair.getLeft();
            final String expectedId = idPair.getRight();
            final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                    .setTransactionId(trId)
                    .setLogTransaction(logged)
                    .build();
            try (FDBRecordContext context = fdb.openContext(config)) {
                context.ensureActive().getReadVersion().join();
                assertEquals(expectedId, context.getTransactionId());
                assertEquals((logged || FDBDatabaseExtension.TRACE) && expectedId != null, context.isLogged());
                context.commit();
            }
        }
    }

    @ParameterizedTest(name = "setTrIdThroughParameterEvenIfInMdc [logged = {0}]")
    @BooleanSource
    public void setIdThroughParameterEvenIfInMdc(boolean logged) {
        final Map<String, String> fakeMdc = Collections.singletonMap("uuid", "id_in_mdc");
        try (FDBRecordContext context = fdb.openContext(fakeMdc, null, null, FDBTransactionPriority.DEFAULT, "id_in_param")) {
            assertEquals("id_in_param", context.getTransactionId());
            assertThat(context.getMdcContext(), hasEntry("uuid", "id_in_mdc"));
            if (logged) {
                context.logTransaction();
            }
            context.ensureActive().getReadVersion().join();
            context.commit();
        }
    }

    @Test
    public void logWithoutSettingId() {
        if (FDBDatabaseExtension.TRACE) {
            FDBDatabaseFactory factory = fdb.getFactory();
            factory.setTransactionIsTracedSupplier(() -> false);
            factory.clear();
            fdb = factory.getDatabase();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            RecordCoreException err = assertThrows(RecordCoreException.class, context::logTransaction);
            assertEquals("Cannot log transaction as ID is not set", err.getMessage());
            context.ensureActive().getReadVersion().join();
            context.commit();
        }
    }

    @Test
    public void logTwice() {
        try (FDBRecordContext context = fdb.openContext(null, null, null, FDBTransactionPriority.DEFAULT, "logTransactionTwice")) {
            context.logTransaction();
            context.logTransaction();
            context.ensureActive().getReadVersion().join();
            context.commit();
        }
    }

    @Test
    public void testSession() {
        try (FDBRecordContext context = fdb.openContext(null, null, null, FDBTransactionPriority.DEFAULT, "logTransactionTwice")) {
            context.putInSessionIfAbsent("Yo", "YOLO");
            assertEquals("YOLO", context.getInSession("Yo", String.class));
            assertEquals("YOLO", context.removeFromSession("Yo", String.class));
            assertNull(context.getInSession("Yo", String.class));
        }
    }


    @Test
    public void logTransactionAfterGettingAReadVersion() {
        final KeySpacePath fakePath = pathManager.createPath(TestKeySpace.RAW_DATA);
        try (FDBRecordContext context = fdb.openContext(null, null, null, FDBTransactionPriority.DEFAULT, "logTransactionAfterGrv")) {
            context.ensureActive().getReadVersion().join();
            context.logTransaction();
            Subspace fakeSubspace = fakePath.toSubspace(context);
            context.ensureActive().addWriteConflictRange(fakeSubspace.range().begin, fakeSubspace.range().end);
            context.commit();
        }
    }

    /**
     * Test using the server request tracing feature. Because this feature creates logs in files that are hard to
     * examine during tests, this test mainly makes sure that using this feature does not throw any errors, but it
     * doesn't make any effort to make sure that setting the option doesn't throw any errors.
     */
    @Test
    public void serverRequestTracing() {
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        byte[] prefix;
        try (FDBRecordContext context = fdb.openContext()) {
            prefix = path.toSubspace(context).getKey();
        }

        // Add a write conflict range and commit with one ID
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTransactionId("serverRequestTracingId")
                .setServerRequestTracing(true)
                .build();
        assertTrue(config.isServerRequestTracing());
        try (FDBRecordContext context = fdb.openContext(config)) {
            context.getReadVersion();
            context.ensureActive().addWriteConflictRange(ByteArrayUtil.join(prefix, ByteArrayUtil2.unprint("hello")), ByteArrayUtil.join(prefix, ByteArrayUtil2.unprint("world")));
            context.commit();
        }
        // Add a write conflict range and commit with a different ID
        final FDBRecordContextConfig config2 = config.toBuilder()
                .setTransactionId("serverRequestTracingId2")
                .build();
        assertTrue(config2.isServerRequestTracing());
        try (FDBRecordContext context = fdb.openContext(config2)) {
            context.getReadVersion();
            context.ensureActive().get(ByteArrayUtil.join(prefix, ByteArrayUtil2.unprint("foo"))).join();
            context.commit();
        }
    }

    /**
     * Test using the server request tracing feature without a transaction ID. Like {@link #serverRequestTracing()},
     * this mainly makes sure that the test completes without throwing errors.
     */
    @Test
    public void serverRequestTracingNoId() {
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        byte[] prefix;
        try (FDBRecordContext context = fdb.openContext()) {
            prefix = path.toSubspace(context).getKey();
        }

        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setServerRequestTracing(true)
                .copyBuilder() // To test to make sure copying the builder preserves the option
                .build();
        assertTrue(config.isServerRequestTracing());
        try (FDBRecordContext context = fdb.openContext(config)) {
            context.getReadVersion();
            context.ensureActive().get(ByteArrayUtil.join(prefix, ByteArrayUtil2.unprint("some_key"))).join();
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext(config)) {
            context.getReadVersion();
            context.ensureActive().get(ByteArrayUtil.join(prefix, ByteArrayUtil2.unprint("some_key_2"))).join();
            context.commit();
        }
    }

    @Test
    void reportConflictingKeys() {
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        final Subspace subspace = fdb.run(path::toSubspace);
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setReportConflictingKeys(true)
                .build();
        try (FDBRecordContext context1 = fdb.openContext(config)) {
            context1.ensureActive().get(subspace.pack(222)).join();
            context1.ensureActive().set(subspace.pack(111), Tuple.from(1).pack());

            try (FDBRecordContext context2 = fdb.openContext(config)) {
                context2.ensureActive().set(subspace.pack(222), Tuple.from(2).pack());
                context2.commit();
            }

            final byte[] conflictBegin = subspace.pack(222);
            // Single key range.
            final List<Range> expected = List.of(new Range(conflictBegin, ByteArrayUtil.join(conflictBegin, new byte[] {0x00})));
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
            assertEquals(expected, context1.getNotCommittedConflictingKeys());
        }
    }

    @Test
    public void setTimeoutInDatabaseFactory() {
        long initialTimeoutMillis = fdb.getFactory().getTransactionTimeoutMillis();
        try {
            fdb.getFactory().setTransactionTimeoutMillis(1066L);
            try (FDBRecordContext context = fdb.openContext()) {
                assertEquals(1066L, context.getTimeoutMillis(), "timeout millis did not match factory timeout");
            }

            fdb.getFactory().setTransactionTimeoutMillis(FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS);
            try (FDBRecordContext context = fdb.openContext()) {
                assertEquals(FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS, context.getTimeoutMillis(), "timeout millis did not match default");
            }
        } finally {
            fdb.getFactory().setTransactionTimeoutMillis(initialTimeoutMillis);
        }
    }

    @Test
    public void setTimeoutInRunner() {
        long initialTimeoutMillis = fdb.getFactory().getTransactionTimeoutMillis();
        try {
            fdb.getFactory().setTransactionTimeoutMillis(1066L);

            try (final FDBDatabaseRunner runner = fdb.newRunner()) {
                runner.setTransactionTimeoutMillis(1415L);
                try (FDBRecordContext context = runner.openContext()) {
                    assertEquals(1415L, context.getTimeoutMillis(), "timeout millis did not match runner timeout");
                }
            }

            try (final FDBDatabaseRunner runner = fdb.newRunner()) {
                runner.setTransactionTimeoutMillis(FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS);
                try (FDBRecordContext context = runner.openContext()) {
                    assertEquals(1066L, context.getTimeoutMillis(), "timeout millis did not match factory timeout");
                }
            }
        } finally {
            fdb.getFactory().setTransactionTimeoutMillis(initialTimeoutMillis);
        }
    }

    @Test
    public void setTimeoutInConfig() {
        long initialTimeoutMillis = fdb.getFactory().getTransactionTimeoutMillis();
        try {
            fdb.getFactory().setTransactionTimeoutMillis(1066L);

            final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                    .setTransactionTimeoutMillis(1415L)
                    .build();
            try (final FDBRecordContext context = fdb.openContext(config)) {
                assertEquals(1415L, context.getTimeoutMillis(), "timeout millis did not match config timeout");
            }

            final FDBRecordContextConfig config2 = FDBRecordContextConfig.newBuilder()
                    .setTransactionTimeoutMillis(FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS)
                    .build();
            try (final FDBRecordContext context = fdb.openContext(config2)) {
                assertEquals(1066L, context.getTimeoutMillis(), "timeout millis did not match factory timeout");
            }
        } finally {
            fdb.getFactory().setTransactionTimeoutMillis(initialTimeoutMillis);
        }
    }

    @Test
    public void timeoutTalkingToFakeCluster() throws IOException {
        final String fakeClusterFile = FakeClusterFileUtil.createFakeClusterFile("for_testing_timeouts");
        final FDBDatabase fakeFdb = dbExtension.getDatabaseFactory().getDatabase(fakeClusterFile);

        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTransactionTimeoutMillis(100L)
                .build();
        try (FDBRecordContext context = fakeFdb.openContext(config)) {
            assertEquals(100L, context.getTimeoutMillis());
            assertThrows(FDBExceptions.FDBStoreTransactionTimeoutException.class, context::getReadVersion);
        }
    }

    @Test
    public void warnAndCloseOldTrackedOpenContexts() {
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTransactionId("leaker")
                .setTrackOpen(true)
                .build();
        fdb.openContext(config);
        int count = fdb.warnAndCloseOldTrackedOpenContexts(0);
        assertEquals(1, count, "should have left one context open");
    }

    @Test
    public void contextExecutor() {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        final int myThreadId = ThreadId.get();

        try {
            factory.setContextExecutor(exec -> new ThreadIdRestoringExecutor(exec, myThreadId));
            factory.clear();


            FDBDatabase database = dbExtension.getDatabase();
            try (FDBRecordContext context = database.openContext()) {
                context.ensureActive().get(new byte[] { 0 }).thenAccept( value -> {
                    assertEquals(myThreadId, ThreadId.get());
                }).join();
            }
        } finally {
            factory.setContextExecutor(Function.identity());
        }
    }

    static class ThreadIdRestoringExecutor extends TaskNotifyingExecutor {
        private int threadId;

        public ThreadIdRestoringExecutor(@Nonnull Executor executor, int threadId) {
            super(executor);
            this.threadId = threadId;
        }

        @Override
        public void beforeTask() {
            ThreadId.set(threadId);
        }

        @Override
        public void afterTask() {
        }
    }

    private static class ThreadId {
        private static final AtomicInteger nextId = new AtomicInteger(0);

        private static final ThreadLocal<Integer> threadId =
                new ThreadLocal<Integer>() {
                    @Override protected Integer initialValue() {
                        return nextId.getAndIncrement();
                    }
                };

        public static int get() {
            return threadId.get();
        }

        private static void set(int id) {
            threadId.set(id);
        }
    }
}
