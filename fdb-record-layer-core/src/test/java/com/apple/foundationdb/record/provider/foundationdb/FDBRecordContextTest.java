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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests of the {@link FDBRecordContext} class.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordContextTest extends FDBTestBase {
    // A list of transaction IDs where the left item is the original ID and the right item is the expected
    // sanitized ID. It is a list of pairs rather than a map to support null as the expected value.
    @Nonnull
    private static final List<Pair<String, String>> trIds;
    private static final int ERR_CODE_READ_VERSION_ALREADY_SET = 2010;
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
        fdb = FDBDatabaseFactory.instance().getDatabase();
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
                        assertEquals(1025, fdbException.getCode()); // transaction_cancelled
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
            assertEquals(ERR_CODE_READ_VERSION_ALREADY_SET, fdbE.getCode());
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
                assertEquals(logged && expectedId != null, context.isLogged());
                context.commit();
            } catch (RuntimeException e) {
                fail("unable to set id to " + trId, e);
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
    public void logTransactionAfterGettingAReadVersion() {
        try (FDBRecordContext context = fdb.openContext(null, null, null, FDBTransactionPriority.DEFAULT, "logTransactionAfterGrv")) {
            context.ensureActive().getReadVersion().join();
            context.logTransaction();
            Subspace fakeSubspace = new Subspace(Tuple.from(UUID.randomUUID()));
            context.ensureActive().addWriteConflictRange(fakeSubspace.range().begin, fakeSubspace.range().end);
            context.commit();
        }
    }
}
