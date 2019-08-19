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
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link FDBRecordContext} class.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordContextTest extends FDBTestBase {
    private static final int ERR_CODE_READ_VERSION_ALREADY_SET = 2010;
    @Nonnull
    private static final FDBDatabase.WeakReadSemantics UNLIMITED_STALE_READ = new FDBDatabase.WeakReadSemantics(0L, Long.MAX_VALUE, true);
    protected FDBDatabase fdb;

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
}
