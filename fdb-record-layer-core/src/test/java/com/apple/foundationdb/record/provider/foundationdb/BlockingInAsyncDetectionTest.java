/*
 * BlockingInAsyncDetectionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(Tags.RequiresFDB)
class BlockingInAsyncDetectionTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @Test
    void testAsyncDetection() {
        // this should be the default
        assertEquals(
                BlockingInAsyncDetection.IGNORE_COMPLETE_EXCEPTION_BLOCKING,
                dbExtension.getDatabaseFactory().getBlockingInAsyncDetectionSupplier().get());
    }

    @Test
    void testBlockingInAsyncException() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.IGNORE_COMPLETE_EXCEPTION_BLOCKING);

        // Make sure that we aren't holding on to previously created databases
        factory.clear();

        FDBDatabase database = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        assertEquals(BlockingInAsyncDetection.IGNORE_COMPLETE_EXCEPTION_BLOCKING, database.getBlockingInAsyncDetection());
        assertThrows(BlockingInAsyncException.class, () -> callAsyncBlocking(database));
    }

    @Test
    void testBlockingInAsyncWarning() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.IGNORE_COMPLETE_WARN_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_IN_ASYNC_CONTEXT_MESSAGE,
                () -> {
                    callAsyncBlocking(database, true);
                    return null;
                });
    }

    @Test
    void testCompletedBlockingInAsyncWarning() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_IN_ASYNC_CONTEXT_MESSAGE,
                () -> database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK,
                        CompletableFuture.supplyAsync(() ->
                                database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_ERROR_CHECK, CompletableFuture.completedFuture(10L)))));
    }

    @Test
    void testBlockingCreatingAsyncDetection() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        TestHelpers.assertLogs(FDBDatabase.class, FDBDatabase.BLOCKING_RETURNING_ASYNC_MESSAGE,
                () -> returnAnAsync(database, MoreAsyncUtil.delayedFuture(200L, TimeUnit.MILLISECONDS, database.getScheduledExecutor())));
    }

    @Test
    void testCompletedBlockingCreatingAsyncDetection() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setBlockingInAsyncDetection(BlockingInAsyncDetection.WARN_COMPLETE_EXCEPTION_BLOCKING);
        factory.clear();

        FDBDatabase database = factory.getDatabase(FDBTestEnvironment.randomClusterFile());
        TestHelpers.assertDidNotLog(FDBDatabase.class, FDBDatabase.BLOCKING_RETURNING_ASYNC_MESSAGE,
                () -> returnAnAsync(database, CompletableFuture.completedFuture(10L)));
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
        }
    }

}
