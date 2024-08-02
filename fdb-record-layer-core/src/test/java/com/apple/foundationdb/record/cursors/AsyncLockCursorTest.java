/*
 * AsyncLockCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.AsyncLockRegistry;
import com.apple.foundationdb.record.AsyncLockRegistryTest;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for the {@link AsyncLockCursor}.
 * Requires FDB only to initialize a {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}.
 */
@Tag(Tags.RequiresFDB)
public class AsyncLockCursorTest {

    @RegisterExtension
    protected final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @RegisterExtension
    protected final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    protected FDBDatabase fdb;
    final AsyncLockRegistry.LockIdentifier identifier = new AsyncLockRegistry.LockIdentifier(new Subspace(Tuple.from(1, 2, 3)));

    @BeforeEach
    void initDatabaseAndPath() {
        fdb = dbExtension.getDatabase();
    }

    @Test
    public void asyncLockCursorTest() throws InterruptedException, ExecutionException {
        try (final var context = fdb.openContext()) {
            final var writeLockAndWait1 = acquireWriteLock(context);
            final var readLockAndCursor = getReadAsyncLockCursor(context, () -> new ListCursor<>(ImmutableList.of(1, 2, 3, 4, 5), null));
            final var writeLockAndWait2 = acquireWriteLock(context);

            // check that the first write don't wait
            AsyncLockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
            // check that the cursor is not ready
            AsyncLockRegistryTest.checkWaiting(ImmutableList.of(readLockAndCursor.getRight()));
            // check that the second write is waiting
            AsyncLockRegistryTest.checkWaiting(ImmutableList.of(writeLockAndWait2.getRight()));

            // complete the first write and check that cursor is ready and all result futures complete
            writeLockAndWait1.getLeft().get().release();
            AsyncLockRegistryTest.checkAllCompletedNormally(ImmutableList.of(readLockAndCursor.getRight()));
            final var cursor = readLockAndCursor.getRight().get();
            final var futures = IntStream.rangeClosed(1, 5).mapToObj(ignore -> cursor.onNext()).collect(Collectors.toList());
            AsyncLockRegistryTest.checkAllCompletedNormally(futures);

            // read the cursor to get no result
            Assertions.assertFalse(cursor.onNext().get().hasNext());
            // check that the other write don't wait
            AsyncLockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
        }
    }


    @Test
    public void asyncLockCursorPreemptiveReleaseTest() throws InterruptedException, ExecutionException {
        try (final var context = fdb.openContext()) {
            final var writeLockAndWait1 = acquireWriteLock(context);
            final var readLockAndCursor = getReadAsyncLockCursor(context, () -> new ListCursor<>(ImmutableList.of(1, 2, 3, 4, 5), null));

            // check that the first write don't wait
            AsyncLockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
            // check that the cursor is not ready
            AsyncLockRegistryTest.checkWaiting(ImmutableList.of(readLockAndCursor.getRight()));

            // complete the first write and check that cursor is ready and all result futures complete
            writeLockAndWait1.getLeft().get().release();
            AsyncLockRegistryTest.checkAllCompletedNormally(ImmutableList.of(readLockAndCursor.getRight()));
            final var cursor = readLockAndCursor.getRight().get();
            final var futures = IntStream.rangeClosed(1, 3).mapToObj(ignore -> cursor.onNext()).collect(Collectors.toList());
            AsyncLockRegistryTest.checkAllCompletedNormally(futures);

            // release the lock preemptively
            readLockAndCursor.getLeft().get().release();

            // read the cursor with released lock
            Assertions.assertThrows(RecordCoreException.class, () -> cursor.onNext().get());
        }
    }

    private NonnullPair<AtomicReference<AsyncLockRegistry.AsyncLock>, CompletableFuture<Void>> acquireWriteLock(@Nonnull FDBRecordContext context) {
        final var asyncLockRef = new AtomicReference<AsyncLockRegistry.AsyncLock>();
        return NonnullPair.of(asyncLockRef,
                context.acquireWriteLock(identifier, (lock) -> {
                    asyncLockRef.set(lock);
                    return null;
                }));
    }

    private NonnullPair<AtomicReference<AsyncLockRegistry.AsyncLock>, CompletableFuture<Void>> acquireReadLock(@Nonnull FDBRecordContext context) {
        final var asyncLockRef = new AtomicReference<AsyncLockRegistry.AsyncLock>();
        return NonnullPair.of(asyncLockRef,
                context.acquireReadLock(identifier, (lock) -> {
                    asyncLockRef.set(lock);
                    return null;
                }));
    }

    private <T> NonnullPair<AtomicReference<AsyncLockRegistry.AsyncLock>, CompletableFuture<AsyncLockCursor<T>>> getReadAsyncLockCursor(@Nonnull FDBRecordContext context, Supplier<RecordCursor<T>> innerSupplier) {
        final var asyncLockRef = new AtomicReference<AsyncLockRegistry.AsyncLock>();
        return NonnullPair.of(asyncLockRef, context.acquireReadLock(identifier, (asyncLock) -> {
            asyncLockRef.set(asyncLock);
            return new AsyncLockCursor<>(asyncLock, innerSupplier.get());
        }));
    }
}
