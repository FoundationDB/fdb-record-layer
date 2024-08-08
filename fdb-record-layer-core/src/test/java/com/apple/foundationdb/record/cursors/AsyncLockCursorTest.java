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

import com.apple.foundationdb.record.LockRegistryTest;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.locking.AsyncLock;
import com.apple.foundationdb.record.locking.LockIdentifier;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
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
public class AsyncLockCursorTest extends FDBRecordStoreTestBase {

    final LockIdentifier identifier = new LockIdentifier(new Subspace(Tuple.from(1, 2, 3)));

    @Test
    public void asyncLockCursorTest() throws InterruptedException, ExecutionException {
        try (final FDBRecordContext context = openContext()) {
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait1 = acquireWriteLock(context);
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<AsyncLockCursor<Integer>>> readLockAndCursor = getReadAsyncLockCursor(context, () -> new ListCursor<>(ImmutableList.of(1, 2, 3, 4, 5), null));
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait2 = acquireWriteLock(context);

            // check that the first write don't wait
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
            // check that the cursor is not ready
            LockRegistryTest.checkWaiting(ImmutableList.of(readLockAndCursor.getRight()));
            // check that the second write is waiting
            LockRegistryTest.checkWaiting(ImmutableList.of(writeLockAndWait2.getRight()));

            // complete the first write and check that cursor is ready and all result futures complete
            writeLockAndWait1.getLeft().get().release();
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(readLockAndCursor.getRight()));
            final AsyncLockCursor<Integer> cursor = readLockAndCursor.getRight().get();
            final List<CompletableFuture<RecordCursorResult<Integer>>> futures = IntStream.rangeClosed(1, 5).mapToObj(ignore -> cursor.onNext()).collect(Collectors.toList());
            LockRegistryTest.checkAllCompletedNormally(futures);

            // read the cursor to get no result
            Assertions.assertFalse(cursor.onNext().get().hasNext());
            // check that the other write don't wait
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
        }
    }

    @Test
    public void asyncLockCursorPreemptiveReleaseTest() throws InterruptedException, ExecutionException {
        try (final FDBRecordContext context = fdb.openContext()) {
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait1 = acquireWriteLock(context);
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<AsyncLockCursor<Integer>>> readLockAndCursor = getReadAsyncLockCursor(context, () -> new ListCursor<>(ImmutableList.of(1, 2, 3, 4, 5), null));

            // check that the first write don't wait
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
            // check that the cursor is not ready
            LockRegistryTest.checkWaiting(ImmutableList.of(readLockAndCursor.getRight()));

            // complete the first write and check that cursor is ready and all result futures complete
            writeLockAndWait1.getLeft().get().release();
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(readLockAndCursor.getRight()));
            final AsyncLockCursor<Integer> cursor = readLockAndCursor.getRight().get();
            final List<CompletableFuture<RecordCursorResult<Integer>>> futures = IntStream.rangeClosed(1, 3).mapToObj(ignore -> cursor.onNext()).collect(Collectors.toList());
            LockRegistryTest.checkAllCompletedNormally(futures);

            // release the lock preemptively
            readLockAndCursor.getLeft().get().release();

            // read the cursor with released lock
            Assertions.assertThrows(RecordCoreException.class, () -> cursor.onNext().get());
        }
    }

    @Test
    public void asyncLockCursorWithLimitTest() throws InterruptedException, ExecutionException {
        try (final FDBRecordContext context = fdb.openContext()) {
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait1 = acquireWriteLock(context);
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<AsyncLockCursor<Integer>>> readLockAndCursor = getReadAsyncLockCursor(context, () -> new ListCursor<>(ImmutableList.of(1, 2, 3, 4, 5), null));
            final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait2 = acquireWriteLock(context);

            // check that the first write don't wait
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
            // check that the cursor is not ready
            LockRegistryTest.checkWaiting(ImmutableList.of(readLockAndCursor.getRight()));

            // complete the first write and check that cursor is ready and all result futures complete
            writeLockAndWait1.getLeft().get().release();
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(readLockAndCursor.getRight()));
            final RecordCursor<Integer> limitCursor = readLockAndCursor.getRight().get().limitRowsTo(2);
            final List<CompletableFuture<RecordCursorResult<Integer>>> futures = IntStream.rangeClosed(1, 3).mapToObj(ignore -> limitCursor.onNext()).collect(Collectors.toList());
            LockRegistryTest.checkAllCompletedNormally(futures);

            // check that the last result returns nothing due to over limit
            final RecordCursorResult<Integer> lastResult = futures.get(2).get();
            Assertions.assertFalse(lastResult.hasNext());
            Assertions.assertEquals(lastResult.getNoNextReason(), RecordCursor.NoNextReason.RETURN_LIMIT_REACHED);

            // Check that the other write is not waiting
            LockRegistryTest.checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
        }
    }

    private NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> acquireWriteLock(@Nonnull FDBRecordContext context) {
        final AtomicReference<AsyncLock> asyncLockRef = new AtomicReference<>();
        return NonnullPair.of(asyncLockRef,
                context.acquireWriteLock(identifier).thenApply(lock -> {
                    asyncLockRef.set(lock);
                    return null;
                }));
    }

    private <T> NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<AsyncLockCursor<T>>> getReadAsyncLockCursor(@Nonnull FDBRecordContext context, Supplier<RecordCursor<T>> innerSupplier) {
        final AtomicReference<AsyncLock> asyncLockRef = new AtomicReference<>();
        return NonnullPair.of(asyncLockRef, context.acquireReadLock(identifier).thenApply(asyncLock -> {
            asyncLockRef.set(asyncLock);
            return new AsyncLockCursor<>(asyncLock, innerSupplier.get());
        }));
    }
}
