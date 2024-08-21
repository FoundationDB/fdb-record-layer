/*
 * LockRegistry.java
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

package com.apple.foundationdb.record.locking;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The {@link LockRegistry} keeps track of locking over resources in the context of a single transaction. In essence,
 * it maps a resource (by means of a {@link LockIdentifier}) to the latest lock that has been assigned on that resource.
 * <p>
 * The registry can be requested to grant the lock in either the {@code read} mode or the {@code write} mode. The
 * difference between the 2 modes is as follows:
 * <ul>
 *     <li>{@code write}: This mode gives exclusive access over the resource, i.e., no other read or write operation can
 *     happen concurrently when the lock has been granted in the current context.</li>
 *     <li>{@code read}: This mode gives access over the resource that can be shared by other read locks concurrently.
 *     Hence, it is possible to have multiple threads that have read access to the the resource at a time, but no write
 *     can happen at that time.</li>
 * </ul>
 * Example, for a bunch of read and write locks accepted in an arbitrary order given by r1, r2, r3, w1, w2, r4, r5, w3,
 * we can expect r1, r2, r3 to occur concurrently. w1 'waits' for all of r1, r2, r3 to complete, completion of which
 * itself is waited by w2. After w2 is completed, r4 and r5 can run concurrently.
 * <p>
 * There are 2 set of methods to interact with the registry:
 * <ul>
 *     <li>{@code acquire}: {@link LockRegistry#acquireReadLock} and {@link LockRegistry#acquireWriteLock}</li>
 *     <li>{@code do}: {@link LockRegistry#doWithReadLock} and {@link LockRegistry#doWithWriteLock}</li>
 * </ul>
 * For most cases, the {@code do} methods should be preferred. However, the {@code acquire} methods can be useful if the
 * lock needs to outlast the supplied lambda. For instance, for APIs that want to return a cursor over results that
 * retain the lock for the entire duration of a scan, the {@link #acquireReadLock} method can be used in conjunction
 * with an {@link com.apple.foundationdb.record.cursors.AsyncLockCursor}.
 * <p>
 * The sample usage with {@code do} method can be:
 * <pre>{@code
 *      final CompletableFuture<Void> task = registry.doWithReadLock(identifier, () -> {
 *          // do your task...
 *      });
 * }</pre>
 *
 * The usage with the {@code acquire} methods is more straight-forward as it returns a {@link CompletableFuture} of the
 * held lock which gets completed after the lock has been granted. This could then be chained to perform required
 * operations and lock management. The thing to take note here, the ownership of the 'granted' lock is returned to the
 * caller, who should most likely keep hold of it for the duration of the operation and ultimately 'releases' it when
 * done. An illustrative example:
 * <pre>{@code
 *      final AtomicReference<AsyncLock> lockRef = new AtomicReference<>();
 *      final CompletableFuture<Void> lock = registry.acquireWithReadLock(identifier).thenApply(asyncLock -> {
 *          // persist the lock
 *          lockRef.set(asyncLock)
 *          // do your task...
 *      }).whenComplete((ignore, error) -> {
 *          lockRef.get().release();
 *      })
 * }</pre>
 *
 * Note that when the task is completed, whether normally or with an exception, the lock is explicitly released.
 */
@API(API.Status.EXPERIMENTAL)
public class LockRegistry {

    @Nonnull
    private final Map<LockIdentifier, AtomicReference<AsyncLock>> heldLocks = new ConcurrentHashMap<>();
    @Nullable
    private final StoreTimer timer;

    public LockRegistry(@Nullable final StoreTimer timer) {
        this.timer = timer;
    }

    /**
     * Attempts to get access for performing read operations on the resource represented by the id and returns a
     * {@link CompletableFuture} of the lock after the access has been granted.

     * @param id the {@link LockIdentifier} for the resource.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public CompletableFuture<AsyncLock> acquireReadLock(@Nonnull final LockIdentifier id) {
        return acquire(id, AsyncLock::withNewRead);
    }

    /**
     * Attempts to get access for performing write operations on the resource represented by the id and returns a
     * {@link CompletableFuture} of the lock after the access has been granted.
     *
     * @param id the {@link LockIdentifier} for the resource.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public CompletableFuture<AsyncLock> acquireWriteLock(@Nonnull final LockIdentifier id) {
        return acquire(id, AsyncLock::withNewWrite);
    }

    private CompletableFuture<AsyncLock> acquire(@Nonnull final LockIdentifier id, @Nonnull final UnaryOperator<AsyncLock> getNewLock) {
        final AsyncLock lock = updateRefAndGetNewLock(id, getNewLock);
        if (timer != null) {
            timer.instrument(FDBStoreTimer.DetailEvents.LOCKS_ACQUIRED, lock.onAcquired()).thenApply(ignore -> lock);
        }
        return lock.onAcquired().thenApply(ignore -> lock);
    }

    /**
     * Attempts to get access for read access on the resource represented by the id to perform an atomic operation. It
     * leaves the lock after the operation is completed.

     * @param <T> type of the value returned from the future of operation.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation to be called after the access is granted.
     * @return the {@link CompletableFuture} of T which is the result of the operation.
     */
    public <T> CompletableFuture<T> doWithReadLock(@Nonnull final LockIdentifier id, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return doOp(id, operation, AsyncLock::withNewRead);
    }

    /**
     * Attempts to get access for write access on the resource represented by the id to perform an atomic operation. It
     * leaves the lock after the operation is completed.

     * @param <T> type of the value returned from the future of operation.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation to be called after the access is granted.
     * @return the {@link CompletableFuture} of T which is the result of the operation.
     */
    public <T> CompletableFuture<T> doWithWriteLock(@Nonnull final LockIdentifier id, @Nonnull final Supplier<CompletableFuture<T>> operation) {
        return doOp(id, operation, AsyncLock::withNewWrite);
    }

    private <T> CompletableFuture<T> doOp(@Nonnull final LockIdentifier id, @Nonnull final Supplier<CompletableFuture<T>> operation,
                                          @Nonnull final UnaryOperator<AsyncLock> getNewLock) {
        final AtomicReference<AsyncLock> lockRef = new AtomicReference<>();
        return acquire(id, getNewLock).thenCompose(lock -> {
            lockRef.set(lock);
            return operation.get();
        }).whenComplete((ignore, err) -> lockRef.get().release());
    }

    private AsyncLock updateRefAndGetNewLock(@Nonnull final LockIdentifier identifier, @Nonnull final UnaryOperator<AsyncLock> getNewLock) {
        final long startTime = System.nanoTime();
        final AtomicReference<AsyncLock> parentLockRef = heldLocks.computeIfAbsent(identifier, ignore ->
                new AtomicReference<>(new AsyncLock(timer, AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE)));
        final AsyncLock newLock = parentLockRef.updateAndGet(getNewLock);
        if (timer != null) {
            timer.record(FDBStoreTimer.DetailEvents.LOCKS_REGISTERED, System.nanoTime() - startTime);
        }
        return newLock;
    }
}
