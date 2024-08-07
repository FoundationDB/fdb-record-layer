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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The {@link LockRegistry} keeps track of locking over resources in the context of a single transaction. In essence,
 * it maps a resource (by means of a {@link LockIdentifier}) to the latest lock that has been assigned on that resource.
 *
 * There are 2 set of methods to interact with the registry:
 * <ul>
 *     <li>acquire: {@link LockRegistry#acquireReadLock} and {@link LockRegistry#acquireWriteLock}</li>
 *     <li>do: {@link LockRegistry#doWithReadLock} and {@link LockRegistry#doWithWriteLock}</li>
 * </ul>
 * While the acquire methods hands over the lock to the consumer, thereby expecting the consumer to release the lock
 * on completion of the task, the do method abstracts the lock management. Typically, acquire is only to be used in case
 * if the task requires cannot be completed atomically and requires persisting the lock to be released later. For
 * everything else, do methods offer a more abstract way of dealing with locks.
 *
 * The sample usage with do method can be:
 * <pre>{@code
 *      final CompletableFuture<Void> task = registry.doWithReadLock(identifier, () -> {
 *          // do your task...
 *      });
 * }</pre>
 *
 * The usage with the acquire methods is more involved as it expects the caller to provide a function that will hand off
 * the produced lock object to its owner for managing. An illustrative example:
 * <pre>{@code
 *      final AtomicReference<AsyncLock> lockRef = new AtomicReference<>();
 *      final CompletableFuture<Void> lock = registry.acquireWithReadLock(identifier, asyncLock -> {
 *          // hand off the lock to owner
 *          lockRef.set(asyncLock)
 *      });
 *      lock.thenCompose(ignore -> {
 *          // do your task...
 *      }).whenComplete((ignore, error) -> {
 *          lockRef.get().release();
 *      })
 *
 * NOTE: Since the lock is handed-off to the owner, care should be taken to release it after the task has been completed.
 *
 * }</pre> */
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
     * {@link CompletableFuture} of the owning object that will be completed after the access has been granted followed
     * by the operation being completed.

     * @param <T> owning class for the granted lock object.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation the function that performs the hand-off of the {@link AsyncLock} to the owning object.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public <T> CompletableFuture<T> acquireReadLock(@Nonnull LockIdentifier id, Function<AsyncLock, T> operation) {
        return acquire(id, operation, AsyncLock::withNewRead);
    }

    /**
     * Attempts to get access for performing write operations on the resource represented by the id and returns a
     * {@link CompletableFuture} of the owning object that will be completed after the access has been granted followed
     * by the operation being completed.

     * @param <T> owning class for the granted lock object.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation the function that performs the hand-off of the {@link AsyncLock} to the owning object.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public <T> CompletableFuture<T> acquireWriteLock(@Nonnull LockIdentifier id, Function<AsyncLock, T> operation) {
        return acquire(id, operation, AsyncLock::withNewWrite);
    }

    private <T> CompletableFuture<T> acquire(@Nonnull LockIdentifier id, Function<AsyncLock, T> operation, UnaryOperator<AsyncLock> getNewLock) {
        final AsyncLock lock = updateRefAndGetNewLock(id, getNewLock);
        final long startTime = System.currentTimeMillis();
        return lock.onAcquired().thenApply(ignore -> {
            if (timer != null) {
                timer.record(FDBStoreTimer.DetailEvents.LOCKS_WAIT, System.currentTimeMillis() - startTime);
            }
            return operation.apply(lock);
        });
    }

    /**
     * Attempts to get access for read access on the resource represented by the id to perform an atomic operation. It
     * leaves the lock after the operation is completed.

     * @param <T> type of the value returned from the future of operation.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation to be called after the access is granted.
     * @return the {@link CompletableFuture} of T which is the result of the operation.
     */
    public <T> CompletableFuture<T> doWithReadLock(LockIdentifier id, Supplier<CompletableFuture<T>> operation) {
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
    public <T> CompletableFuture<T> doWithWriteLock(LockIdentifier id, Supplier<CompletableFuture<T>> operation) {
        return doOp(id, operation, AsyncLock::withNewWrite);
    }

    private <T> CompletableFuture<T> doOp(LockIdentifier id, Supplier<CompletableFuture<T>> operation, UnaryOperator<AsyncLock> getNewLock) {
        final AsyncLock lock = updateRefAndGetNewLock(id, getNewLock);
        final long startTime = System.currentTimeMillis();
        return lock.onAcquired().thenCompose(ignore -> {
            if (timer != null) {
                timer.record(FDBStoreTimer.DetailEvents.LOCKS_WAIT, System.currentTimeMillis() - startTime);
            }
            return operation.get();
        }).whenComplete((ignore, err) -> lock.release());
    }

    private AsyncLock updateRefAndGetNewLock(@Nonnull LockIdentifier identifier, UnaryOperator<AsyncLock> getNewLock) {
        final long startTime = System.currentTimeMillis();
        final AtomicReference<AsyncLock> parentLockRef = heldLocks.computeIfAbsent(identifier, ignore ->
                new AtomicReference<>(new AsyncLock(timer, AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE)));
        final AsyncLock newLock = parentLockRef.updateAndGet(getNewLock);
        if (timer != null) {
            timer.record(FDBStoreTimer.DetailEvents.LOCKS_REGISTER, System.currentTimeMillis() - startTime);
        }
        return newLock;
    }
}
