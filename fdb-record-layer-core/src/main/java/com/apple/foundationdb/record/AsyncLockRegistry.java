/*
 * AsyncLockRegistry.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The {@link AsyncLockRegistry} keeps track of locking over resources in the context of a single transaction. In
 * essence, it maps a resource (by means of a {@link LockIdentifier}) to the latest {@link AsyncLock} that has been
 * assigned on that resource.
 *
 * There are 2 set of methods to interact with the registry:
 * <ul>
 *     <li>acquire: {@link AsyncLockRegistry#acquireReadLock} and {@link AsyncLockRegistry#acquireWriteLock}</li>
 *     <li>do: {@link AsyncLockRegistry#doWithReadLock} and {@link AsyncLockRegistry#doWithWriteLock}</li>
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
 * the produced {@link AsyncLock} to its owner for managing. An illustrative example:
 * <pre>{@code
 *      final var lockRef = new AtomicReference<AsyncLock>();
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
public class AsyncLockRegistry {

    private static final AsyncLock NO_LOCKING_LOCK = new AsyncLock(AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE, AsyncUtil.DONE);

    @Nonnull
    private final Map<LockIdentifier, AtomicReference<AsyncLock>> heldLocks = new ConcurrentHashMap<>();

    /**
     * Tuple-based identifier used to locate a resource in the {@link AsyncLockRegistry}.
     */
    public static class LockIdentifier {
        @Nonnull
        private final Subspace lockingSubspace;
        private final Supplier<Integer> memoizedHashCode = Suppliers.memoize(this::calculateHashCode);

        public LockIdentifier(@Nonnull Subspace lockingSubspace) {
            this.lockingSubspace = lockingSubspace;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof LockIdentifier)) {
                return false;
            }
            return lockingSubspace.equals(((LockIdentifier) obj).lockingSubspace);
        }

        private int calculateHashCode() {
            return Objects.hashCode(lockingSubspace);
        }

        @Override
        public int hashCode() {
            return memoizedHashCode.get();
        }
    }

    /**
     * Structure to maintain the current state of requested locks on a particular resource.
     */
    public static class AsyncLock {
        private final CompletableFuture<Void> pendingReads;
        private final CompletableFuture<Void> pendingWrites;
        private final CompletableFuture<Void> taskFuture;
        private final CompletableFuture<Void> waitFuture;

        private AsyncLock(@Nonnull CompletableFuture<Void> pendingReads, @Nonnull CompletableFuture<Void> pendingWrites,
                          @Nonnull CompletableFuture<Void> taskFuture, @Nonnull CompletableFuture<Void> waitFuture) {
            this.pendingReads = pendingReads;
            this.pendingWrites = pendingWrites;
            this.taskFuture = taskFuture;
            this.waitFuture = waitFuture;
        }

        /**
         * Constructs a new {@link AsyncLock} from the calling lock by stacking the new read future to its pending tasks.
         *
         * @return A pair of the new {@link AsyncLock} to be handed over to the consumer.
         */
        private AsyncLock withNewRead() {
            final var waitFuture = pendingWrites;
            final var taskFuture = new CompletableFuture<Void>();
            final var newPendingReads = CompletableFuture.allOf(this.pendingReads, waitFuture.thenCompose(ignore -> taskFuture));
            return new AsyncLock(newPendingReads, this.pendingWrites, taskFuture, waitFuture);
        }

        /**
         * Constructs a new {@link AsyncLock} from the calling lock by stacking the new write future to its pending tasks.
         *
         * @return A pair of the new {@link AsyncLock} to be handed over to the consumer.
         */
        private AsyncLock withNewWrite() {
            final var waitFuture = CompletableFuture.allOf(this.pendingReads, this.pendingWrites);
            final var taskFuture = new CompletableFuture<Void>();
            final var newPendingWrites = waitFuture.thenCompose(ignore -> taskFuture);
            return new AsyncLock(AsyncUtil.DONE, newPendingWrites, taskFuture, waitFuture);
        }

        private CompletableFuture<Void> asyncWait() {
            return waitFuture;
        }

        /**
         * Checks if the lock is still not released. This is conceptually different from the scenario when the lock is
         * granted. The access to the resource is granted only after {@link AsyncLock#asyncWait()} is completed and
         * this method returns false.
         *
         * @return {@code true} if the lock is still held, else {@code false}.
         */
        public boolean isLockNotReleased() {
            return !taskFuture.isDone();
        }

        /**
         * Releases the lock. The owner of this {@link AsyncLock} should be calling this method to signal completion
         * of their task for which they required the lock.
         */
        public void release() {
            taskFuture.complete(null);
        }
    }

    /**
     * Attempts to get access for performing read operations on the resource represented by the id and returns a
     * {@link CompletableFuture} of the owning object that will be completed after the access has been granted followed
     * by the operation being completed.

     * @param <T> owning class for the granted {@link AsyncLock}.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation the function that performs the hand-off of the {@link AsyncLock} to the owning object.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public <T> CompletableFuture<T> acquireReadLock(@Nonnull LockIdentifier id, Function<AsyncLock, T> operation) {
        final var lock = updateRefAndGetNewLock(id, AsyncLock::withNewRead);
        return lock.asyncWait().thenApply(ignore -> operation.apply(lock));
    }

    /**
     * Attempts to get access for performing write operations on the resource represented by the id and returns a
     * {@link CompletableFuture} of the owning object that will be completed after the access has been granted followed
     * by the operation being completed.

     * @param <T> owning class for the granted {@link AsyncLock}.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation the function that performs the hand-off of the {@link AsyncLock} to the owning object.
     * @return the {@link CompletableFuture} of T that will be produced after the lock access has been granted.
     */
    public <T> CompletableFuture<T> acquireWriteLock(@Nonnull LockIdentifier id, Function<AsyncLock, T> operation) {
        final var lock = updateRefAndGetNewLock(id, AsyncLock::withNewWrite);
        return lock.asyncWait().thenApply(ignore -> operation.apply(lock));
    }

    /**
     * Attempts to get access for read access on the resource represented by the id to perform an atomic operation. It
     * leaves the lock after the operation is completed.

     * @param <T> type of the value returned from the future of operation.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation to be called after the access is granted.
     * @return the {@link CompletableFuture} of T which is the result of the operaion.
     */
    public <T> CompletableFuture<T> doWithReadLock(LockIdentifier id, Supplier<CompletableFuture<T>> operation) {
        final var lock = updateRefAndGetNewLock(id, AsyncLock::withNewRead);
        return lock.asyncWait().thenCompose(ignore -> operation.get())
                .whenComplete((ignore, err) -> lock.release());
    }

    /**
     * Attempts to get access for write access on the resource represented by the id to perform an atomic operation. It
     * leaves the lock after the operation is completed.

     * @param <T> type of the value returned from the future of operation.
     * @param id the {@link LockIdentifier} for the resource.
     * @param operation to be called after the access is granted.
     * @return the {@link CompletableFuture} of T which is the result of the operaion.
     */
    public <T> CompletableFuture<T> doWithWriteLock(LockIdentifier id, Supplier<CompletableFuture<T>> operation) {
        final var lock = updateRefAndGetNewLock(id, AsyncLock::withNewWrite);
        return lock.asyncWait().thenCompose(ignore -> operation.get())
                .whenComplete((ignore, err) -> lock.release());
    }

    private AsyncLock updateRefAndGetNewLock(@Nonnull LockIdentifier identifier, UnaryOperator<AsyncLock> getNewLock) {
        final var parentLockRef = heldLocks.computeIfAbsent(identifier, ignore -> new AtomicReference<>(NO_LOCKING_LOCK));
        return parentLockRef.updateAndGet(getNewLock);
    }
}
