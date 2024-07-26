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

import com.apple.foundationdb.record.util.pair.ImmutablePair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The {@link AsyncLockRegistry} keeps track of locking over resources in the context of a single transaction. In
 * essence, it maps a resource (by means of a {@link LockIdentifier}) to a data-structure that keeps account of
 * outstanding read and write tasks on that resource.
 *
 * The consumer interacts with the registry through {@link AsyncLockRegistry#getReadLock} and
 * {@link AsyncLockRegistry#getWriteLock} that schedule tasks and return wait {@link CompletableFuture}s. These methods
 * take in a resource identifier {@link LockIdentifier} and a taskLock.The taskLock is a {@link CompletableFuture} that
 * is required to be marked complete by the caller when the task is done to "leave" the lock. The methods also return a
 * waitLock {@link CompletableFuture} that itself get marked complete when all dependant backlog tasks for the resource
 * has completed.
 *
 * Hence, the caller can schedule a task as:
 * <pre>{@code
 *      final var taskLock = new CompletableFuture<Void>();
 *      registry.getWriteLock(identifier, taskLock)
 *          .thenComposeAsync(ignore -> {
 *              // do your task...
 *          })
 *          .thenRun(() -> taskLock.complete(null));
 * }</pre>
 *
 */
public class AsyncLockRegistry {

    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private static final AsyncLock NO_LOCKING_LOCK = new AsyncLock(COMPLETED_FUTURE, COMPLETED_FUTURE);

    /**
     * Tuple-based identifier used to locate a resource in the {@link AsyncLockRegistry}.
     */
    public static class LockIdentifier {
        @Nonnull
        private final Subspace lockingSubspace;

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

        @Override
        public int hashCode() {
            return Objects.hashCode(lockingSubspace);
        }
    }

    /**
     * Structure to keep the current state of requested locks on a particular resource by means of read and write
     * {@link CompletableFuture}s on backlog tasks.
     */
    public static class AsyncLock {
        private final CompletableFuture<Void> reads;
        private final CompletableFuture<Void> writes;

        private AsyncLock(@Nonnull CompletableFuture<Void> reads, @Nonnull CompletableFuture<Void> writes) {
            this.reads = reads;
            this.writes = writes;
        }

        /**
         * Constructs a new {@link AsyncLock} from the calling lock by stacking the new read future to its backlog tasks.
         *
         * @param taskFuture the {@link CompletableFuture} to complete when the lock has to be released.
         *
         * @return A pair of the new {@link AsyncLock} and the {@link CompletableFuture} that is completed when the
         * backlog tasks are done and the current new read task can begin executing. The new read task can begin only after
         * all the existing write tasks have been completed, to maintain exclusivity. However, the read task can begin
         * concurrently with other read tasks that themselves should start after all write tasks.
         */
        private Pair<CompletableFuture<Void>, AsyncLock> withNewRead(CompletableFuture<Void> taskFuture) {
            final var waitFuture = writes;
            final var newReadFuture = CompletableFuture.allOf(reads, waitFuture.thenCompose(ignore -> taskFuture));
            return ImmutablePair.of(waitFuture, new AsyncLock(newReadFuture, writes));
        }

        /**
         * Constructs a new {@link AsyncLock} from the calling lock by stacking the new write future to its backlog tasks.
         *
         * @param taskFuture the {@link CompletableFuture} to complete when the lock has to be released.
         *
         * @return A pair of the new {@link AsyncLock} and the {@link CompletableFuture} that is completed when the
         * backlog tasks are done and the current new write task can begin executing. The new write task can begin only after
         * all the existing write tasks have been completed, to maintain exclusivity. It should also wait for one of more
         * read tasks that can happen concurrently.
         */
        private Pair<CompletableFuture<Void>, AsyncLock> withNewWrite(CompletableFuture<Void> taskFuture) {
            final var waitFuture = CompletableFuture.allOf(reads, writes);
            final var newWriteFuture = waitFuture.thenCompose(ignore -> taskFuture);
            return ImmutablePair.of(waitFuture, new AsyncLock(COMPLETED_FUTURE, newWriteFuture));
        }
    }

    Map<LockIdentifier, AtomicReference<AsyncLock>> heldLocks = new ConcurrentHashMap<>();

    public CompletableFuture<Void> getReadLock(@Nonnull LockIdentifier identifier, CompletableFuture<Void> taskFuture) {
        return updateRefAndGetFuture(identifier, (parentLock) -> parentLock.withNewRead(taskFuture));
    }

    public CompletableFuture<Void> getWriteLock(@Nonnull LockIdentifier identifier, CompletableFuture<Void> taskFuture) {
        return updateRefAndGetFuture(identifier, (parentLock) -> parentLock.withNewWrite(taskFuture));
    }

    private CompletableFuture<Void> updateRefAndGetFuture(@Nonnull LockIdentifier identifier, Function<AsyncLock, Pair<CompletableFuture<Void>, AsyncLock>> getNewLock) {
        final var parentLockRef = heldLocks.computeIfAbsent(identifier, ignore -> new AtomicReference<>(NO_LOCKING_LOCK));
        AsyncLock parentLock;
        Pair<CompletableFuture<Void>, AsyncLock> waitFutureAndNewLock;
        do {
            parentLock = parentLockRef.get();
            waitFutureAndNewLock = getNewLock.apply(parentLock);
        } while (!parentLockRef.compareAndSet(parentLock, waitFutureAndNewLock.getRight()));
        return waitFutureAndNewLock.getLeft();
    }
}
