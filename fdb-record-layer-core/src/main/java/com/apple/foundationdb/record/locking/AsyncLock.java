/*
 * AsyncLock.java
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
import java.util.concurrent.CompletableFuture;

/**
 * The data-structure to maintain the current state of requested locks on a particular resource.
 */
@API(API.Status.INTERNAL)
public class AsyncLock {
    // All the read tasks that are pending up till the current lock instance.
    @Nonnull
    private final CompletableFuture<Void> pendingReads;
    // All the write tasks that are pending up till the current lock instance.
    @Nonnull
    private final CompletableFuture<Void> pendingWrites ;
    // Current task, represented by this instance.
    @Nonnull
    private final CompletableFuture<Void> taskFuture;
    // waiting tasks that the current lock instance is waiting upon.
    @Nonnull
    private final CompletableFuture<Void> waitFuture;
    @Nullable
    private final StoreTimer timer;

    AsyncLock(@Nullable final StoreTimer timer, @Nonnull final CompletableFuture<Void> pendingReads,
              @Nonnull final CompletableFuture<Void> pendingWrites, @Nonnull final CompletableFuture<Void> taskFuture,
              @Nonnull final CompletableFuture<Void> waitFuture) {
        this.timer = timer;
        this.pendingReads = pendingReads;
        this.pendingWrites = pendingWrites;
        this.taskFuture = taskFuture;
        this.waitFuture = waitFuture;
        if (timer != null) {
            timer.increment(FDBStoreTimer.Counts.LOCKS_ATTEMPTED);
        }
    }

    /**
     * Constructs a new {@link AsyncLock} from the calling lock by stacking the new read future to its pending tasks.
     *
     * @return A pair of the new {@link AsyncLock} to be handed over to the consumer.
     */
    AsyncLock withNewRead() {
        final CompletableFuture<Void> waitFuture = pendingWrites;
        final CompletableFuture<Void> taskFuture = new CompletableFuture<>();
        final CompletableFuture<Void> newPendingReads = CompletableFuture.allOf(this.pendingReads, waitFuture.thenCompose(ignore -> taskFuture));
        return new AsyncLock(timer, newPendingReads, this.pendingWrites, taskFuture, waitFuture);
    }

    /**
     * Constructs a new {@link AsyncLock} from the calling lock by stacking the new write future to its pending tasks.
     *
     * @return A pair of the new {@link AsyncLock} to be handed over to the consumer.
     */
    AsyncLock withNewWrite() {
        final CompletableFuture<Void> waitFuture = CompletableFuture.allOf(this.pendingReads, this.pendingWrites);
        final CompletableFuture<Void> taskFuture = new CompletableFuture<>();
        final CompletableFuture<Void> newPendingWrites = waitFuture.thenCompose(ignore -> taskFuture);
        return new AsyncLock(timer, AsyncUtil.DONE, newPendingWrites, taskFuture, waitFuture);
    }

    /**
     * Returns a {@link CompletableFuture} that gets completed when the lock is acquired.
     *
     * @return a {@link CompletableFuture}.
     */
    CompletableFuture<Void> onAcquired() {
        return waitFuture;
    }

    /**
     * Checks if the lock is released.
     *
     * @return {@code true} if the lock is still held, else {@code false}.
     */
    public boolean isLockReleased() {
        return taskFuture.isDone();
    }

    /**
     * Releases the lock. The owner of this {@link AsyncLock} should be calling this method to signal completion
     * of their task for which they required the lock.
     */
    public void release() {
        if (!taskFuture.isDone()) {
            taskFuture.complete(null);
            if (timer != null) {
                timer.increment(FDBStoreTimer.Counts.LOCKS_RELEASED);
            }
        }
    }
}
