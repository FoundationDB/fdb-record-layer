/*
 * FutureAutoClose.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.runners;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A helper that completes futures once closed.
 * <p>
 * This is meant to be used together with other {@link FDBDatabaseRunner}s and help manage any {@link CompletableFuture} instances
 * once the runner closes. This helper should be lifecycle coupled to the runners it accompanies.
 * <p>
 * Futures created externally can be registered with this class. This class can also create new futures (and register them).
 * When {@link #close()} is called, any registered {@link CompletableFuture} which is still incomplete will be completed exceptionally
 * with a {@link FDBDatabaseRunner.RunnerClosed} exception.
 * <p>
 * It is the responsibility of the users of the runner to ensure futures that it creates are registered.
 * <p>
 * Normally, only the top level (root) of the future chain needs registration. Completing this future
 * will cause the completion to trickle down to the dependent futures.
 */
@API(API.Status.INTERNAL)
public class FutureAutoClose implements AutoCloseable {
    @Nonnull
    private final List<CompletableFuture<?>> futuresToClose;
    private boolean closed;

    public FutureAutoClose() {
        futuresToClose = new ArrayList<>();
        closed = false;
    }

    public <T> CompletableFuture<T> newFuture() {
        return registerFuture(new CompletableFuture<>());
    }

    public synchronized <T> CompletableFuture<T> registerFuture(CompletableFuture<T> future) {
        if (isClosed()) {
            final FDBDatabaseRunner.RunnerClosed exception = new FDBDatabaseRunner.RunnerClosed();
            future.completeExceptionally(exception);
            throw exception;
        }
        futuresToClose.removeIf(CompletableFuture::isDone);
        futuresToClose.add(future);
        return future;
    }

    @Override
    public synchronized void close() {
        if (!isClosed()) {
            closed = true;

            if (!futuresToClose.stream().allMatch(CompletableFuture::isDone)) {
                final Exception exception = new FDBDatabaseRunner.RunnerClosed();
                for (CompletableFuture<?> future : futuresToClose) {
                    future.completeExceptionally(exception);
                }
            }
            futuresToClose.clear();
        }
    }

    public boolean isClosed() {
        return closed;
    }
}
