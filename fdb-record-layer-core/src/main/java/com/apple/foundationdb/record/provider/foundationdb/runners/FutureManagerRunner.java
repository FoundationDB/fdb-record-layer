/*
 * FutureManagerRunner.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A simple runner that extends {@link TransactionalRunner} by completing futures once the runner closes.
 * Futures created externally can be registered with this runner. This runner can also create new futures (and register them).
 * Once registered, the incomplete futures will be completed exceptionally (with a
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner.RunnerClosed} exception) once the runner closes.
 * It is the responsbility of the users of the runner to ensure futures that it creates are registered.
 * <p>
 * Normally, only the top level (root) of the future chain needs registration. Completing this future
 * will cause the completion to trickle down to the dependent futures.
 */
@API(API.Status.INTERNAL)
public class FutureManagerRunner extends TransactionalRunner {
    @Nonnull
    private final List<CompletableFuture<?>> futuresToClose;

    public FutureManagerRunner(@Nonnull final FDBDatabase database, @Nonnull final FDBRecordContextConfig.Builder contextConfigBuilder) {
        super(database, contextConfigBuilder);
        futuresToClose = new ArrayList<>();
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
            // Close super first, to prevent same-thread nested calls from proceeding
            super.close();

            if (!futuresToClose.stream().allMatch(CompletableFuture::isDone)) {
                final Exception exception = new FDBDatabaseRunner.RunnerClosed();
                for (CompletableFuture<?> future : futuresToClose) {
                    future.completeExceptionally(exception);
                }
            }
            futuresToClose.clear();
        }
    }
}
