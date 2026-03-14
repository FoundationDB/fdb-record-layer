/*
 * TransactionalRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A simple runner that can be used for opening and committing transactions, and making sure they are all closed.
 */
@API(API.Status.INTERNAL)
public class TransactionalRunner implements AutoCloseable {

    @Nonnull
    private final FDBDatabase database;
    @Nonnull
    private final FDBRecordContextConfig.Builder contextConfigBuilder;
    private boolean closed;
    @Nonnull
    private final List<FDBRecordContext> contextsToClose;

    /**
     * Creates a new runner for operating against a given database.
     * @param database the underlying databse to open contexts against
     * @param contextConfig configuration for how to open contexts
     */
    public TransactionalRunner(@Nonnull FDBDatabase database,
                               @Nonnull FDBRecordContextConfig contextConfig) {
        this(database, contextConfig.toBuilder());
    }

    /**
     * Creates a runner for operating against a given database, with a <em>mutable</em>
     * {@link FDBRecordContextConfig.Builder}.
     * <p>
     *     You probably don't want to call this, and should probably call
     *     {@link #TransactionalRunner(FDBDatabase, FDBRecordContextConfig)} instead.
     * </p>
     * @param database the underlying database to open contexts against
     * @param contextConfigBuilder configuration for how to open contexts.
     * Note: The same as FDBDatabaseRunnerImpl, this maintains mutability, but that mutability is not thread safe, so
     * you shouldn't change it, while simultaneously calling {@link #runAsync(boolean, Function)}.
     */
    public TransactionalRunner(@Nonnull FDBDatabase database,
                               @Nonnull FDBRecordContextConfig.Builder contextConfigBuilder) {
        this.database = database;
        this.contextConfigBuilder = contextConfigBuilder;

        contextsToClose = new ArrayList<>();
    }

    /**
     * Run some code with a given context, and commit the context.
     * <p>
     *     The context will be committed if the future returned by the runnable is successful, otherwise it will not
     *     be committed. If this {@code TransactionalRunner} is closed, so will the context passed to the runnable.
     * </p>
     * <p>
     *     Note: {@code runnable} is run in the current thread.
     * </p>
     * @param clearWeakReadSemantics whether to clear the {@link FDBRecordContextConfig#getWeakReadSemantics()} before
     * creating the transaction. These should be cleared if retrying a transaction, particularly in response to a
     * conflict, because reusing the old read version would just cause it to re-conflict.
     * @param runnable some code to run that uses an {@link FDBRecordContext}
     * @param <T> the type of the value returned by the future
     * @return a future containing the result of the runnable, if successfully committed.
     * Note: the future will not be {@code null}, but if the runnable returns a future containing {@code null} then
     * so will the future returned here.
     */
    @Nonnull
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"})
    public <T> CompletableFuture<T> runAsync(final boolean clearWeakReadSemantics,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> runnable) {
        return runAsync(clearWeakReadSemantics, true, runnable);
    }

    /**
     * A flavor of the {@link #runAsync(boolean, Function)} method that supports read-only transactions.
     * @param clearWeakReadSemantics whether to clear the {@link FDBRecordContextConfig#getWeakReadSemantics()} before
     * creating the transaction. These should be cleared if retrying a transaction, particularly in response to a
     * conflict, because reusing the old read version would just cause it to re-conflict.
     * @param commitWhenDone if FALSE the transaction will not be committed. If TRUE, behaves the same as described in {@link #runAsync(boolean, Function)}
     * @param runnable some code to run that uses an {@link FDBRecordContext}
     * @param <T> the type of the value returned by the future
     * @return a future containing the result of the runnable, if successfully committed.
     * Note: the future will not be {@code null}, but if the runnable returns a future containing {@code null} then
     * so will the future returned here.
     */
    @Nonnull
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"})
    public <T> CompletableFuture<T> runAsync(final boolean clearWeakReadSemantics,
                                              boolean commitWhenDone,
                                              @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> runnable) {
        FDBRecordContext context = openContext(clearWeakReadSemantics);
        boolean returnedFuture = false;
        try {
            CompletableFuture<T> future = runnable.apply(context)
                    .thenCompose((T val) -> {
                        if (commitWhenDone) {
                            return context.commitAsync().thenApply(vignore -> val);
                        } else {
                            return CompletableFuture.completedFuture(val);
                        }
                    });
            returnedFuture = true;
            return future.whenComplete((result, exception) -> context.close());
        } finally {
            if (!returnedFuture) {
                // If there are any exceptions in creating the future, then we won't chain the
                // context-closing callback. Handle that case to avoid leaking resources
                context.close();
            }
        }
    }

    /**
     * Run a function against a context synchronously.
     * <p>
     *     Note: since committing the transaction is an inherently async function, this is a blocking call, so if
     *     calling async methods from within {@code runnable}, it is probably better to use
     *     {@link #runAsync(boolean, Function)}.
     * </p>
     * @param clearWeakReadSemantics whether to clear the {@link FDBRecordContextConfig#getWeakReadSemantics()} before
     * creating the transaction. These should be cleared if retrying a transaction, particularly in response to a
     * conflict, because reusing the old read version would just cause it to re-conflict.
     * @param runnable some code to run synchronously that uses an {@link FDBRecordContext}
     * @param <T>  the type of the value returned by the runnable
     * @return the value returned by {@code runnable}.
     */
    public <T> T run(final boolean clearWeakReadSemantics,
                     @Nonnull Function<? super FDBRecordContext, ? extends T> runnable) {
        final T result;
        try (FDBRecordContext context = openContext(clearWeakReadSemantics)) {
            result = runnable.apply(context);
            context.commit();
        }
        return result;
    }

    /**
     * Open a new context with the config attached to this runner, that will be closed when this runner is closed.
     * <p>
     *     It is probably preferable to use {@link #run(boolean, Function)} or {@link #runAsync(boolean, Function)}
     *     over opening transactions directly, but this is exposed because {@link FDBDatabaseRunner} exposes it.
     * </p>
     * @return a new context
     */
    @Nonnull
    public FDBRecordContext openContext() {
        return openContext(true);
    }

    @Nonnull
    private FDBRecordContext openContext(boolean clearWeakReadSemantics) {
        if (closed) {
            throw new FDBDatabaseRunner.RunnerClosed();
        }
        FDBRecordContextConfig contextConfig;
        if (!clearWeakReadSemantics || contextConfigBuilder.getWeakReadSemantics() == null) {
            contextConfig = contextConfigBuilder.build();
        } else {
            // Clear any weak semantics to avoid reusing old read versions
            contextConfig = contextConfigBuilder.copyBuilder().setWeakReadSemantics(null).build();
        }
        FDBRecordContext context = database.openContext(contextConfig);
        addContextToClose(context);
        return context;
    }

    private synchronized void addContextToClose(@Nonnull FDBRecordContext context) {
        if (closed) {
            context.close();
            throw new FDBDatabaseRunner.RunnerClosed();
        }
        contextsToClose.removeIf(FDBRecordContext::isClosed);
        contextsToClose.add(context);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        contextsToClose.forEach(FDBRecordContext::close);
        this.closed = true;
    }
}
