/*
 * FDBDatabaseRunnerImpl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The standard implementation of {@link FDBDatabaseRunner}.
 */
@API(API.Status.INTERNAL)
public class FDBDatabaseRunnerImpl implements FDBDatabaseRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseRunnerImpl.class);

    @Nonnull
    private final FDBDatabase database;
    @Nonnull
    private Executor executor;

    @Nullable
    private FDBStoreTimer timer;
    @Nullable
    private Map<String, String> mdcContext;
    @Nullable
    private FDBDatabase.WeakReadSemantics weakReadSemantics;

    private int maxAttempts;
    private long maxDelayMillis;
    private long initialDelayMillis;

    private boolean closed;
    @Nonnull
    private final List<FDBRecordContext> contextsToClose;
    @Nonnull
    private final List<CompletableFuture<?>> futuresToCompleteExceptionally;

    @API(API.Status.INTERNAL)
    public FDBDatabaseRunnerImpl(@Nonnull FDBDatabase database,
                                 @Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                 @Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        this.database = database;

        this.timer = timer;
        this.mdcContext = mdcContext;
        this.weakReadSemantics = weakReadSemantics;

        final FDBDatabaseFactory factory = database.getFactory();
        this.maxAttempts = factory.getMaxAttempts();
        this.maxDelayMillis = factory.getMaxDelayMillis();
        this.initialDelayMillis = factory.getInitialDelayMillis();

        this.executor = FDBRecordContext.initExecutor(database, mdcContext);

        contextsToClose = new ArrayList<>();
        futuresToCompleteExceptionally = new ArrayList<>();
    }

    @API(API.Status.INTERNAL)
    public FDBDatabaseRunnerImpl(@Nonnull FDBDatabase database,
                                 @Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext) {
        this(database, timer, mdcContext, null);
    }

    @API(API.Status.INTERNAL)
    public FDBDatabaseRunnerImpl(@Nonnull FDBDatabase database) {
        this(database, null, null, null);
    }

    @Override
    @Nonnull
    public FDBDatabase getDatabase() {
        return database;
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    @Nullable
    public FDBStoreTimer getTimer() {
        return timer;
    }

    @Override
    public void setTimer(@Nullable FDBStoreTimer timer) {
        this.timer = timer;
    }

    @Override
    @Nullable
    public Map<String, String> getMdcContext() {
        return mdcContext;
    }

    @Override
    public void setMdcContext(@Nullable Map<String, String> mdcContext) {
        this.mdcContext = mdcContext;
        executor = FDBRecordContext.initExecutor(database, mdcContext);
    }

    @Override
    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    @Override
    public void setWeakReadSemantics(@Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        this.weakReadSemantics = weakReadSemantics;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new RecordCoreException("Cannot set maximum number of attempts to less than or equal to zero");
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public long getMinDelayMillis() {
        return 2;
    }

    @Override
    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }

    @Override
    public void setMaxDelayMillis(long maxDelayMillis) {
        if (maxDelayMillis < 0) {
            throw new RecordCoreException("Cannot set maximum delay milliseconds to less than or equal to zero");
        } else if (maxDelayMillis < initialDelayMillis) {
            throw new RecordCoreException("Cannot set maximum delay to less than minimum delay");
        }
        this.maxDelayMillis = maxDelayMillis;
    }

    @Override
    public long getInitialDelayMillis() {
        return initialDelayMillis;
    }

    @Override
    public void setInitialDelayMillis(long initialDelayMillis) {
        if (initialDelayMillis < 0) {
            throw new RecordCoreException("Cannot set initial delay milleseconds to less than zero");
        } else if (initialDelayMillis > maxDelayMillis) {
            throw new RecordCoreException("Cannot set initial delay to greater than maximum delay");
        }
        this.initialDelayMillis = initialDelayMillis;
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext() {
        if (closed) {
            throw new RunnerClosed();
        }
        FDBRecordContext context = database.openContext(mdcContext, timer, weakReadSemantics);
        addContextToClose(context);
        return context;
    }

    private class RunRetriable<T> {
        private int tries = 0;
        private long currDelay = getInitialDelayMillis();
        @Nullable private FDBRecordContext context;
        @Nullable T retVal = null;
        @Nullable RuntimeException exception = null;
        @Nullable private final List<Object> additionalLogMessageKeyValues;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        private RunRetriable(@Nullable List<Object> additionalLogMessageKeyValues) {
            this.additionalLogMessageKeyValues = additionalLogMessageKeyValues;
        }

        @Nonnull
        private CompletableFuture<Boolean> handle(@Nullable T val, @Nullable Throwable e) {
            if (context != null) {
                context.close();
                context = null;
            }

            if (closed) {
                // Outermost future should be cancelled, but be sure that this doesn't appear to be successful.
                this.exception = new RunnerClosed();
                return AsyncUtil.READY_FALSE;
            } else if (e == null) {
                // Successful completion. We are done.
                retVal = val;
                return AsyncUtil.READY_FALSE;
            } else {
                // Unsuccessful. Possibly retry.
                Throwable t = e;
                String fdbMessage = null;
                int code = -1;
                boolean retry = false;
                while (t != null) {
                    if (t instanceof FDBException) {
                        FDBException fdbE = (FDBException)t;
                        retry = retry || fdbE.isRetryable();
                        fdbMessage = fdbE.getMessage();
                        code = fdbE.getCode();
                    } else if (t instanceof RecordCoreRetriableTransactionException) {
                        retry = true;
                    }
                    t = t.getCause();
                }

                if (tries + 1 < getMaxAttempts() && retry) {
                    long delay = (long)(Math.random() * currDelay);

                    if (LOGGER.isWarnEnabled()) {
                        final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying FDB Exception",
                                                                LogMessageKeys.MESSAGE, fdbMessage,
                                                                LogMessageKeys.CODE, code,
                                                                LogMessageKeys.TRIES, tries,
                                                                LogMessageKeys.MAX_ATTEMPTS, getMaxAttempts(),
                                                                LogMessageKeys.DELAY, delay);
                        if (additionalLogMessageKeyValues != null) {
                            message.addKeysAndValues(additionalLogMessageKeyValues);
                        }
                        LOGGER.warn(message.toString(), e);
                    }
                    CompletableFuture<Void> future = MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
                    addFutureToCompleteExceptionally(future);
                    return future.thenApply(vignore -> {
                        tries += 1;
                        currDelay = Math.max(Math.min(delay * 2, getMaxDelayMillis()), getMinDelayMillis());
                        return true;
                    });
                } else {
                    this.exception = database.mapAsyncToSyncException(e);
                    return AsyncUtil.READY_FALSE;
                }
            }
        }

        @SuppressWarnings("squid:S1181")
        public CompletableFuture<T> runAsync(@Nonnull final Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull final BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction) {
            CompletableFuture<T> future = new CompletableFuture<>();
            addFutureToCompleteExceptionally(future);
            AsyncUtil.whileTrue(() -> {
                try {
                    context = openContext();
                    return retriable.apply(context).thenCompose(val ->
                        context.commitAsync().thenApply( vignore -> val)
                    ).handle((result, ex) -> {
                        Pair<? extends T, ? extends Throwable> newResult = handlePostTransaction.apply(result, ex);
                        return handle(newResult.getLeft(), newResult.getRight());
                    }).thenCompose(Function.identity());
                } catch (Exception e) {
                    return handle(null, e);
                }
            }, getExecutor()).handle((vignore, e) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else if (e != null) {
                    // This handles an uncaught exception
                    // showing up somewhere in the handle function.
                    future.completeExceptionally(e);
                } else {
                    future.complete(retVal);
                }
                return null;
            });
            return future;
        }

        @SuppressWarnings("squid:S1181")
        public T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
            boolean again = true;
            while (again) {
                try {
                    context = openContext();
                    T ret = retriable.apply(context);
                    context.commit();
                    again = database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_RETRY_DELAY, handle(ret, null));
                } catch (Exception e) {
                    again = database.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_RETRY_DELAY, handle(null, e));
                } finally {
                    if (context != null) {
                        context.close();
                        context = null;
                    }
                }
            }
            if (exception == null) {
                return retVal;
            } else {
                throw exception;
            }
        }
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable,
                     @Nullable List<Object> additionalLogMessageKeyValues) {
        return new RunRetriable<T>(additionalLogMessageKeyValues).run(retriable);
    }

    @Override
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public <T> CompletableFuture<T> runAsync(@Nonnull final Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nonnull final BiFunction<? super T, Throwable, ? extends Pair<? extends T, ? extends Throwable>> handlePostTransaction,
                                             @Nullable List<Object> additionalLogMessageKeyValues) {
        return new RunRetriable<T>(additionalLogMessageKeyValues).runAsync(retriable, handlePostTransaction);
    }

    @Override
    @Nullable
    public <T> T asyncToSync(FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        return database.asyncToSync(timer, event, async);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (!futuresToCompleteExceptionally.stream().allMatch(CompletableFuture::isDone)) {
            final Exception exception = new RunnerClosed();
            for (CompletableFuture<?> future : futuresToCompleteExceptionally) {
                future.completeExceptionally(exception);
            }
        }
        contextsToClose.forEach(FDBRecordContext::close);
    }

    @Override
    public CompletableFuture<SynchronizedSessionRunner> startSynchronizedSessionAsync(@Nonnull Subspace lockSubspace, long leaseLengthMillis) {
        return SynchronizedSessionRunner.startSessionAsync(lockSubspace, leaseLengthMillis, this);
    }

    @Override
    public SynchronizedSessionRunner startSynchronizedSession(@Nonnull Subspace lockSubspace, long leaseLengthMillis) {
        return SynchronizedSessionRunner.startSession(lockSubspace, leaseLengthMillis, this);
    }

    @Override
    public SynchronizedSessionRunner joinSynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMillis) {
        return SynchronizedSessionRunner.joinSession(lockSubspace, sessionId, leaseLengthMillis, this);
    }

    private synchronized void addContextToClose(@Nonnull FDBRecordContext context) {
        if (closed) {
            context.close();
            throw new RunnerClosed();
        }
        contextsToClose.removeIf(FDBRecordContext::isClosed);
        contextsToClose.add(context);
    }

    private synchronized void addFutureToCompleteExceptionally(@Nonnull CompletableFuture<?> future) {
        if (closed) {
            final RunnerClosed exception = new RunnerClosed();
            future.completeExceptionally(exception);
            throw exception;
        }
        futuresToCompleteExceptionally.removeIf(CompletableFuture::isDone);
        futuresToCompleteExceptionally.add(future);
    }

}
