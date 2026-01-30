/*
 * AgileContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A floating window (agile) context - create sub contexts and commit them as they reach their time/size quota.
 */
public class AgileContext implements AgilityContext {
    static final Logger LOGGER = LoggerFactory.getLogger(AgileContext.class);
    private final FDBRecordContextConfig.Builder contextConfigBuilder;
    private final FDBDatabase database;
    private final FDBRecordContext callerContext; // for counters updates only

    private FDBRecordContext currentContext;
    private long creationTime;
    private int currentWriteSize;
    private final long timeQuotaMillis;
    private final long sizeQuotaBytes;
    // Lock plan:
    //   apply/accept - use read lock, release it within the future
    //   create context - synced and under the read lock of apply/accept
    //   commitNow - use write lock to ensure exclusivity
    // also:
    //   create and commit context functions are each synchronized
    //   committingNow boolean is used to prevent threads cluttering at the commit function when a quota is reached
    private final StampedLock lock = new StampedLock();
    private final Object createLockSync = new Object();
    private final Object commitLockSync = new Object();
    private boolean committingNow = false;
    private long prevCommitCheckTime;
    private boolean closed = false;
    private Function<FDBRecordContext, CompletableFuture<Void>> commitCheck;
    private Throwable lastException = null;

    protected AgileContext(FDBRecordContext callerContext, @Nullable FDBRecordContextConfig.Builder contextBuilder, final long timeQuotaMillis, final long sizeQuotaBytes) {
        this.callerContext = callerContext;
        contextConfigBuilder = contextBuilder != null ? contextBuilder : callerContext.getConfig().toBuilder();
        contextConfigBuilder.setWeakReadSemantics(null); // We don't want all the transactions to use the same read-version
        database = callerContext.getDatabase();
        this.timeQuotaMillis = timeQuotaMillis;
        this.sizeQuotaBytes = sizeQuotaBytes;
        callerContext.getOrCreateCommitCheck("AgilityContext.Agile:", name -> () -> CompletableFuture.runAsync(this::flush));
        logSelf("Starting agility context");
    }

    private void logSelf(final String staticMessage) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("AgilityContext: " + staticMessage,
                    LogMessageKeys.TIME_LIMIT_MILLIS, this.timeQuotaMillis,
                    LogMessageKeys.LIMIT, this.sizeQuotaBytes,
                    // Log the identity hash code, because any two Agiles will be different.
                    LogMessageKeys.AGILITY_CONTEXT, System.identityHashCode(this)));
        }
    }

    @Override
    public void setCommitCheck(final Function<FDBRecordContext, CompletableFuture<Void>> commitCheck) {
        this.commitCheck = commitCheck;
    }

    @Override
    @Nonnull
    public FDBRecordContext getCallerContext() {
        return callerContext;
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private void createIfNeeded() {
        // Called by accept/apply, protected with a read lock
        synchronized (createLockSync) {
            if (currentContext == null) {
                ensureOpen();
                final FDBRecordContextConfig contextConfig = contextConfigBuilder.build();
                currentContext = database.openContext(contextConfig);
                addCommitCheckToContext(currentContext, commitCheck);
                creationTime = now();
                prevCommitCheckTime = creationTime;
                currentWriteSize = 0;
            }
        }
    }

    private static void addCommitCheckToContext(final FDBRecordContext commitCheckContext, @Nullable final Function<FDBRecordContext, CompletableFuture<Void>> commitCheck) {
        if (commitCheck != null) {
            commitCheckContext.addCommitCheck(() -> commitCheck.apply(commitCheckContext));
        }
    }

    private boolean reachedTimeQuota() {
        return now() > creationTime + timeQuotaMillis;
    }

    private boolean reachedSizeQuota() {
        return currentWriteSize > sizeQuotaBytes;
    }

    private boolean shouldCommit() {
        if (currentContext != null && !committingNow) {
            // Note: committingNow is not atomic nor protected, its role is to make multiple threads waiting
            // to commit the same transaction a rare (yet harmless) event. Not just to boost performance, but
            // also to avoid ForkJoinPool deadlocks.
            if (reachedSizeQuota()) {
                callerContext.increment(LuceneEvents.Counts.LUCENE_AGILE_COMMITS_SIZE_QUOTA);
                return true;
            }
            if (reachedTimeQuota()) {
                callerContext.increment(LuceneEvents.Counts.LUCENE_AGILE_COMMITS_TIME_QUOTA);
                return true;
            }
        }
        return false;
    }

    private void commitIfNeeded() {
        if (shouldCommit()) {
            commitNow();
        }
        prevCommitCheckTime = now();
    }

    public void commitNow() {
        // This function is called:
        // 1. when a time/size quota is reached.
        // 2. during caller's close or callerContext commit - the earlier of the two is the effective one. Calling commitNow
        //    is harmless if the object is already closed.
        synchronized (commitLockSync) {
            if (currentContext != null) {
                committingNow = true;
                final long stamp = lock.writeLock();

                try (FDBRecordContext commitContext = currentContext) {
                    commit(commitContext);
                } catch (RuntimeException ex) {
                    closed = true;
                    reportFdbException(ex);
                    throw ex;
                } finally {
                    currentContext = null;
                    currentWriteSize = 0;

                    lock.unlock(stamp);
                    committingNow = false;
                }
            }
        }
    }

    private void reportFdbException(Throwable ex) {
        if (LOGGER.isDebugEnabled()) {
            long nowMilliseconds = now();
            final long creationAge = nowMilliseconds - creationTime;
            final long prevCheckAge = nowMilliseconds - prevCommitCheckTime;
            LOGGER.debug(KeyValueLogMessage.build("AgilityContext: Commit failed",
                    LogMessageKeys.AGILITY_CONTEXT_AGE_MILLISECONDS, creationAge,
                    LogMessageKeys.AGILITY_CONTEXT_PREV_CHECK_MILLISECONDS, prevCheckAge,
                    LogMessageKeys.AGILITY_CONTEXT_WRITE_SIZE_BYTES, currentWriteSize
            ).toString(), ex);
        }
        lastException = ex;
    }

    @Override
    public <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) {
        ensureOpen();
        commitIfNeeded();
        final long stamp = lock.readLock();
        boolean successfulCreate = false;
        try {
            createIfNeeded();
            successfulCreate = true;
        } finally {
            if (!successfulCreate) {
                lock.unlock(stamp);
            }
        }
        return function.apply(currentContext).whenComplete((result, exception) -> {
            lock.unlock(stamp);
            if (exception == null) {
                commitIfNeeded();
            }
        });
    }

    @Override
    // closed in a future
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"})
    public <R> CompletableFuture<R> applyInRecoveryPath(Function<FDBRecordContext, CompletableFuture<R>> function) {
        // Create a new, dedicated context. Apply, flush, and close it.
        FDBRecordContextConfig contextConfig = contextConfigBuilder.build();
        final FDBRecordContext recoveryContext = database.openContext(contextConfig);
        boolean successful = false;
        final CompletableFuture<R> future;
        try {
            future = function.apply(recoveryContext)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            commit(recoveryContext);
                        }
                        recoveryContext.close();
                    });
            successful = true;
        } finally {
            if (!successful) {
                recoveryContext.close();
            }
        }
        return future;
    }

    @Override
    public void accept(final Consumer<FDBRecordContext> function) {
        ensureOpen();
        commitIfNeeded();
        final long stamp = lock.readLock();
        try {
            createIfNeeded();
            function.accept(currentContext);
        } finally {
            lock.unlock(stamp);
        }
        commitIfNeeded();
    }

    @Override
    public void set(byte[] key, byte[] value) {
        accept(context -> {
            context.ensureActive().set(key, value);
            currentWriteSize += key.length + value.length;
        });
    }

    private void ensureOpen() {
        if (closed) {
            throw new RecordCoreStorageException("Agile context is already closed", lastException);
        }
    }

    @Override
    public void flush() {
        commitNow();
        logSelf("Flushed agility context");
    }

    @Override
    public void flushAndClose() {
        closed = true;
        commitNow();
        logSelf("flushAndClose agility context");
    }

    @Override
    public void abortAndClose() {
        // Here: the lock status is undefined. The main goal of this function is to revive this object for post failure cleanups
        synchronized (commitLockSync) {
            closed = true;
            committingNow = true; // avoid future commits
            currentWriteSize = 0;
            if (currentContext != null) {
                currentContext.close();
                currentContext = null;
            }
            // release the locks, in case another thread is waiting on them
            lock.tryUnlockWrite();
            boolean releasedLock = lock.tryUnlockRead();
            for (int maxTries = 20; releasedLock && maxTries > 0; maxTries--) {
                releasedLock = lock.tryUnlockRead();
            }
        }
        logSelf("AbortAndReset agility context");
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
