/*
 * AgilityContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Create floating sub contexts from a caller context and commit when they reach time/write quota.
 */
public interface AgilityContext {

    static AgilityContext nonAgile(FDBRecordContext callerContext) {
        return new NonAgile(callerContext);
    }

    static AgilityContext agile(FDBRecordContext callerContext, final long timeQuotaMillis, final long sizeQuotaBytes) {
        return new Agile(callerContext, timeQuotaMillis, sizeQuotaBytes);
    }

    // `apply` should be called when a returned value is expected
    <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) ;

    // `accept` should be called when a returned value is not expected
    void accept(Consumer<FDBRecordContext> function);

    // `set` should be called for writes - keeping track of write size (if needed)
    void set(byte[] key, byte[] value);

    void flush();

    default CompletableFuture<byte[]> get(byte[] key) {
        return apply(context -> context.ensureActive().get(key));
    }

    default void clear(byte[] key) {
        accept(context -> context.ensureActive().clear(key));
    }

    default void clear(Range range) {
        accept(context -> context.ensureActive().clear(range));
    }

    default CompletableFuture<List<KeyValue>> getRange(byte[] begin, byte[] end) {
        return apply(context -> context.ensureActive().getRange(begin, end).asList());
    }

    /**
     * This function returns the caller's context. If called by external entities, it should only be used for testing.
     * @return caller's context
     */
    @Nonnull
    FDBRecordContext getCallerContext();

    default <T> CompletableFuture<T> instrument(StoreTimer.Event event,
                                                CompletableFuture<T> future ) {
        return getCallerContext().instrument(event, future);
    }

    default <T> CompletableFuture<T> instrument(StoreTimer.Event event,
                                                CompletableFuture<T> future,
                                                long start) {
        return getCallerContext().instrument(event, future, start);
    }

    default void increment(@Nonnull StoreTimer.Count count) {
        getCallerContext().increment(count);
    }

    default void increment(@Nonnull StoreTimer.Count count, int size) {
        getCallerContext().increment(count, size);
    }

    default void recordEvent(@Nonnull StoreTimer.Event event, long timeDelta) {
        getCallerContext().record(event, timeDelta);
    }

    @Nullable
    default <T> T asyncToSync(StoreTimer.Wait event,
                              @Nonnull CompletableFuture<T> async) {
        return getCallerContext().asyncToSync(event, async);
    }

    @Nullable
    default <T> T getPropertyValue(@Nonnull RecordLayerPropertyKey<T> propertyKey) {
        return getCallerContext().getPropertyStorage().getPropertyValue(propertyKey);
    }

    /**
     * A floating window (agile) context - create sub contexts and commit them as they reach their time/size quota.
     */
    class Agile implements AgilityContext {

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

        Agile(FDBRecordContext callerContext, final long timeQuotaMillis, final long sizeQuotaBytes) {
            this.callerContext = callerContext;
            contextConfigBuilder = callerContext.getConfig().toBuilder();
            contextConfigBuilder.setWeakReadSemantics(null); // We don't want all the transactions to use the same read-version
            database = callerContext.getDatabase();
            this.timeQuotaMillis = timeQuotaMillis;
            this.sizeQuotaBytes = sizeQuotaBytes;
            callerContext.getOrCreateCommitCheck("AgilityContext.Agile:", name -> () -> CompletableFuture.runAsync(this::flush));
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
                    FDBRecordContextConfig contextConfig = contextConfigBuilder.build();
                    currentContext = database.openContext(contextConfig);
                    creationTime = now();
                    prevCommitCheckTime = creationTime;
                    currentWriteSize = 0;
                }
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
            // 2. during caller's close or callerContext commit - the earlier of the two is the effective one.
            synchronized (commitLockSync) {
                if (currentContext != null) {
                    committingNow = true;
                    final long stamp = lock.writeLock();

                    try {
                        currentContext.commit();
                        currentContext.close();
                    } catch (FDBException ex) {
                        final Logger logger = LoggerFactory.getLogger(AgilityContext.Agile.class); // will be used only once, and rarely.
                        if (logger.isTraceEnabled()) {
                            long nowMilliseconds = now();
                            final long creationAge = nowMilliseconds - creationTime;
                            final long  prevCheckAge = nowMilliseconds - prevCommitCheckTime;
                            logger.trace(KeyValueLogMessage.build("AgilityContext: Commit failed",
                                    LogMessageKeys.AGILITY_CONTEXT_AGE_MILLISECONDS, creationAge,
                                    LogMessageKeys.AGILITY_CONTEXT_PREV_CHECK_MILLISECONDS, prevCheckAge
                            ).toString(), ex);
                        }
                        throw ex; // re-throw
                    }
                    currentContext = null;
                    currentWriteSize = 0;

                    lock.unlock(stamp);
                    committingNow = false;
                }
            }
        }

        @Override
        public <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) {
            final long stamp = lock.readLock();
            createIfNeeded();
            return function.apply(currentContext).thenApply(ret -> {
                lock.unlock(stamp);
                commitIfNeeded();
                return ret;
            });
        }

        @Override
        public void accept(final Consumer<FDBRecordContext> function) {
            final long stamp = lock.readLock();
            createIfNeeded();
            function.accept(currentContext);
            lock.unlock(stamp);
            commitIfNeeded();
        }

        @Override
        public void set(byte[] key, byte[] value) {
            accept(context -> {
                context.ensureActive().set(key, value);
                currentWriteSize += key.length + value.length;
            });
        }

        @Override
        public void flush() {
            commitNow();
        }
    }

    /**
     * A non-agile context - plainly use caller's context as context and never commit.
     */
    class NonAgile implements AgilityContext {
        private final FDBRecordContext callerContext;

        public NonAgile(final FDBRecordContext callerContext) {
            this.callerContext = callerContext;
        }

        @Override
        public <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) {
            return function.apply(callerContext);
        }

        @Override
        public void accept(final Consumer<FDBRecordContext> function) {
            function.accept(callerContext);
        }

        @Override
        public void set(byte[] key, byte[] value) {
            accept(context -> context.ensureActive().set(key, value));
        }

        @Override
        public void flush() {
            // This is a no-op as the caller context should be committed by the caller.
        }

        @Override
        @Nonnull
        public FDBRecordContext getCallerContext() {
            return callerContext;
        }
    }

}
