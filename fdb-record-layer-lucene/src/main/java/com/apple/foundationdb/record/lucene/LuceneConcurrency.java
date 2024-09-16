/*
 * LuceneConcurrency.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.apple.foundationdb.record.lucene.LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC;

/**
 * Utility class for methods related to synchronizing Futures.
 */
public class LuceneConcurrency {

    /**
     * An implementation of {@code asyncToSync} that is isolated from external exception injections.
     * This implementation does NOT perform exception mapping, nor does it check whether the calls have
     * "async" in the stack trace (as the original {@link FDBRecordContext#asyncToSync} does).
     * This method is meant to be used internally in places where obtaining and using the result pf asynchronous
     * operation is required.
     * This method uses the {@link FDBDatabase#getAsyncToSyncTimeout} to find the period to use for the timeout.
     * This method will throw the runtime exception that was thrown by the Future's realization in case such error
     * occurred.
     * This method will use the legacy  {@link FDBRecordContext#asyncToSync} if the LUCENE_USE_LEGACY_ASYNC_TO_SYNC
     * property is set to TRUE (the default)
     *
     * @param event the timer event to use for recording the waits
     * @param async the future to wait on
     * @param recordContext the context to use for callback, recording the event and getting the timeout
     *
     * @return the result of the future's operation
     */
    @Nullable
    @API(API.Status.INTERNAL)
    public static <T> T asyncToSync(@Nonnull StoreTimer.Wait event, @Nonnull CompletableFuture<T> async, @Nonnull FDBRecordContext recordContext) {
        if (recordContext.getPropertyStorage().getPropertyValue(LUCENE_USE_LEGACY_ASYNC_TO_SYNC)) {
            return recordContext.asyncToSync(event, async);
        }

        if (recordContext.hasHookForAsyncToSync() && !MoreAsyncUtil.isCompletedNormally(async)) {
            recordContext.getHookForAsyncToSync().accept(event);
        }
        if (async.isDone()) {
            try {
                return async.get();
            } catch (ExecutionException ex) {
                throw FDBExceptions.wrapException(ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                // TODO: The original mapping for InterruptedException would be INTERNAL_ERROR, now it will be RecordCoreInterruptedException
                throw FDBExceptions.wrapException(ex);
            }
        } else {
            final Pair<Long, TimeUnit> asyncToSyncTimeout = recordContext.getDatabase().getAsyncToSyncTimeout(event);
            Timeout timeout = (asyncToSyncTimeout == null) ? null : Timeout.of(asyncToSyncTimeout.getLeft(), asyncToSyncTimeout.getRight());
            final FDBStoreTimer timer = recordContext.getTimer();
            final long startTime = System.nanoTime();
            try {
                if (timeout != null) {
                    return async.get(timeout.getDuration(), timeout.getTimeUnit());
                } else {
                    return async.get();
                }
            } catch (TimeoutException ex) {
                if (timer != null) {
                    timer.recordTimeout(event, startTime);
                    throw new AsyncToSyncTimeoutException(ex.getMessage(), ex, LogMessageKeys.TIME_LIMIT.toString(), timeout.getDuration(), LogMessageKeys.TIME_UNIT.toString(), timeout.getTimeUnit());
                }
                throw new AsyncToSyncTimeoutException(ex.getMessage(), ex);
            } catch (ExecutionException ex) {
                throw FDBExceptions.wrapException(ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                // TODO: The original mapping for InterruptedException would be INTERNAL_ERROR, now it will be RecordCoreInterruptedException
                throw FDBExceptions.wrapException(ex);
            } finally {
                if (timer != null) {
                    timer.recordSinceNanoTime(event, startTime);
                }
            }
        }
    }

    private LuceneConcurrency() {
    }

    /**
     * A representation of a timeout value: A duration and a time unit.
     */
    public static class Timeout {
        private final long duration;
        @Nonnull
        private final TimeUnit timeUnit;

        public Timeout(final long duration, @Nonnull final TimeUnit timeUnit) {
            this.duration = duration;
            this.timeUnit = timeUnit;
        }

        public static Timeout of(final long duration, @Nonnull final TimeUnit timeUnit) {
            return new Timeout(duration, timeUnit);
        }

        public long getDuration() {
            return duration;
        }

        @Nonnull
        public TimeUnit getTimeUnit() {
            return timeUnit;
        }
    }

    /**
     * An exception that is thrown when the async to sync operation times out.
     */
    public static class AsyncToSyncTimeoutException extends RecordCoreException {
        private static final long serialVersionUID = -1L;

        public AsyncToSyncTimeoutException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public AsyncToSyncTimeoutException(final String message, final Throwable cause, final Object... keyValues) {
            super(message, cause);
            addLogInfo(keyValues);
        }
    }
}
