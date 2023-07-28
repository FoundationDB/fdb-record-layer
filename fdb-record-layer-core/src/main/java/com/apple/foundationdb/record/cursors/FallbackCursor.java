/*
 * FallbackCursor.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.util.LoggableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Provide an alternative cursor in case the primary cursor fails. This cursor has an <code>inner</code> cursor that is
 * used to return the results in the sunny day scenarios, and an alternative provider of a <code>fallback</code> cursor
 * that will be used if the primary cursor encounters an error.
 * In order to continue from a failed iteration, the cursor will pass the last successful result to the provider
 * of the fallback cursor. It is the responsibility of the fallback cursor provider to ensure that it can resume
 * the iteration from that point. Among other things, this means starting a scan over the range that starts with
 * the failure point. If it can't, it can check whether the <code>lastSuccessfulResult</code> is <code>null</code> (in
 * which case it can start from the beginning) and reject the case where the <code>inner</code> cursor has already
 * returned records.
 * <p>
 * A note about continuations: As written, the cursor assumes that the <code>inner</code> and <code>fallback</code>
 * cursors each have their own continuation to pick up from. A future enhancement can be to have this cursor store the
 * state of the failover in its continuation and then package that with the appropriate inner continuation so that it
 * can continue from that same state. It therefore makes the assumption that <code>fallback</code> iteration can be
 * continued by an <code>inner</code> one once a new request with a continuation is executed.
 *
 * @param <T> the type of cursor result returned by the cursor
 */
@API(API.Status.EXPERIMENTAL)
public class FallbackCursor<T> implements RecordCursor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FallbackCursor.class);

    @Nonnull
    private final Function<RecordCursorResult<T>, RecordCursor<T>> fallbackCursorSupplier;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private RecordCursor<T> inner;
    @Nullable
    private CompletableFuture<RecordCursorResult<T>> nextResultFuture;

    /*
     * This is effectively the same as nextResultFuture.get() in the case that a successful result was returned. Repeated
     * in a separate variable to simplify handling and null cases.
     */
    @Nullable
    private RecordCursorResult<T> lastSuccessfulResult;

    private boolean alreadyFailed = false;

    /**
     * Creates a new fallback cursor.
     *
     * @param inner the primary (default) cursor to be used when results are successfully returned
     * @param fallbackCursorSupplier the fallback cursor provider to be used when the primary cursor fails. The function
     * takes a parameter that is the last successful result from the primary cursor (null if none was returned). The
     * returned cursor is expected to resume from the record following the last successful one (or fail if it does not
     * support continuing).
     */
    public FallbackCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<RecordCursorResult<T>, RecordCursor<T>> fallbackCursorSupplier) {
        this.inner = inner;
        this.fallbackCursorSupplier = fallbackCursorSupplier;
        this.executor = inner.getExecutor();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        try {
            if (nextResultFuture != null && nextResultFuture.isDone() && !nextResultFuture.join().hasNext()) {
                // This is needed to ensure we return the same terminal value once the cursor is exhausted
                return nextResultFuture;
            }
        } catch (Exception ignored) {
            // This will happen if the future finished exceptionally - just keep going (client will get
            // the exception when they observe the future).
        }
        // The first stage (handle) will calculate the result of the operation if successful, or replace the inner
        // with the fallback cursor if failed, and store future to the result in nextResultFuture.
        // The second stage (thenCompose) will return nextResultFuture once the first stage is done.
        return inner.onNext().handle((result, throwable) -> {
            if (throwable == null) {
                nextResultFuture = CompletableFuture.completedFuture(result);
                lastSuccessfulResult = result;
            } else {
                if (alreadyFailed) {
                    nextResultFuture = CompletableFuture.failedFuture(wrapException("Fallback cursor failed, cannot fallback again", throwable));
                } else {
                    inner.close();
                    inner = fallbackCursorSupplier.apply(lastSuccessfulResult);
                    nextResultFuture = inner.onNext();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(KeyValueLogMessage.of("fallback triggered", LogMessageKeys.MESSAGE, throwable.getMessage()));
                    }
                }
                alreadyFailed = true;
            }
            return null; // return value is ignored by next stage
        }).thenCompose(vignore -> nextResultFuture);
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return this.executor;
    }

    private RecordCursor<T> getInner() {
        return inner;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            getInner().accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    private Throwable wrapException(final String msg, final Throwable ex) {
        if (ex instanceof LoggableException) {
            // In the case of loggable exception, maintain the original exception to simplify exception handling across
            // fallback and non-fallback executions
            LoggableException loggableException = (LoggableException)ex;
            loggableException.addLogInfo("fallback_failed", msg);
            return ex;
        } else if ((ex.getCause() != null) && (ex.getCause() instanceof LoggableException)) {
            // Same but in case the throwable is already wrapping the LoggableException
            LoggableException loggableException = (LoggableException)(ex.getCause());
            loggableException.addLogInfo("fallback_failed", msg);
            return ex;
        } else {
            return new FallbackExecutionFailedException(msg, ex);
        }
    }

    /**
     * Exception thrown when the fallback cursor fails.
     */
    @SuppressWarnings("java:S110")
    public static class FallbackExecutionFailedException extends RecordCoreException {
        public static final long serialVersionUID = 1;

        public FallbackExecutionFailedException(@Nonnull final String msg, @Nullable final Throwable cause) {
            super(msg, cause);
        }
    }
}
