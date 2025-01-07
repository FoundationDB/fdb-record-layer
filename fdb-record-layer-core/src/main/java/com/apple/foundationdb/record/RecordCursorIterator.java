/*
 * RecordCursorIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.cursors.IllegalContinuationAccessChecker;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * An asynchronous iterator that wraps a {@link RecordCursor} and presents an iterator-style interface for advancing
 * the cursor. This adds {@link #getContinuation()} and {@link #getNoNextReason()} methods to the {@link AsyncIterator}
 * interface so that cursors can be resumed and so that their reason for stopping can be inspected.
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public class RecordCursorIterator<T> implements AsyncIterator<T>, AutoCloseable {
    @Nonnull
    private final RecordCursor<T> cursor;
    @Nullable
    private CompletableFuture<Boolean> onHasNextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    RecordCursorIterator(@Nonnull RecordCursor<T> cursor) {
        this.cursor = cursor;
    }

    /**
     * Asynchronously check whether there are more records available from the cursor.
     * @return a future that when complete will hold <code>true</code> if {@link #next()} would return a record.
     * @see com.apple.foundationdb.async.AsyncIterator#onHasNext()
     */
    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return AsyncUtil.READY_FALSE;
        } else if (onHasNextFuture == null) {
            mayGetContinuation = false;
            onHasNextFuture = cursor.onNext().thenApply(result -> {
                nextResult = result;
                mayGetContinuation = !nextResult.hasNext();
                return nextResult.hasNext();
            });
        }
        return onHasNextFuture;
    }

    /**
     * Synchronously check whether there are more records available from the cursor.
     * @return {@code} true if {@link #next()} would return a record and {@code false} otherwise
     */
    @Override
    public boolean hasNext() {
        try {
            return onHasNext().get();
        } catch (ExecutionException ex) {
            throw new RecordCoreException(CompletionExceptionLogHelper.asCause(ex));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecordCoreInterruptedException(ex.getMessage(), ex);
        }
    }

    /**
     * Return the next value. May only be called after the future from {@link #onHasNext()} has completed to
     * {@code true} (or equivalently after {@link #hasNext()} has returned {@code true}).
     * @return the next value
     */
    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        onHasNextFuture = null;
        mayGetContinuation = true;
        return nextResult.get();
    }

    /**
     * Get a byte string that can be used to continue a query after the last record returned.
     *
     * Returns {@code null} if the underlying source is completely exhausted, independent of any limit passed to the
     * cursor creator. Since such creators generally accept {@code null} to mean no continuation, that is, start from
     * the beginning, one must check for {@code null} from {@code getContinuation()} to keep from starting over.
     *
     * Result is not always defined if called before the {@link #onHasNext()} has been called, before the returned
     * future has completed, or after it has completed to {@code true} but before {@link #next()} has been called.
     * That is, a continuation is only guaranteed when called "between" records from a {@code while (hasNext) next} loop
     * or after its end. If configured to do so, an {@link IllegalContinuationAccessChecker} will be used to throw
     * an exception if a continuation is accessed at the wrong time.
     *
     * @return opaque byte array denoting where the cursor should pick up. This can be passed back into a new
     * cursor of the same type, with all other parameters remaining the same.
     */
    @Nullable
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return nextResult.getContinuation().toBytes();
    }

    /**
     * Get the reason that the cursor has reached the end and returned <code>false</code> for {@link #hasNext}.
     * If <code>hasNext</code> was not called or returned <code>true</code> last time, the result is undefined and
     * may be an exception.
     * @return the reason that the cursor stopped
     */
    @Nonnull
    public RecordCursor.NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        if (onHasNextFuture != null) {
            onHasNextFuture.cancel(false);
        }
        cursor.close();
    }

    @Override
    public void cancel() {
        close();
    }
}
