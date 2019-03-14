/*
 * TimeLimitedCursor.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A wrapper cursor that implements a (wall clock) time limit on the execution of a cursor.
 * This class is deprecated (but isn't tagged that way because otherwise Checkstyle complains about its use in the
 * deprecated {@link #limitTimeTo(long)} methods.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.DEPRECATED)
public class TimeLimitedCursor<T> implements RecordCursor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeLimitedCursor.class);

    @Nonnull
    private final RecordCursor<T> inner;

    private final long startTime;
    private final long timeLimitMillis;
    @Nullable
    private RecordCursorResult<T> timedOutResult;

    // needed for supporting old API
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;

    public TimeLimitedCursor(@Nonnull RecordCursor<T> inner, long timeLimitMillis) {
        this(inner, System.currentTimeMillis(), timeLimitMillis);
    }

    public TimeLimitedCursor(@Nonnull RecordCursor<T> inner, long startTime, long timeLimitMillis) {
        this.startTime = startTime;
        this.timeLimitMillis = (timeLimitMillis <= 0L ? ExecuteProperties.UNLIMITED_TIME : timeLimitMillis);
        this.inner = inner;
        // In order to ensure that we always make progress, do not time out before the first record.
        this.timedOutResult = null;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            // It is necessary to check to see if a result has already completed without a value as it would
            // otherwise be possible for the NoNextReason to change. In particular, if this cursor completes
            // because its child cursos hits some limit, one can return a RecordCursorResult where the NoNextReason
            // is that limit. If the time limit then elapses and then one calls onNext again, if the result weren't
            // memoized, one might return a NoNextReason of TIME_LIMIT_REACHED.
            return CompletableFuture.completedFuture(nextResult);
        } else if (timedOutResult != null) {
            nextResult = timedOutResult;
            return CompletableFuture.completedFuture(nextResult);
        } else {
            return inner.onNext().thenApply(innerResult -> {
                if (timeLimitMillis != ExecuteProperties.UNLIMITED_TIME) {
                    final long now = System.currentTimeMillis();
                    if ((now - startTime) >= timeLimitMillis) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(KeyValueLogMessage.of("Cursor time limit exceeded",
                                    "cursorElapsedMillis", (now - startTime),
                                    "cursorTimeLimitMillis", timeLimitMillis));
                        }
                        if (innerResult.getContinuation().isEnd()) {
                            timedOutResult = RecordCursorResult.exhausted();
                        } else {
                            timedOutResult = RecordCursorResult.withoutNextValue(innerResult.getContinuation(), NoNextReason.TIME_LIMIT_REACHED);
                        }
                    }
                }
                nextResult = innerResult;
                return nextResult;
            });
        }
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextFuture = null;
        return nextResult.get();
    }

    @Nullable
    @Override
    @Deprecated
    public byte[] getContinuation() {
        return nextResult.getContinuation().toBytes();
    }

    @Nonnull
    @Override
    @Deprecated
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        if (hasNextFuture != null) {
            hasNextFuture.cancel(false);
        }
        inner.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
