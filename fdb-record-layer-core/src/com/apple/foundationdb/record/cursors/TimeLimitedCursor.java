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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
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
public class TimeLimitedCursor<T> implements RecordCursor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeLimitedCursor.class);

    @Nonnull
    private final RecordCursor<T> inner;

    private final long startTime;
    private final long timeLimitMillis;
    private boolean isTimedOut;

    public TimeLimitedCursor(@Nonnull RecordCursor<T> inner, long timeLimitMillis) {
        this(inner, System.currentTimeMillis(), timeLimitMillis);
    }

    public TimeLimitedCursor(@Nonnull RecordCursor<T> inner, long startTime, long timeLimitMillis) {
        this.startTime = startTime;
        this.timeLimitMillis = (timeLimitMillis <= 0L ? ExecuteProperties.UNLIMITED_TIME : timeLimitMillis);
        this.inner = inner;
        // In order to ensure that we always make progress, do not time out before the first record.
        this.isTimedOut = false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (isTimedOut) {
            return AsyncUtil.READY_FALSE;
        } else {
            return inner.onHasNext();
        }
    }

    @Nullable
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            if (timeLimitMillis != ExecuteProperties.UNLIMITED_TIME) {
                final long now = System.currentTimeMillis();
                if ((now - startTime) >= timeLimitMillis) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.of("Cursor time limit exceeded",
                                "cursorElapsedMillis", (now - startTime),
                                "cursorTimeLimitMillis", timeLimitMillis));
                    }
                    isTimedOut = true;
                }
            }
            return inner.next();
        }
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        return inner.getContinuation();
    }

    @Override
    public NoNextReason getNoNextReason() {
        if (isTimedOut) {
            return NoNextReason.TIME_LIMIT_REACHED;
        } else {
            return inner.getNoNextReason();
        }
    }

    @Override
    public void close() {
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
