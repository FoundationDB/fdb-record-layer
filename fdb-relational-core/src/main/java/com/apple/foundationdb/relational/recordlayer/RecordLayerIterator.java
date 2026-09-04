/*
 * RecordLayerIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursor.NoNextReason;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
import java.util.function.Function;

@API(API.Status.EXPERIMENTAL)
public final class RecordLayerIterator<T> implements ResumableIterator<Row> {
    private final RecordCursor<T> recordCursor;
    private final Function<T, Row> transform;
    // null until the first fetchNextResult() call, and again briefly at the very start of next() (see the
    // "result = null" reset in its finally block, immediately followed by the next hasNext()/fetchNextResult()
    // re-populating it).
    @Nullable
    private RecordCursorResult<T> result;
    private Continuation continuation;
    // null until iteration is exhausted; see fetchNextResult() and getNoNextReason() below.
    @Nullable
    private NoNextReason noNextReason;

    private RecordLayerIterator(RecordCursor<T> cursor, Function<T, Row> transform) throws RelationalException {
        this.recordCursor = cursor;
        this.transform = transform;
        // TODO(sfines,yhatem) perform this in a non-blocking manner for more efficiency.
        this.continuation = ContinuationImpl.BEGIN;
        fetchNextResult();
    }

    public static <T> RecordLayerIterator<T> create(RecordCursor<T> cursor,
                                                    Function<T, Row> transform) throws RelationalException {
        return new RecordLayerIterator<>(cursor, transform);
    }

    @Override
    public void close() throws RelationalException {
        try {
            recordCursor.close();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public Continuation getContinuation() throws RelationalException {
        // If the result is already consumed, we check if there are more rows in the cursor. This is important to
        // update the continuation in case we reach the end of results.
        fetchNextResult();
        //TODO(bfines) replace this with mutable abstraction?
        //Alternatively, store the bytes and create the continuation upon demand
        return ContinuationImpl.copyOf(continuation);
    }

    @Override
    public boolean hasNext() {
        return fetchNextResult().hasNext();
    }

    // Returns the current (always non-null) fetched result, populating it (and, if iteration has ended,
    // noNextReason/continuation) first if necessary. Every other method in this class that needs the
    // current result goes through this method rather than reading the result field directly, so that
    // NullAway sees the narrowing.
    private RecordCursorResult<T> fetchNextResult() {
        RecordCursorResult<T> currentResult = result;
        if (currentResult != null) {
            return currentResult;
        }
        currentResult = recordCursor.getNext();
        result = currentResult;
        if (!currentResult.hasNext()) {
            final NoNextReason reason = currentResult.getNoNextReason();
            noNextReason = reason;
            if (reason == NoNextReason.SOURCE_EXHAUSTED) {
                this.continuation = ContinuationImpl.END;
            } else {
                // NullAway/JSpecify does not reliably track @Nullable on array-typed (byte[]) parameters:
                // fromUnderlyingBytes(@Nullable byte[]) already accepts null, and toBytes() genuinely
                // returns null in some cases (see RecordCursorContinuation#toBytes() javadoc).
                @SuppressWarnings("NullAway")
                final Continuation cont = ContinuationImpl.fromUnderlyingBytes(currentResult.getContinuation().toBytes());
                this.continuation = cont;
            }
        }
        return currentResult;
    }

    @Override
    public Row next() {
        // make a call to hasNext() before executing next() to ensure that the RecordCursorResult is fetched already.
        if (hasNext()) {
            // The current RecordCursorResult has a value to be returned to the consumer.
            try {
                final RecordCursorResult<T> currentResult = fetchNextResult();
                // hasNext() above (via fetchNextResult().hasNext()) is what guarantees get() is non-null here.
                final T nextValue = Objects.requireNonNull(currentResult.get());
                final var row = transform.apply(nextValue);
                // TODO(sfines,yhatem) pass the Record-Layer Continuation object as-is to avoid copying bytes around.
                // See the comment in fetchNextResult() above about this same @SuppressWarnings.
                @SuppressWarnings("NullAway")
                final Continuation cont = ContinuationImpl.fromUnderlyingBytes(currentResult.getContinuation().toBytes());
                this.continuation = cont;
                return row;
            } catch (RecordCoreException exception) {
                throw ExceptionUtil.toRelationalException(exception).toUncheckedWrappedException();
            } finally {
                // free up (maybe) consumed result.
                result = null;
            }
        } else if (terminatedEarly()) {
            // terminatedEarly() (just checked true) is what guarantees terminatedEarlyReason() is non-null here.
            throw new RelationalException(Objects.requireNonNull(terminatedEarlyReason()), ErrorCode.EXECUTION_LIMIT_REACHED).toUncheckedWrappedException();
        } else {
            throw new RelationalException("No next row available", ErrorCode.INVALID_CURSOR_STATE).toUncheckedWrappedException();
        }
    }

    @Override
    public boolean terminatedEarly() {
        return !hasNext() &&
                noNextReason != null &&
                noNextReason != NoNextReason.SOURCE_EXHAUSTED &&
                noNextReason != NoNextReason.RETURN_LIMIT_REACHED;
    }

    @Override
    public NoNextReason getNoNextReason() {
        // Only meaningful (and only ever called) once iteration has ended, at which point fetchNextResult()
        // has already set noNextReason; see terminatedEarly() and RecordLayerResultSet's caller.
        return Objects.requireNonNull(noNextReason, "getNoNextReason() called before iteration ended");
    }

    @Nullable
    private String terminatedEarlyReason() {
        if (!terminatedEarly()) {
            return null;
        }
        switch (Objects.requireNonNull(noNextReason)) {
            case TIME_LIMIT_REACHED:
                return "Time Limit allowed for the current transaction is exhausted";
            case BYTE_LIMIT_REACHED:
                return "Byte Limit allowed for the current transaction is exhausted";
            case SCAN_LIMIT_REACHED:
                return "Scan Limit allowed for the current transaction is exhausted";
            default:
                return null;
        }
    }

    @Override
    public boolean isClosed() {
        return recordCursor.isClosed();
    }
}
