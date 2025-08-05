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
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

@API(API.Status.EXPERIMENTAL)
public final class RecordLayerIterator<T> implements ResumableIterator<Row> {
    private final RecordCursor<T> recordCursor;
    private final Function<T, Row> transform;
    private RecordCursorResult<T> result;
    private Continuation continuation;
    private RecordCursor.NoNextReason noNextReason = null;

    private RecordLayerIterator(@Nonnull RecordCursor<T> cursor, @Nonnull Function<T, Row> transform) throws RelationalException {
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
        fetchNextResult();
        return result.hasNext();
    }

    private void fetchNextResult() {
        if (result != null) {
            return;
        }
        result = recordCursor.getNext();
        if (!result.hasNext()) {
            noNextReason = result.getNoNextReason();
            if (noNextReason == RecordCursor.NoNextReason.SOURCE_EXHAUSTED) {
                this.continuation = ContinuationImpl.END;
            } else {
                this.continuation = ContinuationImpl.fromUnderlyingBytes(result.getContinuation().toBytes());
            }
        }
    }

    @Override
    public Row next() {
        // make a call to hasNext() before executing next() to ensure that the RecordCursorResult is fetched already.
        if (hasNext()) {
            // The current RecordCursorResult has a value to be returned to the consumer.
            try {
                final var row = transform.apply(result.get());
                // TODO(sfines,yhatem) pass the Record-Layer Continuation object as-is to avoid copying bytes around.
                this.continuation = ContinuationImpl.fromUnderlyingBytes(result.getContinuation().toBytes());
                return row;
            } catch (RecordCoreException exception) {
                throw ExceptionUtil.toRelationalException(exception).toUncheckedWrappedException();
            } finally {
                // free up (maybe) consumed result.
                result = null;
            }
        } else if (terminatedEarly()) {
            throw new RelationalException(terminatedEarlyReason(), ErrorCode.EXECUTION_LIMIT_REACHED).toUncheckedWrappedException();
        } else {
            throw new RelationalException("No next row available", ErrorCode.INVALID_CURSOR_STATE).toUncheckedWrappedException();
        }
    }

    @Override
    public boolean terminatedEarly() {
        return !hasNext() &&
                noNextReason != null &&
                noNextReason != RecordCursor.NoNextReason.SOURCE_EXHAUSTED &&
                noNextReason != RecordCursor.NoNextReason.RETURN_LIMIT_REACHED;
    }

    @Override
    public RecordCursor.NoNextReason getNoNextReason() {
        return noNextReason;
    }

    @Nullable
    private String terminatedEarlyReason() {
        if (!terminatedEarly()) {
            return null;
        }
        switch (noNextReason) {
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
