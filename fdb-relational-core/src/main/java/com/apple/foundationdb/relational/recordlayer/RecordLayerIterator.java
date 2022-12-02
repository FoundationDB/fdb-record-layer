/*
 * RecordLayerIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.util.function.Function;

public final class RecordLayerIterator<T> implements ResumableIterator<Row> {
    private final RecordCursor<T> recordCursor;
    private final Function<T, Row> transform;
    private RecordCursorResult<T> result;
    private Continuation continuation;

    private RecordLayerIterator(@Nonnull RecordCursor<T> cursor, @Nonnull Function<T, Row> transform) throws RelationalException {
        this.recordCursor = cursor;
        this.transform = transform;
        // TODO(sfines,yhatem) perform this in a non-blocking manner for more efficiency.
        try {
            this.result = recordCursor.getNext();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        this.continuation = Continuation.BEGIN;
        if (result.getContinuation().isEnd()) {
            this.continuation = Continuation.EMPTY_SET;
        }
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
        //TODO(bfines) replace this with mutable abstraction?
        return ContinuationImpl.copyOf(continuation);
    }

    @Override
    public boolean hasNext() {
        return result.hasNext();
    }

    @Override
    public Row next() {
        try {
            final Row row = transform.apply(result.get());
            // TODO(sfines,yhatem) pass the Record-Layer Continuation object as-is to avoid copying bytes around.
            this.continuation = ContinuationImpl.fromBytes(result.getContinuation().toBytes());
            result = recordCursor.getNext();
            if (result.getContinuation().isEnd()) {
                this.continuation = Continuation.END;
            }
            return row;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex).toUncheckedWrappedException();
        }
    }

    @Override
    public boolean terminatedEarly() {
        return !hasNext() && result.getNoNextReason() != RecordCursor.NoNextReason.SOURCE_EXHAUSTED;
    }
}
