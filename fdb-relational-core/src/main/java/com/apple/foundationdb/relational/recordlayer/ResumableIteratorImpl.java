/*
 * ResumableIteratorImpl.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.INVALID_PARAMETER;

/**
 * A resumable iterator which moves over an {@code Iterator}, optionally starting from a given {@code Continuation}.
 *
 * @param <T> the type of the iterator element.
 */
@NotThreadSafe
public class ResumableIteratorImpl<T> implements ResumableIterator<T> {
    private final Iterator<T> iterator;
    private Continuation continuation;

    public ResumableIteratorImpl(@Nonnull Iterator<T> iterator,
                                 @Nullable Continuation continuation) throws RelationalException {
        this.iterator = iterator;
        alignContinuation(continuation);
    }

    private void alignContinuation(Continuation continuation) throws RelationalException {
        if (continuation == null) {
            if (iterator.hasNext()) {
                this.continuation = ContinuationImpl.BEGIN;
            } else {
                this.continuation = ContinuationImpl.END;
            }
            return;
        }
        if (continuation.atBeginning()) {
            this.continuation = ContinuationImpl.BEGIN;
            return;
        }
        // if the continuation is FINISHED, it indicates no more rows are available.
        if (continuation.atEnd()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            this.continuation = ContinuationImpl.END;
            return;
        }
        assert continuation.getUnderlyingBytes() != null;
        int offset = Ints.fromByteArray(continuation.getUnderlyingBytes());
        int counter = 0;
        while (iterator.hasNext() && counter < offset) {
            counter++;
            iterator.next();
        }
        if (counter < offset) {
            throw new RelationalException("continuation out of iterator bounds", INVALID_PARAMETER); // TODO(yhatem) refine error.
        }
        this.continuation = ContinuationImpl.copyOf(continuation);
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public Continuation getContinuation() throws RelationalException {
        return continuation;
    }

    /**
     * This type of resumable iterator never terminates early.
     *
     * @return always {@code false}.
     */
    @Override
    public boolean terminatedEarly() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean hasNext() {
        boolean result = iterator.hasNext();
        if (!result) {
            continuation = ContinuationImpl.END;
        }
        return result;
    }

    @Override
    public T next() {
        if (iterator.hasNext()) {
            T result = iterator.next();
            // point to the NEXT element
            if (iterator.hasNext()) {
                if (continuation.atBeginning()) {
                    continuation = ContinuationImpl.fromInt(1);
                } else {
                    continuation = ContinuationImpl.fromInt(Ints.fromByteArray(continuation.getUnderlyingBytes()) + 1);
                }
            } else {
                continuation = ContinuationImpl.END;
            }
            return result;
        } else {
            continuation = ContinuationImpl.END;
            // fallthrough the underlying iterator semantics of next() when hasNext() potentially returns NULL
            // e.g. throw an exception.
            return iterator.next();
        }
    }
}
