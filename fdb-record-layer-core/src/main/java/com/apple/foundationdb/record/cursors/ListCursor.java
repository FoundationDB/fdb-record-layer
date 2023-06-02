/*
 * ListCursor.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A cursor that returns the elements of a list.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class ListCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final List<T> list;
    private int nextPosition; // position of the next value to return
    private boolean closed = false;

    public ListCursor(@Nonnull List<T> list, byte []continuation) {
        this(ForkJoinPool.commonPool(), list, continuation != null ? ByteBuffer.wrap(continuation).getInt() : 0);
    }

    public ListCursor(@Nonnull Executor executor, @Nonnull List<T> list, int nextPosition) {
        this.executor = executor;
        this.list = list;
        this.nextPosition = nextPosition;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return CompletableFuture.completedFuture(getNext());
    }

    @Nonnull
    @Override
    public RecordCursorResult<T> getNext() {
        RecordCursorResult<T> nextResult;
        if (nextPosition < list.size()) {
            nextResult = RecordCursorResult.withNextValue(list.get(nextPosition), new Continuation(nextPosition + 1, list.size()));
            nextPosition++;
        } else {
            nextResult = RecordCursorResult.exhausted();
        }
        return nextResult;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    @Override
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    private static class Continuation implements RecordCursorContinuation {
        private final int listSize;
        private final int nextPosition;

        public Continuation(int nextPosition, int listSize) {
            this.nextPosition = nextPosition;
            this.listSize = listSize;
        }

        @Override
        public boolean isEnd() {
            // If a next value is returned as part of a cursor result, the continuation must not be an end continuation
            // (i.e., isEnd() must be false), per the contract of RecordCursorResult. This is the case even if the
            // cursor knows for certain that there is no more after that result, as in the ListCursor.
            // Concretely, this means that we really need a > here, rather than >=.
            return nextPosition > listSize;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            }
            return ZeroCopyByteString.wrap(Objects.requireNonNull(toBytes()));
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            return ByteBuffer.allocate(Integer.BYTES).putInt(nextPosition).array();
        }
    }
}
