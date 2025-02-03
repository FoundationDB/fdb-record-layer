/*
 * RangeCursor.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns a sequence from 0 to an (exclusive) limit.
 */
@API(API.Status.UNSTABLE)
public class RangeCursor implements RecordCursor<Integer> {
    @Nonnull
    private final Executor executor;
    private final int exclusiveLimit;
    private int nextPosition; // position of the next value to return
    private boolean closed = false;

    public RangeCursor(@Nonnull Executor executor, final int exclusiveLimit, byte[] continuation) {
        this(executor, exclusiveLimit, continuation != null ? ByteBuffer.wrap(continuation).getInt() : 0);
    }

    public RangeCursor(@Nonnull Executor executor, final int exclusiveLimit, final int nextPosition) {
        this.executor = executor;
        this.exclusiveLimit = exclusiveLimit;
        this.nextPosition = nextPosition;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<Integer>> onNext() {
        return CompletableFuture.completedFuture(getNext());
    }

    @Nonnull
    @Override
    public RecordCursorResult<Integer> getNext() {
        RecordCursorResult<Integer> nextResult;
        if (nextPosition < exclusiveLimit) {
            nextResult = RecordCursorResult.withNextValue(nextPosition, new Continuation(nextPosition + 1, exclusiveLimit));
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
        private final int size;
        private final int nextPosition;

        public Continuation(int nextPosition, int size) {
            this.nextPosition = nextPosition;
            this.size = size;
        }

        @Override
        public boolean isEnd() {
            // If a next value is returned as part of a cursor result, the continuation must not be an end continuation
            // (i.e., isEnd() must be false), per the contract of RecordCursorResult. This is the case even if the
            // cursor knows for certain that there is no more after that result, as in the ListCursor.
            // Concretely, this means that we really need a > here, rather than >=.
            return nextPosition > size;
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
