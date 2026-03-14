/*
 * OrElseCursor.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A cursor that returns the elements of one cursor followed by the elements of another cursor if the first was empty.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public final class OrElseCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<Executor, RecordCursor<T>> func;
    @Nonnull
    private RecordCursorProto.OrElseContinuation.State state;
    @Nullable
    private RecordCursor<T> other;
    @Nullable
    private RecordCursorResult<T> nextResult;

    @API(API.Status.INTERNAL)
    public OrElseCursor(@Nonnull Function<byte[], ? extends RecordCursor<T>> innerFunc,
                        @Nonnull BiFunction<Executor, byte[], ? extends RecordCursor<T>> elseFunc,
                        @Nullable byte[] continuation) {
        final Function<Executor, RecordCursor<T>> newElseFunc = executor -> elseFunc.apply(executor, null);

        if (continuation == null) {
            this.state = RecordCursorProto.OrElseContinuation.State.UNDECIDED;
            this.inner = innerFunc.apply(null);
            this.func = newElseFunc;
        } else {
            RecordCursorProto.OrElseContinuation parsed;
            try {
                parsed = RecordCursorProto.OrElseContinuation.parseFrom(continuation);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("error parsing continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
            }
            this.state = parsed.getState();

            switch (this.state) {
                case UNDECIDED:
                case USE_INNER:
                    this.inner = innerFunc.apply(parsed.getContinuation().toByteArray());
                    this.func = newElseFunc;
                    break;
                case USE_OTHER:
                    this.inner = RecordCursor.empty();
                    final byte[] otherContinuation = parsed.getContinuation().toByteArray();
                    this.func = executor -> elseFunc.apply(executor, otherContinuation);
                    this.other = func.apply(getExecutor());
                    break;
                default:
                    throw new UnknownOrElseCursorStateException();
            }
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        final CompletableFuture<RecordCursorResult<T>> innerFuture;
        switch (state) {
            case USE_INNER:
                innerFuture = inner.onNext();
                break;
            case USE_OTHER:
                innerFuture = other.onNext();
                break;
            case UNDECIDED:
                innerFuture = inner.onNext().thenCompose(result -> {
                    if (result.hasNext()) {
                        // Inner cursor has produced a value, so we take the inner branch.
                        state = RecordCursorProto.OrElseContinuation.State.USE_INNER;
                        return CompletableFuture.completedFuture(result);
                    } else if (result.getNoNextReason().isOutOfBand()) {
                        // Not sure if the inner cursor will ever produce a value.
                        return CompletableFuture.completedFuture(result);
                    } else {
                        // Inner cursor will never produce a value.
                        state = RecordCursorProto.OrElseContinuation.State.USE_OTHER;
                        other = func.apply(getExecutor());
                        return other.onNext();
                    }
                });
                break;
            default:
                throw new UnknownOrElseCursorStateException();
        }

        return innerFuture.thenApply(result -> result.withContinuation(new Continuation(state, result.getContinuation())))
                .thenApply(this::postProcess);
    }

    // shim to support old continuation style
    @Nonnull
    private RecordCursorResult<T> postProcess(RecordCursorResult<T> result) {
        nextResult = result;
        return result;
    }

    @Override
    public void close() {
        if (other != null) {
            other.close();
        }
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return (other == null || other.isClosed()) && inner.isClosed();
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

    private static class UnknownOrElseCursorStateException extends RecordCoreException {
        public static final long serialVersionUID = 1;

        public UnknownOrElseCursorStateException() {
            super("unknown state for OrElseCursor");
        }
    }

    private static class Continuation implements RecordCursorContinuation {
        private final RecordCursorProto.OrElseContinuation.State state;
        @Nonnull
        private final RecordCursorContinuation innerOrOtherContinuation;

        public Continuation(@Nonnull RecordCursorProto.OrElseContinuation.State state, @Nonnull RecordCursorContinuation innerOrOtherContinuation) {
            this.state = state;
            this.innerOrOtherContinuation = innerOrOtherContinuation;
        }

        @Override
        public boolean isEnd() {
            return innerOrOtherContinuation.isEnd();
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            ByteString bytes = innerOrOtherContinuation.toByteString();
            if (isEnd() || bytes.isEmpty()) {
                return ByteString.EMPTY;
            }
            return RecordCursorProto.OrElseContinuation.newBuilder()
                    .setState(state)
                    .setContinuation(bytes)
                    .build()
                    .toByteString();
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            ByteString byteString = toByteString();
            return byteString.isEmpty() ? null : byteString.toByteArray();
        }
    }

}
