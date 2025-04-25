/*
 * DedupCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncUtil;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A cursor that deduplicates the elements in the input and only return unique values.
 * An example usage for this kind of cursor is the iteration of KV pairs, returning the primary keys of the records. Since
 * there can be multiple Kvs per record, we need to filter out the redundancies.
 * This cursor takes in an <i>inner</i> cursor factory function and a pair of pack/unpack functions. Those are needed as
 * part of the continuation management, as the overall continuation for the cursor needs both the inner cursor continuation
 * and the last found value (so that can can compare it at the beginning of the next iteration).
 *
 * This cursor also assumes that there is always some forward progress made in each iteration (unless the inner record
 * is exhausted), so that we will get some inner result during every iteration to feed into the continuation.
 *
 * The cursor assumes that the inner cursor is sorted (the assumption is actually somewhat weaker: that the repeated elements are grouped)
 * such that all the elements of a certain repeated value appear in sequence, hence it can remove all but the first, and
 * the stored state can be kept to a minimum.
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.EXPERIMENTAL)
public class DedupCursor<T> implements RecordCursor<T> {
    // Inner cursor
    @Nonnull
    private final RecordCursor<T> inner;
    // The result returned from the cursor
    @Nullable
    private RecordCursorResult<T> nextResult;
    // The last value found (to be compared against the next value generated)
    @Nullable
    private T lastValue;
    // The method that can pack a value into a byte[]
    @Nonnull
    private final Function<T, byte[]> packValue;

    /**
     * Constructor.
     * @param innerCursorFactory factory method to create an inner cursor given a continuation
     * @param unpackValue a method that can unpack a value from byte array (used to deserialize from a continuation)
     * @param packValue a method that can pack a value into a byte array (used to serialize to a continuation)
     * @param continuation the cursor continuation (null if none)
     */
    @API(API.Status.EXPERIMENTAL)
    public DedupCursor(@Nonnull Function<byte[], RecordCursor<T>> innerCursorFactory,
                       @Nonnull Function<byte[], T> unpackValue,
                       @Nonnull Function<T, byte[]> packValue,
                       @Nullable byte[] continuation) {
        this.packValue = packValue;

        byte[] innerContinuation = null;
        if (continuation != null) {
            try {
                RecordCursorProto.DedupContinuation dedupContinuation = RecordCursorProto.DedupContinuation.parseFrom(continuation);
                // Both fields are required in the continuation
                innerContinuation = dedupContinuation.getInnerContinuation().toByteArray();
                lastValue = unpackValue.apply(dedupContinuation.getLastValue().toByteArray());
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("Error parsing continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
            }
        }
        inner = innerCursorFactory.apply(innerContinuation);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }

        AtomicReference<RecordCursorResult<T>> currentResult = new AtomicReference<>();
        return AsyncUtil.whileTrue(() -> inner.onNext().thenApply(innerResult -> {
            currentResult.set(innerResult);
            boolean foundNext = (innerResult.hasNext() && !Objects.equals(innerResult.get(), lastValue));
            return innerResult.hasNext() && !foundNext; // keep looping if we have more records and value is the same as before
        }), getExecutor()).thenApply(vignore -> applyResult(currentResult.get()));
    }

    @Nullable
    private RecordCursorResult<T> applyResult(final RecordCursorResult<T> currentResult) {
        if (currentResult.hasNext()) {
            lastValue = currentResult.get();
            nextResult = RecordCursorResult.withNextValue(lastValue, new DedupCursorContinuation(currentResult.getContinuation(), packValue.apply(lastValue)));
        } else {
            if (currentResult.getNoNextReason().isSourceExhausted()) {
                nextResult = RecordCursorResult.exhausted(); //continuation not valid here
            } else {
                nextResult = RecordCursorResult.withoutNextValue(new DedupCursorContinuation(currentResult.getContinuation(), packValue.apply(lastValue)), currentResult.getNoNextReason());
            }
        }
        return nextResult;
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return (inner.isClosed());
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

    //form a continuation that allows us to restart with the current cursor at its next position
    private class DedupCursorContinuation implements RecordCursorContinuation {
        private final RecordCursorContinuation innerContinuation;
        private final byte[] lastValue;
        private byte[] cachedBytes;

        private DedupCursorContinuation(RecordCursorContinuation innerContinuation, byte[] lastValue) {
            this.innerContinuation = innerContinuation;
            this.lastValue = lastValue;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            } else {
                //form bytes exactly once
                if (cachedBytes == null) {
                    cachedBytes = RecordCursorProto.DedupContinuation.newBuilder()
                            .setInnerContinuation(innerContinuation.toByteString())
                            .setLastValue(ByteString.copyFrom(lastValue))
                            .build()
                            .toByteArray();
                }
                return cachedBytes;
            }
        }

        @Override
        public boolean isEnd() {
            return innerContinuation.isEnd();
        }
    }
}
