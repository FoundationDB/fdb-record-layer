/*
 * MapWhileCursor.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A {@code RecordCursor} that calls a function that maps the value and can stop the cursor.
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor after applying the function
 */
@API(API.Status.UNSTABLE)
public class MapWhileCursor<T, V> implements RecordCursor<V> {
    /**
     * What to return for {@link RecordCursorResult#getContinuation()} after stopping.
     */
    public enum StopContinuation {
        /**
         * Return {@code null}.
         */
        NONE,
        /**
         * Return the continuation following the record that stopped the cursor.
         */
        AFTER,
        /**
         * Return the continuation <em>before</em> the record that stopped the cursor.
         * That is, arrange for the cursor to continue by repeating that record.
         */
        BEFORE
    }

    @Nonnull
    private final RecordCursor<T> inner;
    @Nonnull
    private final Function<T, Optional<V>> func;
    @Nonnull
    private final StopContinuation stopContinuation;
    @Nonnull
    private RecordCursorResult<V> nextResult = RecordCursorResult.withNextValue(null, RecordCursorStartContinuation.START);

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    @SuppressWarnings("PMD.UnusedFormalParameter") // for compatibility reasons
    public MapWhileCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, Optional<V>> func,
                          @Nonnull StopContinuation stopContinuation, @Nullable byte[] initialContinuation,
                          @Nonnull NoNextReason noNextReason) {
        this.inner = inner;
        this.func = func;
        this.stopContinuation = stopContinuation;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (!nextResult.hasNext()) {
            // It is necessary to check to see if a result has completed before as it is otherwise possible
            // that the cursor return more results if values later on in the child cursor match the predicate.
            return CompletableFuture.completedFuture(nextResult);
        } else {
            return inner.onNext().thenApply(innerResult -> {
                if (!innerResult.hasNext()) {
                    nextResult = RecordCursorResult.withoutNextValue(innerResult);
                    return nextResult;
                }
                final Optional<V> maybeRecord = func.apply(innerResult.get());
                if (maybeRecord.isPresent()) {
                    nextResult = RecordCursorResult.withNextValue(maybeRecord.get(), innerResult.getContinuation());
                    return nextResult;
                }
                // return no record, handle special cases for continuation
                switch (stopContinuation) {
                    case NONE:
                        nextResult = RecordCursorResult.exhausted();
                        break;
                    case BEFORE:
                        final RecordCursorContinuation continuation = nextResult.getContinuation(); // previous saved result
                        nextResult = RecordCursorResult.withoutNextValue(continuation, NoNextReason.SCAN_LIMIT_REACHED);
                        break;
                    case AFTER:
                    default:
                        nextResult = RecordCursorResult.withoutNextValue(innerResult.getContinuation(), NoNextReason.SCAN_LIMIT_REACHED);
                        break;
                }
                return nextResult;
            });
        }
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
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
