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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A {@code RecordCursor} that calls a function that maps the value and can stop the cursor.
 * @param <T> the type of elements of the source cursor
 * @param <V> the type of elements of the cursor after applying the function
 */
public class MapWhileCursor<T, V> implements RecordCursor<V> {
    /**
     * What to return for {@link #getContinuation()} after stopping.
     */
    public static enum StopContinuation {
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
    StopContinuation stopContinuation;
    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private byte[] continuationBefore;
    @Nullable
    private byte[] continuationAfter;
    private NoNextReason noNextReason;
    @Nullable
    private V record;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public MapWhileCursor(@Nonnull RecordCursor<T> inner, @Nonnull Function<T, Optional<V>> func,
                          @Nonnull StopContinuation stopContinuation, @Nullable byte[] initialContinuation,
                          @Nonnull NoNextReason noNextReason) {
        this.inner = inner;
        this.func = func;
        this.stopContinuation = stopContinuation;
        this.continuationAfter = initialContinuation;
        this.noNextReason = noNextReason;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            nextFuture = inner.onHasNext()
                    .thenApply(innerHasNext -> {
                        if (!innerHasNext) {
                            continuationAfter = inner.getContinuation();
                            noNextReason = inner.getNoNextReason();
                            return false;
                        }
                        continuationBefore = continuationAfter;
                        Optional<V> maybeRecord = func.apply(inner.next());
                        continuationAfter = inner.getContinuation();
                        if (maybeRecord.isPresent()) {
                            record = maybeRecord.get();
                            return true;
                        }
                        switch (stopContinuation) {
                            case NONE:
                                continuationAfter = null;
                                break;
                            case BEFORE:
                                continuationAfter = continuationBefore;
                                break;
                            case AFTER:
                            default:
                                break;
                        }
                        return false;
                    });
        }
        return nextFuture;
    }

    @Nullable
    @Override
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        nextFuture = null;
        return record;
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getContinuation() {
        return continuationAfter;
    }

    @Override
    public NoNextReason getNoNextReason() {
        return noNextReason;
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(false);
            nextFuture = null;
        }
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
