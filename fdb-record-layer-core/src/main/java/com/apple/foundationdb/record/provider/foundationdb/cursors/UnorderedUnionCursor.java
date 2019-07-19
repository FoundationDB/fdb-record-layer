/*
 * UnorderedUnionCursor.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A cursor that returns the results of two or more cursors that may return elements in any order. This cursor will return
 * elements from its child cursors using a round robin strategy. In particular, it will return the first element from
 * its first child, then the first element from its second child, and so on until it has returned one element from each
 * child. It will then return the second element from its first child, then the second element from its second child,
 * and so on. Once a child cursor has been exhausted, it will skip that child. If a child hits a different limit,
 * then the cursor will stop when it hits that child and return a {@link com.apple.foundationdb.record.RecordCursor.NoNextReason}
 * consistent with a stopped child. Note that duplicate values are not removed.
 *
 * <p>
 * If the cursor is resumed from a continuation, the cursor will start from the first child cursor that had hit a limit.
 * In this way, the returned values should be invariant to cursor restarts as long as each child cursor can be restarted
 * from its continuation without changing its returned values and as long as the underlying data do not change while the
 * cursor is running.
 * </p>
 *
 * <p>
 * Internally, this cursor will begin all child cursors concurrently, so even though the return order is deterministic,
 * this can effectively pipeline pulling from each child.
 * </p>
 *
 * @param <T> the type of elements returned by the cursor
 */
@API(API.Status.EXPERIMENTAL)
public class UnorderedUnionCursor<T> extends UnionCursorBase<T, MergeCursorState<T>> {
    @Nonnull
    private RoundRobinCursorChooser<T> cursorChooser;

    private UnorderedUnionCursor(@Nonnull List<MergeCursorState<T>> cursorStates,
                                 int currentChild,
                                 @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
        this.cursorChooser = new RoundRobinCursorChooser<>(cursorStates, currentChild);
    }

    int getCurrentChildPos() {
        return cursorChooser.getNextStatePos();
    }

    @Override
    @Nonnull
    UnorderedUnionCursorContinuation getContinuationObject() {
        return UnorderedUnionCursorContinuation.from(this);
    }

    @Nonnull
    @Override
    CompletableFuture<List<MergeCursorState<T>>> computeNextResultStates() {
        final long startComputingStateTime = System.currentTimeMillis();
        final List<MergeCursorState<T>> cursorStates = getCursorStates();
        startAllStates(cursorStates);

        AtomicReference<MergeCursorState<T>> nextStateRef = new AtomicReference<>();
        return AsyncUtil.whileTrue(() -> {
            checkNextStateTimeout(startComputingStateTime);
            CompletableFuture<RecordCursorResult<T>> nextResultFuture = cursorChooser.getFutureFromNextState();
            if (nextResultFuture == null) {
                // All children are exhausted.
                return AsyncUtil.READY_FALSE;
            }
            return nextResultFuture.thenApply(nextResult -> {
                final MergeCursorState<T> nextState = cursorStates.get(cursorChooser.getNextStatePos());
                if (nextResult.hasNext()) {
                    nextStateRef.set(nextState);
                    cursorChooser.advance();
                    return false;
                } else if (nextResult.getNoNextReason().isSourceExhausted()) {
                    // Cursor is exhausted. Advance to the next cursor and try that one.
                    cursorChooser.advance();
                    return true;
                } else {
                    // The cursor has stopped, but it is not exhausted. Stop looking and bubble up limit.
                    return false;
                }
            });
        }, getExecutor()).thenApply(vignore -> {
            if (nextStateRef.get() == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(nextStateRef.get());
            }
        });
    }

    @Nonnull
    private static <T> List<MergeCursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                    @Nonnull UnionCursorContinuation continuation) {
        final List<MergeCursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(KeyedMergeCursorState.from(cursorFunction, continuation.getContinuations().get(i)));
            i++;
        }
        return cursorStates;
    }

    /**
     * Create a union cursor from two or more cursors. Unlike the other {@link UnionCursor}, this does
     * not require that the child cursors return values in any particular order. The trade-off, however,
     * is that this cursor will not attempt to remove any duplicates from its children.
     *
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing any records from any child cursor
     */
    @Nonnull
    public static <T> UnorderedUnionCursor<T> create(
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        final UnorderedUnionCursorContinuation unionContinuation = UnorderedUnionCursorContinuation.from(continuation, cursorFunctions.size());
        return new UnorderedUnionCursor<>(createCursorStates(cursorFunctions, unionContinuation), unionContinuation.getCurrentChild(), timer);
    }
}
