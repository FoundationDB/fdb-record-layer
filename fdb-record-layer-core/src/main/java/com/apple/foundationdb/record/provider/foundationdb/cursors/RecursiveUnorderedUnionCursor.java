/*
 * RecursiveUnorderedUnionCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.cursors.DoubleBufferCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * TODO.
 */
public class RecursiveUnorderedUnionCursor extends UnorderedUnionCursor<QueryResult> {
    protected RecursiveUnorderedUnionCursor(@Nonnull List<MergeCursorState<QueryResult>> cursorStates,
                                            @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
    }

    @Nonnull
    @Override
    protected CompletableFuture<List<MergeCursorState<QueryResult>>> computeNextResultStates() {
        final long startComputingStateTime = System.currentTimeMillis();
        final List<MergeCursorState<QueryResult>> cursorStates = getCursorStates();
        final var recursionBaseCursorStates = cursorStates.subList(0, cursorStates.size() - 1);
        AtomicReference<MergeCursorState<QueryResult>> nextStateRef = new AtomicReference<>();
        final var recursiveCursor = (DoubleBufferCursor)cursorStates.get(1).getCursor();
        final var recursionCursorState = cursorStates.get(cursorStates.size() - 1);
        return AsyncUtil.whileTrue(() -> whenAny(recursionBaseCursorStates).thenApply(vignore -> {
            checkNextStateTimeout(startComputingStateTime);
            MergeCursorState<QueryResult> nextState = null;
            boolean allDone = true;
            for (MergeCursorState<QueryResult> cursorState : cursorStates) {
                if (!MoreAsyncUtil.isCompletedNormally(cursorState.getOnNextFuture())) {
                    allDone = false;
                    continue;
                }
                final RecordCursorResult<QueryResult> result = cursorState.getResult();
                if (result.hasNext()) {
                    recursiveCursor.add(result.get());
                    // Found a cursor with an element.
                    allDone = false;
                    nextState = cursorState;
                    break;
                }
            }
            if (nextState != null) {
                nextStateRef.set(nextState);
            }
            return nextState == null && !allDone;
        }), getExecutor()).thenApply(
                vignore -> AsyncUtil.whileTrue(() -> {
                    if (!MoreAsyncUtil.isCompletedNormally(recursionCursorState.getOnNextFuture())) {
                        return  AsyncUtil.READY_FALSE;
                    }
                    final var result = recursionCursorState.getResult();
                    if (!result.hasNext()) {
                        recursiveCursor.flip();
                    }
                    return  AsyncUtil.READY_TRUE;
                })
        ).thenApply(vignore -> {
            if (nextStateRef.get() == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(nextStateRef.get());
            }
        });
    }

    @Nonnull
    public static RecursiveUnorderedUnionCursor create(
            @Nonnull List<Function<byte[], RecordCursor<QueryResult>>> baseRecursionCursors,
            @Nonnull Function<byte[] , RecordCursor<QueryResult>> recursiveCursor,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        final ImmutableList.Builder<Function<byte[], RecordCursor<QueryResult>>> cursorFunctionsBuilder = ImmutableList.builder();
        cursorFunctionsBuilder.addAll(baseRecursionCursors);
        cursorFunctionsBuilder.add(recursiveCursor);
        final var cursorFunctions = cursorFunctionsBuilder.build();
        final var cursorStates = createCursorStates(cursorFunctions, continuation);
        return new RecursiveUnorderedUnionCursor(cursorStates, timer);
    }
}
