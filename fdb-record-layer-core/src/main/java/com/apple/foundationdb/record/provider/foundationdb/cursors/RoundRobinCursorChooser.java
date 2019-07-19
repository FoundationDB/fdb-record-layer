/*
 * RoundRobinCursorChooser.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursorResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Given a list of cursor states, this keeps track of the state necessary to go through the list of cursors
 * in a round-robin fashion. This abstracts some of the common logic shared by the {@link UnorderedUnionCursor}
 * and the {@link ProbableIntersectionCursor}, both of which choose cursors this way.
 */
class RoundRobinCursorChooser<T> {
    @Nonnull
    private final List<? extends MergeCursorState<T>> cursorStates;
    private int nextStatePos;

    RoundRobinCursorChooser(@Nonnull List<? extends MergeCursorState<T>> cursorStates, int nextStatePos) {
        this.cursorStates = cursorStates;
        this.nextStatePos = nextStatePos;
    }

    /**
     * Move the round-robin cursor forward. This should be called after consuming a child and returning it.
     */
    void advance() {
        nextStatePos = (nextStatePos + 1) % cursorStates.size();
    }

    int getNextStatePos() {
        return nextStatePos;
    }

    /**
     * Choose the next state (using the round-robin policy). It will skip over any states that it
     * knows are exhausted (though the future itself may correspond to an exhausted state if incomplete).
     *
     * @return a future from the next cursor state or {@code null} if all cursors are known to be exhausted
     */
    @Nullable
    CompletableFuture<RecordCursorResult<T>> getFutureFromNextState() {
        int initialPos = nextStatePos;
        MergeCursorState<T> nextState = cursorStates.get(nextStatePos);
        while (MoreAsyncUtil.isCompletedNormally(nextState.getOnNextFuture())) {
            final RecordCursorResult<T> resultState = nextState.getResult();
            if (!resultState.hasNext() && resultState.getNoNextReason().isSourceExhausted()) {
                advance();
                if (nextStatePos == initialPos) {
                    // We went through all of them, so return null here to indicate that we're exhausted.
                    return null;
                }
                nextState = cursorStates.get(nextStatePos);
            } else {
                break;
            }
        }
        return nextState.getOnNextFuture();
    }
}
