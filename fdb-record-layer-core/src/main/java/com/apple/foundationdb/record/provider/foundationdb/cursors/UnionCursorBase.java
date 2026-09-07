/*
 * UnionCursorBase.java
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

import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableSet;

import org.jspecify.annotations.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Common implementation code for performing a union shared between
 * the different intersection cursors. In particular, this base cursor
 * is agnostic to the type of result it returns to the users
 *
 * @param <T> the type of elements returned by each child cursor
 */
abstract class UnionCursorBase<T, S extends MergeCursorState<T>> extends MergeCursor<T, T, S> {
    static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_UNION);
    static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_UNION_PLAN_UNIQUES);
    static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_UNION_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    UnionCursorBase(List<S> cursorStates, @Nullable FDBStoreTimer timer) {
        super(cursorStates, timer);
    }

    @Override
    protected UnionCursorContinuation getContinuationObject() {
        return UnionCursorContinuation.from(this);
    }

    /**
     * Get the result from the list of chosen states. These states have all been identified
     * as equivalent from the point of view of the union, so only one element should be
     * returned from all of them. Implementations may choose to combine the results
     * from the various child results if that is useful.
     *
     * @param chosenStates the states that contain the next value to return
     * @return the result to return from this cursor given these elements appear in the union
     */
    @Nullable
    @Override
    protected T getNextResult(List<S> chosenStates) {
        return Objects.requireNonNull(chosenStates.get(0).getResult()).get();
    }

    /**
     * Merges all of the cursors and whether they have stopped and returns the "strongest" reason for the result to stop.
     * It will return an out-of-band reason if any of the children stopped for an out-of-band reason, and it will
     * only return {@link NoNextReason#SOURCE_EXHAUSTED SOURCE_EXHAUSTED} if all of the cursors are exhausted.
     * @return the strongest reason for stopping
     */
    @Override
    protected NoNextReason mergeNoNextReasons() {
        return getStrongestNoNextReason(getCursorStates());
    }
}
