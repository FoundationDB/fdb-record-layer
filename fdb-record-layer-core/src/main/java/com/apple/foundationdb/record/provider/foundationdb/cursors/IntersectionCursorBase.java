/*
 * IntersectionCursorBase.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.cursors.IllegalContinuationAccessChecker;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common implementation code for performing an intersection shared between
 * the different intersection cursors. In particular, this base cursor
 * is agnostic to the type of result it returns to the users
 *
 * @param <T> the type of elements returned by each child cursor
 * @param <U> the type of elements returned by this cursor
 */
abstract class IntersectionCursorBase<T, U> implements RecordCursor<U> {
    @Nonnull
    private final Function<? super T, ? extends List<Object>> comparisonKeyFunction;
    private final boolean reverse;
    @Nonnull
    private final List<CursorState<T>> cursorStates;
    @Nullable
    private final FDBStoreTimer timer;
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private RecordCursorResult<U> nextResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_INTERSECTION);
    @Nonnull
    private static final Set<StoreTimer.Count> matchesCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_MATCHES);
    @Nonnull
    private static final Set<StoreTimer.Count> nonmatchesCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_NONMATCHES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    protected static class CursorState<T> {
        @Nonnull
        private final RecordCursor<T> cursor;
        @Nullable
        private CompletableFuture<RecordCursorResult<T>> onNextFuture;
        private List<Object> key;
        @Nonnull
        private RecordCursorContinuation continuation;
        @Nullable
        private RecordCursorResult<T> result;

        CursorState(@Nonnull RecordCursor<T> cursor, @Nonnull RecordCursorContinuation continuation) {
            this.cursor = cursor;
            this.continuation = continuation;
        }

        @Nonnull
        public CompletableFuture<RecordCursorResult<T>> getOnNextFuture(@Nonnull Function<? super T, ? extends List<Object>> keyFunction) {
            if (onNextFuture == null) {
                onNextFuture = cursor.onNext().thenApply(cursorResult -> {
                    result = cursorResult;
                    if (result.hasNext()) {
                        key = keyFunction.apply(result.get());
                    } else {
                        continuation = result.getContinuation(); // no result, so we advanced the cached continuation
                    }
                    return cursorResult;
                });
            }
            return onNextFuture;
        }

        public void consume() {
            // after consuming a element from a cursor, we should never need to query it again,
            // so we update its continuation information now
            onNextFuture = null;
            continuation = result.getContinuation();
        }

        @Nonnull
        public static <T> CursorState<T> from(
                @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
                @Nonnull RecordCursorContinuation continuation) {
            if (continuation.isEnd()) {
                return new CursorState<>(RecordCursor.empty(), RecordCursorEndContinuation.END);
            } else {
                return new CursorState<>(cursorFunction.apply(continuation.toBytes()), continuation);
            }
        }

        @Nullable
        public RecordCursorResult<T> getResult() {
            return result;
        }
    }

    protected IntersectionCursorBase(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                     boolean reverse, @Nonnull List<CursorState<T>> cursorStates,
                                     @Nullable FDBStoreTimer timer) {
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.reverse = reverse;
        this.cursorStates = cursorStates;
        this.timer = timer;
    }

    // Identify the list of maximal (and non-maximal) elements from the list of cursor states.
    private void mergeStates(@Nonnull List<CursorState<T>> maxStates, @Nonnull List<CursorState<T>> nonMaxCursors, long startTime) {
        maxStates.add(cursorStates.get(0));
        List<Object> maxKey = cursorStates.get(0).key;
        for (CursorState<T> cursorState : cursorStates.subList(1, cursorStates.size())) {
            int compare = KeyComparisons.KEY_COMPARATOR.compare(cursorState.key, maxKey) * (reverse ? -1 : 1);
            if (compare == 0) {
                maxStates.add(cursorState);
            } else if (compare < 0) {
                // this new cursor is definitely not in the intersection
                nonMaxCursors.add(cursorState);
            } else {
                // the existing cursors in maxCursors are all not in the intersection
                // this new cursor is now the only max cursor
                nonMaxCursors.addAll(maxStates);
                maxStates.clear();
                maxKey = cursorState.key;
                maxStates.add(cursorState);
            }
        }

        if (!nonMaxCursors.isEmpty()) {
            // Any non-maximal cursor is definitely not in the intersection,
            // so we can consume those records (which updates their continuations).
            nonMaxCursors.forEach(CursorState::consume);
        }

        if (timer != null) {
            if (nonMaxCursors.isEmpty()) {
                // All of the cursors are in the intersection, so return a match.
                // Subtract one from the number of cursors in the matching set in
                // order to make it so that the metric represents the number of
                // records *not* returned because they have been matched away in
                // an intersection.
                timer.record(duringEvents, System.nanoTime() - startTime);
                timer.increment(matchesCounts, maxStates.size() - 1);
            } else {
                timer.record(duringEvents, System.nanoTime() - startTime);
                timer.increment(nonmatchesCounts, nonMaxCursors.size());
            }
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<U>> onNext() {
        mayGetContinuation = false;
        return AsyncUtil.whileTrue(() -> {
            CompletableFuture<?>[] onNextFutures = new CompletableFuture<?>[cursorStates.size()];
            int i = 0;
            for (CursorState<T> cursorState : cursorStates) {
                onNextFutures[i] = cursorState.getOnNextFuture(comparisonKeyFunction);
                i++;
            }
            return CompletableFuture.allOf(onNextFutures).thenApply(vignore -> {
                // If any of the cursors do not have a next element, then we are done.
                if (cursorStates.stream().anyMatch(cursorState -> !cursorState.result.hasNext())) {
                    return false;
                }

                long startTime = System.nanoTime();
                // If everything compares equally, then we have a match and should return it.
                // Otherwise, everything except for the maximum iterator values is guaranteed
                // not to match, so null those values out and then move on.
                List<CursorState<T>> maxCursors = new ArrayList<>(cursorStates.size());
                List<CursorState<T>> nonMaxCursors = new ArrayList<>(cursorStates.size());
                mergeStates(maxCursors, nonMaxCursors, startTime);
                return !nonMaxCursors.isEmpty();
            });
        }, getExecutor()).thenApply(vignore -> {
            boolean hasNext = cursorStates.stream().allMatch(cursorState -> cursorState.result.hasNext());
            mayGetContinuation = !hasNext;
            if (!hasNext) {
                nextResult = RecordCursorResult.withoutNextValue(IntersectionContinuation.from(this), mergeNoNextReasons());
            } else {
                final U result = getNextResult(cursorStates);
                cursorStates.forEach(CursorState::consume);
                nextResult = RecordCursorResult.withNextValue(result, IntersectionContinuation.from(this));
            }
            return nextResult;
        });
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            mayGetContinuation = false;
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    /**
     * Given a list of cursor states, return a value based on the states of
     * an appropriate type. This will only be called when all of the cursor states
     * already all match, so this does not need to check for an intersection. The result
     * of this method then determines the value that the cursor actually omits having
     * found a match.
     *
     * @param cursorStates a list of cursor states with all cursors already having found a match
     * @return the result to return given an intersection
     */
    abstract U getNextResult(@Nonnull List<CursorState<T>> cursorStates);

    @Nullable
    @Override
    public U next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        hasNextFuture = null;
        return nextResult.get();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);

        return nextResult.getContinuation().toBytes();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        cursorStates.forEach(cursorState -> cursorState.cursor.close());
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return cursorStates.get(0).cursor.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            for (CursorState<T> cursorState : cursorStates) {
                if (!cursorState.cursor.accept(visitor)) {
                    break;
                }
            }
        }
        return visitor.visitLeave(this);
    }

    /**
     * Merges all of the cursors and whether they have stopped and returns the "weakest" reason for the result to stop.
     * It will return {@link com.apple.foundationdb.record.RecordCursor.NoNextReason#SOURCE_EXHAUSTED} if any
     * of the cursors are exhausted. If any of the cursors have stopped due to an in-band limit, it will return
     * an in-band limit as well. Finally, if all of the stopped cursors have done so due to hitting an out-of-band
     * limit, it will return an out-of-band limit as well. Note that, in practice, because an intersection cursor
     * will return <code>false</code> from <code>onHasNext</code> if any of its child cursors have stopped, it is
     * likely that there are only a small number (maybe one or two) cursors that have actually stopped when this
     * method is called (e.g., the first cursor to exhaust its source or the first cursor to hit a limit imposed
     * by the element scan limiter).
     * @return the weakest reason for stopping
     */
    private NoNextReason mergeNoNextReasons() {
        if (cursorStates.stream().allMatch(cursorState -> cursorState.result.hasNext())) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all sides have next");
        }

        NoNextReason reason = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (!cursorState.result.hasNext()) {
                NoNextReason cursorReason = cursorState.cursor.getNoNextReason();
                if (cursorReason.isSourceExhausted()) {
                    // one of the cursors is exhausted, so regardless of the other cursors'
                    // reasons, the intersection cursor is also definitely exhausted
                    return cursorReason;
                } else if (reason == null || (reason.isOutOfBand() && !cursorReason.isOutOfBand())) {
                    // if either we haven't chosen a reason yet (which is the case if reason is null)
                    // or if we have found an in-band reason to stop that supersedes a previous out-of-band
                    // reason, then update the current reason to that one
                    reason = cursorReason;
                }
            }
        }

        return reason;
    }

    private static class IntersectionContinuation implements RecordCursorContinuation {
        private static final RecordCursorProto.IntersectionContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                .setStarted(true)
                .build();
        private static final RecordCursorProto.IntersectionContinuation.CursorState START_PROTO = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                .setStarted(false)
                .build();

        @Nonnull
        private final List<RecordCursorContinuation> continuations; // all continuations must themselves be immutable
        @Nullable
        private RecordCursorProto.IntersectionContinuation cachedProto;

        private IntersectionContinuation(@Nonnull List<RecordCursorContinuation> continuations) {
            this(continuations, null);
        }

        private IntersectionContinuation(@Nonnull List<RecordCursorContinuation> continuations, @Nullable RecordCursorProto.IntersectionContinuation proto) {
            this.continuations = continuations;
            this.cachedProto = proto;
        }

        public static IntersectionContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
            if (bytes == null) {
                return new IntersectionContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START));
            }
            try {
                return IntersectionContinuation.from(RecordCursorProto.IntersectionContinuation.parseFrom(bytes), numberOfChildren);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
            }
        }

        public static IntersectionContinuation from(@Nonnull RecordCursorProto.IntersectionContinuation parsed, int numberOfChildren) {
            ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
            if (!parsed.getFirstStarted()) {
                builder.add(RecordCursorStartContinuation.START);
            } else if (parsed.hasFirstContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(parsed.getFirstContinuation().toByteArray()));
            } else {
                builder.add(RecordCursorEndContinuation.END);
            }
            if (!parsed.getSecondStarted()) {
                builder.add(RecordCursorStartContinuation.START);
            } else if (parsed.hasSecondContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(parsed.getSecondContinuation().toByteArray()));
            } else {
                builder.add(RecordCursorEndContinuation.END);
            }
            for (RecordCursorProto.IntersectionContinuation.CursorState state : parsed.getOtherChildStateList()) {
                if (!state.getStarted()) {
                    builder.add(RecordCursorStartContinuation.START);
                } else if (state.hasContinuation()) {
                    builder.add(ByteArrayContinuation.fromNullable(state.getContinuation().toByteArray()));
                } else {
                    builder.add(RecordCursorEndContinuation.END);
                }
            }
            ImmutableList<RecordCursorContinuation> children = builder.build();
            if (children.size() != numberOfChildren) {
                throw new RecordCoreArgumentException("invalid continuation (extraneous child state information present)")
                        .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, numberOfChildren - 2)
                        .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getOtherChildStateCount());
            }
            return new IntersectionContinuation(children, parsed);
        }

        public static <U, T> IntersectionContinuation from(@Nonnull IntersectionCursorBase<U, T> cursor) {
            return new IntersectionContinuation(cursor.cursorStates.stream().map(cursorState -> cursorState.continuation).collect(Collectors.toList()));
        }

        public RecordCursorProto.IntersectionContinuation toProto() {
            if (cachedProto == null) {
                final RecordCursorProto.IntersectionContinuation.Builder builder = RecordCursorProto.IntersectionContinuation.newBuilder();
                final Iterator<RecordCursorContinuation> continuationIterator = continuations.iterator();

                // A CursorState can have a null continuation for one of two reasons:
                //
                // 1. Its cursor has been exhausted.
                // 2. The intersection cursor has not "consumed" that cursor yet.
                //
                // If any child has been exhausted, then the intersection cursor should return a null (i.e., exhausted)
                // continuation as it will never return any more results. If the cursor state hasn't been consumed yet
                // (which can happen if the first element of that cursor compares greater than all elements seen from
                // other cursors), then the correct behavior is to restart that child from the beginning on subsequent
                // runs of this intersection cursor, so the corresponding entry in the continuation proto is marked as
                // not "started" for that child.

                // First two cursors are handled differently (essentially for compatibility reasons)
                final RecordCursorContinuation firstContinuation = continuationIterator.next();
                byte[] asBytes = firstContinuation.toBytes();
                if (asBytes == null && !firstContinuation.isEnd()) { // first cursor has not started
                    builder.setFirstStarted(false);
                } else {
                    builder.setFirstStarted(true);
                    if (asBytes != null) {
                        builder.setFirstContinuation(ByteString.copyFrom(asBytes));
                    }
                }
                final RecordCursorContinuation secondContinuation = continuationIterator.next();
                asBytes = secondContinuation.toBytes();
                if (asBytes == null && !secondContinuation.isEnd()) { // second cursor not started
                    builder.setSecondStarted(false);
                } else {
                    builder.setSecondStarted(true);
                    if (asBytes != null) {
                        builder.setSecondContinuation(ByteString.copyFrom(asBytes));
                    }
                }

                // The rest of the cursor states get written as elements in a repeated message field
                while (continuationIterator.hasNext()) {
                    final RecordCursorProto.IntersectionContinuation.CursorState cursorState;
                    final RecordCursorContinuation continuation = continuationIterator.next();
                    if (continuation.isEnd()) {
                        cursorState = EXHAUSTED_PROTO;
                    } else {
                        asBytes = continuation.toBytes();
                        if (asBytes == null && !continuation.isEnd()) {
                            cursorState = START_PROTO;
                        } else {
                            cursorState = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                                    .setStarted(true)
                                    .setContinuation(ByteString.copyFrom(asBytes))
                                    .build();
                        }
                    }
                    builder.addOtherChildState(cursorState);
                }
                cachedProto = builder.build();
            }
            return cachedProto;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            return toProto().toByteArray();
        }

        @Override
        public boolean isEnd() {
            // one of the children have actually stopped, so the intersection will have no more records
            return continuations.stream().anyMatch(RecordCursorContinuation::isEnd);
        }
    }

    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left, @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                 @Nullable byte[] byteContinuation) {
        final IntersectionContinuation continuation = IntersectionContinuation.from(byteContinuation, 2);
        return ImmutableList.of(
                CursorState.from(left, continuation.continuations.get(0)),
                CursorState.from(right, continuation.continuations.get(1)));
    }

    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions, @Nullable byte[] byteContinuation) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to IntersectionCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
        final List<CursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final IntersectionContinuation continuation = IntersectionContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(CursorState.from(cursorFunction, continuation.continuations.get(i)));
            i++;
        }
        return cursorStates;
    }
}
