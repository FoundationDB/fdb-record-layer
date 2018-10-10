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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.cursors.IllegalContinuationAccessChecker;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
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
    private CompletableFuture<Boolean> nextFuture;

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
        private static final RecordCursorProto.IntersectionContinuation.CursorState NOT_STARTED_PROTO = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                .setStarted(false)
                .build();

        @Nonnull
        private final RecordCursor<T> cursor;
        @Nullable
        private CompletableFuture<Void> onHasNextFuture;
        private boolean hasNext;
        @Nullable
        private T element;
        private List<Object> key;
        @Nullable
        private byte[] continuation;

        CursorState(@Nonnull RecordCursor<T> cursor, @Nullable byte[] continuation) {
            this.cursor = cursor;
            this.continuation = continuation;
        }

        @Nonnull
        public CompletableFuture<Void> getOnHasNextFuture() {
            if (onHasNextFuture == null) {
                onHasNextFuture = cursor.onHasNext().thenAccept(cursorHasNext -> {
                    this.hasNext = cursorHasNext;
                    if (!cursorHasNext) {
                        this.continuation = cursor.getContinuation();
                    }
                });
            }
            return onHasNextFuture;
        }

        public void ready(@Nonnull Function<? super T, ? extends List<Object>> keyFunction) {
            if (element == null) {
                element = cursor.next();
                key = keyFunction.apply(element);
            }
        }

        public void consume() {
            // after consuming a element from a cursor, we should never need to query it again,
            // so we update its continuation information now
            onHasNextFuture = null;
            element = null;
            continuation = cursor.getContinuation();
        }

        @Nonnull
        public RecordCursorProto.IntersectionContinuation.CursorState getContinuationProto() {
            if (continuation == null) {
                return NOT_STARTED_PROTO;
            } else {
                return RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                        .setStarted(true)
                        .setContinuation(ByteString.copyFrom(continuation))
                        .build();
            }
        }

        @Nonnull
        public static <T> CursorState<T> exhausted() {
            return new CursorState<>(RecordCursor.empty(), null);
        }

        @Nonnull
        public static <T> CursorState<T> fromProto(
                @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
                @Nonnull RecordCursorProto.IntersectionContinuation.CursorStateOrBuilder proto) {
            if (proto.getStarted()) {
                byte[] continuation = proto.hasContinuation() ? proto.getContinuation().toByteArray() : null;
                return from(cursorFunction, continuation);
            } else {
                return from(cursorFunction, null);
            }
        }

        @Nonnull
        public static <T> CursorState<T> from(
                @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
                @Nullable byte[] continuation) {
            return new CursorState<>(cursorFunction.apply(continuation), continuation);
        }

        @Nullable
        public T getElement() {
            return element;
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
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = AsyncUtil.whileTrue(() -> {
                // The continuation handling here is tricky. We need to save "the previous continuation" (i.e. the
                // continuation from before we called onHasNext()) for each child, in case one of them (but not both!)
                // ends for an out-of-band reason.
                // The idea is that we update the continuation and advance it only when we are certain that we
                // won't need the previous value ever again.
                // This _almost_ works, but it fails if we resume a cursor which immediately runs out of records for an
                // "in band" reason (because the underlying source is exhausted, or because it hit a limit on the
                // number of returned records). In this case, we need to update the cached cursor or we'll loop forever!
                // In all cases except this one, this update is redundant but harmless.
                CompletableFuture<?>[] onHasNextFutures = new CompletableFuture<?>[cursorStates.size()];
                int i = 0;
                for (CursorState<T> cursorState : cursorStates) {
                    onHasNextFutures[i] = cursorState.getOnHasNextFuture();
                    i++;
                }
                return CompletableFuture.allOf(onHasNextFutures).thenApply(vignore -> {
                    // If any of the cursors do not have a next element, then we are done.
                    if (cursorStates.stream().anyMatch(cursorState -> !cursorState.hasNext)) {
                        return false;
                    }

                    long startTime = System.nanoTime();

                    cursorStates.forEach(cursorState -> cursorState.ready(comparisonKeyFunction));

                    // If everything compares equally, then we have a match and should return it.
                    // Otherwise, everything except for the maximum iterator values is guaranteed
                    // not to match, so null those values out and then move on.
                    List<CursorState<T>> maxCursors = new ArrayList<>(cursorStates.size());
                    List<CursorState<T>> nonMaxCursors = new ArrayList<>(cursorStates.size());
                    mergeStates(maxCursors, nonMaxCursors, startTime);
                    return !nonMaxCursors.isEmpty();
                });
            }, getExecutor()).thenApply(vignore -> {
                boolean result = cursorStates.stream().allMatch(cursorState -> cursorState.hasNext);
                mayGetContinuation = !result;
                return result;
            });
        }
        return nextFuture;
    }

    abstract U getNextResult(@Nonnull List<CursorState<T>> cursorStates);

    @Nullable
    @Override
    public U next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final U result = getNextResult(cursorStates);
        // never need to query these records again, so consume all cursors
        cursorStates.forEach(CursorState::consume);

        nextFuture = null;
        mayGetContinuation = true;
        return result;
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        if (cursorStates.stream().anyMatch(cursorState -> !cursorState.hasNext) && !getNoNextReason().isOutOfBand()
                && cursorStates.stream().anyMatch(cursorState -> cursorState.continuation == null)) {
            // one of the children have actually stopped, so the intersection will have no more records
            return null;
        }

        final RecordCursorProto.IntersectionContinuation.Builder builder = RecordCursorProto.IntersectionContinuation.newBuilder();
        final Iterator<CursorState<T>> cursorStateIterator = cursorStates.iterator();
        // First two cursors are handled differently (essentially for compatibility reasons)
        final CursorState<T> firstCursorState = cursorStateIterator.next();
        if (firstCursorState.continuation == null) { // first cursor has not started
            builder.setFirstStarted(false);
        } else {
            builder.setFirstStarted(true);
            builder.setFirstContinuation(ByteString.copyFrom(firstCursorState.continuation));
        }
        final CursorState<T> secondCursorState = cursorStateIterator.next();
        if (secondCursorState.continuation == null) { // second cursor has not started
            builder.setSecondStarted(false);
        } else {
            builder.setSecondStarted(true);
            builder.setSecondContinuation(ByteString.copyFrom(secondCursorState.continuation));
        }
        // The rest of the cursor states get written as elements in a repeated message field
        while (cursorStateIterator.hasNext()) {
            builder.addOtherChildState(cursorStateIterator.next().getContinuationProto());
        }
        return builder.build().toByteArray();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return mergeNoNextReasons();
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
        if (cursorStates.stream().allMatch(cursorState -> cursorState.hasNext)) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all sides have next");
        }

        NoNextReason reason = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (!cursorState.hasNext) {
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

    @Nonnull
    protected static RecordCursorProto.IntersectionContinuation parseContinuation(@Nonnull byte[] continuation) {
        try {
            return RecordCursorProto.IntersectionContinuation.parseFrom(continuation);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation));
        }
    }

    protected static <T> void addFirstTwoCursorStates(
            @Nullable RecordCursorProto.IntersectionContinuation parsed,
            @Nonnull Function<byte[], RecordCursor<T>> first,
            @Nonnull Function<byte[], RecordCursor<T>> second,
            @Nonnull List<CursorState<T>> destination) {
        if (parsed != null) {
            byte[] firstContinuation = null;
            if (parsed.getFirstStarted()) {
                firstContinuation = parsed.getFirstContinuation().toByteArray();
            }
            destination.add(CursorState.from(first, firstContinuation));

            byte[] secondContinuation = null;
            if (parsed.getSecondStarted()) {
                secondContinuation = parsed.getSecondContinuation().toByteArray();
            }
            destination.add(CursorState.from(second, secondContinuation));
        } else {
            destination.add(CursorState.from(first, null));
            destination.add(CursorState.from(second, null));
        }
    }

    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left, @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                 @Nullable byte[] continuation) {
        final RecordCursorProto.IntersectionContinuation parsed;
        if (continuation != null) {
            parsed = parseContinuation(continuation);
            if (parsed.getOtherChildStateCount() != 0) {
                throw new RecordCoreArgumentException("invalid continuation (extraneous child state information present)")
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation))
                        .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, 0)
                        .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getOtherChildStateCount());
            }
        } else {
            parsed = null;
        }
        final List<CursorState<T>> cursorStates = new ArrayList<>(2);
        addFirstTwoCursorStates(parsed, left, right, cursorStates);
        return cursorStates;
    }

    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions, @Nullable byte[] continuation) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to IntersectionCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
        final List<CursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        if (continuation != null) {
            final RecordCursorProto.IntersectionContinuation parsed = parseContinuation(continuation);
            if (cursorFunctions.size() != parsed.getOtherChildStateCount() + 2) {
                throw new RecordCoreArgumentException("invalid continuation (expected continuation count does not match read)")
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation))
                        .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, cursorFunctions.size() - 2)
                        .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getOtherChildStateCount());
            }
            addFirstTwoCursorStates(parsed, cursorFunctions.get(0), cursorFunctions.get(1), cursorStates);
            Iterator<RecordCursorProto.IntersectionContinuation.CursorState> protoStateIterator = parsed.getOtherChildStateList().iterator();
            for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions.subList(2, cursorFunctions.size())) {
                cursorStates.add(CursorState.fromProto(cursorFunction, protoStateIterator.next()));
            }
        } else {
            for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
                cursorStates.add(CursorState.from(cursorFunction, null));
            }
        }
        return cursorStates;
    }
}
