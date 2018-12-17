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

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.cursors.EmptyCursor;
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
 * Common implementation code for performing a union shared between
 * the different intersection cursors. In particular, this base cursor
 * is agnostic to the type of result it returns to the users
 *
 * @param <T> the type of elements returned by each child cursor
 */
@API(API.Status.INTERNAL)
abstract class UnionCursorBase<T> implements RecordCursor<T> {
    @Nonnull
    private final Function<? super T, ? extends List<Object>> comparisonKeyFunction;
    @Nonnull
    private final List<CursorState<T>> cursorStates;
    @Nonnull
    private final Executor executor;
    @Nullable
    private final FDBStoreTimer timer;

    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_UNION);
    @Nonnull
    private static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_UNION_PLAN_UNIQUES);
    @Nonnull
    private static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_UNION_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    // for detecting incorrect cursor usage
    protected boolean mayGetContinuation = false;

    protected static class CursorState<T> {
        private static final RecordCursorProto.UnionContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.UnionContinuation.CursorState.newBuilder()
                .setExhausted(true)
                .build();

        @Nonnull
        protected final RecordCursor<T> cursor;
        @Nullable
        private CompletableFuture<Void> onHasNextFuture;
        protected boolean hasNext;
        @Nullable
        protected T element;
        protected List<Object> key;
        @Nullable
        private byte[] continuationBefore;
        @Nullable
        private byte[] continuationAfter;
        @Nullable
        protected byte[] continuation;
        private boolean exhausted;

        @SpotBugsSuppressWarnings(value = {"EI_EXPOSE_REP2"}, justification = "copying byte arrays is expensive")
        public CursorState(@Nonnull RecordCursor<T> cursor, @Nullable byte[] continuationStart) {
            this.cursor = cursor;
            this.continuation = continuationStart;
            this.continuationAfter = continuationStart;
        }

        @Nonnull
        public CompletableFuture<Void> getOnHasNextFuture() {
            if (exhausted) {
                return AsyncUtil.DONE;
            } else if (onHasNextFuture == null) {
                onHasNextFuture = cursor.onHasNext().thenAccept(cursorHasNext -> {
                    this.hasNext = cursorHasNext;
                    if (!cursorHasNext && cursor.getNoNextReason().isSourceExhausted()) {
                        continuationBefore = continuationAfter;
                        continuation = cursor.getContinuation();
                        continuationAfter = continuation;
                        exhausted = true;
                    }
                });
            }
            return onHasNextFuture;
        }

        public void ready(@Nonnull Function<? super T, ? extends List<Object>> keyFunction) {
            if (element == null && hasNext) {
                continuationBefore = continuationAfter;
                element = cursor.next();
                continuationAfter = cursor.getContinuation();
                key = keyFunction.apply(element);
            }
        }

        public void consume() {
            onHasNextFuture = null;
            element = null;
            hasNext = false;
            continuation = continuationAfter;
            if (continuation == null) {
                // Some cursors, if they know they are at the end, wil return null for
                // their continuation *before* returning "false" from "hasNext" to signal
                // that they are done. Take that into account now if possible.
                exhausted = true;
            }
        }

        public boolean isExhausted() {
            return exhausted;
        }

        @Nonnull
        public RecordCursorProto.UnionContinuation.CursorState getContinuationProto() {
            if (continuation != null) {
                return RecordCursorProto.UnionContinuation.CursorState.newBuilder()
                        .setContinuation(ByteString.copyFrom(continuation))
                        .build();
            } else if (isExhausted()) {
                return EXHAUSTED_PROTO;
            } else {
                return RecordCursorProto.UnionContinuation.CursorState.getDefaultInstance();
            }
        }

        @Nonnull
        public static <T> CursorState<T> exhausted() {
            return new CursorState<>(RecordCursor.empty(), null);
        }

        @Nonnull
        public static <T> CursorState<T> fromProto(
                @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
                @Nonnull RecordCursorProto.UnionContinuation.CursorStateOrBuilder proto) {
            if (proto.getExhausted()) {
                return exhausted();
            } else {
                byte[] continuation = proto.hasContinuation() ? proto.getContinuation().toByteArray() : null;
                RecordCursor<T> cursor = cursorFunction.apply(continuation);
                return new CursorState<>(cursor, continuation);
            }
        }

        @Nonnull
        public static <T> CursorState<T> from(
                @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
                boolean exhausted, @Nullable byte[] continuation) {
            if (exhausted) {
                return exhausted();
            } else {
                return new CursorState<>(cursorFunction.apply(continuation), continuation);
            }
        }
    }

    private static <T> CompletableFuture<?>[] getOnHasNextFutures(@Nonnull List<CursorState<T>> cursorStates) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[cursorStates.size()];
        int i = 0;
        for (CursorState<T> cursorState : cursorStates) {
            futures[i] = cursorState.getOnHasNextFuture();
            i++;
        }
        return futures;
    }

    // This returns a "wildcard" to handle the fact that the signature of AsyncUtil.DONE is
    // CompletableFuture<Void> whereas CompletableFuture.anyOf returns a CompletableFuture<Object>.
    // The caller always ignores the result anyway and just uses this as a signal, so it's not
    // a big loss.
    @SuppressWarnings("squid:S1452")
    @Nonnull
    protected static <T> CompletableFuture<?> whenAny(@Nonnull List<CursorState<T>> cursorStates) {
        List<CursorState<T>> nonExhausted = new ArrayList<>(cursorStates.size());
        for (CursorState<T> cursorState : cursorStates) {
            if (!cursorState.isExhausted()) {
                if (cursorState.getOnHasNextFuture().isDone()) {
                    // Short-circuit and return immediately if we find a state that is already done.
                    return AsyncUtil.DONE;
                } else {
                    nonExhausted.add(cursorState);
                }
            }
        }
        if (nonExhausted.isEmpty()) {
            // Everything is exhausted. Can return immediately.
            return AsyncUtil.DONE;
        } else {
            return CompletableFuture.anyOf(getOnHasNextFutures(nonExhausted));
        }
    }

    @Nonnull
    protected static <T> CompletableFuture<Void> whenAll(@Nonnull List<CursorState<T>> cursorStates) {
        return CompletableFuture.allOf(getOnHasNextFutures(cursorStates));
    }

    protected UnionCursorBase(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                              @Nonnull List<CursorState<T>> cursorStates,
                              @Nullable FDBStoreTimer timer) {
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.cursorStates = cursorStates;
        this.timer = timer;

        // Choose the executor from the first non-empty cursor. The executors for empty cursors are just
        // the default one, whereas non-empty cursors may have an executor set by the user.
        Executor cursorExecutor = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (!(cursorState.cursor instanceof EmptyCursor)) {
                cursorExecutor = cursorState.cursor.getExecutor();
                break;
            }
        }
        this.executor = cursorExecutor != null ? cursorExecutor : cursorStates.get(0).cursor.getExecutor();
    }

    /**
     * Determine if there is a next element to be returned by looking at the
     * current cursor states.
     *
     * @param cursorStates the list of states of all child cursors
     */
    @Nonnull
    abstract CompletableFuture<Boolean> getIfAnyHaveNext(@Nonnull List<CursorState<T>> cursorStates);

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        mayGetContinuation = false;
        return getIfAnyHaveNext(cursorStates).thenApply(doesHaveNext -> {
            mayGetContinuation = !doesHaveNext;
            return doesHaveNext;
        });
    }

    /**
     * Choose which states should be returned by the union cursor as the next element. If this
     * method adds more than one state to the <code>chosenStates</code> list, then all results
     * from those cursors should be considered equivalent, and every cursor in that list
     * of states will be consumed when the {@link #next()} method of this cursor is called. If the
     * extender wishes to combine all duplicates for a given step in some way, they can control how the elements
     * are combined by overriding the {@link #getNextResult(List)} method.
     *
     * @param allStates all states of all children of this cursor
     * @param chosenStates the list to populate with states to choose as part of the union
     * @param otherStates the list to populate with all other states
     */
    abstract void chooseStates(@Nonnull List<CursorState<T>> allStates, @Nonnull List<CursorState<T>> chosenStates,
                               @Nonnull List<CursorState<T>> otherStates);

    /**
     * Get the result from the list of chosen states. These states have all been identified
     * as equivalent from the point of view of the union, so only one element should be
     * returned from all of them. Implementations may choose to combine the results
     * from the various child results if that is useful.
     *
     * @param chosenStates the states that contain the next value to return
     * @return the result to return from this cursor given these elements appear in the union
     */
    T getNextResult(@Nonnull List<CursorState<T>> chosenStates) {
        return chosenStates.get(0).element;
    }

    @Nonnull
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        mayGetContinuation = true;
        for (CursorState<T> cursorState : cursorStates) {
            cursorState.ready(comparisonKeyFunction);
        }
        final long startTime = System.nanoTime();
        List<CursorState<T>> chosenStates = new ArrayList<>(cursorStates.size());
        List<CursorState<T>> otherStates = new ArrayList<>(cursorStates.size());
        chooseStates(cursorStates, chosenStates, otherStates);
        if (chosenStates.isEmpty()) {
            throw new RecordCoreException("union with additional items had no next states");
        }
        if (timer != null) {
            if (chosenStates.size() == 1) {
                timer.increment(uniqueCounts);
            } else {
                // The number of duplicates is the number of minimum states
                // for this value except for the first one (hence the "- 1").
                timer.increment(duplicateCounts, chosenStates.size() - 1);
            }
        }
        final T result = getNextResult(chosenStates);
        if (result == null) {
            throw new RecordCoreException("minimum element had null result");
        }
        // Advance each returned state.
        chosenStates.forEach(CursorState::consume);
        if (timer != null) {
            timer.record(duringEvents, System.nanoTime() - startTime);
        }
        return result;
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        boolean allExhausted = cursorStates.stream().allMatch(CursorState::isExhausted);
        if (allExhausted) {
            return null;
        }
        final RecordCursorProto.UnionContinuation.Builder builder = RecordCursorProto.UnionContinuation.newBuilder();
        final Iterator<CursorState<T>> cursorStateIterator = cursorStates.iterator();
        // The first two continuations are special (essentially for compatibility reasons)
        final CursorState<T> firstCursorState = cursorStateIterator.next();
        if (firstCursorState.isExhausted()) {
            builder.setFirstExhausted(true);
        } else if (firstCursorState.continuation != null) {
            builder.setFirstContinuation(ByteString.copyFrom(firstCursorState.continuation));
        }
        final CursorState<T> secondCursorState = cursorStateIterator.next();
        if (secondCursorState.isExhausted()) {
            builder.setSecondExhausted(true);
        } else if (secondCursorState.continuation != null) {
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

    /**
     * Merges all of the cursors and whether they have stopped and returns the "strongest" reason for the result to stop.
     * It will return an out-of-band reason if any of the children stopped for an out-of-band reason, and it will
     * only return {@link NoNextReason#SOURCE_EXHAUSTED SOURCE_EXHAUSTED} if all of the cursors are exhausted.
     * @return the strongest reason for stopping
     */
    private NoNextReason mergeNoNextReasons() {
        if (cursorStates.stream().allMatch(cursorState -> cursorState.hasNext)) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all children have next");
        }
        NoNextReason reason = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (!cursorState.getOnHasNextFuture().isDone()) {
                continue;
            }
            if (!cursorState.hasNext) {
                // Combine the current reason so far with this child's reason.
                // In particular, choose the child reason if it is at least as strong
                // as the current one. This guarantees that it will choose an
                // out of band reason if there is one and will only return
                // SOURCE_EXHAUSTED if every child ended with SOURCE_EXHAUSTED.
                NoNextReason childReason = cursorState.cursor.getNoNextReason();
                if (reason == null || childReason.isOutOfBand() || reason.isSourceExhausted()) {
                    reason = childReason;
                }
            }
        }
        return reason;
    }

    @Override
    public void close() {
        cursorStates.forEach(cursorState -> cursorState.cursor.close());
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
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

    @Nonnull
    private static RecordCursorProto.UnionContinuation parseContinuation(@Nonnull byte[] continuation) {
        try {
            return RecordCursorProto.UnionContinuation.parseFrom(continuation);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation));
        }
    }

    private static <T> void addFirstTwoCursorStates(
            @Nullable RecordCursorProto.UnionContinuation parsed,
            @Nonnull Function<byte[], RecordCursor<T>> first,
            @Nonnull Function<byte[], RecordCursor<T>> second,
            @Nonnull List<CursorState<T>> destination) {
        if (parsed != null) {
            byte[] firstContinuation = null;
            boolean firstExhausted = false;
            if (parsed.hasFirstContinuation()) {
                firstContinuation = parsed.getFirstContinuation().toByteArray();
            } else {
                firstExhausted = parsed.getFirstExhausted();
            }
            destination.add(CursorState.from(first, firstExhausted, firstContinuation));

            byte[] secondContinuation = null;
            boolean secondExhausted = false;
            if (parsed.hasSecondContinuation()) {
                secondContinuation = parsed.getSecondContinuation().toByteArray();
            } else {
                secondExhausted = parsed.getSecondExhausted();
            }
            destination.add(CursorState.from(second, secondExhausted, secondContinuation));
        } else {
            destination.add(CursorState.from(first, false, null));
            destination.add(CursorState.from(second, false, null));
        }
    }

    @Nonnull
    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left, @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                 @Nullable byte[] continuation) {
        final List<CursorState<T>> cursorStates = new ArrayList<>(2);
        final RecordCursorProto.UnionContinuation parsed;
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
        addFirstTwoCursorStates(parsed, left, right, cursorStates);
        return cursorStates;
    }

    @Nonnull
    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions, @Nullable byte[] continuation) {
        final List<CursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        if (continuation != null) {
            final RecordCursorProto.UnionContinuation parsed = parseContinuation(continuation);
            if (cursorFunctions.size() != parsed.getOtherChildStateCount() + 2) {
                throw new RecordCoreArgumentException("invalid continuation (expected continuation count does not match read)")
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(continuation))
                        .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, cursorFunctions.size() - 2)
                        .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getOtherChildStateCount());
            }
            addFirstTwoCursorStates(parsed, cursorFunctions.get(0), cursorFunctions.get(1), cursorStates);
            Iterator<RecordCursorProto.UnionContinuation.CursorState> protoStateIterator = parsed.getOtherChildStateList().iterator();
            for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions.subList(2, cursorFunctions.size())) {
                cursorStates.add(CursorState.fromProto(cursorFunction, protoStateIterator.next()));
            }
        } else {
            for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
                cursorStates.add(CursorState.from(cursorFunction, false, null));
            }
        }
        return cursorStates;
    }
}
