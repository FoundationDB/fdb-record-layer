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
import com.apple.foundationdb.record.cursors.EmptyCursor;
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
 * Common implementation code for performing a union shared between
 * the different intersection cursors. In particular, this base cursor
 * is agnostic to the type of result it returns to the users
 *
 * @param <T> the type of elements returned by each child cursor
 */
abstract class UnionCursorBase<T> implements RecordCursor<T> {
    @Nonnull
    private final List<CursorState<T>> cursorStates;
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture;
    @Nullable
    private RecordCursorResult<T> nextResult;

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
        @Nonnull
        private final RecordCursor<T> cursor;
        @Nullable
        private CompletableFuture<RecordCursorResult<T>> onNextFuture;
        @Nonnull
        private RecordCursorContinuation continuation;
        @Nullable
        private RecordCursorResult<T> result;

        public CursorState(@Nonnull RecordCursor<T> cursor, @Nonnull RecordCursorContinuation continuationStart) {
            this.cursor = cursor;
            this.continuation = continuationStart;
        }

        @Nonnull
        public CompletableFuture<RecordCursorResult<T>> getOnNextFuture() {
            if (onNextFuture == null) {
                onNextFuture = cursor.onNext().thenApply(cursorResult -> {
                    result = cursorResult;
                    if (!result.hasNext()) {
                        continuation = result.getContinuation(); // no result, so we can advance the cached continuation
                    }
                    return cursorResult;
                });
            }
            return onNextFuture;
        }

        public void consume() {
            onNextFuture = null;
            continuation = result.getContinuation();
        }

        public boolean isExhausted() {
            return result != null && !result.hasNext() && result.getNoNextReason().isSourceExhausted();
        }

        @Nonnull
        public RecordCursorResult<T> getResult() {
            if (result == null) {
                throw new RecordCoreException("tried to get result before any result was ready");
            }
            return result;
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
    }

    private static <T> CompletableFuture<?>[] getOnNextFutures(@Nonnull List<CursorState<T>> cursorStates) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[cursorStates.size()];
        int i = 0;
        for (CursorState<T> cursorState : cursorStates) {
            futures[i] = cursorState.getOnNextFuture();
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
                if (cursorState.getOnNextFuture().isDone()) {
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
            return CompletableFuture.anyOf(getOnNextFutures(nonExhausted));
        }
    }

    @Nonnull
    protected static <T> CompletableFuture<Void> whenAll(@Nonnull List<CursorState<T>> cursorStates) {
        return CompletableFuture.allOf(getOnNextFutures(cursorStates));
    }

    protected UnionCursorBase(@Nonnull List<CursorState<T>> cursorStates, @Nullable FDBStoreTimer timer) {
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
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return getIfAnyHaveNext(cursorStates).thenApply(hasNext -> {
            if (hasNext) {
                final long startTime = System.nanoTime();
                List<CursorState<T>> chosenStates = new ArrayList<>(cursorStates.size());
                List<CursorState<T>> otherStates = new ArrayList<>(cursorStates.size());
                chooseStates(cursorStates, chosenStates, otherStates);
                logDuplicates(chosenStates);
                // Advance each chosen state
                chosenStates.forEach(CursorState::consume);
                final T result = getNextResult(chosenStates);
                if (result == null) {
                    throw new RecordCoreException("minimum element had null result");
                }
                nextResult = RecordCursorResult.withNextValue(result, UnionContinuation.from(this));
                if (timer != null) {
                    timer.record(duringEvents, System.nanoTime() - startTime);
                }
            } else {
                mayGetContinuation = true;
                nextResult = RecordCursorResult.withoutNextValue(UnionContinuation.from(this), mergeNoNextReasons());
            }
            return nextResult;
        });
    }

    private void logDuplicates(List<?> chosenStates) {
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
        return chosenStates.get(0).result.get();
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

    @Nonnull
    @Override
    public T next() {
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

    /**
     * Merges all of the cursors and whether they have stopped and returns the "strongest" reason for the result to stop.
     * It will return an out-of-band reason if any of the children stopped for an out-of-band reason, and it will
     * only return {@link NoNextReason#SOURCE_EXHAUSTED SOURCE_EXHAUSTED} if all of the cursors are exhausted.
     * @return the strongest reason for stopping
     */
    private NoNextReason mergeNoNextReasons() {
        if (cursorStates.stream().allMatch(cursorState -> cursorState.result.hasNext())) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all children have next");
        }
        NoNextReason reason = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (!cursorState.getOnNextFuture().isDone()) {
                continue;
            }
            if (!cursorState.result.hasNext()) {
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
        if (hasNextFuture != null) {
            hasNextFuture.cancel(false);
        }
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


    private static class UnionContinuation implements RecordCursorContinuation {
        private static final RecordCursorProto.UnionContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.UnionContinuation.CursorState.newBuilder()
                .setExhausted(true)
                .build();
        private static final RecordCursorProto.UnionContinuation.CursorState START_PROTO = RecordCursorProto.UnionContinuation.CursorState.newBuilder()
                .setExhausted(false)
                .build();

        @Nonnull
        private final List<RecordCursorContinuation> continuations; // all continuations must themselves be immutable
        @Nullable
        private RecordCursorProto.UnionContinuation cachedProto;

        private UnionContinuation(@Nonnull List<RecordCursorContinuation> continuations) {
            this(continuations, null);
        }

        private UnionContinuation(@Nonnull List<RecordCursorContinuation> continuations, @Nullable RecordCursorProto.UnionContinuation proto) {
            this.continuations = continuations;
            this.cachedProto = proto;
        }

        @SuppressWarnings("PMD.PreserveStackTrace")
        public static UnionContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
            if (bytes == null) {
                return new UnionContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START));
            }
            try {
                return UnionContinuation.from(RecordCursorProto.UnionContinuation.parseFrom(bytes), numberOfChildren);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
            } catch (RecordCoreArgumentException ex) {
                throw ex.addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
            }
        }

        public static UnionContinuation from(@Nonnull RecordCursorProto.UnionContinuation parsed, int numberOfChildren) {
            ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
            if (parsed.hasFirstContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(parsed.getFirstContinuation().toByteArray()));
            } else if (parsed.getFirstExhausted()) {
                builder.add(RecordCursorEndContinuation.END);
            } else {
                builder.add(RecordCursorStartContinuation.START);
            }
            if (parsed.hasSecondContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(parsed.getSecondContinuation().toByteArray()));
            } else if (parsed.getSecondExhausted()) {
                builder.add(RecordCursorEndContinuation.END);
            } else {
                builder.add(RecordCursorStartContinuation.START);
            }
            for (RecordCursorProto.UnionContinuation.CursorState state : parsed.getOtherChildStateList()) {
                if (state.hasContinuation()) {
                    builder.add(ByteArrayContinuation.fromNullable(state.getContinuation().toByteArray()));
                } else if (state.getExhausted()) {
                    builder.add(RecordCursorEndContinuation.END);
                } else {
                    builder.add(RecordCursorStartContinuation.START);
                }
            }
            ImmutableList<RecordCursorContinuation> children = builder.build();
            if (children.size() != numberOfChildren) {
                throw new RecordCoreArgumentException("invalid continuation (expected continuation count does not match read)")
                        .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, numberOfChildren)
                        .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, children.size());
            }
            return new UnionContinuation(children, parsed);
        }

        public static <T> UnionContinuation from(@Nonnull UnionCursorBase<T> cursor) {
            return new UnionContinuation(cursor.cursorStates.stream().map(cursorState -> cursorState.continuation).collect(Collectors.toList()));
        }

        public RecordCursorProto.UnionContinuation toProto() {
            if (cachedProto == null) {
                final RecordCursorProto.UnionContinuation.Builder builder = RecordCursorProto.UnionContinuation.newBuilder();
                final Iterator<RecordCursorContinuation> continuationIterator = continuations.iterator();
                // The first two continuations are special (essentially for compatibility reasons)
                final RecordCursorContinuation firstContinuation = continuationIterator.next();
                if (firstContinuation.isEnd()) {
                    builder.setFirstExhausted(true);
                } else {
                    final byte[] asBytes = firstContinuation.toBytes();
                    if (asBytes != null) {
                        builder.setFirstContinuation(ByteString.copyFrom(asBytes));
                    }
                }
                final RecordCursorContinuation secondContinuation = continuationIterator.next();
                if (secondContinuation.isEnd()) {
                    builder.setSecondExhausted(true);
                } else {
                    final byte[] asBytes = secondContinuation.toBytes();
                    if (asBytes != null) {
                        builder.setSecondContinuation(ByteString.copyFrom(asBytes));
                    }
                }
                // The rest of the cursor states get written as elements in a repeated message field
                while (continuationIterator.hasNext()) {
                    RecordCursorProto.UnionContinuation.CursorState cursorState;
                    RecordCursorContinuation continuation = continuationIterator.next();
                    if (continuation.isEnd()) {
                        cursorState = EXHAUSTED_PROTO;
                    } else {
                        final byte[] asBytes = continuation.toBytes();
                        if (asBytes == null) {
                            cursorState = START_PROTO;
                        } else {
                            cursorState = RecordCursorProto.UnionContinuation.CursorState.newBuilder()
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
            return continuations.stream().allMatch(RecordCursorContinuation::isEnd);
        }
    }

    @Nonnull
    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull Function<byte[], RecordCursor<T>> left, @Nonnull Function<byte[], RecordCursor<T>> right,
                                                                 @Nullable byte[] byteContinuation) {
        final UnionContinuation continuation = UnionContinuation.from(byteContinuation, 2);
        return ImmutableList.of(
                CursorState.from(left, continuation.continuations.get(0)),
                CursorState.from(right, continuation.continuations.get(1)));
    }

    @Nonnull
    protected static <T> List<CursorState<T>> createCursorStates(@Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
                                                                 @Nullable byte[] byteContinuation) {
        final List<CursorState<T>> cursorStates = new ArrayList<>(cursorFunctions.size());
        final UnionContinuation continuation = UnionContinuation.from(byteContinuation, cursorFunctions.size());
        int i = 0;
        for (Function<byte[], RecordCursor<T>> cursorFunction : cursorFunctions) {
            cursorStates.add(CursorState.from(cursorFunction, continuation.continuations.get(i)));
            i++;
        }
        return cursorStates;
    }
}
