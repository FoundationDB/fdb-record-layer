/*
 * UnionCursor.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.cursors.IllegalContinuationAccessChecker;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

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
 * A cursor that implements a union of all the records from a set of cursors all of whom are ordered compatibly.
 * @param <T> the type of elements returned by the cursor
 */
public class UnionCursor<T> implements RecordCursor<T> {
    @Nonnull
    private final Function<? super T, ? extends List<Object>> comparisonKeyFunction;
    private final boolean reverse;
    @Nonnull
    private final List<CursorState<T>> cursorStates;
    @Nullable
    private final FDBStoreTimer timer;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_UNION);
    @Nonnull
    private static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_UNION_PLAN_UNIQUES);
    @Nonnull
    private static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_UNION_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    private static class CursorState<T> {
        private static final RecordCursorProto.UnionContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.UnionContinuation.CursorState.newBuilder()
                .setExhausted(true)
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
        private byte[] continuationBefore;
        @Nullable
        private byte[] continuationAfter;
        @Nullable
        private byte[] continuation;

        public CursorState(@Nonnull RecordCursor<T> cursor, @Nullable byte[] continuationStart) {
            this.cursor = cursor;
            this.continuation = continuationStart;
            this.continuationAfter = continuationStart;
        }

        @Nonnull
        public CompletableFuture<Void> getOnHasNextFuture() {
            if (onHasNextFuture == null) {
                onHasNextFuture = cursor.onHasNext().thenAccept(cursorHasNext -> this.hasNext = cursorHasNext);
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
            continuation = continuationAfter;
        }

        public boolean isExhausted() {
            return !hasNext && cursor.getNoNextReason().isSourceExhausted();
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

    private UnionCursor(@Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                        boolean reverse, @Nonnull List<CursorState<T>> cursorStates,
                        @Nullable FDBStoreTimer timer) {
        this.comparisonKeyFunction = comparisonKeyFunction;
        this.reverse = reverse;
        this.cursorStates = cursorStates;
        this.timer = timer;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        mayGetContinuation = false;
        CompletableFuture<?>[] futures = new CompletableFuture<?>[cursorStates.size()];
        int i = 0;
        for (CursorState<T> cursorState : cursorStates) {
            futures[i] = cursorState.getOnHasNextFuture();
            i++;
        }
        return CompletableFuture.allOf(futures).thenApply(vignore -> {
            cursorStates.forEach(cursorState -> {
                if (!cursorState.hasNext) { // continuation is valid immediately
                    cursorState.continuation = cursorState.cursor.getContinuation();
                }
            });
            boolean anyHasNext = false;
            for (CursorState<T> cursorState : cursorStates) {
                if (!cursorState.hasNext && cursorState.cursor.getNoNextReason().isLimitReached()) { // continuation is valid immediately
                    // If any side stopped due to limit reached, need to stop completely,
                    // since might otherwise duplicate ones after that, if other side still available.
                    mayGetContinuation = true;
                    return false;
                } else {
                    if (cursorState.hasNext) {
                        anyHasNext = true;
                    }
                }
            }
            mayGetContinuation = !anyHasNext;
            return anyHasNext;
        });
    }

    private void mergeStates(@Nonnull List<CursorState<T>> minStates, @Nonnull List<CursorState<T>> otherStates) {
        List<Object> nextKey = null;
        for (CursorState<T> cursorState : cursorStates) {
            if (cursorState.element == null) {
                cursorState.continuation = null;
            } else {
                int compare;
                if (nextKey == null) {
                    // This is the first key we've seen, so always chose it.
                    compare = -1;
                } else {
                    // Choose the minimum of the previous minimum key and this next one
                    // If doing a reverse scan, choose the maximum.
                    compare = KeyComparisons.KEY_COMPARATOR.compare(cursorState.key, nextKey) * (reverse ? -1 : 1);
                }
                if (compare < 0) {
                    // We have a new next key. Reset the book-keeping information.
                    otherStates.addAll(minStates);
                    minStates.clear();
                    nextKey = cursorState.key;
                }
                if (compare <= 0) {
                    minStates.add(cursorState);
                } else {
                    otherStates.add(cursorState);
                }
            }
        }
        if (timer != null) {
            if (minStates.size() == 1) {
                timer.increment(uniqueCounts);
            } else {
                // The number of duplicates is the number of minimum states
                // for this value except for the first one (hence the "- 1").
                timer.increment(duplicateCounts, minStates.size() - 1);
            }
        }
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
        List<CursorState<T>> minStates = new ArrayList<>(cursorStates.size());
        List<CursorState<T>> otherStates = new ArrayList<>(cursorStates.size());
        mergeStates(minStates, otherStates);
        if (minStates.isEmpty()) {
            throw new RecordCoreException("union with additional items had no next states");
        }
        final T result = minStates.get(0).element;
        if (result == null) {
            throw new RecordCoreException("minimum element had null result");
        }
        // Advance each minimum state.
        minStates.forEach(CursorState::consume);
        for (CursorState<T> cursorState : otherStates) {
            cursorState.continuation = cursorState.continuationBefore;
        }
        if (timer != null) {
            timer.record(duringEvents, System.nanoTime() - startTime);
        }
        return result;
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        boolean allNull = cursorStates.stream().allMatch(cursorState -> cursorState.continuation == null);
        if (allNull) {
            return null;
        }
        final RecordCursorProto.UnionContinuation.Builder builder = RecordCursorProto.UnionContinuation.newBuilder();
        final Iterator<CursorState<T>> cursorStateIterator = cursorStates.iterator();
        // The first two continuations are special (essentially for compatibility reasons)
        final CursorState<T> firstCursorState = cursorStateIterator.next();
        if (firstCursorState.continuation != null) {
            builder.setFirstContinuation(ByteString.copyFrom(firstCursorState.continuation));
        } else if (firstCursorState.isExhausted()) {
            builder.setFirstExhausted(true);
        }
        final CursorState<T> secondCursorSatate = cursorStateIterator.next();
        if (secondCursorSatate.continuation != null) {
            builder.setSecondContinuation(ByteString.copyFrom(secondCursorSatate.continuation));
        } else if (secondCursorSatate.isExhausted()) {
            builder.setSecondExhausted(true);
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

    /**
     * Create a union cursor from two compatibly-ordered cursors. This cursor
     * is identical to the cursor that would be produced by calling the overload of
     * {@link #create(FDBEvaluationContext, KeyExpression, boolean, List, byte[]) create()}
     * that takes a list of cursors.
     *
     * @param context the context to use when evaluating the comparison key against records
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param continuation any continuation from a previous scan
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @return a cursor containing any records in any child cursors
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> UnionCursor<S> create(
            @Nonnull FDBEvaluationContext<M> context,
            @Nonnull KeyExpression comparisonKey, boolean reverse,
            @Nonnull Function<byte[], RecordCursor<S>> left,
            @Nonnull Function<byte[], RecordCursor<S>> right,
            @Nullable byte[] continuation) {
        return create(
                (S record) -> comparisonKey.evaluateSingleton(context, record).toTupleAppropriateList(),
                reverse, left, right, continuation, context.getTimer());
    }

    /**
     * Create a union cursor from two compatibly-ordered cursors. This cursor
     * is identical to the cursor that would be produced by calling the overload of
     * {@link #create(Function, boolean, List, byte[], FDBStoreTimer) create()}
     * that takes a list of cursors.
     *
     * @param comparisonKeyFunction the function expression used to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param left a function to produce the first {@link RecordCursor} from a continuation
     * @param right a function to produce the second {@link RecordCursor} from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by the cursor
     * @return a cursor containing all elements in both child cursors
     * @see #create(Function, boolean, Function, Function, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <T> UnionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull Function<byte[], RecordCursor<T>> left,
            @Nonnull Function<byte[], RecordCursor<T>> right,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
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
        return new UnionCursor<>(comparisonKeyFunction, reverse, cursorStates, timer);
    }

    /**
     * Create a union cursor from two or more compatibly-ordered cursors.
     * As its comparison key function, it will evaluate the provided comparison key
     * on each record from each cursor. This otherwise behaves exactly the same way
     * as the overload of this function that takes a function to extract a comparison
     * key.
     *
     * @param context the context to use when evaluating the comparison key against records
     * @param comparisonKey the key expression used to compare records from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param <M> the type of the Protobuf record elements of the cursor
     * @param <S> the type of record wrapping a record of type <code>M</code>
     * @return a cursor containing any records in any child cursors
     * @see #create(Function, boolean, List, byte[], FDBStoreTimer)
     */
    @Nonnull
    public static <M extends Message, S extends FDBRecord<M>> UnionCursor<S> create(
            @Nonnull FDBEvaluationContext<M> context,
            @Nonnull KeyExpression comparisonKey, boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<S>>> cursorFunctions,
            @Nullable byte[] continuation) {
        return create(
                (S record) -> comparisonKey.evaluateSingleton(context, record).toTupleAppropriateList(),
                reverse, cursorFunctions, continuation, context.getTimer());
    }

    /**
     * Create a union cursor from two or more compatibly-ordered cursors.
     * Note that this will throw an error if the list of cursors does not have at least two elements.
     * The returned cursor will return any records that appear in any of the provided
     * cursors, preserving order. All cursors must return records in the same order,
     * and that order should be determined by the comparison key function, i.e., if <code>reverse</code>
     * is <code>false</code>, then the records should be returned in ascending order by that key,
     * and if <code>reverse</code> is <code>true</code>, they should be returned in descending
     * order. Additionally, if the comparison key function evaluates to the same value when applied
     * to two records (possibly from different cursors), then those two elements
     * should be equal. In other words, the value of the comparison key should be the <i>only</i>
     * necessary data that need to be extracted from each element returned by the child cursors to
     * perform the union. Additionally, the provided comparison key function should not have
     * any side-effects and should produce the same output every time it is applied to the same input.
     *
     * <p>
     * The cursors are provided as a list of functions rather than a list of {@link RecordCursor}s.
     * These functions should create a new <code>RecordCursor</code> instance with a given
     * continuation appropriate for that cursor type. The value of that continuation will be determined
     * by this function from the <code>continuation</code> parameter for the union
     * cursor as a whole.
     * </p>
     *
     * @param comparisonKeyFunction the function evaluated to compare elements from different cursors
     * @param reverse whether records are returned in descending or ascending order by the comparison key
     * @param cursorFunctions a list of functions to produce {@link RecordCursor}s from a continuation
     * @param continuation any continuation from a previous scan
     * @param timer the timer used to instrument events
     * @param <T> the type of elements returned by this cursor
     * @return a cursor containing any records in any child cursors
     */
    @Nonnull
    public static <T> UnionCursor<T> create(
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            boolean reverse,
            @Nonnull List<Function<byte[], RecordCursor<T>>> cursorFunctions,
            @Nullable byte[] continuation,
            @Nullable FDBStoreTimer timer) {
        if (cursorFunctions.size() < 2) {
            throw new RecordCoreArgumentException("not enough child cursors provided to UnionCursor")
                    .addLogInfo(LogMessageKeys.CHILD_COUNT, cursorFunctions.size());
        }
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
        return new UnionCursor<>(comparisonKeyFunction, reverse, cursorStates, timer);
    }
}
