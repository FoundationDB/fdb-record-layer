/*
 * MergeCursor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.cursors.EmptyCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An abstract class that corresponds to some kind of cursor merging multiple children together.
 * It forms a common basis for various union and intersection cursors. Extenders should also extend:
 *
 * <ul>
 *     <li>{@link MergeCursorContinuation} with an implementation that constructs an appropriate Protobuf message.</li>
 *     <li>{@link MergeCursorState} if additional state is needed to accompany each cursor.</li>
 * </ul>
 *
 * @param <T> type of input elements
 * @param <U> type of output elements
 * @param <S> type of merge state
 */
@API(API.Status.INTERNAL)
public abstract class MergeCursor<T, U, S extends MergeCursorState<T>> implements RecordCursor<U> {
    // Maximum amount of time to wait before bailing on getting the next state.
    // Added to investigate: https://github.com/FoundationDB/fdb-record-layer/issues/546
    // This is not particularly pretty, but it is meant for some rough debugging.
    private static final long MAX_NEXT_STATE_MILLIS = TimeUnit.SECONDS.toMillis(15);
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(UnorderedUnionCursor.class);
    @Nonnull
    private final List<S> cursorStates;
    @Nullable
    private final FDBStoreTimer timer;
    @Nonnull
    private final Executor executor;
    @Nullable
    private RecordCursorResult<U> nextResult;

    @SuppressWarnings("PMD.CloseResource")
    protected MergeCursor(@Nonnull List<S> cursorStates, @Nullable FDBStoreTimer timer) {
        this.cursorStates = cursorStates;
        this.timer = timer;
        // Choose the executor from the first non-empty cursor. The executors for empty cursors are just
        // the default one, whereas non-empty cursors may have an executor set by the user.
        Executor cursorExecutor = null;
        for (S cursorState : cursorStates) {
            if (!(cursorState.getCursor() instanceof EmptyCursor)) {
                cursorExecutor = cursorState.getExecutor();
                break;
            }
        }
        this.executor = cursorExecutor != null ? cursorExecutor : cursorStates.get(0).getExecutor();
    }

    @SuppressWarnings("PMD.CloseResource")
    private static <T, S extends MergeCursorState<T>> CompletableFuture<?>[] getOnNextFutures(@Nonnull List<S> cursorStates) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[cursorStates.size()];
        int i = 0;
        for (S cursorState : cursorStates) {
            futures[i] = cursorState.getOnNextFuture();
            i++;
        }
        return futures;
    }

    @Nonnull
    protected static <T, S extends MergeCursorState<T>> CompletableFuture<Void> whenAll(@Nonnull List<S> cursorStates) {
        return CompletableFuture.allOf(getOnNextFutures(cursorStates));
    }

    // This returns a "wildcard" to handle the fact that the signature of AsyncUtil.DONE is
    // CompletableFuture<Void> whereas CompletableFuture.anyOf returns a CompletableFuture<Object>.
    // The caller always ignores the result anyway and just uses this as a signal, so it's not
    // a big loss.
    @Nonnull
    @SuppressWarnings({"squid:S1452", "PMD.CloseResource"})
    protected static <T, S extends MergeCursorState<T>> CompletableFuture<?> whenAny(@Nonnull List<S> cursorStates) {
        List<S> nonDoneCursors = new ArrayList<>(cursorStates.size());
        for (S cursorState : cursorStates) {
            if (cursorState.mightHaveNext()) {
                if (MoreAsyncUtil.isCompletedNormally(cursorState.getOnNextFuture())) {
                    // Short-circuit and return immediately if we find a state that is already done.
                    return AsyncUtil.DONE;
                } else {
                    // The cursor might have a next element but its onNext future has either not completed or has
                    // completed exceptionally. Add it to the list of cursors to wait on so that either its value
                    // completes the returned future when ready or its error is propagated.
                    nonDoneCursors.add(cursorState);
                }
            }
        }
        if (nonDoneCursors.isEmpty()) {
            // No cursor will return any more elements, so we can return immediately.
            return AsyncUtil.DONE;
        } else {
            return CompletableFuture.anyOf(getOnNextFutures(nonDoneCursors));
        }
    }

    protected void checkNextStateTimeout(long startTime) {
        long checkStateTime = System.currentTimeMillis();
        if (checkStateTime - startTime > MAX_NEXT_STATE_MILLIS) {
            KeyValueLogMessage logMessage = KeyValueLogMessage.build("time computing next state exceeded",
                                                LogMessageKeys.TIME_STARTED, startTime * 1.0e-3,
                                                LogMessageKeys.TIME_ENDED, checkStateTime * 1.0e-3,
                                                LogMessageKeys.DURATION_MILLIS, checkStateTime - startTime,
                                                LogMessageKeys.CHILD_COUNT, cursorStates.size());
            if (LOGGER.isDebugEnabled()) {
                logMessage.addKeyAndValue("child_states", cursorStates.stream()
                        .map(cursorState -> "(future=" + cursorState.getOnNextFuture() +
                                            ", result=" + (cursorState.getResult() == null ? "null" : cursorState.getResult().hasNext()) +
                                            ", cursorClass=" + cursorState.getCursor().getClass().getName() + ")"
                        )
                        .collect(Collectors.toList())
                );
            }
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(logMessage.toString());
            }
            throw new RecordCoreException("time computing next state exceeded");
        }
    }

    /**
     * Gets the strongest reason for stopping of all cursor states. An out-of-band reason is stronger
     * than an in-band limit, and an in-band limit is stronger than {@link com.apple.foundationdb.record.RecordCursor.NoNextReason#SOURCE_EXHAUSTED}.
     * Cursors that have not been stopped are ignored, but if no cursor has stopped, this throws an error.
     *
     * @param cursorStates the list of cursor states to consider
     * @param <T> the type of elements returned by this cursor
     * @param <S> the type of cursor state in the list of cursors
     * @return the strongest reason to stop from the list of all cursors
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    protected static <T, S extends MergeCursorState<T>> NoNextReason getStrongestNoNextReason(@Nonnull List<S> cursorStates) {
        NoNextReason reason = null;
        for (S cursorState : cursorStates) {
            final RecordCursorResult<T> childResult = cursorState.getResult();
            if (childResult != null && !childResult.hasNext()) {
                // Combine the current reason so far with this child's reason.
                // In particular, choose the child reason if it is at least as strong
                // as the current one. This guarantees that it will choose an
                // out of band reason if there is one and will only return
                // SOURCE_EXHAUSTED if every child ended with SOURCE_EXHAUSTED.
                NoNextReason childReason = cursorState.getResult().getNoNextReason();
                if (reason == null || childReason.isOutOfBand() || reason.isSourceExhausted()) {
                    reason = childReason;
                }
            }
        }
        if (reason == null) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all children have next");
        }
        return reason;
    }

    /**
     * Gets the weakest reason for stopping of all cursor states. The weakest reason is
     * {@link com.apple.foundationdb.record.RecordCursor.NoNextReason#SOURCE_EXHAUSTED}, followed by an
     * in-band limit, then an out of band limit.
     *
     * @param cursorStates the list of cursor states to consider
     * @param <T> the type of elements returned by this cursor
     * @param <S> the type of cursor state in the list of cursors
     * @return the strongest reason to stop from the list of all cursors
     */
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    protected static <T, S extends MergeCursorState<T>> NoNextReason getWeakestNoNextReason(@Nonnull List<S> cursorStates) {
        NoNextReason reason = null;
        for (S cursorState : cursorStates) {
            final RecordCursorResult<T> childResult = cursorState.getResult();
            if (childResult != null && !childResult.hasNext()) {
                NoNextReason childReason = childResult.getNoNextReason();
                if (childReason.isSourceExhausted()) {
                    // one of the cursors is exhausted, so regardless of the other cursors'
                    // reasons, the cursor has stopped due to source exhaustion
                    return childReason;
                } else if (reason == null || (reason.isOutOfBand() && !childReason.isOutOfBand())) {
                    // if either we haven't chosen a reason yet (which is the case if reason is null)
                    // or if we have found an in-band reason to stop that supersedes a previous out-of-band
                    // reason, then update the current reason to that one
                    reason = childReason;
                }
            }
        }
        if (reason == null) {
            throw new RecordCoreException("mergeNoNextReason should not be called when all children have next");
        }
        return reason;
    }

    /**
     * Get the child cursors along with associated mutable state.
     *
     * @return the child cursors of this cursor
     */
    @Nonnull
    protected List<S> getCursorStates() {
        return cursorStates;
    }

    /**
     * Determine which cursors should have their values included in the next iteration.
     * This list should include the state associated with each cursor, and the returned value should
     * be a (not necessarily proper) sublist of the result states of this cursor. (This
     * list can be retrieved by calling {@link #getCursorStates()}.) To indicate that this cursor
     * is done returning elements, this function should return the empty list.
     *
     * @return the list of cursors to include in the next iteration
     */
    @Nonnull
    protected abstract CompletableFuture<List<S>> computeNextResultStates();

    /**
     * Determine the next result to return based on the results of the child cursors. This will
     * be called with the return value of {@link #computeNextResultStates()}.
     *
     * @param resultStates the list of cursors to be included in the result
     * @return a result somehow combining the results of the input cursors
     */
    @Nonnull
    protected abstract U getNextResult(@Nonnull List<S> resultStates);

    /**
     * Merge the {@link NoNextReason}s for child cursors. This will only be called after it is
     * determined that the merge cursor itself will not return a {@code NoNextReason}. Note that
     * this may (or may) not imply that all child cursors are done (depending on the implementation
     * of the extending cursor). This {@code NoNextReason} will be the reason returned by the
     * merge cursor.
     *
     * @return a {@link NoNextReason} based on the child cursors' {@code NoNextReason}s
     */
    @Nonnull
    protected abstract NoNextReason mergeNoNextReasons();

    /**
     * Produce a {@link RecordCursorContinuation} for this cursor. The super-class itself handles
     * constructing the byte array continuation from this continuation. Most continuations will
     * be based on the continuations of this cursors child cursors. These can be retrieved by
     * calling {@link #getChildContinuations()}. This will only be called after {@link #computeNextResultStates()}
     * has completed.
     *
     * @return a {@link RecordCursorContinuation} for this cursor based on the state of its child cursors
     */
    @Nonnull
    protected abstract RecordCursorContinuation getContinuationObject();

    @Override
    @Nonnull
    public CompletableFuture<RecordCursorResult<U>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        return computeNextResultStates().thenApply(resultStates -> {
            boolean hasNext = !resultStates.isEmpty();
            if (!hasNext) {
                nextResult = RecordCursorResult.withoutNextValue(getContinuationObject(), mergeNoNextReasons());
            } else {
                final U result = getNextResult(resultStates);
                resultStates.forEach(S::consume);
                nextResult = RecordCursorResult.withNextValue(result, getContinuationObject());
            }
            return nextResult;
        });
    }

    @Nonnull
    protected List<RecordCursorContinuation> getChildContinuations() {
        return cursorStates.stream().map(S::getContinuation).collect(Collectors.toList());
    }

    @Override
    public void close() {
        cursorStates.forEach(S::close);
    }

    @Override
    public boolean isClosed() {
        return cursorStates.stream().allMatch(S::isClosed);
    }

    @Override
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get the {@link FDBStoreTimer} used to instrument events of this cursor.
     *
     * @return the timer used to instrument this cursor
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        return timer;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            for (S cursorState : getCursorStates()) {
                if (!cursorState.getCursor().accept(visitor)) {
                    break;
                }
            }
        }
        return visitor.visitLeave(this);
    }
}
