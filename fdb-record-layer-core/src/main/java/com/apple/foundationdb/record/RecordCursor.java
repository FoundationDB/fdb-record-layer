/*
 * RecordCursor.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.cursors.AsyncIteratorCursor;
import com.apple.foundationdb.record.cursors.EmptyCursor;
import com.apple.foundationdb.record.cursors.FilterCursor;
import com.apple.foundationdb.record.cursors.FlatMapPipelinedCursor;
import com.apple.foundationdb.record.cursors.FutureCursor;
import com.apple.foundationdb.record.cursors.IteratorCursor;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.cursors.MapCursor;
import com.apple.foundationdb.record.cursors.MapPipelinedCursor;
import com.apple.foundationdb.record.cursors.OrElseCursor;
import com.apple.foundationdb.record.cursors.RowLimitedCursor;
import com.apple.foundationdb.record.cursors.SkipCursor;
import com.apple.foundationdb.record.cursors.TimeLimitedCursor;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An asynchronous iterator that supports continuations.
 *
 * Much like an {@code Iterator}, a {@code RecordCursor} provides one-item-at-a-time access to an ordered collection.
 * It differs in three primary respects:
 *
 * <ol>
 * <li>
 * A {@code RecordCursor} is <em>asynchronous</em>. Instead of the synchronous {@link Iterator#hasNext()} and
 * {@link Iterator#next()} methods, {@code RecordCursor} advances the iteration using the asynchronous {@link #onNext()}
 * method, which returns a {@link CompletableFuture}.
 * </li>
 *
 * <li>
 * {@code RecordCursor} supports <em>continuations</em>, which are opaque tokens that represent the position of the
 * cursor between records. Continuations can be used to restart the iteration at the same point later. Continuations
 * represent all of the state needed to do this restart, even if the original objects no longer exist.
 * </li>
 *
 * <li>
 * Finally, {@code RecordCursor}'s API offers <em>correctness by construction</em>. In contrast to the {@code hasNext()}/
 * {@code next()} API used by {@code Iterator}, {@code RecordCursor}'s primary API is{@link RecordCursor#onNext()} which
 * produces the next value if one is present, or an object indicating that there is no such record. The presence of a
 * next value is instead indicated by {@link RecordCursorResult#hasNext()}. This API serves to bundle a continuation
 * (and possible a {@link NoNextReason}) with the result, ensuring that a continuation is obtained only when it is valid.
 * For compatibility with {@code Iterator}, {@code RecordCursor} also supports {@link RecordCursor#onHasNext()} and
 * {@link RecordCursor#next()}, but continuations must be used carefully with this API, as described below.
 * </li>
 * </ol>
 *
 * <p>
 * {@code RecordCursor} supports {@link #getContinuation} for use with the {@code Iterator}-style API. A cursor is
 * between records and valid for getting its continuation after calling {@link #next} or after {@link #hasNext} returns
 * {@code false}.
 * </p>
 *
 * <p>
 * When a cursor stops producing values, it can report why using a {@link NoNextReason}. This can be returned as part of
 * a {@link RecordCursorResult} if using the {@link #onNext()} API or using {@link #getNoNextReason()} if using the
 * {@code Iterator}-style API. No-next-reasons are fundamentally distinguished between those that are due to the data
 * itself (in-band) and those that are due to the environment / context (out-of-band). For example, running out of data
 * or having returned the maximum number of records requested are in-band, while reaching a limit on the number of
 * key-value pairs scanned by the transaction or the time that a transaction has been open are out-of-band.
 * </p>
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.STABLE)
public interface RecordCursor<T> extends AutoCloseable, Iterator<T> {
    /**
     * Asynchronously check whether there are more records available from the cursor.
     * @return a future that when complete will hold <code>true</code> if {@link #next()} would return a record.
     * @see com.apple.foundationdb.async.AsyncIterator#onHasNext()
     */
    @Nonnull
    CompletableFuture<Boolean> onHasNext();

    @Override
    default boolean hasNext() {
        try {
            return onHasNext().get();
        } catch (ExecutionException ex) {
            throw new RecordCoreException(CompletionExceptionLogHelper.asCause(ex));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecordCoreInterruptedException(ex.getMessage(), ex);
        }
    }

    @Nullable
    @Override
    T next();

    /**
     * Get a byte string that can be used to continue a query after the last record returned.
     *
     * @return opaque byte array denoting where the cursor should pick up. This can be passed back into a new
     * cursor of the same type, with all other parameters remaining the same.
     *
     * Returns <code>null</code> if the underlying source is completely exhausted, independent of any limit
     * passed to the cursor creator. Since such creators generally accept <code>null</code> to mean no continuation,
     * that is, start from the beginning, one must check for <code>null</code> from <code>getContinuation</code> to
     * keep from starting over.
     *
     * Result is not always defined if called before <code>onHasNext</code> or before <code>next</code> after
     * <code>onHasNext</code> has returned <code>true</code>. That is, a continuation is only guaranteed when called
     * "between" records from a <code>while (hasNext) next</code> loop or after its end.
     */
    @Nullable
    byte[] getContinuation();

    /**
     * The reason that {@link #hasNext} returned <code>false</code>.
     */
    enum NoNextReason {
        /**
         * The underlying scan, irrespective of any limit, has reached the end.
         * {@link #getContinuation()} should return <code>null</code>.
         */
        SOURCE_EXHAUSTED(false),

        /**
         * The limit on the number record to return was reached.
         * This limit may be specified by a an explicit {@link ExecuteProperties#setReturnedRowLimit} of by an implicit
         * limit based on a predicate for continuing, as in {@link com.apple.foundationdb.record.cursors.MapWhileCursor}.
         * {@link #getContinuation()} may return a continuation for after the requested limit.
         * @see ExecuteProperties#setReturnedRowLimit
         * @see #limitRowsTo
         * @see com.apple.foundationdb.record.cursors.MapWhileCursor
         */
        RETURN_LIMIT_REACHED(false),

        /**
         * The limit on the amount of time that a scan can take was reached.
         * {@link #getContinuation()} may return a continuation for resuming the scan.
         *
         * Note that is it possible for <code>TIME_LIMIT_REACHED</code> to be returned before
         * any actual records if a complex scan takes a lot of work to reach the requested records,
         * such as a query that does filtering.
         * @see ExecuteProperties.Builder#setTimeLimit(long)
         * @see TimeScanLimiter
         */
        TIME_LIMIT_REACHED(true),

        /**
         * The limit on the number of records to scan was reached.
         * {@link #getContinuation()} may return a continuation for resuming the scan.
         *
         * Note that it is possible for <code>SCAN_LIMIT_REACHED</code> to be returned before any actual records if
         * a scan retrieves many records that are discarded, such as by a filter or type filter.
         * @see ExecuteProperties.Builder#setScannedRecordsLimit(int)
         * @see RecordScanLimiter
         */
        SCAN_LIMIT_REACHED(true),

        /**
         * The limit on the number of bytes to scan was reached.
         * {@link #getContinuation()} may return a continuation for resuming the scan.
         * Note that it is possible for <code>BYTE_LIMIT_REACHED</code> to be returned before any actual records if
         * a scan retrieves many bytes for records that are discarded, such as by a filter or type filter.
         * @see ExecuteProperties.Builder#setScannedBytesLimit(long)
         * @see ByteScanLimiter
         */
        BYTE_LIMIT_REACHED(true);

        final boolean outOfBand;

        NoNextReason(boolean outOfBand) {
            this.outOfBand = outOfBand;
        }


        /**
         * Does this reason represent an out-of-band (that is, not solely dependent on the records returned) completion?
         * In general, when an out-of-band reason is encountered, the entire cursor tree unwinds and returns to the
         * client to start over fresh with a new cursor.
         * @return {@code true} if this cursor stopped early
         */
        public boolean isOutOfBand() {
            return outOfBand;
        }

        /**
         * Does this reason indicate that there is no more data available?
         * These are the cases in which {@link #getContinuation} would return <code>null</code>.
         * @return {@code true} if the source of this cursor is completely exhausted and no completion is possible
         */
        public boolean isSourceExhausted() {
            return this == SOURCE_EXHAUSTED;
        }

        /**
         * Does this reason indicate that some limit was reached?
         * This includes both the in-band returned row limit and the out-of-band time and other resource limits.
         * @return {@code true} if this cursor stopped due to any kind of limit being reached
         */
        public boolean isLimitReached() {
            return this != SOURCE_EXHAUSTED;
        }
    }

    /**
     * Get the reason that the cursor has reached the end and returned <code>false</code> for {@link #hasNext}.
     * If <code>hasNext</code> was not called or returned <code>true</code> last time, the result is undefined and
     * may be an exception.
     * @return the reason that the cursor stopped
     */
    NoNextReason getNoNextReason();

    /**
     * Asynchronously return the next result from this cursor. When complete, the future will contain a
     * {@link RecordCursorResult}, which represents exactly one of the following:
     * <ol>
     *     <li>
     *         The next object of type {@code T} produced by the cursor. In addition to the next record, this result
     *         includes a {@link RecordCursorContinuation} that can be used to continue the cursor after the last record
     *         returned. The returned continuation is guaranteed not to be an "end continuation" representing the end of
     *         the cursor: specifically, {@link RecordCursorContinuation#isEnd()} is always {@code false} on the returned
     *         continuation.
     *     </li>
     *     <li>
     *         The fact that the cursor is stopped and cannot produce another record and a {@link NoNextReason} that
     *         explains why no record could be produced. The result include a continuation that can be used to continue
     *         the cursor after the last record returned.
     *
     *         If the result's {@code NoNextReason} is anything other than {@link NoNextReason#SOURCE_EXHAUSTED}, the
     *         returned continuation must not be an end continuation. Conversely, if the result's {@code NoNextReason}
     *         is {@code SOURCE_EXHAUSTED}, then the returned continuation must be an an "end continuation".
     *     </li>
     * </ol>
     * In either case, the returned {@code RecordCursorContinuation} can be serialized to an opaque byte array using
     * {@link RecordCursorContinuation#toBytes()}. This can be passed back into a new cursor of the same type, with all
     * other parameters remaining the same.
     *
     * @return a future for the next result from this cursor representing either the next record or an indication of
     *         why the cursor stopped
     * @see RecordCursorResult
     * @see RecordCursorContinuation
     */
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    CompletableFuture<RecordCursorResult<T>> onNext();

    @Override
    void close();

    @Nonnull
    Executor getExecutor();

    /**
     * Accept a visit from hierarchical visitor, which implements {@link RecordCursorVisitor}.
     * By contract, implementations of this method must return the value of <code>visitor.visitLeave(this)</code>,
     * which determines whether or not subsequent siblings of this cursor should be visited.
     * @param visitor a hierarchical visitor
     * @return <code>true</code> if the subsequent siblings of the <code>cursor</code> should be visited, and <code>false</code> otherwise
     */
    boolean accept(@Nonnull RecordCursorVisitor visitor);

    /**
     * Return the entire stream of records as an asynchronous list.
     * @return a future that when complete has a list with all remaining records.
     */
    @Nonnull
    default CompletableFuture<List<T>> asList() {
        final List<T> result = new ArrayList<>();
        return forEach(result::add).thenApply(vignore -> result);
    }

    /**
     * Count the number of records remaining.
     * @return a future that completes to the number of records in the cursor
     */
    @Nonnull
    default CompletableFuture<Integer> getCount() {
        final int[] i = new int[] {0};
        return AsyncUtil.whileTrue(() -> onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                next();
                i[0] = i[0] + 1;
            }
            return hasNext;
        }), getExecutor()).thenApply(vignore -> i[0]);
    }

    /**
     * Fetches the first item returned by the cursor.
     *
     * @return <code>Optional.empty()</code> if the cursor had no results or if the first record was null,
     *   otherwise returns an <code>Optional</code> of the first item returned by the cursor.
     */
    @Nonnull default CompletableFuture<Optional<T>> first() {
        return onHasNext().thenApply( hasNext -> hasNext ? Optional.ofNullable(next()) : Optional.empty() );
    }

    /**
     * Get a new cursor by applying the given function to the records in this cursor.
     * @param func the function to apply
     * @param <V> the type of the record elements
     * @return a new cursor that applies the given function
     */
    @Nonnull
    default <V> RecordCursor<V> map(@Nonnull Function<T, V> func) {
        return new MapCursor<>(this, func);
    }

    /**
     * Get a new cursor that applies the given consumer to the records in this cursor, without modifying the cursor records.
     * @param consumer the consumer to apply
     * @return a new cursor that applies the given consumer
     */
    @Nonnull
    default RecordCursor<T> mapEffect(@Nonnull Consumer<T> consumer) {
        return new MapCursor<>(this, record -> {
            consumer.accept(record);
            return record;
        });
    }

    /**
     * Get a new cursor that runs the given runnable every time a record arrives, without modifying the cursor records.
     * @param runnable the runnable to call
     * @return a new cursor that runs the given runnable as record pass through
     */
    @Nonnull
    default RecordCursor<T> mapEffect(@Nonnull Runnable runnable) {
        return new MapCursor<>(this, record -> {
            runnable.run();
            return record;
        });
    }

    /**
     * Get a new cursor that skips records that do not satisfy the given predicate.
     * @param pred the predicate to apply
     * @return a new cursor that filters out records for which {@code pred} returns {@code false}
     */
    @Nonnull
    default RecordCursor<T> filter(@Nonnull Function<T, Boolean> pred) {
        return new FilterCursor<>(this, pred);
    }

    @Nonnull
    default RecordCursor<T> filterInstrumented(@Nonnull Function<T, Boolean> pred,
                                                      @Nullable StoreTimer timer, @Nullable StoreTimer.Count in,
                                                      @Nullable StoreTimer.Event during,
                                                      @Nullable StoreTimer.Count success, @Nullable StoreTimer.Count failure) {
        Set<StoreTimer.Count> inSet = in != null ? Collections.singleton(in) : Collections.emptySet();
        Set<StoreTimer.Event> duringSet = during != null ? Collections.singleton(during) : Collections.emptySet();
        Set<StoreTimer.Count> successSet = success != null ? Collections.singleton(success) : Collections.emptySet();
        Set<StoreTimer.Count> failureSet = failure != null ? Collections.singleton(failure) : Collections.emptySet();

        return filterInstrumented(pred, timer, inSet, duringSet, successSet, failureSet);
    }

    /**
     * Get a new cursor that skips records that do not satisfy the given predicate, while instrumenting the number of
     * records that the filter sees, the number that it passes, the number that it fails, and the amount of time that it
     * spends computing the predicate.
     * @param pred a boolean predicate to filter on
     * @param timer a StoreTimer to log the counts and events
     * @param inSet a set StoreTimer.Count that will be incremented for each record the filter sees
     * @param duringSet a set of StoreTimer.Event that will log the time spent computing the predicate
     * @param successSet a set of StoreTimer.Count that will be increment for each record on which the predicate evaluates <code>true</code>
     * @param failureSet a set of StoreTimer.Count that will be increment for each record on which the predicate evaluates <code>false</code>
     * @return a new cursor that skips records for which {@code pred} returns {@code false}
     */
    @Nonnull
    default RecordCursor<T> filterInstrumented(@Nonnull Function<T, Boolean> pred,
                                                      @Nullable StoreTimer timer,
                                                      @Nonnull Set<StoreTimer.Count> inSet,
                                                      @Nonnull Set<StoreTimer.Event> duringSet,
                                                      @Nonnull Set<StoreTimer.Count> successSet,
                                                      @Nonnull Set<StoreTimer.Count> failureSet) {
        if (timer == null) {
            return filter(pred);
        }

        return filter(record -> {
            for (StoreTimer.Count in : inSet) {
                timer.increment(in);
            }

            long startTime = System.nanoTime();
            Boolean p = pred.apply(record);
            for (StoreTimer.Event during : duringSet) {
                timer.recordSinceNanoTime(during, startTime);
            }

            if (Boolean.TRUE.equals(p)) {
                for (StoreTimer.Count success : successSet) {
                    timer.increment(success);
                }
            } else {
                for (StoreTimer.Count failure : failureSet) {
                    timer.increment(failure);
                }
            }
            return p;
        });
    }

    /**
     * Get a new cursor that skips the given number of records.
     * @param skip number of records to skip
     * @return a new cursor that starts after {@code skip} records
     */
    default RecordCursor<T> skip(int skip) {
        if (skip < 0) {
            throw new RecordCoreException("Invalid skip count: " + skip);
        }
        if (skip == 0) {
            return this;
        }
        return new SkipCursor<>(this, skip);
    }

    /**
     * @deprecated Use {@link #limitRowsTo(int)} instead.
     * @param limit the maximum number of records to return
     * @return a new cursor that will return at most {@code limit} records
     */
    @Deprecated
    default RecordCursor<T> limitTo(int limit) {
        return limitRowsTo(limit);
    }

    /**
     * Get a new cursor that will only return records up to the given limit.
     * @param limit the maximum number of records to return
     * @return a new cursor that will return at most {@code limit} records
     */
    @Nonnull
    default RecordCursor<T> limitRowsTo(int limit) {
        if (limit < 0) {
            throw new RecordCoreException("Invalid row limit: " + limit);
        }

        if (limit > 0 && limit < Integer.MAX_VALUE) {
            return new RowLimitedCursor<>(this, limit);
        } else {
            return this;
        }
    }

    /**
     * Get a new cursor that will only return records up to the specified time limit (in milliseconds).
     * @param timeLimit the maximum number of milliseconds to run
     * @return a new cursor that stops early if {@code timeLimit} is exceeded
     * @deprecated in favor of a {@link TimeScanLimiter} as part of every {@link com.apple.foundationdb.record.cursors.BaseCursor}
     */
    @Deprecated
    @Nonnull
    default RecordCursor<T> limitTimeTo(long timeLimit) {
        return limitTimeTo(System.currentTimeMillis(), timeLimit);
    }

    /**
     * Get a new cursor that will only return records up to the specified time limit (in milliseconds).
     * @param timeStartingFrom the starting time from which to measure the time limit
     * @param timeLimit the maximum number of milliseconds to run 
     * @return a new cursor that stops early when {@code timeLimit} after {@code timeStartingFrom} is reached
     * @deprecated in favor of a {@link TimeScanLimiter} as part of every {@link com.apple.foundationdb.record.cursors.BaseCursor}
     */
    @Deprecated
    @Nonnull
    default RecordCursor<T> limitTimeTo(long timeStartingFrom, long timeLimit) {
        if (timeLimit < 0L) {
            throw new RecordCoreException("Invalid time limit: " + timeLimit);
        }

        if (timeLimit > 0L && timeLimit < Long.MAX_VALUE) {
            return new TimeLimitedCursor<>(this, timeStartingFrom, timeLimit);
        } else {
            return this;
        }
    }

    @Nonnull
    default RecordCursor<T> skipThenLimit(int skip, int limit) {
        return skip(skip).limitRowsTo(limit);
    }

    /**
     * Get a new cursor by applying the given asynchronous function to the records in this cursor.
     * @param func the function to apply to each record
     * @param pipelineSize the number of futures from applications of the mapping function to start ahead of time
     * @param <V> the result type of the mapping function
     * @return a new cursor that applies the given function to each record
     */
    @Nonnull
    default <V> RecordCursor<V> mapPipelined(@Nonnull Function<T, CompletableFuture<V>> func, int pipelineSize) {
        return new MapPipelinedCursor<>(this, func, pipelineSize);
    }

    /**
     * Get a new cursor by applying the given cursor generating function to the records in this cursor.
     * @param func the function to apply to each record
     * @param pipelineSize the number of cursors from applications of the mapping function to open ahead of time
     * @param <V> the result type of the mapping function
     * @return a new cursor that applies the given function to produce a cursor of records that gets flattened
     */
    @Nonnull
    default <V> RecordCursor<V> flatMapPipelined(@Nonnull Function<T, ? extends RecordCursor<V>> func, int pipelineSize) {
        return new FlatMapPipelinedCursor<>(this, (t, cignore) -> func.apply(t),
                null, null, null, null,
                pipelineSize);
    }

    @Nonnull
    static <T, V> RecordCursor<V> flatMapPipelined(@Nonnull Function<byte[], ? extends RecordCursor<T>> outerFunc,
                                                          @Nonnull BiFunction<T, byte[], ? extends RecordCursor<V>> innerFunc,
                                                          @Nullable byte[] continuation,
                                                          int pipelineSize) {
        return flatMapPipelined(outerFunc, innerFunc, null, continuation, pipelineSize);
    }

    @Nonnull
    /**
     * Resume a nested cursor with the given continuation or start if <code>null</code>.
     * @param outerFunc a function that takes the outer continuation and returns the outer cursor.
     * @param innerFunc a function that takes an outer record and an inner continuation and returns the inner cursor.
     * @param checker a function that takes an outer record and returns a way of recognizing it again or <code>null</code>.
     * When computing the continuation, this is called on the current outer record and the result, if not <code>null</code>,
     * becomes part of the continuation. When this continuation is used, the function (presumably the same one) is called
     * on the outer record again. If the results match, the inner cursor picks up where it left off. If not, the entire
     * inner cursor is run.
     * This handles common cases of the data changing between transactions, such as the outer record being deleted (skip rest of inner record)
     * or a new record being inserted right before it (do full inner cursor, not partial based on previous).
     * @param continuation the continuation returned from a previous instance of this pipeline or <code>null</code> at start.
     * @param pipelineSize the number of outer items to work ahead; inner cursors for these will be started in parallel.
     */
    static <T, V> RecordCursor<V> flatMapPipelined(@Nonnull Function<byte[], ? extends RecordCursor<T>> outerFunc,
                                                          @Nonnull BiFunction<T, byte[], ? extends RecordCursor<V>> innerFunc,
                                                          @Nullable Function<T, byte[]> checker,
                                                          @Nullable byte[] continuation,
                                                          int pipelineSize) {
        if (continuation == null) {
            return new FlatMapPipelinedCursor<>(outerFunc.apply(null), innerFunc, checker,
                    null, null, null,
                    pipelineSize);
        }
        RecordCursorProto.FlatMapContinuation parsed;
        try {
            parsed = RecordCursorProto.FlatMapContinuation.parseFrom(continuation);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("error parsing continuation", ex)
                    .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
        }
        final byte[] outerContinuation = parsed.hasOuterContinuation() ? parsed.getOuterContinuation().toByteArray() : null;
        final byte[] innerContinuation = parsed.hasInnerContinuation() ? parsed.getInnerContinuation().toByteArray() : null;
        final byte[] checkValue = parsed.hasCheckValue() ? parsed.getCheckValue().toByteArray() : null;
        final RecordCursor<T> outerCursor = outerFunc.apply(outerContinuation);
        return new FlatMapPipelinedCursor<>(outerCursor, innerFunc, checker, outerContinuation, checkValue, innerContinuation, pipelineSize);
    }

    /**
     * Get a new cursor that skips records that do not satisfy the given asynchronous predicate.
     * @param pred a predicate to apply
     * @param pipelineSize the number of futures from applications of the predicate to start ahead of time
     * @return a new cursor that filters out records for which {@code pred} returned a future that completed to {@code false}
     */
    @Nonnull
    default RecordCursor<T> filterAsync(@Nonnull Function<T, CompletableFuture<Boolean>> pred, int pipelineSize) {
        return mapPipelined(t -> pred.apply(t).thenApply((Function<Boolean, Optional<T>>) matches -> matches != null && matches ? Optional.of(t) : Optional.empty()),
                            pipelineSize)
            .filter(Optional::isPresent)
            .map(Optional::get);
    }

    @Nonnull
    default RecordCursor<T> filterAsyncInstrumented(@Nonnull Function<T, CompletableFuture<Boolean>> pred, int pipelineSize,
                                                           @Nullable StoreTimer timer,
                                                           @Nullable StoreTimer.Count in,
                                                           @Nullable StoreTimer.Event during,
                                                           @Nullable StoreTimer.Count success,
                                                           @Nullable StoreTimer.Count failure) {
        Set<StoreTimer.Count> inSet = in != null ? Collections.singleton(in) : Collections.emptySet();
        Set<StoreTimer.Event> duringSet = during != null ? Collections.singleton(during) : Collections.emptySet();
        Set<StoreTimer.Count> successSet = success != null ? Collections.singleton(success) : Collections.emptySet();
        Set<StoreTimer.Count> failureSet = failure != null ? Collections.singleton(failure) : Collections.emptySet();

        return filterAsyncInstrumented(pred, pipelineSize, timer, inSet, duringSet, successSet, failureSet);
    }

    /**
     * Get a new cursor that skips records that do not satisfy the given asynchronous predicate, while instrumenting the
     * number of records that the filter sees, the number that it passes, the number that it fails, and the amount of
     * time that it spends computing the predicate.
     * @param pred a boolean predicate to filter on
     * @param pipelineSize the number of futures from applications of the predicate to start ahead of time
     * @param timer a StoreTimer to log the counts and events
     * @param inSet a set StoreTimer.Count that will be incremented for each record the filter sees
     * @param duringSet a set of StoreTimer.Event that will log the time spent computing the predicate
     * @param successSet a set of StoreTimer.Count that will be increment for each record on which the predicate evaluates <code>true</code>
     * @param failureSet a set of StoreTimer.Count that will be increment for each record on which the predicate evaluates <code>false</code>
     * @return a new cursor that filters out records for which {@code pred} returned a future that completed to {@code false}
     */
    @Nonnull
    @SuppressWarnings("squid:S1604") // need annotation so no lambda
    default RecordCursor<T> filterAsyncInstrumented(@Nonnull Function<T, CompletableFuture<Boolean>> pred,
                                                           int pipelineSize,
                                                           @Nullable StoreTimer timer,
                                                           @Nonnull Set<StoreTimer.Count> inSet,
                                                           @Nonnull Set<StoreTimer.Event> duringSet,
                                                           @Nonnull Set<StoreTimer.Count> successSet,
                                                           @Nonnull Set<StoreTimer.Count> failureSet) {
        if (timer == null) {
            return filterAsync(pred, pipelineSize);
        }

        Function<T, CompletableFuture<Optional<T>>> mapper = new Function<T, CompletableFuture<Optional<T>>>() {
            @Override
            @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "https://github.com/spotbugs/spotbugs/issues/552")
            public CompletableFuture<Optional<T>> apply(T t) {
                for (StoreTimer.Count in : inSet) {
                    timer.increment(in);
                }
                return timer.instrument(duringSet,
                        pred.apply(t).thenApply((Function<Boolean, Optional<T>>)matches -> {
                            if (matches != null && matches) {
                                for (StoreTimer.Count success : successSet) {
                                    timer.increment(success);
                                }
                                return Optional.of(t);
                            } else {
                                for (StoreTimer.Count failure : failureSet) {
                                    timer.increment(failure);
                                }
                                return Optional.empty();
                            }
                        }), RecordCursor.this.getExecutor());
            }
        };

        return mapPipelined(mapper, pipelineSize).filter(Optional::isPresent).map(Optional::get);
    }


    /**
     * Call the given consumer as each record becomes available.
     * @param consumer function to be applied to each record
     * @return a future that is complete when the consumer has been called on all remaining records
     */
    @Nonnull
    default CompletableFuture<Void> forEach(Consumer<T> consumer) {
        return AsyncUtil.whileTrue(() -> onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                consumer.accept(next());
            }
            return hasNext;
        }), getExecutor());
    }

    /**
     * Call the function as each record becomes available. This will be ready when
     * all of the elements of this cursor have been read and when all of the futures
     * associated with those elements have completed.
     * @param func function to be applied to each record
     * @param pipelineSize the number of futures from applications of the function to start ahead of time
     * @return a future that is complete when the function has been called and all remaining
     * records and the result has then completed
     */
    @Nonnull
    default CompletableFuture<Void> forEachAsync(@Nonnull Function<T,CompletableFuture<Void>> func, int pipelineSize) {
        return mapPipelined(func, pipelineSize).reduce(null, (v1, v2) -> null);
    }

    /**
     * Get a new cursor that substitutes another cursor if this cursor is empty.
     * @param func function to be called if the cursor is empty to give another source of records
     * @return a new cursor that returns the same records as this cursor 
     * or the result of {@code func} if this cursor does not produce any records
     */
    @Nonnull
    default RecordCursor<T> orElse(@Nonnull Function<Executor, RecordCursor<T>> func) {
        return new OrElseCursor<>(this, func);
    }

    /**
     * Get a new cursor from an ordinary <code>Iterator</code>.
     * @param iterator the iterator of records
     * @param <T> the type of elements of {@code iterator}
     * @return a new cursor that produces the elements of the given iterator
     */
    @Nonnull
    static <T> RecordCursor<T> fromIterator(@Nonnull Iterator<T> iterator) {
        return fromIterator(ForkJoinPool.commonPool(), iterator);
    }

    @Nonnull
    static <T> RecordCursor<T> fromIterator(@Nonnull Executor executor, @Nonnull Iterator<T> iterator) {
        if (iterator instanceof RecordCursor) {
            return (RecordCursor<T>)iterator;
        }
        if (iterator instanceof AsyncIterator) {
            return new AsyncIteratorCursor<>(executor, (AsyncIterator<T>)iterator);
        }
        return new IteratorCursor<>(executor, iterator);
    }

    /**
     * Get a new cursor from an ordinary <code>List</code>.
     * @param list the list of records
     * @param <T> the type of elements of {@code list}
     * @return a new cursor that produces the items of {@code list}
     */
    @Nonnull
    static <T> RecordCursor<T> fromList(@Nonnull List<T> list) {
        return fromList(ForkJoinPool.commonPool(), list);
    }

    @Nonnull
    static <T> RecordCursor<T> fromList(@Nonnull Executor executor, @Nonnull List<T> list) {
        return new ListCursor<>(executor, list, 0);
    }

    /**
     * Get a new cursor from an ordinary <code>List</code>, skipping ahead according to the given continuation.
     * @param list the list of records
     * @param continuation the result of {@link #getContinuation()} from an earlier list cursor.
     * @param <T> the type of elements of {@code list}
     * @return a new cursor that produces the items of {@code list}, resuming if {@code continuation} is not {@code null}
     */
    @Nonnull
    static <T> RecordCursor<T> fromList(@Nonnull List<T> list, @Nullable byte[] continuation) {
        return fromList(ForkJoinPool.commonPool(), list, continuation);
    }

    @Nonnull
    static <T> RecordCursor<T> fromList(@Nonnull Executor executor, @Nonnull List<T> list, @Nullable byte[] continuation) {
        int position = 0;
        if (continuation != null) {
            position = ByteBuffer.wrap(continuation).getInt();
        }
        return new ListCursor<>(executor, list, position);
    }

    /**
     * Get a new cursor that has the contents of the given future as its only record
     * The record will be available when the future is complete.
     * @param future a future that completes to the only element of the cursor
     * @param <T> the result type of the future
     * @return a new cursor producing the contents of {@code future}
     */
    @Nonnull
    static <T> RecordCursor<T> fromFuture(@Nonnull CompletableFuture<T> future) {
        return fromFuture(ForkJoinPool.commonPool(), future);
    }

    @Nonnull
    static <T> RecordCursor<T> fromFuture(@Nonnull Executor executor, @Nonnull CompletableFuture<T> future) {
        return new FutureCursor<>(executor, future);
    }

    /**
     * Get a new cursor by applying the contents of the given future
     * to the given function, with proper continuation handling.
     * @param executor an executor in which to run asynchronous calls
     * @param future a future that completes to the argument to the function
     * @param continuation any continuation from a previous invocation of this method
     * @param function a function that takes the contents of the future and a continuation and returns a cursor
     * @param <T> the type of elements of the cursor
     * @param <V> the return type of the function
     * @return a new cursor from applying {@code function} to {@code future} and {@code continuation}
     */
    static <T, V> RecordCursor<T> mapFuture(@Nonnull Executor executor, @Nonnull CompletableFuture<V> future,
                                                   @Nullable byte[] continuation,
                                                   @Nonnull BiFunction<V, byte[], ? extends RecordCursor<T>> function) {
        return flatMapPipelined(
                // Futures do not need complex continuations
                outerContinuation -> RecordCursor.fromFuture(executor, future),
                function, null, continuation, 1);
    }

    /**
     * Get a new cursor that does not return any records.
     * @param <T> the type of elements of the cursor
     * @return a new empty cursor
     */
    @Nonnull
    static <T> RecordCursor<T> empty() {
        return empty(ForkJoinPool.commonPool());
    }

    @Nonnull
    static <T> RecordCursor<T> empty(@Nonnull Executor executor) {
        return new EmptyCursor<>(executor);
    }

    /**
     * Reduce contents of cursor to single value.
     * @param identity initial value for reduction
     * @param accumulator function that takes previous reduced value and computes new value by combining with each record
     * @param <U> the result type of the reduction
     * @return a future that completes to the result of reduction
     */
    @Nullable
    default <U> CompletableFuture<U> reduce(U identity, BiFunction<U, ? super T, U> accumulator) {
        MoreAsyncUtil.Holder<U> holder = new MoreAsyncUtil.Holder<>(identity);
        return AsyncUtil.whileTrue(() -> onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                holder.value = accumulator.apply(holder.value, next());
            }
            return hasNext;
        }), getExecutor()).thenApply(vignore -> holder.value);
    }

}
