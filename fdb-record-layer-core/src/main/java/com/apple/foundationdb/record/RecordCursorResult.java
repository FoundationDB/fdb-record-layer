/*
 * RecordCursorResult.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A result obtained when a {@link RecordCursor} advances.
 *
 * A {@code RecordCursorResult} represents everything that one can learn each time a {@code RecordCursor} advances.
 * This is precisely one of the following:
 * <ol>
 *     <li>
 *         The next object of type {@code T} produced by the cursor. In addition to the next record, this result
 *         includes a {@link RecordCursorContinuation} that can be used to continue the cursor after the last record
 *         returned. The returned continuation is guaranteed not to be an "end continuation" representing the end of
 *         the cursor; specifically, {@link RecordCursorContinuation#isEnd()} is always {@code false} on the returned
 *         continuation.
 *     </li>
 *     <li>
 *         The fact that the cursor is stopped and cannot produce another record and a
 *         {@link com.apple.foundationdb.record.RecordCursor.NoNextReason} that explains why no record could be produced.
 *         The result includes a continuation that can be used to continue the cursor after the last record returned.
 *
 *         If the result's {@code NoNextReason} is anything other than {@code SOURCE_EXHAUSTED}, the returned
 *         continuation must not be an end continuation. Conversely, if the result's {@code NoNextReason} is
 *         {@code SOURCE_EXHAUSTED}, then the returned continuation must be an an "end continuation".
 *     </li>
 * </ol>
 *
 * <p>
 * The implementation of {@code RecordCursorResult} guarantees the dichotomy described above using an API that encourages
 * static correctness (for example, the {@link #withNextValue} and {@link #withoutNextValue} builder methods) and a
 * large number of correctness checks that encourage fast failures instead of subtle contract violations.
 * </p>
 *
 * @param <T> the type of result produced when the result includes a value
 */
@API(API.Status.UNSTABLE)
public class RecordCursorResult<T> {

    @Nonnull
    private static final RecordCursorResult<Object> EXHAUSTED = new RecordCursorResult<>(RecordCursorEndContinuation.END,
            RecordCursor.NoNextReason.SOURCE_EXHAUSTED);

    private final boolean hasNext;
    @Nullable
    private final T nextValue;
    @Nonnull
    private final RecordCursorContinuation continuation;
    @Nullable
    private final RecordCursor.NoNextReason noNextReason;

    private RecordCursorResult(@Nullable final T nextValue, @Nonnull final RecordCursorContinuation continuation) {
        this.hasNext = true;
        this.nextValue = nextValue;
        this.continuation = continuation;
        this.noNextReason = null;
    }

    private RecordCursorResult(@Nonnull final RecordCursorContinuation continuation, @Nonnull final RecordCursor.NoNextReason noNextReason) {
        this.hasNext = false;
        this.nextValue = null;
        this.continuation = continuation;
        this.noNextReason = noNextReason;
    }

    /**
     * Return whether or not this result includes a next value.
     * @return {@code true} if this result includes a next value and {@code false} if it does not
     */
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * Return the value of this result. If no result is present, throw an exception.
     * @return the value of this result
     * @throws IllegalResultValueAccessException if this result does not contain a value
     */
    @Nullable
    public T get() {
        if (!hasNext()) {
            throw new IllegalResultValueAccessException(continuation, noNextReason);
        }
        return nextValue;
    }

    /**
     * Return the continuation of this result. The continuation will have {@link RecordCursorContinuation#isEnd()}
     * return {@code true} only if this result has a no-next-reason and that reason is {@code SOURCE_EXHAUSTED}.
     * @return the continuation of this result
     */
    @Nonnull
    public RecordCursorContinuation getContinuation() {
        return continuation;
    }

    /**
     * Return the no-next-reason of this result. If a result is present, throw an exception.
     * @return the no-next-reason of this result
     * @throws IllegalResultNoNextReasonAccessException if this result contains a value
     */
    @Nonnull
    public RecordCursor.NoNextReason getNoNextReason() {
        if (hasNext()) {
            throw new IllegalResultNoNextReasonAccessException(nextValue, continuation);
        }
        return noNextReason;
    }

    @Override
    public String toString() {
        if (hasNext()) {
            return "Result(value=" + nextValue + ", cont=" + continuation + ")";
        } else {
            return "Result(cont=" + continuation + ", reason=" + noNextReason + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordCursorResult<?> that = (RecordCursorResult<?>)o;
        return hasNext == that.hasNext &&
               Objects.equals(nextValue, that.nextValue) &&
               Objects.equals(continuation, that.continuation) &&
               noNextReason == that.noNextReason;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasNext, nextValue, continuation, noNextReason);
    }

    /**
     * Apply a function to the value inside this result, like {@link java.util.Optional#map(Function)}.
     * If a value is present, apply the given function to the value and return a new result with that value.
     * If no value is present in this result, simply return a value-less result of the correct type.
     * @param func the function to apply to the value, if present
     * @param <U> the type of the function's result and the type of value in the returned result
     * @return a new result with a value equal to the value of the function on the current result, if one is present
     */
    @Nonnull
    @SuppressWarnings("unchecked") // allows us to reuse this object if we don't have a value
    public <U> RecordCursorResult<U> map(Function<? super T, ? extends U>  func) {
        if (hasNext()) {
            return withNextValue(func.apply(get()), getContinuation());
        } else {
            return (RecordCursorResult<U>) this;
        }
    }

    /**
     * Apply an asynchronous function inside this result and return a future for the result containing the future's value.
     * If a value is present, apply the given function to the value and return a future that, when complete, contains a
     * result wrapping the value of the function's completed future.
     * If no value is present, return a completed future containing a result with this result's continuation and
     * no-next-reason.
     * @param func a function taking a value of type {@code T} and returning a {@code CompletableFuture<U>}
     * @param <U> the type of the value for the returned result
     * @return a future that, when complete, contains the result of type {@code U}
     */
    @Nonnull
    @SuppressWarnings("unchecked") // allows us to reuse this object if we don't have a value
    public <U> CompletableFuture<RecordCursorResult<U>> mapAsync(Function<? super T, ? extends CompletableFuture<? extends  U>> func) {
        if (hasNext()) {
            return func.apply(nextValue).thenApply(mappedValue -> withNextValue(mappedValue, continuation));
        } else {
            return CompletableFuture.completedFuture((RecordCursorResult<U>) this);
        }
    }

    /**
     * Return a new result with the given continuation in place of this result's continuation, but the same value or
     * no-next-reason as this result.
     * @param newContinuation the new continuation for the result
     * @return a new result with the same value or no-next-reason, but the given continuation
     */
    @Nonnull
    public RecordCursorResult<T> withContinuation(@Nonnull RecordCursorContinuation newContinuation) {
        if (hasNext()) {
            return RecordCursorResult.withNextValue(nextValue, newContinuation);
        } else {
            return RecordCursorResult.withoutNextValue(newContinuation, noNextReason);
        }
    }

    /**
     * Returns {@code true} if the cursor has reached its end but a continuation is not an end continuation (i.e., the source is not yet exhausted).
     * @return {@code true} if the cursor has reached its end but a continuation is not an end continuation and {@code false} otherwise
     */
    public boolean hasStoppedBeforeEnd() {
        return !hasNext && !continuation.isEnd();
    }

    /**
     * Create a new {@code RecordCursorResult} that has a value, using the given value and continuation.
     * @param nextValue the value of the result
     * @param continuation the continuation of the result
     * @param <T> the type of the value
     * @return a new {@code RecordCursorResult} with the given value and continuation
     * @throws RecordCoreException if the given continuation is an end continuation
     */
    @Nonnull
    public static <T> RecordCursorResult<T> withNextValue(@Nullable final T nextValue, @Nonnull final RecordCursorContinuation continuation) {
        if (continuation.isEnd()) {
            throw new RecordCoreException("cannot return end continuation with next value");
        }
        return new RecordCursorResult<>(nextValue, continuation);
    }

    /**
     * Create a new {@code RecordCursorResult} that does not have a next value, using the given continuation and no-next-reason.
     * The continuation may be an end continuation if and only if the no-next-reason is {@code SOURCE_EXHAUSTED}.
     * @param continuation the continuation of the result
     * @param noNextReason the {@link com.apple.foundationdb.record.RecordCursor.NoNextReason} that no value was present
     * @param <T> the type of the value if it were present
     * @return a new {@code RecordCursorResult} with the given continuation and no-next-reason
     * @throws RecordCoreException if an incompatible continuation and no-next-reason are provided
     */
    @Nonnull
    public static <T> RecordCursorResult<T> withoutNextValue(@Nonnull final RecordCursorContinuation continuation, @Nonnull final RecordCursor.NoNextReason noNextReason) {
        if (continuation.isEnd() && !noNextReason.isSourceExhausted()) {
            throw new RecordCoreException("attempted to return a result with an end continuation and NoNextReason other than SOURCE_EXHAUSTED");
        }
        if (noNextReason.isSourceExhausted() && !continuation.isEnd()) {
            throw new RecordCoreException("attempted to return a result with NoNextReason of SOURCE_EXHAUSTED but a non-end continuation");
        }
        if (noNextReason.isSourceExhausted()) {
            return exhausted();
        }
        return new RecordCursorResult<>(continuation, noNextReason);
    }

    /**
     * Cast a {@code RecordCursorResult} to one with a new type from a result without a next value.
     * @param withoutNext a result without a next value
     * @param <T> the type of value that would be included in the desired result if one were present
     * @param <U> the type of value that would be included in the given result if one were present
     * @return a cast version of {@code withoutNext} with the same continuation and no-next-reason
     */
    @Nonnull
    @SuppressWarnings("unchecked") // allows us to reuse this object if we don't have a value
    public static <T, U> RecordCursorResult<T> withoutNextValue(@Nonnull RecordCursorResult<U> withoutNext) {
        if (withoutNext.hasNext()) {
            throw new RecordCoreException("tried to build record cursor result without next from a result with next");
        }
        return (RecordCursorResult<T>) withoutNext;
    }

    /**
     * Obtain the static result that a cursor can return when it is completely exhausted.
     * @param <T> the type of value that would be returned if a value were present
     * @return a {@code RecordCursorResult} containing an end continuation and a no-next-reason of {@code SOURCE_EXHAUSTED}
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T> RecordCursorResult<T> exhausted() {
        return (RecordCursorResult<T>) EXHAUSTED;
    }

    /**
     * An exception thrown when {@link #get()} is called on a result without a next value.
     */
    public static final class IllegalResultValueAccessException extends RecordCoreException {
        private static final long serialVersionUID = 1;

        public IllegalResultValueAccessException(@Nonnull RecordCursorContinuation continuation, @Nonnull RecordCursor.NoNextReason noNextReason) {
            super("Tried to call get() on a RecordCoreResult that did not have a next value.");
            addLogInfo("continuation", continuation);
            addLogInfo("noNextReason", noNextReason);
        }
    }

    /**
     * An exception thrown when {@link #getNoNextReason()} is called on a result that has a next value.
     */
    public static final class IllegalResultNoNextReasonAccessException extends RecordCoreException {
        private static final long serialVersionUID = 1;

        public IllegalResultNoNextReasonAccessException(@Nullable Object value, @Nonnull RecordCursorContinuation continuation) {
            super("Tried to call noNextReason() on a RecordCoreResult that had a next value.");
            addLogInfo("value", value);
            addLogInfo("continuation", continuation);
        }
    }
}
