/*
 * Result.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A struct representing the result of an operation, containing either the return value or an error.
 * This type behaves like an "either" type. It should never be the case that both {@link #getValue()} and
 * {@link #getError()} return {@code null}. Either the result was an error, in which case the value will be
 * {@code null} and the error will be non-{@code null}, or the result was a success, in which case the error will
 * be {@code null} and the value will be non-{@code null} <em>unless</em> the underlying operation returned
 * {@code null}. Note that because the value can be {@code null} in the success case, it is advised that the user
 * should either check the {@link #getError() error} value or call {@link #isSuccess()} instead of checking the value.
 *
 * @param <V> the type of a successful result
 * @param <E> the type of error from a failed result
 */
@API(API.Status.UNSTABLE)
public final class Result<V, E extends Throwable> {
    @Nullable
    private final V value;
    @Nullable
    private final E error;

    private Result(@Nullable V value, @Nullable E error) {
        this.value = value;
        this.error = error;
    }

    /**
     * The value returned by the operation, if successful. If the
     * operation was unsuccessful, this will return {@code null}. Note that
     * it may also be {@code null} if the operation happened to return {@code null}.
     *
     * @return the value returned by the operation if successful or {@code null} otherwise
     */
    @Nullable
    public V getValue() {
        return value;
    }

    /**
     * The error encountered by the operation, if not successful. If the
     * operation was successful, this will return {@code null}.
     *
     * @return the error encountered by the operation if unsuccessful or {@code null} otherwise
     */
    @Nullable
    public E getError() {
        return error;
    }

    /**
     * Whether the underlying result was successful or not. If it was, then
     * {@link #getError()} should be {@code null}, and {@link #getValue()} will
     * be {@code null} if and only if the underlying operation returned {@code null}.
     * If it was not, then {@link #getError()} will be non-{@code null} and
     * {@link #getValue()} will be {@code null}.
     *
     * @return whether the underlying result was a success
     */
    public boolean isSuccess() {
        return error == null;
    }

    /**
     * Create a successful result wrapping a value.
     *
     * @param value the value of the operation
     * @param <V> the result's value type
     * @param <E> the result's error type
     * @return a new successful result wrapping the value
     */
    @Nonnull
    public static <V, E extends Throwable> Result<V, E> success(@Nullable V value) {
        return new Result<>(value, null);
    }

    /**
     * Create an unsuccessful result wrapping an error.
     *
     * @param error the non-null error encountered by the operation
     * @param <V> the result's value type
     * @param <E> the result's error type
     * @return a new unsuccessful result wrapping the error
     */
    @Nonnull
    public static <V, E extends Throwable> Result<V, E> failure(@Nonnull E error) {
        return new Result<>(null, Objects.requireNonNull(error));
    }

    /**
     * Create a new result wrapping a value and an error. Note that it cannot
     * be the case that both parameters are non-{@code null}. That case
     * results in an {@link RecordCoreArgumentException}.
     *
     * @param value the value to associate with the result or {@code null} if unsuccessful
     * @param error the error to associate with the result or {@code null} if successful
     * @param <V> the type of value
     * @param <E> the type of error
     * @return a new result wrapping the value and error
     */
    @Nonnull
    public static <V, E extends Throwable> Result<V, E> of(@Nullable V value, @Nullable E error) {
        if (value != null && error != null) {
            throw new RecordCoreArgumentException("Failure result can not have value");
        }
        return new Result<>(value, error);
    }
}
