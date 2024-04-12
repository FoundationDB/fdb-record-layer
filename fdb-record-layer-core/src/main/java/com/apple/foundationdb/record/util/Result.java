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
 * todo.
 *
 * @param <V> the type of a successful result
 * @param <E> the type of error from a failed result
 */
@API(API.Status.MAINTAINED)
public final class Result<V, E extends Throwable> {
    @Nullable
    private final V value;
    @Nullable
    private final E error;

    private Result(@Nullable V value, @Nullable E error) {
        this.value = value;
        this.error = error;
    }

    @Nullable
    public V getValue() {
        return value;
    }

    @Nullable
    public E getError() {
        return error;
    }

    public boolean isSuccess() {
        return error != null;
    }

    @Nonnull
    public static <V, E extends Throwable> Result<V, E> success(@Nullable V value) {
        return new Result<>(value, null);
    }

    @Nonnull
    public static <V, E extends Throwable> Result<V, E> failure(@Nonnull E error) {
        return new Result<>(null, Objects.requireNonNull(error));
    }

    @Nonnull
    public static <V, E extends Throwable> Result<V, E> of(@Nullable V value, @Nullable E error) {
        if (value != null && error != null) {
            throw new RecordCoreArgumentException("Failure result can not have value");
        }
        return new Result<>(value, error);
    }
}
