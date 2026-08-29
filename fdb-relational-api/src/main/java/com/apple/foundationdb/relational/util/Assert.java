/*
 * Assert.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;

import org.jspecify.annotations.Nullable;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A set of helper methods for validating input, pre-conditions, ... etc.
 */
@API(API.Status.EXPERIMENTAL)
public final class Assert {

    public static void that(boolean mustBeTrue) throws RelationalException {
        that(mustBeTrue, "condition is not met!");
    }

    public static void that(boolean mustBeTrue, final String messageIfNotTrue) throws RelationalException {
        that(mustBeTrue, ErrorCode.INTERNAL_ERROR, messageIfNotTrue);
    }

    public static void that(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final Supplier<String> messageSupplier) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageIfNotTrue) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(messageIfNotTrue, errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageFormat, Object messageValue) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(Locale.ROOT, messageFormat, messageValue), errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageFormat, Object messageValue1, Object messageValue2) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(Locale.ROOT, messageFormat, messageValue1, messageValue2), errorCodeIfNotTrue);
        }
    }

    public static <T> T notNull(@Nullable T object) throws RelationalException {
        return notNull(object, "unexpected null object");
    }

    public static <T> T notNull(@Nullable T object, final String messageIfNull) throws RelationalException {
        return notNull(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static <T> T notNull(@Nullable T object, final ErrorCode errorCodeIfNotTrue, final String messageIfNull) throws RelationalException {
        if (object == null) {
            throw new RelationalException(messageIfNull, errorCodeIfNotTrue);
        } else {
            return object;
        }
    }

    public static void isNull(@Nullable Object object) throws RelationalException {
        isNull(object, "expected object to be null");
    }

    public static void isNull(@Nullable Object object, final String messageIfNull) throws RelationalException {
        isNull(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static void isNull(@Nullable Object object, final ErrorCode errorCodeIfNotTrue, final String messageIfNull) throws RelationalException {
        if (object != null) {
            throw new RelationalException(messageIfNull, errorCodeIfNotTrue);
        }
    }

    public static RelationalException fail() throws RelationalException {
        throw fail("unexpected error");
    }

    public static RelationalException fail(final String failMessage) throws RelationalException {
        throw fail(ErrorCode.INTERNAL_ERROR, failMessage);
    }

    public static RelationalException fail(final ErrorCode failErrorCode, final String failMessage) throws RelationalException {
        throw new RelationalException(failMessage, failErrorCode);
    }

    public static void thatUnchecked(boolean mustBeTrue) {
        thatUnchecked(mustBeTrue, "condition is not met!");
    }

    public static void thatUnchecked(boolean mustBeTrue, final String messageIfNotTrue) {
        thatUnchecked(mustBeTrue, ErrorCode.INTERNAL_ERROR, messageIfNotTrue);
    }

    public static void thatUnchecked(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final Supplier<String> messageSupplier) {
        if (!mustBeTrue) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageIfNotTrue) {
        if (!mustBeTrue) {
            throw new RelationalException(messageIfNotTrue, errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageTemplate, final Object messageValue) {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(Locale.ROOT, messageTemplate, messageValue), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, final ErrorCode errorCodeIfNotTrue, final String messageTemplate, final Object messageValue1, final Object messageValue2) {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(Locale.ROOT, messageTemplate, messageValue1, messageValue2), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static <T> T notNullUnchecked(@Nullable T object) {
        return notNullUnchecked(object, "unexpected null object");
    }

    public static <T> T notNullUnchecked(@Nullable T object, final String messageIfNull) {
        return notNullUnchecked(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static <T> T notNullUnchecked(@Nullable T object, final ErrorCode errorCodeIfNull, Supplier<String> messageSupplier) {
        if (object == null) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static <T> T notNullUnchecked(@Nullable T object, final ErrorCode errorCodeIfNull, final String messageIfNull) {
        if (object == null) {
            throw new RelationalException(messageIfNull, errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static <T> T notNullUnchecked(@Nullable T object, final ErrorCode errorCodeIfNull, final String messageTemplate, final Object messageValue) {
        if (object == null) {
            throw new RelationalException(String.format(Locale.ROOT, messageTemplate, messageValue), errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static void isNullUnchecked(@Nullable Object object) {
        isNullUnchecked(object, "expected object to be null");
    }

    public static void isNullUnchecked(@Nullable Object object, final String messageIfNotNull) {
        isNullUnchecked(object, ErrorCode.INTERNAL_ERROR, messageIfNotNull);
    }

    public static void isNullUnchecked(@Nullable Object object, final ErrorCode errorCodeIfNotNull, final Supplier<String> messageSupplier) {
        if (object != null) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotNull).toUncheckedWrappedException();
        }
    }

    public static void isNullUnchecked(@Nullable Object object, final ErrorCode errorCodeIfNotNull, final String messageIfNotNull) {
        if (object != null) {
            throw new RelationalException(messageIfNotNull, errorCodeIfNotNull).toUncheckedWrappedException();
        }
    }

    public static UncheckedRelationalException failUnchecked() {
        throw failUnchecked("unexpected error");
    }

    public static UncheckedRelationalException failUnchecked(final String failMessage) {
        throw failUnchecked(ErrorCode.INTERNAL_ERROR, failMessage);
    }

    public static UncheckedRelationalException failUnchecked(final ErrorCode failErrorCode, final String failMessage) {
        throw new RelationalException(failMessage, failErrorCode).toUncheckedWrappedException();
    }

    public static UncheckedRelationalException failUnchecked(final ErrorCode failErrorCode, final String failMessage,
                                                             final Throwable cause) {
        throw new RelationalException(failMessage, failErrorCode, cause).toUncheckedWrappedException();
    }

    public static <S, T> S castUnchecked(@Nullable T object, Class<S> clazz) {
        return castUnchecked(object, clazz, ErrorCode.INTERNAL_ERROR, () -> "expected " + clazz.getSimpleName() +
                " but got " + (object == null ? "null" : object.getClass().getSimpleName()));
    }

    public static <S, T> S castUnchecked(@Nullable T object, Class<S> clazz, final ErrorCode errorCodeIfCastFailed,
                                         final Supplier<String> messageSupplier) {
        final var notNullObject = notNullUnchecked(object, errorCodeIfCastFailed, messageSupplier);
        if (clazz.isInstance(notNullObject)) {
            return clazz.cast(notNullObject);
        }
        throw failUnchecked(errorCodeIfCastFailed, messageSupplier.get());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <V> V optionalUnchecked(final Optional<V> optional) {
        return optionalUnchecked(optional, ErrorCode.INTERNAL_ERROR,  () -> "expected non-empty Optional");
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> T optionalUnchecked(final Optional<T> optional, final ErrorCode errorCodeIfOptionalEmpty,
                                          final Supplier<String> messageSupplier) {
        if (optional.isPresent()) {
            return optional.get();
        }
        throw failUnchecked(errorCodeIfOptionalEmpty, messageSupplier.get());
    }

    private Assert() {
    }
}
