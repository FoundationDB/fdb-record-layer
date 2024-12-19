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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A set of helper methods for validating input, pre-conditions, ... etc.
 */
@ExcludeFromJacocoGeneratedReport //just assertions, hard to test in a useful way
public final class Assert {

    public static void that(boolean mustBeTrue) throws RelationalException {
        that(mustBeTrue, "condition is not met!");
    }

    public static void that(boolean mustBeTrue, @Nonnull final String messageIfNotTrue) throws RelationalException {
        that(mustBeTrue, ErrorCode.INTERNAL_ERROR, messageIfNotTrue);
    }

    public static void that(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final Supplier<String> messageSupplier) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageIfNotTrue) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(messageIfNotTrue, errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageFormat, @Nonnull Object messageValue) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(messageFormat, messageValue), errorCodeIfNotTrue);
        }
    }

    public static void that(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageFormat, @Nonnull Object messageValue1, @Nonnull Object messageValue2) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(messageFormat, messageValue1, messageValue2), errorCodeIfNotTrue);
        }
    }

    public static <T> T notNull(T object) throws RelationalException {
        return notNull(object, "unexpected null object");
    }

    public static <T> T notNull(T object, @Nonnull final String messageIfNull) throws RelationalException {
        return notNull(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static <T> T notNull(T object, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageIfNull) throws RelationalException {
        if (object == null) {
            throw new RelationalException(messageIfNull, errorCodeIfNotTrue);
        } else {
            return object;
        }
    }

    public static void isNull(Object object) throws RelationalException {
        isNull(object, "expected object to be null");
    }

    public static void isNull(Object object, @Nonnull final String messageIfNull) throws RelationalException {
        isNull(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static void isNull(Object object, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageIfNull) throws RelationalException {
        if (object != null) {
            throw new RelationalException(messageIfNull, errorCodeIfNotTrue);
        }
    }

    public static RelationalException fail() throws RelationalException {
        throw fail("unexpected error");
    }

    public static RelationalException fail(@Nonnull final String failMessage) throws RelationalException {
        throw fail(ErrorCode.INTERNAL_ERROR, failMessage);
    }

    public static RelationalException fail(@Nonnull final ErrorCode failErrorCode, @Nonnull final String failMessage) throws RelationalException {
        throw new RelationalException(failMessage, failErrorCode);
    }

    public static void thatUnchecked(boolean mustBeTrue) {
        thatUnchecked(mustBeTrue, "condition is not met!");
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final String messageIfNotTrue) {
        thatUnchecked(mustBeTrue, ErrorCode.INTERNAL_ERROR, messageIfNotTrue);
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final Supplier<String> messageSupplier) {
        if (!mustBeTrue) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageIfNotTrue) {
        if (!mustBeTrue) {
            throw new RelationalException(messageIfNotTrue, errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageTemplate, @Nonnull final Object messageValue) {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(messageTemplate, messageValue), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final String messageTemplate, @Nonnull final Object messageValue1, @Nonnull final Object messageValue2) {
        if (!mustBeTrue) {
            throw new RelationalException(String.format(messageTemplate, messageValue1, messageValue2), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static <T> T notNullUnchecked(T object) {
        return notNullUnchecked(object, "unexpected null object");
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final String messageIfNull) {
        return notNullUnchecked(object, ErrorCode.INTERNAL_ERROR, messageIfNull);
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final ErrorCode errorCodeIfNull, @Nonnull Supplier<String> messageSupplier) {
        if (object == null) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final ErrorCode errorCodeIfNull, @Nonnull final String messageIfNull) {
        if (object == null) {
            throw new RelationalException(messageIfNull, errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final ErrorCode errorCodeIfNull, @Nonnull final String messageTemplate, @Nonnull final Object messageValue) {
        if (object == null) {
            throw new RelationalException(String.format(messageTemplate, messageValue), errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static void isNullUnchecked(Object object) {
        isNullUnchecked(object, "expected object to be null");
    }

    public static void isNullUnchecked(Object object, @Nonnull final String messageIfNotNull) {
        isNullUnchecked(object, ErrorCode.INTERNAL_ERROR, messageIfNotNull);
    }

    public static void isNullUnchecked(Object object, @Nonnull final ErrorCode errorCodeIfNotNull, @Nonnull final Supplier<String> messageSupplier) {
        if (object != null) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotNull).toUncheckedWrappedException();
        }
    }

    public static void isNullUnchecked(Object object, @Nonnull final ErrorCode errorCodeIfNotNull, @Nonnull final String messageIfNotNull) {
        if (object != null) {
            throw new RelationalException(messageIfNotNull, errorCodeIfNotNull).toUncheckedWrappedException();
        }
    }

    public static UncheckedRelationalException failUnchecked() {
        throw failUnchecked("unexpected error");
    }

    public static UncheckedRelationalException failUnchecked(@Nonnull final String failMessage) {
        throw failUnchecked(ErrorCode.INTERNAL_ERROR, failMessage);
    }

    public static UncheckedRelationalException failUnchecked(@Nonnull final ErrorCode failErrorCode, @Nonnull final String failMessage) {
        throw new RelationalException(failMessage, failErrorCode).toUncheckedWrappedException();
    }

    @Nonnull
    public static <S, T> S castUnchecked(T object, Class<S> clazz) {
        return castUnchecked(object, clazz, ErrorCode.INTERNAL_ERROR, () -> "expected " + clazz.getSimpleName() +
                " but got " + (object == null ? "null" : object.getClass().getSimpleName()));
    }

    @Nonnull
    public static <S, T> S castUnchecked(T object, Class<S> clazz, @Nonnull final ErrorCode errorCodeIfCastFailed,
                                         @Nonnull final Supplier<String> messageSupplier) {
        final var notNullObject = notNullUnchecked(object, errorCodeIfCastFailed, messageSupplier);
        if (clazz.isInstance(notNullObject)) {
            return clazz.cast(object);
        }
        failUnchecked(errorCodeIfCastFailed, messageSupplier.get());
        return null;
    }

    private Assert() {
    }
}
