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
        that(mustBeTrue, messageIfNotTrue, ErrorCode.INTERNAL_ERROR);
    }

    public static void that(boolean mustBeTrue, @Nonnull final String messageIfNotTrue, @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
        if (!mustBeTrue) {
            throw new RelationalException(messageIfNotTrue, errorCodeIfNotTrue);
        }
    }

    public static <T> T notNull(T object) throws RelationalException {
        return notNull(object, "unexpected null object");
    }

    public static <T> T notNull(T object, @Nonnull final String messageIfNull) throws RelationalException {
        return notNull(object, messageIfNull, ErrorCode.INTERNAL_ERROR);
    }

    public static <T> T notNull(T object, @Nonnull final String messageIfNull, @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
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
        isNull(object, messageIfNull, ErrorCode.INTERNAL_ERROR);
    }

    public static void isNull(Object object, @Nonnull final String messageIfNull, @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
        if (object != null) {
            throw new RelationalException(messageIfNull, errorCodeIfNotTrue);
        }
    }

    public static RelationalException fail() throws RelationalException {
        throw fail("unexpected error");
    }

    public static RelationalException fail(@Nonnull final String failMessage) throws RelationalException {
        throw fail(failMessage, ErrorCode.INTERNAL_ERROR);
    }

    public static RelationalException fail(@Nonnull final String failMessage, @Nonnull final ErrorCode failErrorCode) throws RelationalException {
        throw new RelationalException(failMessage, failErrorCode);
    }

    public static void thatUnchecked(boolean mustBeTrue) {
        thatUnchecked(mustBeTrue, "condition is not met!");
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final String messageIfNotTrue) {
        thatUnchecked(mustBeTrue, messageIfNotTrue, ErrorCode.INTERNAL_ERROR);
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final String messageIfNotTrue, @Nonnull final ErrorCode errorCodeIfNotTrue) {
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

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final ErrorCode errorCodeIfNotTrue, @Nonnull final Supplier<String> messageSupplier) {
        if (!mustBeTrue) {
            throw new RelationalException(messageSupplier.get(), errorCodeIfNotTrue).toUncheckedWrappedException();
        }
    }

    public static <T> T notNullUnchecked(T object) {
        return notNullUnchecked(object, "unexpected null object");
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final String messageIfNull) {
        return notNullUnchecked(object, messageIfNull, ErrorCode.INTERNAL_ERROR);
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final String messageIfNull, @Nonnull final ErrorCode errorCodeIfNull) {
        if (object == null) {
            throw new RelationalException(messageIfNull, errorCodeIfNull).toUncheckedWrappedException();
        } else {
            return object;
        }
    }

    public static void isNullUnchecked(Object object) {
        isNullUnchecked(object, "expected object to be null");
    }

    public static void isNullUnchecked(Object object, @Nonnull final String messageIfNotNull) {
        isNullUnchecked(object, messageIfNotNull, ErrorCode.INTERNAL_ERROR);
    }

    public static void isNullUnchecked(Object object, @Nonnull final String messageIfNotNull, @Nonnull final ErrorCode errorCodeIfNotNull) {
        if (object != null) {
            throw new RelationalException(messageIfNotNull, errorCodeIfNotNull).toUncheckedWrappedException();
        }
    }

    public static UncheckedRelationalException failUnchecked() {
        throw failUnchecked("unexpected error");
    }

    public static UncheckedRelationalException failUnchecked(@Nonnull final String failMessage) {
        throw failUnchecked(failMessage, ErrorCode.INTERNAL_ERROR);
    }

    public static UncheckedRelationalException failUnchecked(@Nonnull final String failMessage, @Nonnull final ErrorCode failErrorCode) {
        throw new RelationalException(failMessage, failErrorCode).toUncheckedWrappedException();
    }

    private Assert() {
    }
}
