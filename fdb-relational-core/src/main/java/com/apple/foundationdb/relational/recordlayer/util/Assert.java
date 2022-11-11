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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;

/**
 * A set of helper methods for validating input, pre-conditions, ... etc.
 * TODO: Remove. Use com.apple.foundationdb.relational.util.Assert.that instead. This class
 * delegates to it (com.apple.foundationdb.relational.util.Assert is a copy of this class.
 * For now we just do a delegation to com.apple.foundationdb.relational.util.Assert. In
 * a follow-up PR, we'll have all references to this class point to
 * com.apple.foundationdb.relational.util.Assert after fdb-relational-api gets committed).
 */
@ExcludeFromJacocoGeneratedReport //just assertions, hard to test in a useful way
public final class Assert {

    public static void that(boolean mustBeTrue) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.that(mustBeTrue);
    }

    public static void that(boolean mustBeTrue, @Nonnull final String messageIfNotTrue) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.that(mustBeTrue, messageIfNotTrue);
    }

    public static void that(boolean mustBeTrue, @Nonnull final String messageIfNotTrue,
                            @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.that(mustBeTrue, messageIfNotTrue, errorCodeIfNotTrue);
    }

    public static <T> T notNull(T object) throws RelationalException {
        return com.apple.foundationdb.relational.util.Assert.notNull(object);
    }

    public static <T> T notNull(T object, @Nonnull final String messageIfNull) throws RelationalException {
        return com.apple.foundationdb.relational.util.Assert.notNull(object, messageIfNull);
    }

    public static <T> T notNull(T object, @Nonnull final String messageIfNull,
                                @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
        return com.apple.foundationdb.relational.util.Assert.notNull(object, messageIfNull, errorCodeIfNotTrue);
    }

    public static void isNull(Object object) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.isNull(object);
    }

    public static void isNull(Object object, @Nonnull final String messageIfNull) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.isNull(object, messageIfNull);
    }

    public static void isNull(Object object, @Nonnull final String messageIfNull, @Nonnull final ErrorCode errorCodeIfNotTrue) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.isNull(object, messageIfNull, errorCodeIfNotTrue);
    }

    public static void fail() throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.fail();
    }

    public static void fail(@Nonnull final String failMessage) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.fail(failMessage);
    }

    public static void fail(@Nonnull final String failMessage, @Nonnull final ErrorCode failErrorCode) throws RelationalException {
        com.apple.foundationdb.relational.util.Assert.fail(failMessage, failErrorCode);
    }

    public static void thatUnchecked(boolean mustBeTrue) {
        com.apple.foundationdb.relational.util.Assert.thatUnchecked(mustBeTrue);
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final String messageIfNotTrue) {
        com.apple.foundationdb.relational.util.Assert.thatUnchecked(mustBeTrue, messageIfNotTrue);
    }

    public static void thatUnchecked(boolean mustBeTrue, @Nonnull final String messageIfNotTrue,
                                     @Nonnull final ErrorCode errorCodeIfNotTrue) {
        com.apple.foundationdb.relational.util.Assert.thatUnchecked(mustBeTrue, messageIfNotTrue, errorCodeIfNotTrue);
    }

    public static <T> T notNullUnchecked(T object) {
        return com.apple.foundationdb.relational.util.Assert.notNullUnchecked(object);
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final String messageIfNull) {
        return com.apple.foundationdb.relational.util.Assert.notNullUnchecked(object, messageIfNull);
    }

    public static <T> T notNullUnchecked(T object, @Nonnull final String messageIfNull,
                                         @Nonnull final ErrorCode errorCodeIfNull) {
        return com.apple.foundationdb.relational.util.Assert.notNullUnchecked(object, messageIfNull, errorCodeIfNull);
    }

    public static void isNullUnchecked(Object object) {
        com.apple.foundationdb.relational.util.Assert.isNullUnchecked(object);
    }

    public static void isNullUnchecked(Object object, @Nonnull final String messageIfNotNull) {
        com.apple.foundationdb.relational.util.Assert.isNullUnchecked(object, messageIfNotNull);
    }

    public static void isNullUnchecked(Object object, @Nonnull final String messageIfNotNull,
                                       @Nonnull final ErrorCode errorCodeIfNotNull) {
        com.apple.foundationdb.relational.util.Assert.isNullUnchecked(object, messageIfNotNull, errorCodeIfNotNull);
    }

    public static void failUnchecked() {
        com.apple.foundationdb.relational.util.Assert.failUnchecked();
    }

    public static void failUnchecked(@Nonnull final String failMessage) {
        com.apple.foundationdb.relational.util.Assert.failUnchecked(failMessage);
    }

    public static void failUnchecked(@Nonnull final String failMessage, @Nonnull final ErrorCode failErrorCode) {
        com.apple.foundationdb.relational.util.Assert.failUnchecked(failMessage, failErrorCode);
    }

    private Assert() {
    }
}
