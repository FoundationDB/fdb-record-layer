/*
 * TypeContract.java
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

package com.apple.foundationdb.relational.api.options;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.function.Function;

@API(API.Status.EXPERIMENTAL)
public class TypeContract<T> implements OptionContract, OptionContractWithConversion<T> {

    @Nonnull
    private static final TypeContract<Boolean> BOOLEAN_TYPE = new TypeContract<>(Boolean.class, Boolean::parseBoolean, false);

    @Nonnull
    private static final TypeContract<Integer> INTEGER_TYPE = new TypeContract<>(Integer.class, Integer::parseInt, false);

    @Nonnull
    private static final TypeContract<Long> LONG_TYPE = new TypeContract<>(Long.class, Long::parseLong, false);

    @Nonnull
    private static final TypeContract<String> STRING_TYPE = new TypeContract<>(String.class, Function.identity(), false);

    @Nonnull
    private static final TypeContract<String> NULLABLE_STRING_TYPE = new TypeContract<>(String.class, Function.identity(), true);

    @Nonnull
    private final Class<T> clazz;

    @Nonnull
    private final Function<String, T> fromStringFunction;
    private final boolean nullable;

    private TypeContract(@Nonnull Class<T> clazz, @Nonnull Function<String, T> fromStringFunction, boolean nullable) {
        this.clazz = clazz;
        this.fromStringFunction = fromStringFunction;
        this.nullable = nullable;
    }

    @Override
    public void validate(Options.Name name, Object value) throws SQLException {
        if (value == null) {
            if (nullable) {
                return;
            }
            throw new SQLException("Option " + name + " should not be null", ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
        if (!clazz.isInstance(value)) {
            throw new SQLException("Option " + name + " should be of type " + clazz + " but is " + value.getClass(), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    @Nullable
    @Override
    public T fromString(String valueAsString) throws SQLException {
        return fromStringFunction.apply(valueAsString);
    }

    @Nonnull
    public static <T> TypeContract<T> of(@Nonnull final Class<T> clazz, @Nonnull Function<String, T> fromStringFunction) {
        return new TypeContract<>(clazz, fromStringFunction, false);
    }

    @Nonnull
    public static TypeContract<Boolean> booleanType() {
        return BOOLEAN_TYPE;
    }

    @Nonnull
    public static TypeContract<String> stringType() {
        return STRING_TYPE;
    }

    @Nonnull
    public static TypeContract<String> nullableStringType() {
        return NULLABLE_STRING_TYPE;
    }

    @Nonnull
    public static TypeContract<Integer> intType() {
        return INTEGER_TYPE;
    }

    @Nonnull
    public static TypeContract<Long> longType() {
        return LONG_TYPE;
    }
}
