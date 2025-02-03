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
import java.sql.SQLException;

@API(API.Status.EXPERIMENTAL)
public class TypeContract<T> implements OptionContract {

    @Nonnull
    private static final TypeContract<Boolean> BOOLEAN_TYPE = new TypeContract<>(Boolean.class);

    @Nonnull
    private static final TypeContract<Integer> INTEGER_TYPE = new TypeContract<>(Integer.class);

    @Nonnull
    private static final TypeContract<Long> LONG_TYPE = new TypeContract<>(Long.class);

    @Nonnull
    private static final TypeContract<String> STRING_TYPE = new TypeContract<>(String.class);

    private final Class<T> clazz;

    public TypeContract(Class<T> clazz) {
        this.clazz = clazz;

    }

    @Override
    public void validate(Options.Name name, Object value) throws SQLException {
        if (!clazz.isInstance(value)) {
            throw new SQLException("Option " + name + " should be of type " + clazz + " but is " + value.getClass(), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    @Nonnull
    public static <T> TypeContract<T> of(@Nonnull final Class<T> clazz) {
        return new TypeContract<>(clazz);
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
    public static TypeContract<Integer> intType() {
        return INTEGER_TYPE;
    }

    @Nonnull
    public static TypeContract<Long> longType() {
        return LONG_TYPE;
    }
}
