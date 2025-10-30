/*
 * UserDefinedFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Functions that are defined by customers and serialized to {@link com.apple.foundationdb.record.RecordMetaDataProto.MetaData}.
 *
 * note: user defined functions do not currently support variadic parameters.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class UserDefinedFunction extends CatalogedFunction {

    /**
     * Creates a new instance of {@link UserDefinedFunction}.
     * @param functionName The name of the function.
     * @param parameterTypes The types of function parameters.
     */
    public UserDefinedFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes) {
        super(functionName, parameterTypes, null);
    }

    /**
     * Creates a new instance of {@link UserDefinedFunction}.
     * @param functionName The name of the function.
     * @param parameterNames The names of the function parameters.
     * @param parameterTypes The types of the function parameters.
     * @param parameterDefaults The default values of function parameters, with {@link Optional#empty()} indicating
     * the absence of default value.
     */
    public UserDefinedFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                                @Nonnull final List<Type> parameterTypes,
                                @Nonnull final List<Optional<? extends Typed>> parameterDefaults) {
        super(functionName, parameterNames, parameterTypes, parameterDefaults);
    }

    /**
     * Serializes the {@link UserDefinedFunction} instance as a protobuf message.
     * @return A serialized version of the {@link UserDefinedFunction} as a protobuf message.
     */
    @Nonnull
    public abstract RecordMetaDataProto.PUserDefinedFunction toProto();

    @Nonnull
    public static UserDefinedFunction fromProto(@Nonnull final RecordMetaDataProto.PUserDefinedFunction function) {
        if (function.hasUserDefinedMacroFunction()) {
            return UserDefinedMacroFunction.fromProto(function.getUserDefinedMacroFunction());
        } else {
            return RawSqlFunction.fromProto(function.getSqlFunction());
        }
    }
}
