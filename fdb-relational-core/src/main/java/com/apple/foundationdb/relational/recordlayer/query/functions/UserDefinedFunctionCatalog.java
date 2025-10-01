/*
 * UserDefinedFunctionCatalog.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.query.plan.cascades.CatalogedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

final class UserDefinedFunctionCatalog {

    @Nonnull
    private final Map<String, Function<Boolean, ? extends UserDefinedFunction>> functionsMap;

    private final boolean isCaseSensitive;

    UserDefinedFunctionCatalog(boolean isCaseSensitive) {
        this.isCaseSensitive = isCaseSensitive;
        this.functionsMap = new LinkedHashMap<>();
    }

    void registerFunction(@Nonnull final String functionName,
                          @Nonnull final Function<Boolean, ? extends UserDefinedFunction> function) {
        functionsMap.put(functionName, function);
    }

    public boolean containsFunction(@Nonnull final String name) {
        return functionsMap.containsKey(name);
    }

    @Nonnull
    public Optional<? extends CatalogedFunction> lookup(@Nonnull final String functionName, Expressions arguments) {
        final var functionSupplier = functionsMap.get(functionName);
        if (functionSupplier == null) {
            return Optional.empty();
        }

        // lazy-compile the function
        final var function = functionSupplier.apply(isCaseSensitive);

        // either all arguments are named, or none is named.
        // I think partial naming is supported in SQL standard, but we do not support that at the moment.
        int countNamed = 0;
        int countUnnamed = 0; // Java container builders do not have size for some reason.
        final var namedArgumentsBuilder = ImmutableMap.<String, Value>builder();
        final var unnamedArgumentsBuilder = ImmutableList.<Value>builder();
        for (final var argument : arguments) {
            if (argument.isNamedArgument()) {
                Assert.thatUnchecked(countUnnamed == 0, ErrorCode.UNSUPPORTED_OPERATION,
                        "mixing named and unnamed arguments is not supported");
                countNamed++;
                namedArgumentsBuilder.put(argument.getName().get().toString(), argument.getUnderlying());
            } else {
                Assert.thatUnchecked(countNamed == 0, ErrorCode.UNSUPPORTED_OPERATION,
                        "mixing named and unnamed arguments is not supported");
                countUnnamed++;
                unnamedArgumentsBuilder.add(argument.getUnderlying());
            }
        }
        final var namedArguments = namedArgumentsBuilder.build();
        if (!namedArguments.isEmpty()) {
            return function.validateCall(arguments.toNamedArgumentInvocation());
        }
        // todo: this should go throw validateCall for unnamed arguments, however that function is not considering
        // proper type promotion, instead, we're delegating this logic to the actual encapsuation logic of each
        // individual instance, therefore, we return the function as-is.
        return Optional.of(function);
    }
}
