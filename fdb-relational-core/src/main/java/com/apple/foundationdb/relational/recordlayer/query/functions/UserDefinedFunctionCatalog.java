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

import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.CatalogedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
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
    public Optional<CatalogedFunction> lookup(@Nonnull final String functionName, @Nonnull final CallSiteArguments arguments) {
        final var functionSupplier = functionsMap.get(functionName);
        if (functionSupplier == null) {
            return Optional.empty();
        }

        // lazy-compile the function
        final var function = functionSupplier.apply(isCaseSensitive);

        // These validations don't include checking if the type of the provided arguments matches the expected function
        // parameter types or the provided values can be promoted to the expected types. Instead, this is delegated
        // to the encapsulation logic.
        if (arguments.isNamed()) {
            return function.validateCall(arguments.asNamedArguments().namedArguments());
        }
        return function.validateCall(arguments.getArgumentsList());
    }
}
