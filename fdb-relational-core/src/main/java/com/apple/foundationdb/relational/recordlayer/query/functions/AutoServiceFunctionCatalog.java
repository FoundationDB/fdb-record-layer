/*
 * AutoServiceFunctionCatalog.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import javax.annotation.Nonnull;
import java.util.Locale;

@API(API.Status.EXPERIMENTAL)
public class AutoServiceFunctionCatalog implements FunctionCatalog {

    @Nonnull
    private static final AutoServiceFunctionCatalog INSTANCE = new AutoServiceFunctionCatalog();

    @Nonnull
    private final BiMap<String, BuiltInFunction<? extends Typed>> registeredFunctions;

    private AutoServiceFunctionCatalog() {
        this.registeredFunctions = lookUpAllLoadedFunctions();
    }

    @Nonnull
    @Override
    public BuiltInFunction<? extends Typed> lookUpFunction(@Nonnull String name) {
        return Assert.notNullUnchecked(registeredFunctions.get(name), ErrorCode.INTERNAL_ERROR,
                () -> String.format("could not find function %s", name));
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    BiMap<String, BuiltInFunction<? extends Typed>> lookUpAllLoadedFunctions() {
        final Iterable<BuiltInFunction<? extends Typed>> builtInFunctions = (Iterable<BuiltInFunction<? extends Typed>>) (Object)
                ServiceLoaderProvider.load(BuiltInFunction.class);
        final ImmutableBiMap.Builder<String, BuiltInFunction<? extends Typed>> resultBuilder = ImmutableBiMap.builder();
        builtInFunctions.forEach(builtInFunction -> resultBuilder.put(builtInFunction.getFunctionName().toLowerCase(Locale.ROOT), builtInFunction));
        return resultBuilder.build();
    }

    @Override
    public boolean containsFunction(@Nonnull String name) {
        return registeredFunctions.containsKey(name);
    }

    @Nonnull
    public static AutoServiceFunctionCatalog instance() {
        return INSTANCE;
    }
}
