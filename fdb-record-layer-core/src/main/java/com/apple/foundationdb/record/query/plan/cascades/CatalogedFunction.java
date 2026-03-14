/*
 * CatalogedFunction.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.base.Functions;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Main interface for defining all functions that can be evaluated against a number of arguments.
 * Two major sub interfaces inherit this interface: {@link BuiltInFunction} and {@link UserDefinedFunction}
 * {@link BuiltInFunction} represents all functions that are built-in, and stored in code, while
 * {@link UserDefinedFunction} represents all functions defined by users, and stored in
 * {@link com.apple.foundationdb.record.RecordMetaDataProto.MetaData}
 */
@API(API.Status.EXPERIMENTAL)
public abstract class CatalogedFunction {
    @Nonnull
    protected final String functionName;

    @Nonnull
    protected final List<Type> parameterTypes;

    @Nonnull
    protected final BiMap<String, Integer> parameterNamesMap;

    @Nonnull
    protected final List<Optional<? extends Typed>> parameterDefaults;

    /**
     * The type of the function's variadic parameters (if any).
     */
    @Nullable
    private final Type variadicSuffixType;

    protected CatalogedFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                @Nullable final Type variadicSuffixType) {
        this.functionName = functionName;
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.parameterDefaults = ImmutableList.of();
        this.parameterNamesMap = ImmutableBiMap.of();
        this.variadicSuffixType = variadicSuffixType;
    }

    protected CatalogedFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                                @Nonnull final List<Type> parameterTypes,
                                @Nonnull final List<Optional<? extends Typed>> parameterDefaults) {
        Verify.verify(parameterNames.size() == parameterTypes.size());
        this.functionName = functionName;
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.parameterNamesMap = IntStream.range(0, parameterNames.size()).boxed().collect(ImmutableBiMap.toImmutableBiMap(parameterNames::get,
                Functions.identity()));
        this.parameterDefaults = ImmutableList.copyOf(parameterDefaults);
        this.variadicSuffixType = null; // no support yet with named parameters.
    }

    @Nonnull
    public String getFunctionName() {
        return functionName;
    }

    @Nonnull
    public List<Type> getParameterTypes() {
        return parameterTypes;
    }

    public boolean hasNamedParameters() {
        return !parameterNamesMap.isEmpty();
    }

    @Nonnull
    public String getParameterName(int index) {
        return parameterNamesMap.inverse().get(index);
    }

    @Nonnull
    public Collection<String> getParameterNames() {
        return parameterNamesMap.keySet();
    }

    @Nullable
    public Type getVariadicSuffixType() {
        return variadicSuffixType;
    }

    public boolean hasVariadicSuffix() {
        return variadicSuffixType != null;
    }

    @Nonnull
    public Type computeParameterType(int index) {
        Verify.verify(index >= 0, "unexpected negative parameter index");
        if (index < parameterTypes.size()) {
            return parameterTypes.get(index);
        } else {
            if (hasVariadicSuffix()) {
                return variadicSuffixType;
            }
            throw new IllegalArgumentException("cannot resolve declared parameter at index " + index);
        }
    }

    @Nonnull
    public Type computeParameterType(@Nonnull final String parameterName) {
        return computeParameterType(getParamIndex(parameterName));
    }

    @Nonnull
    public List<Type> getParameterTypes(int numberOfArguments) {
        Verify.verify(numberOfArguments > 0, "unexpected number of arguments");
        final ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfArguments; i ++) {
            resultBuilder.add(computeParameterType(i));
        }
        return resultBuilder.build();
    }

    public int getParamIndex(@Nonnull final String parameter) {
        return parameterNamesMap.get(parameter);
    }

    public Optional<? extends Typed> getDefaultValue(@Nonnull final String paramName) {
        return parameterDefaults.get(getParamIndex(paramName));
    }

    public Optional<? extends Typed> getDefaultValue(int paramIndex) {
        return parameterDefaults.get(paramIndex);
    }

    public boolean hasDefaultValue(@Nonnull final String paramName) {
        return getDefaultValue(paramName).isPresent();
    }

    public boolean hasDefaultValue(int paramIndex) {
        return getDefaultValue(paramIndex).isPresent();
    }

    public int getDefaultValuesCount() {
        return parameterDefaults.size();
    }

    /**
     * Validates the function invocation using named arguments. It checks whether the argument names match parameter
     * names, and that any missing parameter has a default value. It effectively leaves all the work related to handling
     * permisslbe implicit promotions to the actual function invocation.
     * @param namedArgumentsTypeMap a list of named arguments.
     * @return if the arguments type match, an {@link Optional} containing <code>this</code> instance, otherwise
     * and empty {@link Optional}.
     */
    @Nonnull
    public Optional<? extends CatalogedFunction> validateCall(@Nonnull final Map<String, ? extends Typed> namedArgumentsTypeMap) {
        if (parameterNamesMap.isEmpty()) {
            return Optional.empty();
        }
        final var argumentNames = namedArgumentsTypeMap.keySet();
        final var parameterNames = parameterNamesMap.keySet();
        final var unknownArgs = Sets.difference(argumentNames, parameterNames);
        if (!unknownArgs.isEmpty()) {
            return Optional.empty();
        }
        final var missingParams = Sets.difference(parameterNames, argumentNames);
        for (final var missingParam : missingParams) {
            if (!hasDefaultValue(missingParam)) {
                return Optional.empty();
            }
        }
        return Optional.of(this);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public String toString() {
        String variadicSuffixString = "";
        if (variadicSuffixType != null) {
            variadicSuffixString = variadicSuffixType + "...";
            if (!parameterTypes.isEmpty()) {
                variadicSuffixString = ", " + variadicSuffixString;
            }
        }

        final var parameterNames = parameterNamesMap.keySet();
        if (!parameterNamesMap.isEmpty()) {
            return functionName + "(" + Streams.zip(parameterNames.stream(), parameterTypes.stream(),
                    (name, type) -> name + " -> " + type).collect(Collectors.joining(",")) + variadicSuffixString + ")";
        }

        return functionName + "(" + parameterTypes.stream().map(Object::toString).collect(Collectors.joining(",")) + variadicSuffixString + ")";
    }

    @Nonnull
    public abstract Typed encapsulate(@Nonnull List<? extends Typed> arguments);

    @Nonnull
    public abstract Typed encapsulate(@Nonnull Map<String, ? extends Typed> namedArguments);
}
