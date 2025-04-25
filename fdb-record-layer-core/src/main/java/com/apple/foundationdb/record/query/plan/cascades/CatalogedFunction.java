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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.base.Functions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Main interface for defining all functions that can be evaluated against a number of arguments.
 * Two major sub interfaces inherit this interface: {@link BuiltInFunction} and {@link UserDefinedFunction}
 * {@link BuiltInFunction} represents all functions that are built-in, and stored in code, while
 * {@link UserDefinedFunction} represents all functions defined by users, and stored in
 * {@link com.apple.foundationdb.record.RecordMetaDataProto.MetaData}
 *
 * @param <T> The type of the default parameters (if any).
 */
@API(API.Status.EXPERIMENTAL)
public abstract class CatalogedFunction<T extends Typed> {
    @Nonnull
    protected final String functionName;

    @Nonnull
    protected final List<Type> parameterTypes;

    @Nonnull
    protected final Map<String, Integer> parameterNamesMap;

    @Nonnull
    protected final List<Optional<T>> parameterDefaults;

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
        this.parameterNamesMap = ImmutableMap.of();
        this.variadicSuffixType = variadicSuffixType;
    }

    protected CatalogedFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                                @Nonnull final List<Type> parameterTypes,
                                @Nonnull final List<Optional<T>> parameterDefaults) {
        Verify.verify(parameterNames.size() == parameterTypes.size());
        this.functionName = functionName;
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.parameterNamesMap = IntStream.range(0, parameterNames.size()).boxed().collect(Collectors.toMap(parameterNames::get,
                Functions.identity(), (v1, v2) -> { throw new RecordCoreException("illegal duplicate parameter names"); },
                LinkedHashMap::new));
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
    public List<String> getParameterNames() {
        return ImmutableList.copyOf(parameterNamesMap.keySet()); // todo: amortize
    }

    @Nullable
    public Type getVariadicSuffixType() {
        return variadicSuffixType;
    }

    public boolean hasVariadicSuffix() {
        return variadicSuffixType != null;
    }

    @Nonnull
    public Type getParameterType(int index) {
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
    public Type getParameterType(@Nonnull final String parameterName) {
        return getParameterType(getParamIndex(parameterName));
    }

    @Nonnull
    public List<Type> getParameterTypes(int numberOfArguments) {
        Verify.verify(numberOfArguments > 0, "unexpected number of arguments");
        final ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfArguments; i ++) {
            resultBuilder.add(getParameterType(i));
        }
        return resultBuilder.build();
    }

    public int getParamIndex(@Nonnull final String parameter) {
        return parameterNamesMap.get(parameter);
    }

    public Optional<T> getDefaultValue(@Nonnull final String paramName) {
        return parameterDefaults.get(getParamIndex(paramName));
    }

    public Optional<T> getDefaultValue(int paramIndex) {
        return parameterDefaults.get(paramIndex);
    }

    public boolean hasDefaultValue(@Nonnull final String paramName) {
        return getDefaultValue(paramName).isPresent();
    }

    public boolean hasDefaultValue(int paramIndex) {
        return getDefaultValue(paramIndex).isPresent();
    }

    /**
     * Checks whether the provided list of argument types matches the list of function's parameter types.
     *
     * @param argumentTypes The argument types list.
     * @return if the arguments type match, an {@link Optional} containing <code>this</code> instance, otherwise
     * and empty {@link Optional}.
     *
     * TODO: this does not work correct with permissible implicit type promotion.
     */
    @SuppressWarnings("java:S3776")
    @Nonnull
    public Optional<? extends CatalogedFunction<T>> validateCall(@Nonnull final List<Type> argumentTypes) {
        int numberOfArguments = argumentTypes.size();

        final List<Type> functionParameterTypes = getParameterTypes();

        if (numberOfArguments > functionParameterTypes.size() && !hasVariadicSuffix()) {
            return Optional.empty();
        }

        // check the type codes of the fixed parameters
        for (int i = 0; i < functionParameterTypes.size(); i ++) {
            final Type typeI = functionParameterTypes.get(i);
            if (typeI.getTypeCode() != Type.TypeCode.ANY && typeI.getTypeCode() != argumentTypes.get(i).getTypeCode()) {
                return Optional.empty();
            }
        }

        if (hasVariadicSuffix()) { // This is variadic function, match the rest of arguments, if any.
            final Type functionVariadicSuffixType = Objects.requireNonNull(getVariadicSuffixType());
            if (functionVariadicSuffixType.getTypeCode() != Type.TypeCode.ANY) {
                for (int i = getParameterTypes().size(); i < numberOfArguments; i++) {
                    if (argumentTypes.get(i).getTypeCode() != functionVariadicSuffixType.getTypeCode()) {
                        return Optional.empty();
                    }
                }
            }
        }

        return Optional.of(this);
    }

    // TODO: this does not work correct with permissible implicit type promotion.
    @Nonnull
    public Optional<? extends CatalogedFunction<T>> validateCall(@Nonnull final Map<String, ? extends Typed> namedArgumentsTypeMap) {
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
