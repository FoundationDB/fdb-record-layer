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
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
    protected final List<Optional<Value>> parameterDefaults;

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
                                @Nonnull final List<Optional<Value>> parameterDefaults) {
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

    public Optional<Value> getDefaultValue(@Nonnull final String paramName) {
        return parameterDefaults.get(getParamIndex(paramName));
    }

    public Optional<Value> getDefaultValue(int paramIndex) {
        return parameterDefaults.isEmpty() ? Optional.empty() : parameterDefaults.get(paramIndex);
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
     * permissible implicit promotions to the actual function invocation.
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

        // Check unknown arguments and argument types
        for (var namedArgument : namedArgumentsTypeMap.entrySet()) {
            if (!parameterNames.contains(namedArgument.getKey())) {
                return Optional.empty();
            }
        }

        // Check missing required arguments
        final var missingParams = Sets.difference(parameterNames, argumentNames);
        for (final var missingParam : missingParams) {
            if (!hasDefaultValue(missingParam)) {
                return Optional.empty();
            }
        }

        return Optional.of(this);
    }

    /**
     * Validates the function invocation using unnamed arguments. It checks whether the argument names match parameter
     * names, and that any missing parameter has a default value. It effectively leaves all the work related to handling
     * permissible implicit promotions to the actual function invocation.
     * @param unnamedArguments a list of named arguments.
     * @return if the arguments type match, an {@link Optional} containing <code>this</code> instance, otherwise
     * and empty {@link Optional}.
     */
    @Nonnull
    public Optional<? extends CatalogedFunction> validateCall(@Nonnull final List<? extends Typed> unnamedArguments) {
        if (unnamedArguments.size() > getParameterTypes().size()) {
            return Optional.empty();
        }
        for (var paramIdx = unnamedArguments.size(); paramIdx < getParameterTypes().size(); paramIdx++) {
            if (!hasDefaultValue(paramIdx)) {
                return Optional.empty();
            }
        }
        return Optional.of(this);
    }


    /**
     * Resolves the list of {@link Value}s to be passed to this function from an unnamed (positional) argument list.
     * <p>
     * Each positional argument is matched to the parameter at the same index. If fewer arguments are supplied than
     * the function declares parameters, the remaining parameters must have default values; otherwise a
     * {@link com.apple.foundationdb.record.query.plan.cascades.SemanticException} is thrown. Each resolved argument
     * is promoted to the declared parameter type when necessary (see {@link #promoteArgumentValueIfNeeded}).
     *
     * @param arguments the positional arguments provided to the function call
     * @return a list of {@link Value}s, one per declared parameter
     */
    protected List<Value> resolveParameterValuesFromArguments(@Nonnull List<? extends Typed> arguments) {
        checkArgumentCount(arguments.size());
        ImmutableList.Builder<Value> valueBuilder = ImmutableList.builder();
        for (var paramIdx = 0; paramIdx < getParameterTypes().size(); paramIdx++) {
            if (paramIdx >= arguments.size()) {
                final var defaultArgumentValue = getDefaultValue(paramIdx);
                SemanticException.check(defaultArgumentValue.isPresent(),
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                        "required argument must be specified");
                valueBuilder.add(defaultArgumentValue.get());
            } else {
                final var providedArgValue = arguments.get(paramIdx);
                Verify.verify(providedArgValue instanceof Value);
                valueBuilder.add(promoteArgumentValueIfNeeded((Value)providedArgValue, computeParameterType(paramIdx)));
            }
        }
        return valueBuilder.build();
    }

    /**
     * Resolves the list of {@link Value}s to be passed to this function from a named argument map.
     * <p>
     * Arguments are matched to parameters by name. The function must support named arguments; otherwise a
     * {@link com.apple.foundationdb.record.query.plan.cascades.SemanticException} is thrown. Parameters absent from the
     * map must have default values; otherwise the same exception is thrown as well. Each resolved argument is promoted
     * to the declared parameter type when necessary (see {@link #promoteArgumentValueIfNeeded}).
     * </p>
     * @param namedArguments a map from parameter name to argument value provided to the function call
     * @return a list of {@link Value}s, one per declared parameter
     */
    protected List<Value> resolveParameterValuesFromArguments(@Nonnull final Map<String, ? extends Typed> namedArguments) {
        checkArgumentCount(namedArguments.size());
        ImmutableList.Builder<Value> valueBuilder = ImmutableList.builder();
        SemanticException.check(
                hasNamedParameters(),
                SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                "unexpected invocation of function with named arguments");
        for (final var name : getParameterNames()) {
            if (!namedArguments.containsKey(name)) {
                final var defaultArgumentValue = getDefaultValue(name);
                SemanticException.check(defaultArgumentValue.isPresent(),
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                        "required argument for function is not specified");
                valueBuilder.add(defaultArgumentValue.get());
            } else {
                final var providedArgValue = namedArguments.get(name);
                Verify.verify(providedArgValue instanceof Value);
                valueBuilder.add(promoteArgumentValueIfNeeded((Value)providedArgValue, computeParameterType(name)));
            }
        }
        return valueBuilder.build();
    }

    /**
     * Promotes {@code providedArgumentValue} to {@code parameterType} when the two types differ.
     * <p>
     * Throws a {@link com.apple.foundationdb.record.query.plan.cascades.SemanticException}
     * if the argument cannot be promoted to the parameter type. When promotion is not needed the original value is
     * returned unchanged.
     * </p>
     * @param providedArgumentValue the call-site argument value
     * @param parameterType the declared parameter type
     * @return {@link Value} that is the same as providedArgumentValue if no promotion is needed, otherwise a promoted {@link Value}.
     */
    private Value promoteArgumentValueIfNeeded(@Nonnull final Value providedArgumentValue, final Type parameterType) {
        if (!PromoteValue.isPromotionNeeded(providedArgumentValue.getResultType(), parameterType)) {
            return providedArgumentValue;
        }
        SemanticException.check(PromoteValue.isPromotable(providedArgumentValue.getResultType(), parameterType),
                SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                "argument type doesn't match with function definition");
        return PromoteValue.inject(providedArgumentValue, parameterType);
    }

    /**
     * Validates that the number of supplied arguments does not exceed the number of declared parameters for a function.
     *
     * @param argumentCount the number of arguments supplied at the call site
     */
    private void checkArgumentCount(final int argumentCount) {
        SemanticException.check(argumentCount <= getParameterTypes().size(),
                SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                "argument length doesn't match with function definition");
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
