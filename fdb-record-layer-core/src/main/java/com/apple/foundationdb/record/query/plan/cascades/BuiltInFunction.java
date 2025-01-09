/*
 * BuiltInFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Main interface for defining a built-in function that can be evaluated against a number of arguments.
 *
 * A function could have a fixed number of arguments such as <code>Value Add(Value, Value)</code>, or a
 * variable number of arguments, such as <code>Value TEXT_CONTAINS_ALL_PREFIXES(Value, Value, Value, Value, ...)</code>.
 *
 * @param <T> The resulting type of the function.
 */
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class BuiltInFunction<T extends Typed> extends Function<T>{
    /**
     * Creates a new instance of {@link BuiltInFunction}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    public BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, null, encapsulationFunction);
    }

    /**
     * Creates a new instance of {@link BuiltInFunction}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param variadicSuffixType The type of the function's vararg.
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, variadicSuffixType, encapsulationFunction);
    }

    /**
     * Checks whether the provided list of argument types matches the list of function's parameter types.
     *
     * @param argumentTypes The argument types list.
     * @return if the arguments type match, an {@link Optional} containing <code>this</code> instance, otherwise
     * and empty {@link Optional}.
     */
    @SuppressWarnings("java:S3776")
    @Nonnull
    public Optional<BuiltInFunction<T>> validateCall(@Nonnull final List<Type> argumentTypes) {
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

    @Nonnull
    @Override
    public T encapsulate(@Nonnull final List<? extends Typed> arguments) {
        return encapsulationFunction.encapsulate(this, arguments);
    }
}
