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
public abstract class Function<T extends Typed>{
    @Nonnull
    final String functionName;

    @Nonnull
    final List<Type> parameterTypes;

    /**
     * The type of the function's variadic parameters (if any).
     */
    @Nullable
    final Type variadicSuffixType;

    @Nonnull
    final EncapsulationFunction<T> encapsulationFunction;

    /**
     * Creates a new instance of {@link Function}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    public Function(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        this(functionName, parameterTypes, null, encapsulationFunction);
    }

    /**
     * Creates a new instance of {@link Function}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param variadicSuffixType The type of the function's vararg.
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    protected Function(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        this.functionName = functionName;
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.variadicSuffixType = variadicSuffixType;
        this.encapsulationFunction = encapsulationFunction;
    }

    @Nonnull
    public String getFunctionName() {
        return functionName;
    }

    @Nonnull
    public List<Type> getParameterTypes() {
        return parameterTypes;
    }

    @Nonnull
    public Type resolveParameterType(int index) {
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
    public List<Type> resolveParameterTypes(int numberOfArguments) {
        Verify.verify(numberOfArguments > 0, "unexpected number of arguments");
        final ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfArguments; i ++) {
            resultBuilder.add(resolveParameterType(i));
        }
        return resultBuilder.build();
    }

    @Nullable
    public Type getVariadicSuffixType() {
        return variadicSuffixType;
    }

    public boolean hasVariadicSuffix() {
        return variadicSuffixType != null;
    }

    @Nonnull
    public EncapsulationFunction<T> getEncapsulationFunction() {
        return encapsulationFunction;
    }

    @Nonnull
    // in Function abstract encapsulate()
    public abstract T encapsulate(@Nonnull final List<? extends Typed> arguments);

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

        return functionName + "(" + parameterTypes.stream().map(Object::toString).collect(Collectors.joining(",")) + variadicSuffixString + ")";
    }
}
