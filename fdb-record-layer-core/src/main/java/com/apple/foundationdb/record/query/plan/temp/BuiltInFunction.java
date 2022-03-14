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

package com.apple.foundationdb.record.query.plan.temp;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class BuiltInFunction<T extends Typed> {
    @Nonnull
    final String functionName;

    @Nonnull
    final List<Type> parameterTypes;

    @Nullable
    final Type variadicSuffixType;

    @Nonnull
    final EncapsulationFunction<T> encapsulationFunction;

    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        this(functionName, parameterTypes, null, encapsulationFunction);
    }

    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
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
        if (index < parameterTypes.size()) {
            return parameterTypes.get(index);
        } else {
            if (hasVariadicSuffix()) {
                return Objects.requireNonNull(variadicSuffixType);
            }
            throw new IllegalArgumentException("cannot resolve declared parameter at index " + index);
        }
    }

    @Nonnull
    public List<Type> resolveParameterTypes(int numberOfArguments) {
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
    public Optional<BuiltInFunction<T>> validateCall(@Nonnull final List<Type> argumentTypes) {
        int numberOfArguments = argumentTypes.size();

        if (hasVariadicSuffix()) {
            // This is variadic function
            final Type functionVariadicSuffixType = Objects.requireNonNull(getVariadicSuffixType());
            if (functionVariadicSuffixType.getTypeCode() != Type.TypeCode.ANY) {
                for (int i = getParameterTypes().size(); i < numberOfArguments; i++) {
                    if (argumentTypes.get(i).getTypeCode() != functionVariadicSuffixType.getTypeCode()) {
                        return Optional.empty();
                    }
                }
            }
            return Optional.of(this);
        }

        // check the type codes of the fixed parameters
        final List<Type> functionParameterTypes = getParameterTypes();
        for (int i = 0; i < functionParameterTypes.size(); i ++) {
            final Type typeI = functionParameterTypes.get(i);
            if (typeI.getTypeCode() != Type.TypeCode.ANY && typeI.getTypeCode() != argumentTypes.get(i).getTypeCode()) {
                return Optional.empty();
            }
        }

        return Optional.of(this);
    }

    @Nonnull
    public EncapsulationFunction<T> getEncapsulationFunction() {
        return encapsulationFunction;
    }

    @Nonnull
    public Typed encapsulate(@Nonnull final ParserContext parserContext, @Nonnull final List<Typed> arguments) {
        return encapsulationFunction.encapsulate(parserContext, this, arguments);
    }

    @Nonnull
    @Override
    public String toString() {
        return functionName + "(" + parameterTypes.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
    }
}
