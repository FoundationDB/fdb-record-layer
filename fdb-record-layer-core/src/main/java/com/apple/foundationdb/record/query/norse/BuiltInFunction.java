/*
 * Function.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Typed;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class BuiltInFunction<T extends Typed> {
    @Nonnull
    final String functionName;

    @Nonnull
    final List<Type> parameterTypes;

    @Nonnull
    final BiFunction<BuiltInFunction<T>, List<Typed>, T> encapsulationFunction;

    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nonnull final BiFunction<BuiltInFunction<T>, List<Typed>, T> encapsulationFunction) {
        this.functionName = functionName;
        this.parameterTypes = parameterTypes;
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
    public BiFunction<BuiltInFunction<T>, List<Typed>, T> getEncapsulationFunction() {
        return encapsulationFunction;
    }

    @Nonnull
    public Typed encapsulate(@Nonnull final List<Typed> arguments) {
        return encapsulationFunction.apply(this, arguments);
    }

    @Nonnull
    public String toString() {
        return functionName + "(" + parameterTypes.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
    }
}
