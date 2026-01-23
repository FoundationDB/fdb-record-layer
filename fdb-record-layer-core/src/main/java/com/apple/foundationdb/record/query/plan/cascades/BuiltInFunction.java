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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Main interface for defining a built-in function that can be evaluated against a number of arguments.
 *
 * A function could have a fixed number of arguments such as <code>Value Add(Value, Value)</code>, or a
 * variable number of arguments, such as <code>Value TEXT_CONTAINS_ALL_PREFIXES(Value, Value, Value, Value, ...)</code>.
 *
 * @param <T> The resulting type of the function.
 */
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class BuiltInFunction<T extends Typed> extends CatalogedFunction {
    @Nonnull
    final EncapsulationFunction<T> encapsulationFunction;

    /**
     * Creates a new instance of {@link BuiltInFunction}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        this(functionName, parameterTypes, null, encapsulationFunction);
    }

    /**
     * Creates a new instance of {@link BuiltInFunction}.
     * @param functionName The name of the function.
     * @param parameterTypes The type of the parameter(s).
     * @param variadicSuffixType The type of the function's vararg.
     * @param encapsulationFunction An encapsulation of the function's runtime computation.
     */
    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes, @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, variadicSuffixType);
        this.encapsulationFunction = encapsulationFunction;
    }

    protected BuiltInFunction(@Nonnull final String functionName, @Nonnull final List<String> parameterNames,
                              @Nonnull final List<Type> parameterTypes,
                              @Nonnull final List<Optional<? extends Typed>> parameterDefaults,
                              @Nonnull final EncapsulationFunction<T> encapsulationFunction) {
        super(functionName, parameterNames, parameterTypes, parameterDefaults);
        this.encapsulationFunction = encapsulationFunction;
    }

    @Nonnull
    @Override
    public Typed encapsulate(@Nonnull final List<? extends Typed> arguments) {
        return Verify.verifyNotNull(encapsulationFunction).encapsulate(this, arguments);
    }

    @Nonnull
    @Override
    public Typed encapsulate(@Nonnull final Map<String, ? extends Typed> namedArguments) {
        throw new RecordCoreException("built-in functions do not support named argument calling conventions");
    }
}
