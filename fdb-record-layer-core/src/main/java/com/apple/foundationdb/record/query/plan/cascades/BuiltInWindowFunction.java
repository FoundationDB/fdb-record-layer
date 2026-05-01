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
import com.apple.foundationdb.record.query.plan.cascades.values.WindowedValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;


public abstract class BuiltInWindowFunction<T extends Typed> extends CatalogedFunction<Object> {

    @Nonnull
    private final EncapsulationWindowFunction<T> encapsulationFunction;

    protected BuiltInWindowFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                    @Nonnull final EncapsulationWindowFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, null);
        this.encapsulationFunction = encapsulationFunction;
    }

    protected BuiltInWindowFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                    @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationWindowFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, variadicSuffixType);
        this.encapsulationFunction = encapsulationFunction;
    }

    @Nonnull
    @Override
    public BuiltInFunction<T> encapsulate(@Nonnull final List<Object> arguments) {
        WindowedValue.FrameSpecification frameSpecification = null;
        List<OrderingPart.RequestedOrderingPart> sortOrder = null;

        return new BuiltInFunction<>(getFunctionName(), getParameterTypes(), getVariadicSuffixType(),
                (builtInFunction, arguments1) -> encapsulationFunction.encapsulate(this, frameSpecification, sortOrder, arguments1));
    }

    @Nonnull
    public Typed encapsulate(@Nonnull final Map<String, Object> namedArguments) {
        throw new RecordCoreException("built-in functions do not support named argument calling conventions");
    }
}
