/*
 * BuiltInTableFunction.java
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public abstract class BuiltInTableFunction extends BuiltInFunction<StreamingValue> {
    protected BuiltInTableFunction(@Nonnull final String functionName,
                                   @Nonnull final List<Type> parameterTypes,
                                   @Nonnull final EncapsulationFunction<StreamingValue> encapsulationFunction) {
        super(functionName, parameterTypes, encapsulationFunction);
    }

    protected BuiltInTableFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                   @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationFunction<StreamingValue> encapsulationFunction) {
        super(functionName, parameterTypes, variadicSuffixType, encapsulationFunction);
    }

    @Override
    public boolean isTableFunction() {
        return true;
    }
}
