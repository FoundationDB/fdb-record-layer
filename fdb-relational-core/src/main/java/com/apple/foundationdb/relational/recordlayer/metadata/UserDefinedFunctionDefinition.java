/*
 * UserDefinedFunctionDefinition.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.metadata.Udf;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.MacroFunctionValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.List;

public class UserDefinedFunctionDefinition {
    @Nonnull private final String functionName;
    @Nonnull private final Value functionValue;

    public UserDefinedFunctionDefinition(@Nonnull String functionName, @Nonnull Value value) {
        this.functionName = functionName;
        this.functionValue = value;
    }

    @Nonnull
    public String getName() {
        return functionName;
    }

    @Nonnull
    public BuiltInFunction<? extends Typed> getBuiltInFunction() {
        return new BuiltInFunction<Value>(functionName, List.of(), (builtInFunction, arguments) -> ((MacroFunctionValue) functionValue).call(arguments)) {};
    }

    @Nonnull
    public Udf toUdf() {
        return new Udf(functionName, functionValue);
    }

    @Nonnull
    public static UserDefinedFunctionDefinition fromUdf(Udf udf) {
        return new UserDefinedFunctionDefinition(udf.getFunctionName(), udf.getValue());
    }
}
