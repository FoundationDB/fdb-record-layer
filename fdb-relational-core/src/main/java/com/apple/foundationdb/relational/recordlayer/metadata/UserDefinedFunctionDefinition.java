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

import com.apple.foundationdb.record.metadata.UDF;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.List;

public class UserDefinedFunctionDefinition {
    @Nonnull private final String functionName;
    @Nonnull private final Value value;
    @Nonnull private final ObjectValue argumentValue;

    public UserDefinedFunctionDefinition(@Nonnull String functionName, @Nonnull Value value, @Nonnull ObjectValue argumentValue) {
        this.functionName = functionName;
        this.value = value;
        this.argumentValue = argumentValue;
    }

    @Nonnull
    public String getName() {
        return functionName;
    }

    public BuiltInFunction<? extends Typed> getBuiltInFunction() {
        return new BuiltInFunction<Value>(functionName, List.of(), (builtInFunction, arguments) -> replaceArgument((Value) arguments.get(0))) {};
    }

    public UDF toUDF() {
        return new UDF(functionName, value, argumentValue);
    }

    public static UserDefinedFunctionDefinition fromUDF(UDF udf) {
        return new UserDefinedFunctionDefinition(udf.getUdfName(), udf.getValue(), (ObjectValue) udf.getArgumentValue());
    }

    // replace the argumentValue with argument in value
    private Value replaceArgument(Value argument) {
        return value.replace((v) -> {
            if (v instanceof ObjectValue && v.getResultType().equals(argumentValue.getResultType())) {
                return argument;
            } else {
                return v;
            }
        });
    }
}
