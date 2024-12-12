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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.List;

public class UserDefinedFunctionDefinition {
    @Nonnull private final String functionName;
    @Nonnull private final Value value;

    public UserDefinedFunctionDefinition(@Nonnull String functionName, @Nonnull Value value) {
        this.functionName = functionName;
        this.value = value;
    }

    @Nonnull
    public String getName() {
        return functionName;
    }

    public BuiltInFunction<? extends Typed> getBuiltInFunction() {
        Assert.thatUnchecked(value instanceof FieldValue, "Invalid UDF definition.");
        return new BuiltInFunction<Value>(functionName, List.of(), (builtInFunction, arguments) -> FieldValue.ofFieldNames((Value) arguments.get(0), ((FieldValue) value).getFieldPathNames())) {};
    }

    public UDF toUDF() {
        return new UDF(functionName, value);
    }

    public static UserDefinedFunctionDefinition fromUDF(UDF udf) {
        return new UserDefinedFunctionDefinition(udf.getUdfName(), udf.getValue());
    }
}
