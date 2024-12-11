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

import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.UDF;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserDefinedFunctionDefinition {
    @Nonnull private final String functionName;
    @Nonnull private final KeyExpression keyExpression;

    public UserDefinedFunctionDefinition(@Nonnull String functionName, @Nonnull KeyExpression keyExpression) {
        this.functionName = functionName;
        this.keyExpression = keyExpression;
    }

    @Nonnull
    public String getName() {
        return functionName;
    }

    @Nonnull
    public KeyExpression getKeyExpression() {return keyExpression;}

    public BuiltInFunction<? extends Typed> getBuiltInFunction() {
        return new BuiltInFunction<Value>(functionName, List.of(), (builtInFunction, arguments) -> FieldValue.ofFieldNames((Value) arguments.get(0), getFieldNamesFromKeyExpression(keyExpression))) {
            @Nonnull
            @Override
            public String getFunctionName() {
                return functionName;
            }
        };
    }

    private List<String> getFieldNamesFromKeyExpression(KeyExpression keyExpression) {
        if (keyExpression instanceof NestingKeyExpression) {
            return Stream.concat(List.of(((NestingKeyExpression) keyExpression).getParent().getFieldName().toUpperCase(Locale.ROOT)).stream(), getFieldNamesFromKeyExpression(((NestingKeyExpression) keyExpression).getChild()).stream()).collect(Collectors.toList());
        } else {
            return List.of(((FieldKeyExpression) keyExpression).getFieldName().toUpperCase(Locale.ROOT));
        }
    }

    public UDF toUDF() {
        return new UDF(functionName, keyExpression);
    }

    public static UserDefinedFunctionDefinition fromUDF(UDF udf) {
        return new UserDefinedFunctionDefinition(udf.getUdfName(), udf.getKeyExpression());
    }
}
